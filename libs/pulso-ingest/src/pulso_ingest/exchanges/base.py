"""Contrato de um client de exchange + loop de conexao resiliente.

Cada exchange implementa `ExchangeClient` (url, mensagens de subscribe, parsing
puro e chave de gap). O loop generico `run_client` cuida da parte que e igual em
todas: conectar, subscrever, reconectar com backoff exponencial + jitter,
**circuit breaker** (pulso_infra) para nao martelar uma fonte caida, deteccao de
**gap de sequencia** no order book e atualizacao das metricas Prometheus.

Separacao deliberada: o parsing (`parse`) e puro e testavel sem rede; o I/O e a
politica de resiliencia ficam aqui.
"""

from __future__ import annotations

import asyncio
import json
import random
from abc import ABC, abstractmethod

import websockets
from loguru import logger
from pulso_domain import Exchange
from pulso_infra import CircuitBreaker, Settings

from pulso_ingest import metrics
from pulso_ingest.gap import OrderBookGapDetector
from pulso_ingest.models import MarketEvent, OrderBookDeltaEvent, TradeEvent
from pulso_ingest.producer import MarketDataProducer
from pulso_ingest.util import now_ms


class ExchangeClient(ABC):
    """Adaptador de uma exchange: protocolo WebSocket -> eventos canonicos."""

    exchange: Exchange

    def __init__(self, settings: Settings, symbols: tuple) -> None:
        self.settings = settings
        self.symbols = symbols  # tuple[Symbol, ...] do seed (apenas ativos)

    @abstractmethod
    def ws_url(self) -> str:
        """Endpoint WebSocket a conectar."""

    @abstractmethod
    def subscribe_messages(self) -> list[dict]:
        """Mensagens JSON a enviar logo apos conectar (subscribe nos canais)."""

    @abstractmethod
    def parse(self, raw: dict, ingest_ms: int) -> list[MarketEvent]:
        """Traduz uma mensagem crua em eventos canonicos. Puro, sem I/O.

        Mensagens de controle (acks, heartbeats) -> lista vazia. Simbolos fora do
        seed sao descartados (e contabilizados em `events_dropped`).
        """

    @abstractmethod
    def gap_stream_key(self, event: OrderBookDeltaEvent) -> str | None:
        """Identidade de stream para a deteccao de gap, ou None para nao detectar.

        Binance retorna `(exchange, symbol)` (update_id por simbolo). Coinbase
        retorna None: nao expoe update_id por simbolo, entao nao ha gap detection
        confiavel (ver docstring de coinbase.py)."""

    def on_connect(self, detector: OrderBookGapDetector) -> None:  # noqa: B027
        """Hook opcional chamado a cada (re)conexao. Default no-op (nao abstrato).

        Exchanges cuja sequencia e por-conexao (Coinbase) sobrescrevem para
        resetar o estado do detector e evitar falso-gap apos reconectar.
        """


async def run_client(client: ExchangeClient, producer: MarketDataProducer) -> None:
    """Roda um client para sempre: conecta, consome, reconecta sob falha.

    Nunca retorna em operacao normal; so propaga se cancelado. Cada falha de
    conexao alimenta o circuit breaker e agenda backoff exponencial com jitter.
    """
    s = client.settings
    ex = str(client.exchange)
    breaker = CircuitBreaker(
        failure_threshold=s.circuit_failure_threshold,
        reset_timeout=s.circuit_reset_timeout,
    )
    detector = OrderBookGapDetector()
    attempt = 0

    while True:
        metrics.circuit_breaker_state.labels(ex).set(int(breaker.state))
        if not breaker.allow():
            logger.warning("[{}] circuito OPEN; aguardando cooldown.", ex)
            await asyncio.sleep(s.reconnect_backoff_base)
            continue

        try:
            async with websockets.connect(
                client.ws_url(), ping_interval=20, max_size=s.ws_max_message_bytes
            ) as ws:
                breaker.record_success()
                attempt = 0
                client.on_connect(detector)
                metrics.ws_connected.labels(ex).set(1)
                metrics.circuit_breaker_state.labels(ex).set(int(breaker.state))
                for sub in client.subscribe_messages():
                    await ws.send(json.dumps(sub))
                logger.info("[{}] conectado e subscrito.", ex)

                async for raw_msg in ws:
                    ingest_ms = now_ms()
                    for event in client.parse(json.loads(raw_msg), ingest_ms):
                        _dispatch(event, client, producer, detector)

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — qualquer falha de rede vira reconnect
            metrics.ws_connected.labels(ex).set(0)
            breaker.record_failure()
            metrics.ws_reconnects.labels(ex).inc()
            metrics.circuit_breaker_state.labels(ex).set(int(breaker.state))
            attempt += 1
            delay = _backoff(attempt, s.reconnect_backoff_base, s.reconnect_backoff_max)
            logger.warning("[{}] desconectado ({}); reconectando em {:.1f}s.", ex, exc, delay)
            await asyncio.sleep(delay)


def _dispatch(
    event: MarketEvent,
    client: ExchangeClient,
    producer: MarketDataProducer,
    detector: OrderBookGapDetector,
) -> None:
    """Produz um evento; em deltas, checa gap de sequencia antes (fail-loud)."""
    if isinstance(event, TradeEvent):
        producer.produce_trade(event)
        return
    # OrderBookDeltaEvent
    stream_key = client.gap_stream_key(event)
    if stream_key is None:  # exchange sem deteccao de gap confiavel (Coinbase)
        producer.produce_orderbook_delta(event)
        return
    check = detector.observe(stream_key, event.first_update_id, event.final_update_id)
    if check.is_gap:
        metrics.orderbook_gaps.labels(str(client.exchange), event.symbol).inc()
        logger.warning(
            "[{}] GAP no order book de {}: {} update_id(s) perdidos (book precisa re-sync).",
            client.exchange,
            event.symbol,
            check.missing,
        )
    producer.produce_orderbook_delta(event)


def _backoff(attempt: int, base: float, ceiling: float) -> float:
    """Backoff exponencial com teto e jitter (full jitter)."""
    capped = min(ceiling, base * (2 ** (attempt - 1)))
    return random.uniform(0, capped)
