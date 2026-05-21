"""Push de candles selados por WebSocket (Marco 4).

Fecha o caminho live da arquitetura: candle selado pelo ksqlDB -> topico
`candles.m1` -> esta API -> navegador, sem polling. Tres pecas:

- `ConnectionManager` — conjunto de WebSockets abertos, com filtro opcional por
  simbolo; faz o fan-out de cada candle.
- `kafka_candle_stream` — consumer Kafka *bloqueante* de `candles.m1` (Avro via
  Schema Registry). E uma funcao geradora, rodada numa thread.
- `CandleBroadcaster` — ponte thread (Kafka, bloqueante) -> asyncio (WebSocket):
  a thread enfileira, uma task async esvazia a fila e retransmite.

O stream e injetavel — os testes passam um gerador falso e exercitam o fan-out
sem broker, como o resto do projeto.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Iterator

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer
from loguru import logger
from pulso_infra import Settings

from pulso_serve import metrics

# Um stream de candles: dado um stop event, gera dicts de candle ate parar.
CandleStream = Callable[[threading.Event], Iterator[dict]]


class ConnectionManager:
    """Gerencia os WebSockets abertos e faz broadcast dos candles."""

    def __init__(self) -> None:
        # WebSocket -> filtro de simbolo (None = recebe todos).
        self._connections: dict[object, str | None] = {}

    async def connect(self, websocket, symbol: str | None = None) -> None:
        await websocket.accept()
        self._connections[websocket] = symbol
        metrics.ws_connections.set(len(self._connections))

    def disconnect(self, websocket) -> None:
        self._connections.pop(websocket, None)
        metrics.ws_connections.set(len(self._connections))

    async def broadcast(self, candle: dict) -> None:
        """Envia `candle` a cada conexao cujo filtro casa (ou nao tem filtro)."""
        message = json.dumps(candle, default=str)  # default=str: datetime -> ISO
        symbol = candle.get("symbol")
        dead: list[object] = []
        for websocket, symbol_filter in list(self._connections.items()):
            if symbol_filter is not None and symbol_filter != symbol:
                continue
            try:
                await websocket.send_text(message)
            except Exception:  # noqa: BLE001 — conexao morta: remove no fim
                dead.append(websocket)
        for websocket in dead:
            self.disconnect(websocket)

    @property
    def count(self) -> int:
        return len(self._connections)


def kafka_candle_stream(settings: Settings) -> CandleStream:
    """Stream real: consome `serve_candle_topic` do Kafka (Avro) e gera candles.

    `auto.offset.reset=latest` — o push e do agora em diante; o historico vem da
    API REST (marts). `symbol` e a chave Kafka (ver ksqldb/README.md); injetado
    no dict como o sink do Marco 3 faz.
    """

    def stream(stop: threading.Event) -> Iterator[dict]:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": "pulso-serve-ws",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                **settings.kafka_security_config(),
            }
        )
        sr = SchemaRegistryClient(settings.schema_registry_config())
        avro = AvroDeserializer(sr)
        key_de = StringDeserializer("utf_8")
        consumer.subscribe([settings.serve_candle_topic])
        logger.info("Feed WebSocket consumindo '{}'.", settings.serve_candle_topic)
        try:
            while not stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error() is not None:
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Erro no feed de candles: {}", msg.error())
                    continue
                key = key_de(msg.key()) if msg.key() is not None else None
                value = avro(
                    msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
                )
                yield {**value, "symbol": key}
        finally:
            consumer.close()

    return stream


class CandleBroadcaster:
    """Roda um `CandleStream` numa thread e retransmite via `ConnectionManager`."""

    def __init__(
        self,
        manager: ConnectionManager,
        stream: CandleStream,
        *,
        max_queue: int = 1000,
    ) -> None:
        self._manager = manager
        self._stream = stream
        self._stop = threading.Event()
        self._max_queue = max_queue
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=max_queue)

    async def run(self) -> None:
        """Task de longa duracao (lifespan do app): bombeia o stream -> broadcast."""
        loop = asyncio.get_running_loop()
        thread = threading.Thread(
            target=self._pump, args=(loop,), name="ws-candle-feed", daemon=True
        )
        thread.start()
        try:
            while True:
                candle = await self._queue.get()
                metrics.candles_broadcast.inc()
                await self._manager.broadcast(candle)
        except asyncio.CancelledError:
            self.stop()
            raise

    def _pump(self, loop: asyncio.AbstractEventLoop) -> None:
        """Corpo da thread: itera o stream (bloqueante) e enfileira no loop async."""
        try:
            for candle in self._stream(self._stop):
                if self._stop.is_set():
                    break
                loop.call_soon_threadsafe(self._offer, candle)
        except Exception:  # noqa: BLE001 — fail-loud no log; nao derruba o app web
            logger.exception("Feed de candles WebSocket encerrou com erro.")

    def _offer(self, candle: dict) -> None:
        try:
            self._queue.put_nowait(candle)
        except asyncio.QueueFull:
            logger.warning("Fila do feed WebSocket cheia — candle descartado.")

    def stop(self) -> None:
        self._stop.set()
