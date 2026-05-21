"""Orquestrador da ingestao (Marco 1).

Sobe o endpoint de metricas, cria o producer idempotente e roda um client por
exchange concorrentemente (asyncio). Os simbolos vem do seed (`pulso_domain`),
nunca hardcoded. Encerra com flush do producer para nao perder o que esta na fila.

Uso:
    uv run python -m pulso_ingest                 # Binance + Coinbase
    uv run python -m pulso_ingest --exchange binance
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib

from loguru import logger
from pulso_domain import Exchange, active_symbols
from pulso_infra import OpenLineageEmitter, get_settings, setup_logging, start_metrics_server

from pulso_ingest.exchanges import BinanceClient, CoinbaseClient, run_client
from pulso_ingest.producer import MarketDataProducer

_CLIENTS = {
    Exchange.BINANCE: BinanceClient,
    Exchange.COINBASE: CoinbaseClient,
}


async def run(exchanges: list[Exchange]) -> None:
    """Roda a ingestao das exchanges dadas ate ser cancelada (SIGINT)."""
    settings = get_settings()
    symbols = active_symbols()
    start_metrics_server(settings.metrics_port)
    logger.info(
        "Ingestao iniciando | exchanges={} | simbolos={} | /metrics:{}",
        [str(e) for e in exchanges],
        [s.canonical for s in symbols],
        settings.metrics_port,
    )

    emitter = OpenLineageEmitter.from_settings(settings)
    run_id = emitter.emit_ingest_start("ingest-producer", settings.topic_trades)

    producer = MarketDataProducer(settings)
    clients = [_CLIENTS[ex](settings, symbols) for ex in exchanges]
    tasks = [asyncio.create_task(run_client(c, producer), name=str(c.exchange)) for c in clients]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Encerrando ingestao; cancelando clients.")
        raise
    finally:
        for t in tasks:
            t.cancel()
        pending = producer.flush()
        if pending:
            logger.warning("{} mensagem(ns) nao confirmadas no flush final.", pending)
        emitter.emit_ingest_complete("ingest-producer", settings.topic_trades, run_id, records=0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer de ingestao do Pulso (Marco 1).")
    parser.add_argument(
        "--exchange",
        action="append",
        choices=[str(e) for e in Exchange],
        help="Exchange a ingerir (repetivel). Default: todas.",
    )
    args = parser.parse_args()
    settings = get_settings()
    setup_logging(level=settings.log_level, json=settings.log_json)

    exchanges = [Exchange(e) for e in args.exchange] if args.exchange else list(Exchange)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(exchanges))


if __name__ == "__main__":
    main()
