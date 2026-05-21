"""Orquestrador do lakehouse (Marco 3).

Modos:

    uv run python -m pulso_storage              # roda o sink (trades + candles)
    uv run python -m pulso_storage --pipeline trades
    uv run python -m pulso_storage maintain     # expire_snapshots nas tabelas
    uv run python -m pulso_storage mirror       # espelha o lake num .duckdb (dbt dev)

No modo sink, cada pipeline roda em sua thread (consumer Kafka e bloqueante): cada
uma tem seu catalogo, sink e consumer. SIGINT seta o stop event compartilhado e
encerra as duas em ordem. Se uma thread morre com excecao, e fail-loud — o processo
inteiro cai (principio nao-negociavel #4).
"""

from __future__ import annotations

import argparse
import signal
import threading

from loguru import logger
from pulso_infra import Settings, get_settings, setup_logging, start_metrics_server

from pulso_storage.catalog import build_catalog
from pulso_storage.consumer import BatchingConsumer
from pulso_storage.duckdb_mirror import mirror_to_duckdb
from pulso_storage.maintenance import expire_snapshots
from pulso_storage.pipelines import Pipeline, build_pipelines
from pulso_storage.sink import IcebergSink, read_committed_offsets
from pulso_storage.tables import ensure_table


def _run_pipeline(
    pipeline: Pipeline,
    settings: Settings,
    stop_event: threading.Event,
    errors: list[tuple[str, BaseException]],
) -> None:
    """Loop de um pipeline: consome batches e faz MERGE no Iceberg ate o stop."""
    consumer: BatchingConsumer | None = None
    try:
        catalog = build_catalog(settings)
        table = ensure_table(catalog, pipeline.spec)
        sink = IcebergSink(table, pipeline.spec)
        consumer = BatchingConsumer(
            settings,
            pipeline.topics,
            pipeline.decode,
            read_committed_offsets(table),
            stop_event,
        )
        logger.info("Pipeline '{}' ativo -> {}", pipeline.name, pipeline.spec.identifier)
        for records, offsets in consumer.batches():
            sink.write(records, offsets)
    except Exception as exc:  # noqa: BLE001 — fail-loud: registra e derruba o processo
        logger.exception("Pipeline '{}' caiu", pipeline.name)
        errors.append((pipeline.name, exc))
        stop_event.set()
    finally:
        if consumer is not None:
            consumer.close()
        logger.info("Pipeline '{}' encerrado.", pipeline.name)


def run_sink(settings: Settings, pipeline_names: list[str] | None) -> None:
    """Sobe o endpoint de metricas e roda os pipelines do sink ate SIGINT."""
    pipelines = build_pipelines(settings)
    if pipeline_names:
        pipelines = tuple(p for p in pipelines if p.name in pipeline_names)
    if not pipelines:
        raise SystemExit(f"Nenhum pipeline casa com {pipeline_names}.")

    start_metrics_server(settings.sink_metrics_port)
    logger.info(
        "Sink iniciando | pipelines={} | catalogo={} | /metrics:{}",
        [p.name for p in pipelines],
        settings.iceberg_catalog_name,
        settings.sink_metrics_port,
    )

    stop_event = threading.Event()
    errors: list[tuple[str, BaseException]] = []
    threads = [
        threading.Thread(
            target=_run_pipeline,
            args=(p, settings, stop_event, errors),
            name=f"sink-{p.name}",
        )
        for p in pipelines
    ]

    # SIGINT/SIGTERM apenas setam o stop_event — nunca desenrolam a pilha. Assim um
    # upsert Iceberg em andamento numa thread termina antes do shutdown (do contrario
    # a finalizacao do interpretador derruba o pool de threads do PyIceberg no meio).
    # SIGTERM tambem cobre o encerramento por container (Cloud Run, Marco 8).
    def _request_stop(signum: int, _frame: object) -> None:
        logger.info("Sinal {} recebido; encerrando pipelines.", signal.Signals(signum).name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _request_stop)

    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=0.5)
    if errors:
        raise SystemExit(f"Sink encerrado com falha: {[name for name, _ in errors]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sink/lakehouse do Pulso (Marco 3).")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("run", help="Roda o sink (default).")
    parser.add_argument(
        "--pipeline",
        action="append",
        choices=["trades", "candles"],
        help="Pipeline a rodar (repetivel). Default: todos.",
    )
    maintain = sub.add_parser("maintain", help="Manutencao: expire_snapshots.")
    maintain.add_argument("--retain-hours", type=float, default=168.0)
    mirror = sub.add_parser("mirror", help="Espelha o lake num .duckdb (dbt dev).")
    mirror.add_argument("--db", default=None, help="Arquivo .duckdb destino (default: settings).")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(level=settings.log_level, json=settings.log_json)

    if args.command == "maintain":
        expire_snapshots(build_catalog(settings), retain_hours=args.retain_hours)
    elif args.command == "mirror":
        mirror_to_duckdb(build_catalog(settings), args.db or settings.dbt_duckdb_path)
    else:
        run_sink(settings, args.pipeline)


if __name__ == "__main__":
    main()
