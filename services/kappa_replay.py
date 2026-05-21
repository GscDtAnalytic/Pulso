"""Kappa replay — reprocessamento do log de eventos com nova lógica (Marco 6).

Lê os tópicos Kafka a partir de um ponto configurável, processa pelo pipeline
de sink existente (IcebergSink idempotente) e para ao alcançar o HWM registrado
no startup. Resultado: o lake reflete exatamente os dados que existiam no log
até aquele instante.

Quando usar:
  - Corrigiu um bug de decodificação/decode no pipeline → `--from-beginning`
  - Precisa preencher o lake a partir de um timestamp → `--from-timestamp ISO`
  - Migrou o schema do Iceberg e quer backfill idempotente → `--from-beginning`
  - Quer simular sem escrever → `--dry-run`

Uso:
    uv run python services/kappa_replay.py --from-beginning
    uv run python services/kappa_replay.py --from-beginning --pipeline trades
    uv run python services/kappa_replay.py --from-timestamp 2026-05-20T00:00:00Z
    uv run python services/kappa_replay.py --from-beginning --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime

from loguru import logger
from pulso_infra import get_settings
from pulso_storage.catalog import build_catalog
from pulso_storage.pipelines import build_pipelines
from pulso_storage.replay import BoundedConsumer, ReplaySummary
from pulso_storage.sink import IcebergSink
from pulso_storage.tables import ensure_table


def _parse_timestamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def run_replay(
    pipeline_name: str,
    group_id: str,
    from_beginning: bool,
    from_ts: datetime | None,
    dry_run: bool,
) -> ReplaySummary:
    """Executa o replay de um único pipeline e devolve o sumário."""
    settings = get_settings()
    all_pipelines = build_pipelines(settings)
    pipelines = [
        p for p in all_pipelines
        if pipeline_name == "all" or p.name == pipeline_name
    ]

    if not pipelines:
        raise ValueError(f"Pipeline desconhecido: {pipeline_name!r}")

    if dry_run:
        logger.info("[DRY-RUN] pipelines={} | group={}", [p.name for p in pipelines], group_id)
        logger.info("[DRY-RUN] from-beginning={} | from-timestamp={}", from_beginning, from_ts)
        logger.info("[DRY-RUN] Nenhuma escrita será feita no lake.")
        return ReplaySummary(received=0, inserted=0, skipped=0, duration_seconds=0.0)

    catalog = build_catalog(settings)
    total_received = total_inserted = total_skipped = 0
    t0 = time.monotonic()

    for pipeline in pipelines:
        logger.info("Iniciando replay do pipeline '{}' (group={})...", pipeline.name, group_id)

        table = ensure_table(catalog, pipeline.spec)
        sink = IcebergSink(table, pipeline.spec)

        consumer = BoundedConsumer(
            settings=settings,
            topics=pipeline.topics,
            decode=pipeline.decode,
            group_id=f"{group_id}-{pipeline.name}",
            start_offsets={} if from_beginning else None,
            from_timestamp=from_ts if not from_beginning else None,
        )
        try:
            for records, offsets in consumer.batches(
                batch_size=settings.sink_batch_max_records,
                timeout_s=settings.sink_batch_max_seconds,
            ):
                result = sink.write(records, offsets)
                total_received += result.received
                total_inserted += result.inserted
                total_skipped += result.skipped
        finally:
            consumer.close()

    duration = time.monotonic() - t0
    return ReplaySummary(
        received=total_received,
        inserted=total_inserted,
        skipped=total_skipped,
        duration_seconds=duration,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kappa replay — reprocessamento idempotente do log de eventos."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--from-beginning",
        action="store_true",
        help="Replay desde o início do log (offset 0).",
    )
    source.add_argument(
        "--from-timestamp",
        metavar="ISO",
        help="Replay desde o primeiro registro >= o timestamp ISO 8601.",
    )
    parser.add_argument(
        "--pipeline",
        choices=["trades", "candles", "all"],
        default="all",
        help="Pipeline a reprocessar (default: all).",
    )
    parser.add_argument(
        "--consumer-group",
        metavar="GROUP",
        default=None,
        help="Consumer group ID. Default: prefixo configurado + timestamp Unix.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o que seria feito sem escrever no lake.",
    )
    args = parser.parse_args()

    settings = get_settings()
    ts_suffix = int(time.time())
    group_id = args.consumer_group or f"{settings.replay_consumer_group_prefix}-{ts_suffix}"

    from_ts: datetime | None = None
    if args.from_timestamp:
        try:
            from_ts = _parse_timestamp(args.from_timestamp)
        except ValueError as exc:
            logger.error("Timestamp inválido: {}. Use ISO 8601 (ex: 2026-05-20T00:00:00Z).", exc)
            sys.exit(1)

    try:
        summary = run_replay(
            pipeline_name=args.pipeline,
            group_id=group_id,
            from_beginning=args.from_beginning,
            from_ts=from_ts,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        logger.error("Replay falhou: {}", exc)
        sys.exit(1)

    if args.dry_run:
        return

    logger.info(
        "Replay concluído: {} registros em {:.1f}s ({:.0f} rec/s) | "
        "inseridos={} ignorados={}",
        summary.received,
        summary.duration_seconds,
        summary.throughput_rps,
        summary.inserted,
        summary.skipped,
    )

    if summary.received == 0:
        logger.warning(
            "Replay processou zero registros. "
            "Verifique se o tópico tem dados e se o HWM não era 0."
        )


if __name__ == "__main__":
    main()
