"""Backtest reproduzível — time-travel sobre o lakehouse Iceberg (Marco 6).

Usa o mecanismo de snapshots imutáveis do Iceberg para carregar o estado exato
de uma tabela em qualquer ponto histórico. Prova a reprodutibilidade: rodar o
mesmo backtest com o mesmo `--as-of` hoje ou daqui a um mês devolve o mesmo
resultado — nenhum dado é sobrescrito, cada commit é um snapshot permanente.

Isso é o pilar #3 da arquitetura: "reprodutibilidade — backtest sobre o estado
exato de uma data via Iceberg time-travel".

Uso:
    uv run python services/backtest.py --list-snapshots
    uv run python services/backtest.py --list-snapshots --table bronze.trades
    uv run python services/backtest.py --as-of 2026-05-20T12:00:00Z
    uv run python services/backtest.py --snapshot-id 1234567890
    uv run python services/backtest.py --as-of 2026-05-20T12:00:00Z --table bronze.trades

Exit code 0 = dados consistentes; 1 = invariante violada ou erro.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

import duckdb
from loguru import logger
from pulso_infra import get_settings
from pulso_storage.catalog import build_catalog
from pulso_storage.timetravel import list_snapshots, scan_as_of, to_duckdb


def _print_snapshots(table_id: str, snapshots: list) -> None:
    if not snapshots:
        logger.info("Nenhum snapshot em '{}'.", table_id)
        return
    logger.info("Snapshots de '{}' ({} total):", table_id, len(snapshots))
    for s in snapshots:
        logger.info(
            "  id={} | {} | op={}",
            s.snapshot_id,
            s.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            s.operation,
        )


def _analytics_trades(con: duckdb.DuckDBPyConnection) -> dict:
    """Analytics para bronze.trades."""
    row = con.execute(
        """
        SELECT
            COUNT(*)                                          AS total_trades,
            MIN(event_time)                                   AS earliest,
            MAX(event_time)                                   AS latest,
            COUNT(DISTINCT symbol)                            AS symbols,
            COUNT(CASE WHEN price <= 0 OR quantity <= 0
                       THEN 1 END)                            AS price_violations,
            ROUND(AVG(price), 4)                              AS avg_price
        FROM snapshot
        """
    ).fetchone()
    if row is None:
        return {}
    return {
        "total_trades": row[0],
        "earliest": str(row[1]),
        "latest": str(row[2]),
        "symbols": row[3],
        "price_violations": row[4],
        "avg_price": row[5],
    }


def _analytics_candles(con: duckdb.DuckDBPyConnection) -> dict:
    """Analytics para silver.candles — verifica invariantes OHLC."""
    row = con.execute(
        """
        SELECT
            COUNT(*)                                            AS total_candles,
            MIN(window_start)                                   AS earliest,
            MAX(window_end)                                     AS latest,
            COUNT(DISTINCT symbol)                              AS symbols,
            COUNT(CASE WHEN high < open OR high < close
                            OR low  > open OR low  > close
                            OR low  > high
                       THEN 1 END)                              AS ohlc_violations,
            ROUND(SUM(volume), 4)                               AS total_volume
        FROM snapshot
        """
    ).fetchone()
    if row is None:
        return {}
    return {
        "total_candles": row[0],
        "earliest": str(row[1]),
        "latest": str(row[2]),
        "symbols": row[3],
        "ohlc_violations": row[4],
        "total_volume": row[5],
    }


def run_backtest(
    table_id: str,
    as_of: datetime | None,
    snapshot_id: int | None,
) -> dict:
    """Carrega o estado histórico da tabela e executa analytics.

    Devolve um dict com as métricas calculadas. Lança ValueError se a tabela
    ou snapshot não existirem.
    """
    settings = get_settings()
    catalog = build_catalog(settings)

    if not catalog.table_exists(table_id):
        raise ValueError(f"Tabela '{table_id}' não encontrada no catálogo.")

    table = catalog.load_table(table_id)

    if snapshot_id is not None:
        scan = scan_as_of(table, snapshot_id=snapshot_id)
        label = f"snapshot_id={snapshot_id}"
    else:
        assert as_of is not None
        scan = scan_as_of(table, as_of=as_of)
        label = f"as-of={as_of.isoformat()}"

    logger.info("Carregando '{}' @ {}...", table_id, label)
    con = to_duckdb(scan, table_name="snapshot")

    results = {"label": label, "table": table_id}
    if "trades" in table_id:
        results.update(_analytics_trades(con))
    else:
        results.update(_analytics_candles(con))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backtest reproduzível via time-travel Iceberg."
    )
    parser.add_argument(
        "--table",
        default="silver.candles",
        metavar="NAMESPACE.NAME",
        help="Tabela a consultar (default: silver.candles).",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--list-snapshots",
        action="store_true",
        help="Lista todos os snapshots da tabela.",
    )
    source.add_argument(
        "--as-of",
        metavar="ISO",
        help="Timestamp ISO 8601 para time-travel (ex: 2026-05-20T12:00:00Z).",
    )
    source.add_argument(
        "--snapshot-id",
        type=int,
        metavar="ID",
        help="ID numérico de um snapshot específico.",
    )
    args = parser.parse_args()

    settings = get_settings()
    catalog = build_catalog(settings)

    if not catalog.table_exists(args.table):
        logger.error("Tabela '{}' não encontrada no catálogo.", args.table)
        sys.exit(1)

    table = catalog.load_table(args.table)

    if args.list_snapshots:
        _print_snapshots(args.table, list_snapshots(table))
        return

    as_of: datetime | None = None
    if args.as_of:
        as_of = datetime.fromisoformat(args.as_of)
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=UTC)

    try:
        results = run_backtest(
            table_id=args.table,
            as_of=as_of,
            snapshot_id=args.snapshot_id,
        )
    except ValueError as exc:
        logger.error("{}", exc)
        sys.exit(1)

    logger.info("Resultados do backtest (reproduzíveis para '{}'):", results["label"])
    for key, value in results.items():
        if key not in {"label", "table"}:
            logger.info("  {}: {}", key, value)

    violations = results.get("ohlc_violations", 0) + results.get("price_violations", 0)
    total = results.get("total_trades", results.get("total_candles", 0))

    if total == 0:
        logger.warning(
            "Snapshot vazio para '{}'. "
            "Verifique se o lake foi populado antes do instante consultado.",
            results["label"],
        )

    if violations > 0:
        logger.error(
            "INVARIANTE VIOLADA: {} registro(s) com preço/OHLC inválido em '{}'.",
            violations,
            results["label"],
        )
        sys.exit(1)

    logger.info("Backtest OK — dados consistentes para '{}'.", results["label"])


if __name__ == "__main__":
    main()
