"""Testes offline dos modelos dbt (Marco 4).

Monta um lake Iceberg minúsculo, espelha para DuckDB e roda `dbt build` contra
o target `dev`. 100% offline — sem broker, sem MinIO, sem Trino.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from pulso_storage.duckdb_mirror import mirror_to_duckdb
from pulso_storage.sink import IcebergSink
from pulso_storage.tables import CANDLES, TRADES, ensure_table

_T0 = datetime(2026, 5, 21, 10, 0, 0, tzinfo=UTC)
_T1 = datetime(2026, 5, 21, 10, 1, 0, tzinfo=UTC)


def _trade(trade_id: str, price: float = 100.0) -> dict:
    return {
        "trade_id": trade_id,
        "exchange": "binance",
        "symbol": "BTC-USD",
        "price": price,
        "quantity": 0.5,
        "side": "BUY",
        "event_time": _T0,
        "ingest_time": _T0,
    }


def _candle(symbol: str = "BTC-USD", interval: str = "M1") -> dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "window_start": _T0,
        "window_end": _T1,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 10.0,
        "vwap": 102.0,
        "trade_count": 5,
        "is_final": True,
    }


@pytest.fixture
def lake_duckdb(iceberg_catalog, tmp_path):
    """Popula o lake Iceberg e espelha para um arquivo .duckdb temporário."""
    # Garante tabelas e insere fixtures
    t_trades = ensure_table(iceberg_catalog, TRADES)
    t_candles = ensure_table(iceberg_catalog, CANDLES)

    sink_trades = IcebergSink(t_trades, TRADES)
    sink_candles = IcebergSink(t_candles, CANDLES)
    sink_trades.write([_trade("T1"), _trade("T2", price=200.0)], offsets={})
    sink_candles.write([_candle("BTC-USD", "M1"), _candle("ETH-USD", "M1")], offsets={})

    db_path = str(tmp_path / "pulso_lake.duckdb")
    mirror_to_duckdb(iceberg_catalog, db_path)
    return db_path


def test_dbt_build_succeeds(lake_duckdb, tmp_path):
    """dbt build --target dev deve rodar sem erros no lake espelhado."""
    import pathlib

    # dbt-core requer Python <=3.12 nas versões atuais; pula se nao for importavel.
    try:
        from dbt.cli.main import dbtRunner  # type: ignore[import]
    except Exception:
        pytest.skip("dbt.cli.main nao importavel (verifique compatibilidade com Python 3.14+)")

    project_dir = str(pathlib.Path(__file__).parents[1] / "dbt")

    env_before = os.environ.get("PULSO_DBT_DUCKDB_PATH")
    os.environ["PULSO_DBT_DUCKDB_PATH"] = lake_duckdb
    try:
        runner = dbtRunner()
        result = runner.invoke(
            [
                "build",
                "--project-dir", project_dir,
                "--profiles-dir", project_dir,
                "--target", "dev",
            ]
        )
    finally:
        if env_before is None:
            os.environ.pop("PULSO_DBT_DUCKDB_PATH", None)
        else:
            os.environ["PULSO_DBT_DUCKDB_PATH"] = env_before

    assert result.success, f"dbt build falhou: {result.exception}"


def test_dbt_seed_matches_domain_seed():
    """O seed dbt deve ser idêntico ao seed do pulso-domain (fonte de verdade única)."""
    import pathlib

    root = pathlib.Path(__file__).parents[1]
    domain_seed = root / "libs/pulso-domain/src/pulso_domain/seeds/symbols.csv"
    dbt_seed = root / "dbt/seeds/symbols.csv"

    assert dbt_seed.exists(), "dbt/seeds/symbols.csv não encontrado — rode 'make dbt-seed-sync'"
    assert domain_seed.read_text() == dbt_seed.read_text(), (
        "dbt/seeds/symbols.csv diverge do seed de domínio — rode 'make dbt-seed-sync'"
    )
