"""Testes do MarketStore — leitura das marts dbt via DuckDB."""

from __future__ import annotations

import duckdb
import pytest
from pulso_serve.store import MarketStore, StoreUnavailable

_SCHEMA = "analytics"

# Timestamps ISO compatíveis com DuckDB TIMESTAMP
_T1 = "2026-05-21 10:00:00"
_T2 = "2026-05-21 10:01:00"
_T3 = "2026-05-21 10:02:00"


@pytest.fixture
def duck_store(tmp_path):
    """MarketStore apontando para um DuckDB temporário com fixtures de mart."""
    db_path = str(tmp_path / "test.duckdb")
    con = duckdb.connect(db_path)
    con.execute(f"CREATE SCHEMA {_SCHEMA}")
    con.execute(f"""
        CREATE TABLE {_SCHEMA}.fct_candle (
            symbol VARCHAR, interval VARCHAR,
            window_start TIMESTAMP, window_end TIMESTAMP,
            open_price DOUBLE, high_price DOUBLE,
            low_price  DOUBLE, close_price DOUBLE,
            volume DOUBLE, vwap DOUBLE, trade_count INTEGER,
            direction VARCHAR, return_pct DOUBLE
        )
    """)
    con.execute(f"""
        INSERT INTO {_SCHEMA}.fct_candle VALUES
        ('BTC-USD','M1','{_T1}','{_T2}',100,110,90,105,10,102,5,'UP',5.0),
        ('BTC-USD','M1','{_T2}','{_T3}',105,115,95,110,12,108,6,'UP',4.8)
    """)
    con.execute(f"""
        CREATE TABLE {_SCHEMA}.fct_symbol_daily (
            symbol VARCHAR, trade_date DATE,
            open_price DOUBLE, high_price DOUBLE,
            low_price  DOUBLE, close_price DOUBLE,
            volume DOUBLE, vwap DOUBLE, trade_count INTEGER, candle_count INTEGER
        )
    """)
    con.execute(f"""
        INSERT INTO {_SCHEMA}.fct_symbol_daily VALUES
        ('BTC-USD','2026-05-20',99,120,88,110,1000,105,100,60),
        ('BTC-USD','2026-05-21',110,130,100,125,1200,115,120,61)
    """)
    con.close()

    def connect():
        return duckdb.connect(db_path, read_only=True)

    return MarketStore(connect, _SCHEMA)


def test_candles_returns_chronological_order(duck_store):
    rows = duck_store.candles("BTC-USD", "M1", limit=10)
    assert len(rows) == 2
    assert rows[0]["window_start"] < rows[1]["window_start"]


def test_candles_limit_respected(duck_store):
    rows = duck_store.candles("BTC-USD", "M1", limit=1)
    assert len(rows) == 1
    # limit=1 -> pega o mais recente e o inverte -> deve ser o segundo candle
    assert rows[0]["window_start"].isoformat().replace(" ", "T").startswith("2026-05-21T10:01")


def test_candles_unknown_symbol_returns_empty(duck_store):
    rows = duck_store.candles("ETH-USD", "M1", limit=10)
    assert rows == []


def test_daily_returns_chronological_order(duck_store):
    rows = duck_store.daily("BTC-USD", limit=10)
    assert len(rows) == 2
    assert rows[0]["trade_date"] < rows[1]["trade_date"]


def test_store_unavailable_on_bad_path():
    def bad_connect():
        raise RuntimeError("arquivo nao existe")

    store = MarketStore(bad_connect, _SCHEMA)
    with pytest.raises(StoreUnavailable):
        store.candles("BTC-USD", "M1")
