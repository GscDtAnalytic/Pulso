"""Testes de tabelas, time-travel, manutencao e pipelines do lakehouse (Marco 3)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pulso_infra import get_settings
from pulso_storage.maintenance import expire_snapshots
from pulso_storage.pipelines import CANDLE_TOPICS, _decode_candle, _decode_trade, build_pipelines
from pulso_storage.sink import IcebergSink
from pulso_storage.tables import ALL_SPECS, TRADES, ensure_table
from pulso_storage.timetravel import list_snapshots, scan_as_of

_T0 = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)


def _trade(trade_id: str) -> dict:
    return {
        "trade_id": trade_id,
        "exchange": "binance",
        "symbol": "BTC-USD",
        "price": 100.0,
        "quantity": 0.5,
        "side": "BUY",
        "event_time": _T0,
        "ingest_time": _T0,
    }


# --- Tabelas / particionamento ---------------------------------------------------


def test_ensure_table_is_idempotent(iceberg_catalog):
    first = ensure_table(iceberg_catalog, TRADES)
    second = ensure_table(iceberg_catalog, TRADES)
    assert first.name() == second.name() == ("bronze", "trades")


def test_bronze_partitioned_by_day_and_symbol(iceberg_catalog):
    table = ensure_table(iceberg_catalog, TRADES)
    fields = {f.name for f in table.spec().fields}
    assert fields == {"event_day", "symbol"}


def test_silver_partitioned_by_day_and_interval(iceberg_catalog):
    from pulso_storage.tables import CANDLES

    table = ensure_table(iceberg_catalog, CANDLES)
    fields = {f.name for f in table.spec().fields}
    assert fields == {"window_day", "interval"}


def test_dedup_keys_match_business_keys():
    assert TRADES.dedup_keys == ("exchange", "symbol", "trade_id")
    assert all(spec.namespace in {"bronze", "silver"} for spec in ALL_SPECS)


# --- Time-travel -----------------------------------------------------------------


def test_time_travel_reads_an_earlier_snapshot(iceberg_catalog):
    """scan_as_of(snapshot antigo) ve so o que existia naquele commit."""
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    sink.write([_trade("1")], {("trades.raw", 0): 1})
    sink.write([_trade("2"), _trade("3")], {("trades.raw", 0): 3})

    snapshots = list_snapshots(sink.table)
    assert len(snapshots) == 2

    historic = scan_as_of(sink.table, snapshot_id=snapshots[0].snapshot_id)
    assert historic.to_arrow().num_rows == 1  # so o primeiro batch
    assert sink.table.scan().to_arrow().num_rows == 3  # estado corrente


def test_scan_as_of_requires_exactly_one_selector(iceberg_catalog):
    table = ensure_table(iceberg_catalog, TRADES)
    with pytest.raises(ValueError, match="exatamente um"):
        scan_as_of(table)


# --- Manutencao ------------------------------------------------------------------


def test_expire_snapshots_keeps_only_current(iceberg_catalog):
    """retain negativo => corte no futuro => todo snapshot antigo expira."""
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    sink.write([_trade("1")], {("trades.raw", 0): 1})
    sink.write([_trade("2")], {("trades.raw", 0): 2})
    sink.write([_trade("3")], {("trades.raw", 0): 3})
    assert len(list_snapshots(sink.table)) == 3

    expire_snapshots(iceberg_catalog, retain_hours=-1.0)

    reloaded = iceberg_catalog.load_table(TRADES.identifier)
    assert len(reloaded.snapshots()) == 1
    assert reloaded.scan().to_arrow().num_rows == 3  # dados intactos


def test_expire_snapshots_skips_missing_tables(iceberg_catalog):
    # Nenhuma tabela criada — nao deve levantar.
    expire_snapshots(iceberg_catalog, retain_hours=1.0)


# --- Pipelines -------------------------------------------------------------------


def test_decode_trade_is_identity():
    value = _trade("1")
    assert _decode_trade("BTC-USD", value) is value


def test_decode_candle_injects_symbol_from_key():
    decoded = _decode_candle("ETH-USD", {"interval": "M1", "open": 1.0})
    assert decoded["symbol"] == "ETH-USD"


def test_decode_candle_rejects_missing_key():
    with pytest.raises(ValueError, match="sem chave"):
        _decode_candle(None, {"interval": "M1"})


def test_pipelines_cover_both_layers():
    pipelines = build_pipelines(get_settings())
    assert {p.name for p in pipelines} == {"trades", "candles"}
    candles = next(p for p in pipelines if p.name == "candles")
    assert candles.topics == CANDLE_TOPICS
