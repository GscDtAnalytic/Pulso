"""Testes offline do Marco 6 — reprocessamento Kappa.

Cobrem três propriedades centrais:
1. `is_caught_up` — lógica de detecção de HWM (pura, sem broker).
2. `ReplaySummary.throughput_rps` — cálculo de throughput.
3. Reproducibilidade do backtest via Iceberg time-travel (sem broker, sem Kafka).
4. Idempotência do replay via IcebergSink (o cerne do Kappa: replay é noop).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pulso_storage.replay import ReplaySummary, is_caught_up
from pulso_storage.sink import IcebergSink
from pulso_storage.tables import CANDLES, TRADES, ensure_table
from pulso_storage.timetravel import list_snapshots, scan_as_of, to_duckdb

_T0 = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
_T1 = datetime(2026, 5, 21, 12, 1, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# is_caught_up — pura, sem broker
# ---------------------------------------------------------------------------


def test_caught_up_when_current_matches_hwm():
    hwm = {("trades.raw", 0): 10, ("trades.raw", 1): 5}
    current = {("trades.raw", 0): 10, ("trades.raw", 1): 5}
    assert is_caught_up(current, hwm)


def test_not_caught_up_when_behind():
    hwm = {("trades.raw", 0): 10}
    current = {("trades.raw", 0): 9}
    assert not is_caught_up(current, hwm)


def test_caught_up_exceeds_hwm():
    """Offset além do HWM (raro mas possível) também é caught up."""
    hwm = {("trades.raw", 0): 10}
    current = {("trades.raw", 0): 15}
    assert is_caught_up(current, hwm)


def test_empty_partition_is_always_caught_up():
    """Partição com HWM == 0 está vazia — considerada caught up desde o início."""
    hwm = {("trades.raw", 0): 0}
    current: dict = {}
    assert is_caught_up(current, hwm)


def test_mixed_partitions_not_caught_up_if_one_lags():
    """Uma partição atrasada é suficiente para bloquear o progresso."""
    hwm = {("trades.raw", 0): 10, ("trades.raw", 1): 5}
    current = {("trades.raw", 0): 10, ("trades.raw", 1): 3}
    assert not is_caught_up(current, hwm)


def test_caught_up_empty_hwm():
    """HWM vazio (sem tópicos) => trivialmente caught up."""
    assert is_caught_up({}, {})


def test_not_caught_up_partition_absent_in_current():
    """Partição no HWM mas ausente em current => não caught up."""
    hwm = {("trades.raw", 0): 5}
    current: dict = {}  # nunca consumimos nada
    assert not is_caught_up(current, hwm)


# ---------------------------------------------------------------------------
# ReplaySummary — throughput
# ---------------------------------------------------------------------------


def test_replay_summary_throughput():
    s = ReplaySummary(received=1000, inserted=800, skipped=200, duration_seconds=10.0)
    assert s.throughput_rps == pytest.approx(100.0)


def test_replay_summary_zero_duration():
    """Sem divisão por zero quando o replay é instantâneo."""
    s = ReplaySummary(received=50, inserted=50, skipped=0, duration_seconds=0.0)
    assert s.throughput_rps == 0.0


def test_replay_summary_all_skipped_is_idempotent():
    """Se tudo foi ignorado (replay idêntico), received == skipped."""
    s = ReplaySummary(received=100, inserted=0, skipped=100, duration_seconds=5.0)
    assert s.inserted == 0
    assert s.skipped == s.received


# ---------------------------------------------------------------------------
# Reproducibilidade via time-travel (offline — sqlite + warehouse local)
# ---------------------------------------------------------------------------


def _trade(trade_id: str, price: float = 100.0) -> dict:
    return {
        "trade_id": trade_id,
        "exchange": "binance",
        "symbol": "BTC-USD",
        "price": price,
        "quantity": 1.0,
        "side": "BUY",
        "event_time": _T0,
        "ingest_time": _T0,
    }


def test_backtest_reproducible_same_snapshot(iceberg_catalog):
    """scan_as_of(snapshot_id) sempre devolve o mesmo conjunto de linhas."""
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    sink.write([_trade("1"), _trade("2")], {("trades.raw", 0): 2})
    snapshot_a = list_snapshots(sink.table)[0].snapshot_id

    # Escritas posteriores NÃO afetam o snapshot A.
    sink.write([_trade("3")], {("trades.raw", 0): 3})

    scan_1 = scan_as_of(sink.table, snapshot_id=snapshot_a)
    scan_2 = scan_as_of(sink.table, snapshot_id=snapshot_a)

    rows_1 = scan_1.to_arrow().num_rows
    rows_2 = scan_2.to_arrow().num_rows
    assert rows_1 == rows_2 == 2  # sempre 2, independente de escritas futuras


def test_backtest_latest_snapshot_sees_all_data(iceberg_catalog):
    """O snapshot mais recente contém todos os dados acumulados."""
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    sink.write([_trade("1")], {("trades.raw", 0): 1})
    sink.write([_trade("2")], {("trades.raw", 0): 2})
    sink.write([_trade("3")], {("trades.raw", 0): 3})

    assert sink.table.scan().to_arrow().num_rows == 3
    snapshots = list_snapshots(sink.table)
    assert len(snapshots) == 3


def test_time_travel_to_duckdb_analytics(iceberg_catalog):
    """to_duckdb materializa snapshot histórico e permite SQL ad-hoc."""
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    sink.write([_trade("1", 100.0), _trade("2", 200.0)], {("trades.raw", 0): 2})
    snap_a = list_snapshots(sink.table)[0].snapshot_id

    sink.write([_trade("3", 300.0)], {("trades.raw", 0): 3})

    # Analytics sobre snapshot histórico — não vê o trade "3".
    con = to_duckdb(scan_as_of(sink.table, snapshot_id=snap_a), table_name="hist")
    avg = con.execute("SELECT ROUND(AVG(price), 1) FROM hist").fetchone()[0]
    assert avg == pytest.approx(150.0)  # (100+200)/2, sem o 300


def test_time_travel_as_of_timestamp(iceberg_catalog):
    """scan_as_of(as_of=datetime) resolve para o snapshot correto."""
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    sink.write([_trade("1")], {("trades.raw", 0): 1})

    snapshots = list_snapshots(sink.table)
    assert len(snapshots) == 1

    # as_of com qualquer timestamp >= o snapshot deve encontrá-lo.
    from datetime import timedelta

    ts_after = snapshots[0].timestamp + timedelta(seconds=1)
    scan = scan_as_of(sink.table, as_of=ts_after)
    assert scan.to_arrow().num_rows == 1


# ---------------------------------------------------------------------------
# Idempotência do replay — o cerne do Kappa (sem broker, via IcebergSink)
# ---------------------------------------------------------------------------


def test_kappa_replay_is_noop_for_same_batch(iceberg_catalog):
    """Replay do mesmo batch é no-op: o MERGE garante exactly-once no lake.

    Isso é a prova formal do pilar Kappa: reprocessar com os mesmos eventos
    não altera o estado do lake.
    """
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    batch = [_trade("1"), _trade("2"), _trade("3")]

    # Ingestão original.
    r1 = sink.write(batch, {("trades.raw", 0): 3})
    assert r1.inserted == 3

    # Replay idêntico (simula crash-recovery ou reprocessamento Kappa).
    r2 = sink.write(batch, {("trades.raw", 0): 3})
    assert r2.inserted == 0
    assert r2.skipped == 3

    # Estado do lake inalterado.
    assert sink.table.scan().to_arrow().num_rows == 3


def test_kappa_partial_replay_inserts_only_new(iceberg_catalog):
    """Replay parcial com registros extras insere só o delta — sem duplicatas."""
    sink = IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)
    sink.write([_trade("1"), _trade("2")], {("trades.raw", 0): 2})

    # Replay do início com dados originais + novos.
    r = sink.write(
        [_trade("1"), _trade("2"), _trade("3"), _trade("4")],
        {("trades.raw", 0): 4},
    )
    assert r.inserted == 2   # só "3" e "4" são novos
    assert r.skipped == 2    # "1" e "2" são duplicatas silenciosas
    assert sink.table.scan().to_arrow().num_rows == 4


def _candle(symbol: str, interval: str, start: datetime) -> dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "window_start": start,
        "window_end": start,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 10.0,
        "vwap": 102.0,
        "trade_count": 5,
        "is_final": True,
    }


def test_kappa_candle_replay_is_idempotent(iceberg_catalog):
    """Replay de candles selados (EMIT FINAL) não cria duplicatas."""
    sink = IcebergSink(ensure_table(iceberg_catalog, CANDLES), CANDLES)
    batch = [
        _candle("BTC-USD", "M1", _T0),
        _candle("ETH-USD", "M1", _T0),
    ]

    sink.write(batch, {("candles.m1", 0): 2})
    r = sink.write(batch, {("candles.m1", 0): 2})  # replay

    assert r.inserted == 0
    assert r.skipped == 2
    assert sink.table.scan().to_arrow().num_rows == 2
