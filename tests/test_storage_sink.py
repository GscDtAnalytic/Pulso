"""Testes do sink idempotente Kafka -> Iceberg (Marco 3).

Cobrem o que torna o sink exactly-once: o MERGE por chave de negocio deduplica
batches reentregues e duplicatas internas; os offsets Kafka vao no snapshot junto
com os dados. Tudo offline (sqlite + warehouse local) — sem broker.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pulso_storage.sink import IcebergSink, read_committed_offsets
from pulso_storage.tables import CANDLES, TRADES, ensure_table

_T0 = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)


def _trade(trade_id: str, price: float = 100.0, symbol: str = "BTC-USD") -> dict:
    return {
        "trade_id": trade_id,
        "exchange": "binance",
        "symbol": symbol,
        "price": price,
        "quantity": 0.5,
        "side": "BUY",
        "event_time": _T0,
        "ingest_time": _T0,
    }


def _candle(symbol: str, interval: str, start: datetime) -> dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "window_start": start,
        "window_end": start,
        "open": 100.0,
        "high": 110.0,
        "low": 95.0,
        "close": 105.0,
        "volume": 12.0,
        "vwap": 103.0,
        "trade_count": 7,
        "is_final": True,
    }


@pytest.fixture
def trade_sink(iceberg_catalog):
    return IcebergSink(ensure_table(iceberg_catalog, TRADES), TRADES)


def _rows(sink: IcebergSink) -> int:
    return sink.table.scan().to_arrow().num_rows


def test_sink_inserts_new_records(trade_sink):
    result = trade_sink.write([_trade("1"), _trade("2")], {("trades.raw", 0): 2})
    assert (result.received, result.inserted, result.skipped) == (2, 2, 0)
    assert _rows(trade_sink) == 2


def test_replayed_batch_is_deduplicated(trade_sink):
    """Reentregar o mesmo batch nao duplica linhas — efeito exactly-once."""
    batch = [_trade("1"), _trade("2")]
    trade_sink.write(batch, {("trades.raw", 0): 2})
    result = trade_sink.write(batch, {("trades.raw", 0): 2})  # replay identico

    assert result.inserted == 0
    assert result.skipped == 2
    assert _rows(trade_sink) == 2


def test_partial_replay_inserts_only_new(trade_sink):
    trade_sink.write([_trade("1"), _trade("2")], {("trades.raw", 0): 2})
    result = trade_sink.write(
        [_trade("1"), _trade("2"), _trade("3")], {("trades.raw", 0): 3}
    )
    assert (result.inserted, result.skipped) == (1, 2)
    assert _rows(trade_sink) == 3


def test_duplicate_keys_within_batch_collapse(trade_sink):
    """O MERGE do PyIceberg aborta com chaves repetidas: o sink deduplica antes."""
    result = trade_sink.write([_trade("1", 100.0), _trade("1", 999.0)], {("trades.raw", 0): 2})
    assert result.inserted == 1
    assert _rows(trade_sink) == 1


def test_kafka_offsets_round_trip_through_snapshot(trade_sink):
    """Offsets consumidos vao para o snapshot e voltam para a retomada do consumer."""
    trade_sink.write([_trade("1")], {("trades.raw", 0): 1, ("trades.raw", 1): 9})
    assert read_committed_offsets(trade_sink.table) == {
        ("trades.raw", 0): 1,
        ("trades.raw", 1): 9,
    }


def test_empty_batch_is_noop(trade_sink):
    result = trade_sink.write([], {})
    assert (result.received, result.inserted) == (0, 0)
    assert trade_sink.table.current_snapshot() is None


def test_candle_sink_uses_composite_key(iceberg_catalog):
    """Candles deduplicam por (symbol, interval, window_start) — chave composta."""
    sink = IcebergSink(ensure_table(iceberg_catalog, CANDLES), CANDLES)
    w0, w1 = datetime(2026, 5, 21, 12, 0, tzinfo=UTC), datetime(2026, 5, 21, 12, 1, tzinfo=UTC)
    batch = [
        _candle("BTC-USD", "M1", w0),
        _candle("BTC-USD", "M1", w1),  # mesmo symbol/interval, janela diferente
        _candle("ETH-USD", "M1", w0),  # mesma janela, symbol diferente
    ]
    sink.write(batch, {("candles.m1", 0): 3})
    assert sink.table.scan().to_arrow().num_rows == 3

    # Reemitir a mesma janela selada (EMIT FINAL reentregue) nao duplica.
    result = sink.write([_candle("BTC-USD", "M1", w0)], {("candles.m1", 0): 3})
    assert result.inserted == 0


def test_candle_windowed_key_extracts_symbol():
    """A chave de uma TABLE janelada do ksqlDB e `symbol + window-start (8 bytes)`.

    Decodificar a chave inteira como UTF-8 quebra nos bytes binarios do timestamp
    (o bug que derrubava o pipeline de candles em producao). O decode janelado
    fatia o sufixo e recupera so o symbol.
    """
    import struct

    from pulso_storage.consumer import decode_key_utf8
    from pulso_storage.pipelines import _decode_windowed_symbol_key

    windowed = b"BTC-USD" + struct.pack(">q", 1779451200000)  # symbol + window-start
    assert _decode_windowed_symbol_key(windowed) == "BTC-USD"
    assert _decode_windowed_symbol_key(None) is None
    # A chave inteira como UTF-8 (comportamento antigo) falharia:
    with pytest.raises(UnicodeDecodeError):
        windowed.decode("utf-8")
    # Chave simples (trades.raw) continua via decode UTF-8 plano.
    assert decode_key_utf8(b"BTC-USD") == "BTC-USD"


def test_candle_pipeline_uses_windowed_key_decoder():
    """O pipeline de candles tem que vir cabeado com o decode de chave janelada."""
    from pulso_infra import get_settings
    from pulso_storage.consumer import decode_key_utf8
    from pulso_storage.pipelines import _decode_windowed_symbol_key, build_pipelines

    pipelines = {p.name: p for p in build_pipelines(get_settings())}
    assert pipelines["candles"].key_decode is _decode_windowed_symbol_key
    assert pipelines["trades"].key_decode is decode_key_utf8
