"""Eventos normalizados: skew e formato Avro (espelho dos .avsc)."""

from pulso_ingest.models import OrderBookDeltaEvent, PriceLevel, TradeEvent


def _trade(**over) -> TradeEvent:
    base = dict(
        trade_id="1",
        exchange="binance",
        symbol="BTC-USD",
        price=50000.0,
        quantity=0.01,
        side="BUY",
        event_time=1_700_000_000_000,
        ingest_time=1_700_000_000_250,
    )
    base.update(over)
    return TradeEvent(**base)


def test_skew_e_ingest_menos_event():
    assert _trade().skew_ms == 250


def test_trade_to_avro_tem_todos_os_campos_do_contrato():
    avro = _trade().to_avro()
    assert set(avro) == {
        "trade_id",
        "exchange",
        "symbol",
        "price",
        "quantity",
        "side",
        "event_time",
        "ingest_time",
    }
    # timestamps sao long (ms), nao datetime
    assert isinstance(avro["event_time"], int)


def test_orderbook_delta_to_avro_serializa_niveis():
    ev = OrderBookDeltaEvent(
        exchange="binance",
        symbol="BTC-USD",
        first_update_id=100,
        final_update_id=110,
        bids=(PriceLevel(49999.0, 1.0),),
        asks=(PriceLevel(50001.0, 0.0),),
        event_time=1_700_000_000_000,
        ingest_time=1_700_000_000_100,
    )
    avro = ev.to_avro()
    assert avro["bids"] == [{"price": 49999.0, "quantity": 1.0}]
    assert avro["asks"] == [{"price": 50001.0, "quantity": 0.0}]
    assert avro["first_update_id"] == 100
    assert ev.skew_ms == 100
