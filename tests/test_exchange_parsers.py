"""Parsing puro dos clients de exchange (sem rede).

Verifica a normalizacao ticker-da-exchange -> simbolo canonico (via seed), o
mapeamento de lado agressor e o descarte de simbolos fora do universo.
"""

from pulso_domain import active_symbols
from pulso_infra import get_settings
from pulso_ingest.exchanges import BinanceClient, CoinbaseClient
from pulso_ingest.models import OrderBookDeltaEvent, TradeEvent

INGEST_MS = 1_700_000_000_500


def _binance() -> BinanceClient:
    return BinanceClient(get_settings(), active_symbols())


def _coinbase() -> CoinbaseClient:
    return CoinbaseClient(get_settings(), active_symbols())


def test_binance_trade_normaliza_e_mapeia_side():
    msg = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1_700_000_000_000,
            "s": "BTCUSDT",
            "t": 42,
            "p": "50000.5",
            "q": "0.01",
            "T": 1_700_000_000_000,
            "m": True,  # comprador e maker => agressor vendeu
        },
    }
    (ev,) = _binance().parse(msg, INGEST_MS)
    assert isinstance(ev, TradeEvent)
    assert ev.symbol == "BTC-USD"  # BTCUSDT -> canonico
    assert ev.side == "SELL"
    assert ev.price == 50000.5
    assert ev.ingest_time == INGEST_MS


def test_binance_depth_traz_update_ids():
    msg = {
        "stream": "btcusdt@depth",
        "data": {
            "e": "depthUpdate",
            "E": 1_700_000_000_000,
            "s": "BTCUSDT",
            "U": 100,
            "u": 110,
            "b": [["49999.0", "1.0"]],
            "a": [["50001.0", "2.0"]],
        },
    }
    (ev,) = _binance().parse(msg, INGEST_MS)
    assert isinstance(ev, OrderBookDeltaEvent)
    assert (ev.first_update_id, ev.final_update_id) == (100, 110)
    assert ev.bids[0].price == 49999.0


def test_binance_ignora_ack_e_simbolo_desconhecido():
    assert _binance().parse({"result": None, "id": 1}, INGEST_MS) == []
    unknown = {"stream": "x@trade", "data": {"e": "trade", "s": "FOOBAR", "t": 1,
               "p": "1", "q": "1", "T": 1, "m": False, "E": 1}}
    assert _binance().parse(unknown, INGEST_MS) == []


def test_coinbase_market_trades():
    msg = {
        "channel": "market_trades",
        "sequence_num": 7,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "99",
                        "product_id": "ETH-USD",
                        "price": "3000.0",
                        "size": "1.5",
                        "side": "BUY",
                        "time": "2026-05-21T12:00:00.000Z",
                    }
                ],
            }
        ],
    }
    (ev,) = _coinbase().parse(msg, INGEST_MS)
    assert isinstance(ev, TradeEvent)
    assert ev.symbol == "ETH-USD"
    assert ev.side == "BUY"
    assert ev.quantity == 1.5


def test_coinbase_l2_usa_sequence_num_como_update_id():
    msg = {
        "channel": "l2_data",
        "sequence_num": 5,
        "timestamp": "2026-05-21T12:00:00.000Z",
        "events": [
            {
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {"side": "bid", "price_level": "49000", "new_quantity": "0.3"},
                    {"side": "offer", "price_level": "51000", "new_quantity": "0.0"},
                ],
            }
        ],
    }
    (ev,) = _coinbase().parse(msg, INGEST_MS)
    assert isinstance(ev, OrderBookDeltaEvent)
    assert ev.first_update_id == ev.final_update_id == 5
    assert len(ev.bids) == 1 and len(ev.asks) == 1
    assert ev.bids[0].price == 49000.0


def test_coinbase_ignora_subscriptions_ack():
    assert _coinbase().parse({"channel": "subscriptions", "events": []}, INGEST_MS) == []
