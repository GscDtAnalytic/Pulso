"""Testes de integração da API FastAPI — TestClient com deps falsas."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pulso_serve.ksql import KsqlUnavailable
from pulso_serve.routes import router
from pulso_serve.store import StoreUnavailable

# ---------------------------------------------------------------------------
# Stubs de Store e KsqlClient
# ---------------------------------------------------------------------------


class _FakeStore:
    def __init__(self, candles=None, daily=None, raise_=False):
        self._candles = candles or []
        self._daily = daily or []
        self._raise = raise_

    def candles(self, symbol, interval, limit=200):
        if self._raise:
            raise StoreUnavailable("backend offline")
        return self._candles

    def daily(self, symbol, limit=90):
        if self._raise:
            raise StoreUnavailable("backend offline")
        return self._daily


class _FakeKsql:
    def __init__(self, result=None, raise_=False):
        self._result = result
        self._raise = raise_

    def live_candle(self, symbol, interval):
        if self._raise:
            raise KsqlUnavailable("ksqldb offline")
        return self._result

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Fixture de app
# ---------------------------------------------------------------------------

_CANDLE = {
    "symbol": "BTC-USD", "interval": "M1",
    "window_start": "2026-05-21T10:00:00", "window_end": "2026-05-21T10:01:00",
    "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0,
    "volume": 10.0, "vwap": 102.0, "trade_count": 5,
    "direction": "UP", "return_pct": 5.0,
}

_DAILY = {
    "symbol": "BTC-USD", "trade_date": "2026-05-21",
    "open": 100.0, "high": 120.0, "low": 88.0, "close": 110.0,
    "volume": 1000.0, "vwap": 105.0, "trade_count": 100, "candle_count": 60,
}

_LIVE_ROW = {
    "interval": "M1",
    "window_start": "2026-05-21T10:00:00",
    "window_end": "2026-05-21T10:01:00",
    "open": 100.0, "high": 108.0, "low": 99.0, "close": 107.0,
    "volume": 8.0, "vwap": 103.0, "trade_count": 3,
}


def _make_app(store=None, ksql=None) -> FastAPI:
    from pulso_infra import get_settings
    from pulso_serve.live import ConnectionManager

    settings = get_settings()
    app = FastAPI()
    app.state.settings = settings
    app.state.store = store or _FakeStore()
    app.state.ksql = ksql or _FakeKsql()
    app.state.manager = ConnectionManager()
    app.include_router(router)
    return app


# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------


def test_health():
    client = TestClient(_make_app())
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_list_symbols():
    client = TestClient(_make_app())
    res = client.get("/api/symbols")
    assert res.status_code == 200
    assert isinstance(res.json(), list)
    # Deve ter ao menos um símbolo do seed de domínio
    assert len(res.json()) > 0


def test_get_candles_ok():
    client = TestClient(_make_app(store=_FakeStore(candles=[_CANDLE])))
    res = client.get("/api/candles?symbol=BTC-USD&interval=M1")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 1
    assert data[0]["symbol"] == "BTC-USD"


def test_get_candles_unknown_symbol_404():
    client = TestClient(_make_app())
    res = client.get("/api/candles?symbol=FAKE-USD&interval=M1")
    assert res.status_code == 404


def test_get_candles_backend_down_503():
    client = TestClient(_make_app(store=_FakeStore(raise_=True)))
    res = client.get("/api/candles?symbol=BTC-USD&interval=M1")
    assert res.status_code == 503


def test_get_daily_ok():
    client = TestClient(_make_app(store=_FakeStore(daily=[_DAILY])))
    res = client.get("/api/symbols/BTC-USD/daily")
    assert res.status_code == 200
    assert res.json()[0]["trade_date"] == "2026-05-21"


def test_get_daily_unknown_symbol_404():
    client = TestClient(_make_app())
    res = client.get("/api/symbols/FAKE-USD/daily")
    assert res.status_code == 404


def test_live_candle_ok():
    client = TestClient(_make_app(ksql=_FakeKsql(result=_LIVE_ROW)))
    res = client.get("/api/candles/live?symbol=BTC-USD&interval=M1")
    assert res.status_code == 200
    assert res.json()["symbol"] == "BTC-USD"
    assert res.json()["is_final"] is False


def test_live_candle_no_window_204():
    client = TestClient(_make_app(ksql=_FakeKsql(result=None)))
    res = client.get("/api/candles/live?symbol=BTC-USD&interval=M1")
    assert res.status_code == 204


def test_live_candle_ksql_down_503():
    client = TestClient(_make_app(ksql=_FakeKsql(raise_=True)))
    res = client.get("/api/candles/live?symbol=BTC-USD&interval=M1")
    assert res.status_code == 503
