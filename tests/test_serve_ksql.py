"""Testes do KsqlClient — pull query ao ksqlDB (simulado via httpx.MockTransport)."""

from __future__ import annotations

import httpx
import pytest
from pulso_serve.ksql import KsqlClient, KsqlUnavailable


def _mock_client(payload: object, status_code: int = 200) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=payload)

    return httpx.Client(transport=httpx.MockTransport(handler))


_KSQL_ROW = [
    {"header": {"queryId": "q1", "schema": "`symbol` STRING"}},
    {"row": {"columns": ["M1", "2026-05-21T10:00:00", "2026-05-21T10:01:00",
                         100.0, 110.0, 90.0, 105.0, 10.0, 102.0, 5]}},
]


def test_live_candle_returns_dict():
    client = KsqlClient("http://fake", _mock_client(_KSQL_ROW))
    result = client.live_candle("BTC-USD", "M1")
    assert result is not None
    assert result["interval"] == "M1"
    assert result["open"] == 100.0
    assert result["trade_count"] == 5


def test_live_candle_returns_none_when_no_rows():
    client = KsqlClient("http://fake", _mock_client([{"header": {}}]))
    assert client.live_candle("BTC-USD", "M1") is None


def test_live_candle_invalid_interval():
    client = KsqlClient("http://fake", _mock_client([]))
    with pytest.raises(ValueError, match="interval invalido"):
        client.live_candle("BTC-USD", "INVALID")


def test_ksql_unavailable_on_network_error():
    def fail_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = KsqlClient("http://fake", httpx.Client(transport=httpx.MockTransport(fail_handler)))
    with pytest.raises(KsqlUnavailable):
        client.live_candle("BTC-USD", "M1")


def test_ksql_unavailable_on_http_error():
    client = KsqlClient("http://fake", _mock_client({"error": "not found"}, status_code=404))
    with pytest.raises(KsqlUnavailable):
        client.live_candle("BTC-USD", "M1")
