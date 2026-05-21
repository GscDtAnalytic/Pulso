"""Testes do ConnectionManager e CandleBroadcaster (WebSocket push, Marco 4)."""

from __future__ import annotations

import asyncio
import contextlib
import threading

import pytest
from pulso_serve.live import CandleBroadcaster, ConnectionManager

# ---------------------------------------------------------------------------
# Stub de WebSocket suficiente para os testes
# ---------------------------------------------------------------------------


class _FakeWS:
    def __init__(self):
        self.sent: list[str] = []
        self._fail_on_send = False

    async def accept(self):
        pass

    async def send_text(self, text: str):
        if self._fail_on_send:
            raise RuntimeError("conexao morta")
        self.sent.append(text)


# ---------------------------------------------------------------------------
# ConnectionManager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broadcast_sends_to_all_without_filter():
    manager = ConnectionManager()
    ws1, ws2 = _FakeWS(), _FakeWS()
    await manager.connect(ws1)
    await manager.connect(ws2)

    candle = {"symbol": "BTC-USD", "close": 100.0}
    await manager.broadcast(candle)

    assert len(ws1.sent) == 1
    assert len(ws2.sent) == 1


@pytest.mark.asyncio
async def test_broadcast_filters_by_symbol():
    manager = ConnectionManager()
    ws_btc = _FakeWS()
    ws_eth = _FakeWS()
    await manager.connect(ws_btc, "BTC-USD")
    await manager.connect(ws_eth, "ETH-USD")

    await manager.broadcast({"symbol": "BTC-USD", "close": 100.0})

    assert len(ws_btc.sent) == 1
    assert len(ws_eth.sent) == 0  # filtro impediu


@pytest.mark.asyncio
async def test_broadcast_removes_dead_connections():
    manager = ConnectionManager()
    dead = _FakeWS()
    dead._fail_on_send = True
    alive = _FakeWS()
    await manager.connect(dead)
    await manager.connect(alive)

    await manager.broadcast({"symbol": "X", "close": 1.0})

    assert manager.count == 1  # dead removido


@pytest.mark.asyncio
async def test_disconnect_decrements_count():
    manager = ConnectionManager()
    ws = _FakeWS()
    await manager.connect(ws)
    assert manager.count == 1
    manager.disconnect(ws)
    assert manager.count == 0


# ---------------------------------------------------------------------------
# CandleBroadcaster
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broadcaster_delivers_candles():
    """Stream falso emite dois candles; ambos devem chegar ao WebSocket."""
    candles_to_emit = [
        {"symbol": "BTC-USD", "close": 100.0},
        {"symbol": "BTC-USD", "close": 101.0},
    ]

    def fake_stream(stop: threading.Event):
        for c in candles_to_emit:
            if stop.is_set():
                break
            yield c

    manager = ConnectionManager()
    ws = _FakeWS()
    await manager.connect(ws)

    broadcaster = CandleBroadcaster(manager, lambda stop: fake_stream(stop))
    task = asyncio.create_task(broadcaster.run())

    # Aguarda os dois candles serem processados (timeout conservador)
    for _ in range(50):
        await asyncio.sleep(0.05)
        if len(ws.sent) >= 2:
            break

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(ws.sent) == 2
