"""Rotas da API de serving (Marco 4).

Tres origens de dado, um so contrato pydantic:

- **historico** (`/api/candles`, `/api/symbols/{s}/daily`) — marts dbt via `MarketStore`.
- **live** (`/api/candles/live`) — pull query no ksqlDB (janela aberta, parcial).
- **push** (`/ws/candles`) — WebSocket, candles selados retransmitidos do Kafka.

Falha de backend (marts nao materializadas, ksqlDB fora) vira **HTTP 503** — nunca
um 200 com lista vazia (principio nao-negociavel #4: fail-loud).
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response, WebSocket
from fastapi.websockets import WebSocketDisconnect
from pulso_domain import load_symbols

from pulso_serve import metrics
from pulso_serve.ksql import KsqlUnavailable
from pulso_serve.models import Candle, DailyStat, LiveCandle, Symbol
from pulso_serve.store import StoreUnavailable

router = APIRouter()

Interval = Literal["M1", "M5", "H1"]


def _known_symbols() -> set[str]:
    """Universo canonico de simbolos — do seed do pulso-domain (fonte de verdade)."""
    return {s.canonical for s in load_symbols()}


def _require_symbol(symbol: str) -> None:
    """404 se `symbol` nao esta no universo configurado — barra injecao na pull query."""
    if symbol not in _known_symbols():
        raise HTTPException(status_code=404, detail=f"Simbolo desconhecido: {symbol!r}")


@router.get("/health", tags=["meta"])
def health(request: Request) -> dict:
    """Liveness + qual backend de historico esta ativo."""
    return {"status": "ok", "backend": request.app.state.settings.serve_history_backend}


@router.get("/api/symbols", response_model=list[Symbol], tags=["reference"])
def list_symbols() -> list[Symbol]:
    """Simbolos negociados. Servidos do seed do dominio — sempre disponiveis."""
    return [
        Symbol(
            symbol=s.canonical,
            base_asset=s.base,
            quote_asset=s.quote,
            is_active=s.active,
        )
        for s in load_symbols()
    ]


@router.get("/api/candles", response_model=list[Candle], tags=["history"])
def get_candles(
    request: Request,
    symbol: str = Query(..., description="Simbolo canonico (ex.: BTC-USD)."),
    interval: Interval = "M1",
    limit: int = Query(200, ge=1, le=5000),
) -> list:
    """Historico de candles OHLCV selados (`fct_candle`), em ordem cronologica."""
    _require_symbol(symbol)
    try:
        return request.app.state.store.candles(symbol, interval, limit)
    except StoreUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/api/symbols/{symbol}/daily", response_model=list[DailyStat], tags=["history"])
def get_daily(
    request: Request,
    symbol: str,
    limit: int = Query(90, ge=1, le=2000),
) -> list:
    """Resumo diario por simbolo (`fct_symbol_daily`), em ordem cronologica."""
    _require_symbol(symbol)
    try:
        return request.app.state.store.daily(symbol, limit)
    except StoreUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/api/candles/live", response_model=LiveCandle, tags=["live"])
def get_live_candle(
    request: Request,
    symbol: str = Query(..., description="Simbolo canonico."),
    interval: Interval = "M1",
):
    """Estado da janela ABERTA via pull query no ksqlDB. 204 se a janela nao existe."""
    _require_symbol(symbol)
    with metrics.live_query_seconds.time():
        try:
            row = request.app.state.ksql.live_candle(symbol, interval)
        except KsqlUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    if row is None:
        return Response(status_code=204)
    return LiveCandle(symbol=symbol, **row)


@router.websocket("/ws/candles")
async def ws_candles(websocket: WebSocket) -> None:
    """Push de candles selados. `?symbol=` filtra; sem ele, recebe todos."""
    symbol = websocket.query_params.get("symbol")
    if symbol is not None and symbol not in _known_symbols():
        await websocket.close(code=1008, reason=f"Simbolo desconhecido: {symbol}")
        return

    manager = websocket.app.state.manager
    await manager.connect(websocket, symbol)
    try:
        # O cliente nao precisa enviar nada; o receive mantem a conexao viva e
        # detecta o disconnect. Mensagens recebidas sao ignoradas de proposito.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:  # noqa: BLE001 — qualquer erro de socket: encerra limpo
        manager.disconnect(websocket)
