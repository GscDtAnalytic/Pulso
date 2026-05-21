"""Factory e entrypoint da API FastAPI (Marco 4).

`create_app` recebe deps injetáveis (útil em testes); `main` monta as deps reais
e sobe o Uvicorn. Lifespan cuida de iniciar/parar o broadcaster WebSocket.
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from pulso_infra import get_settings

from pulso_serve import metrics
from pulso_serve.ksql import KsqlClient
from pulso_serve.live import CandleBroadcaster, CandleStream, ConnectionManager, kafka_candle_stream
from pulso_serve.routes import router
from pulso_serve.store import MarketStore, build_store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def create_app(
    settings,
    store: MarketStore,
    ksql: KsqlClient,
    stream: CandleStream,
) -> FastAPI:
    """Constrói o app FastAPI com deps injetadas — chamado por `main` e por testes."""

    manager = ConnectionManager()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        broadcaster = CandleBroadcaster(manager, stream)
        task = asyncio.create_task(broadcaster.run())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            ksql.close()

    app = FastAPI(
        title="Pulso Market API",
        version="4.0.0",
        description="Histórico (marts dbt), estado live (ksqlDB) e push WebSocket de candles.",
        lifespan=lifespan,
    )

    app.state.settings = settings
    app.state.store = store
    app.state.ksql = ksql
    app.state.manager = manager

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def _metrics_middleware(request: Request, call_next) -> Response:
        route = request.scope.get("route")
        route_path = route.path if route is not None else "unknown"
        method = request.method
        with metrics.http_request_seconds.labels(method=method, route=route_path).time():
            response: Response = await call_next(request)
        metrics.http_requests.labels(
            method=method,
            route=route_path,
            status=str(response.status_code),
        ).inc()
        return response

    app.include_router(router)
    app.mount("/metrics", make_asgi_app())

    return app


def main() -> None:
    """Entrypoint de produção: monta deps reais e sobe Uvicorn."""
    settings = get_settings()
    store = build_store(settings)
    ksql = KsqlClient(settings.ksqldb_url)
    stream = kafka_candle_stream(settings)

    app = create_app(settings, store, ksql, stream)
    uvicorn.run(
        app,
        host=settings.serve_host,
        port=settings.serve_port,
        log_level=settings.log_level.lower(),
    )
