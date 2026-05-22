"""Factory e entrypoint da API FastAPI (Marco 4).

`create_app` recebe deps injetáveis (útil em testes); `main` monta as deps reais
e sobe o Uvicorn. Lifespan cuida de iniciar/parar o broadcaster WebSocket.
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app
from pulso_infra import get_settings
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

# Caminho do dist/ compilado pelo Dockerfile (web-builder stage).
# Em dev local sem build, o diretório não existe e o serving estático é omitido.
_DIST = Path(__file__).parents[3] / "web" / "dist"

from pulso_serve import metrics
from pulso_serve.anomaly_store import AnomalyStore, build_anomaly_store
from pulso_serve.ksql import KsqlClient
from pulso_serve.live import CandleBroadcaster, CandleStream, ConnectionManager, kafka_candle_stream
from pulso_serve.routes import router
from pulso_serve.store import MarketStore, build_store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _client_ip(request: Request) -> str:
    """IP do cliente para o rate limiting. Atrás do Cloud Run o IP real está no
    primeiro hop do X-Forwarded-For; `request.client` é o proxy do Google."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def create_app(
    settings,
    store: MarketStore,
    ksql: KsqlClient,
    stream: CandleStream,
    anomaly_store: AnomalyStore | None = None,
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
    app.state.anomaly_store = anomaly_store  # None se o Marco 7 nao esta rodando

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Rate limiting (item 9): a API é pública (vitrine), mas limitada por IP
    # para conter abuso. Limiter por app — em testes cada create_app é isolado.
    limiter = Limiter(key_func=_client_ip, default_limits=["120/minute"])
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

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

    # Serving do frontend React compilado (Dockerfile web-builder stage).
    # /assets/* são os JS/CSS com hash; tudo o mais retorna index.html (SPA).
    # Omitido em dev local (dist/ não existe) para não interferir com o proxy Vite.
    if _DIST.exists():
        app.mount("/assets", StaticFiles(directory=_DIST / "assets"), name="assets")

        @app.get("/{full_path:path}", include_in_schema=False)
        async def _spa_fallback(full_path: str) -> FileResponse:
            return FileResponse(_DIST / "index.html")

    return app


def main() -> None:
    """Entrypoint de produção: monta deps reais e sobe Uvicorn."""
    import os

    settings = get_settings()
    store = build_store(settings)
    ksql = KsqlClient(settings.ksqldb_url)
    stream = kafka_candle_stream(settings)
    # Anomaly store: opcional — ativo apenas se o arquivo DuckDB ja existe
    # (criado pelo llm_explainer na primeira execucao).
    anomaly_store: AnomalyStore | None = None
    if os.path.exists(settings.anomaly_duckdb_path):
        # serve é leitor: o DuckDB é montado read-only (volume GCS); o escritor
        # é o llm_explainer. Abrir em modo escrita falharia no mount read-only.
        anomaly_store = build_anomaly_store(settings.anomaly_duckdb_path, read_only=True)

    app = create_app(settings, store, ksql, stream, anomaly_store)
    # Cloud Run injeta $PORT; fallback para serve_port em dev.
    port = int(os.environ.get("PORT", settings.serve_port))
    uvicorn.run(
        app,
        host=settings.serve_host,
        port=port,
        log_level=settings.log_level.lower(),
    )
