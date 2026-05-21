"""Metricas Prometheus da API de serving (Marco 4).

Fecha o ciclo de observabilidade dos tres processos do Pulso: producer (`:8001`),
sink (`:8002`) e agora a API. Exposto em `/metrics` no proprio app (a API ja e
HTTP — nao precisa de um servidor de metricas a parte como o producer/sink).

Cardinalidade controlada: labels sao verbo HTTP, rota *template* (`/api/candles`,
nunca a URL com querystring) e familia de status — universos pequenos e fechados.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

http_requests = Counter(
    "pulso_serve_http_requests_total",
    "Requisicoes HTTP atendidas pela API.",
    ["method", "route", "status"],
)
http_request_seconds = Histogram(
    "pulso_serve_http_request_seconds",
    "Latencia de atendimento por rota.",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
live_query_seconds = Histogram(
    "pulso_serve_live_query_seconds",
    "Latencia da pull query ao ksqlDB (estado live de janela aberta).",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
ws_connections = Gauge(
    "pulso_serve_ws_connections",
    "Conexoes WebSocket abertas no momento (push de candles).",
)
candles_broadcast = Counter(
    "pulso_serve_candles_broadcast_total",
    "Candles selados retransmitidos para clientes WebSocket.",
)
