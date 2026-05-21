"""Metricas Prometheus da ingestao (Marco 1).

Definidas no registry global default; o endpoint `/metrics` e exposto por
`pulso_infra.start_metrics_server`. Sao a contraparte "producer-side" da metrica
#1 de streaming (consumer lag vem no consumo). Cobrem os pilares do marco:
saude da conexao, throughput, skew event-time->ingest, e gaps de order book.

Cardinalidade controlada: labels sao `exchange` e `symbol` (universo do seed,
pequeno e fechado), nunca IDs unicos (anti-padrao da wiki: tecnologias/prometheus).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# --- Throughput ---
trades_produced = Counter(
    "pulso_trades_produced_total",
    "Trades serializados e enfileirados para trades.raw.",
    ["exchange", "symbol"],
)
orderbook_deltas_produced = Counter(
    "pulso_orderbook_deltas_produced_total",
    "Deltas de order book enfileirados para orderbook.delta.",
    ["exchange", "symbol"],
)
produce_errors = Counter(
    "pulso_produce_errors_total",
    "Falhas de entrega reportadas pelo broker (delivery callback).",
    ["topic"],
)
events_dropped = Counter(
    "pulso_events_dropped_total",
    "Mensagens descartadas antes de produzir (simbolo fora do seed, parse invalido).",
    ["exchange", "reason"],
)

# --- Saude da conexao ---
ws_connected = Gauge(
    "pulso_ws_connected",
    "1 se o WebSocket da exchange esta conectado, 0 caso contrario.",
    ["exchange"],
)
ws_reconnects = Counter(
    "pulso_ws_reconnects_total",
    "Tentativas de reconexao do WebSocket (fail-loud: deve ser raro e visivel).",
    ["exchange"],
)
circuit_breaker_state = Gauge(
    "pulso_circuit_breaker_state",
    "Estado do circuit breaker por exchange: 0=closed, 1=half_open, 2=open.",
    ["exchange"],
)

# --- Correcao / order book ---
orderbook_gaps = Counter(
    "pulso_orderbook_gaps_total",
    "Gaps de sequencia detectados no order book (mensagens perdidas / reconexao).",
    ["exchange", "symbol"],
)
event_skew_ms = Histogram(
    "pulso_event_skew_ms",
    "Skew ingest_time - event_time em ms. Calibra o grace de janelamento (Marco 2).",
    ["exchange"],
    # Buckets de ms: rede saudavel (<100ms) ate desordem severa (>30s).
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000),
)
