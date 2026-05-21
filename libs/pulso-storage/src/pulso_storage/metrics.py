"""Metricas Prometheus do sink (Marco 3).

Contraparte "consumer-side" da metrica #1 de streaming (o producer do Marco 1 cobre
o lado de producao). Fail-loud (principio nao-negociavel #4): consumer lag e
freshness sao cidadaos de primeira classe, nao afterthought.

Cardinalidade controlada: labels sao `table`/`topic`/`partition` — universos
pequenos e fechados, nunca `trade_id` (anti-padrao da wiki: tecnologias/prometheus).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# --- Idempotencia / throughput do MERGE ---
records_written = Counter(
    "pulso_sink_records_written_total",
    "Linhas efetivamente inseridas no Iceberg (apos o MERGE por chave de negocio).",
    ["table"],
)
duplicates_skipped = Counter(
    "pulso_sink_duplicates_skipped_total",
    "Linhas que o MERGE ignorou por ja existirem — efeito exactly-once do sink.",
    ["table"],
)
batches_committed = Counter(
    "pulso_sink_batches_committed_total",
    "Commits Iceberg bem-sucedidos (dados + offsets Kafka, atomico).",
    ["table"],
)
commit_errors = Counter(
    "pulso_sink_commit_errors_total",
    "Falhas ao commitar um batch no Iceberg (fail-loud: deve ser raro e visivel).",
    ["table"],
)

# --- Latencia / correcao ---
commit_seconds = Histogram(
    "pulso_sink_commit_seconds",
    "Tempo de um ciclo de batch: decode + MERGE + commit Iceberg.",
    ["table"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
freshness_seconds = Gauge(
    "pulso_sink_freshness_seconds",
    "now - max(event_time) do ultimo batch escrito. Atraso e2e barramento->lake.",
    ["table"],
)
consumer_lag = Gauge(
    "pulso_sink_consumer_lag",
    "Offsets entre a posicao do consumer e o fim da particao (metrica #1 de streaming).",
    ["topic", "partition"],
)
