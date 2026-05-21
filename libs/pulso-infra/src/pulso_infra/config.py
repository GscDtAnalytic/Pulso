"""Configuracao tipada via variaveis de ambiente (12-factor).

Todos os endpoints (Kafka, Schema Registry, object storage) vem do ambiente.
Em dev, `.env` aponta para o docker-compose local; em prod (GCP), as mesmas
chaves apontam para Redpanda self-hosted/Cloud e GCS.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PULSO_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Kafka / Redpanda ---
    kafka_bootstrap: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"

    # --- Object storage (Iceberg). Local = MinIO; prod = GCS. ---
    # Endpoint vazio => usa GCS nativo (prod). Preenchido => S3-compativel (MinIO/dev).
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    iceberg_warehouse: str = "s3://pulso-lakehouse/warehouse"

    # --- Topicos ---
    topic_trades: str = "trades.raw"
    topic_orderbook: str = "orderbook.delta"
    topic_dlq_suffix: str = ".dlq"

    # --- Lakehouse / Iceberg (sink, Marco 3) ---
    # Catalogo SQL do Iceberg. Dev/prod = Postgres do compose; testes = sqlite.
    # PyIceberg usa URI SQLAlchemy; Trino le o mesmo catalogo via conector jdbc.
    iceberg_catalog_uri: str = "postgresql+psycopg2://pulso:pulso@localhost:5432/pulso"
    iceberg_catalog_name: str = "pulso"
    # Grupo do consumer do sink; batch = quantos eventos por commit Iceberg.
    sink_consumer_group: str = "pulso-storage-sink"
    sink_batch_max_records: int = 500
    sink_batch_max_seconds: float = 5.0
    sink_metrics_port: int = 8002  # /metrics do sink (producer usa 8001)

    # --- Ingestao (producer, Marco 1) ---
    producer_client_id: str = "pulso-ingest"
    binance_ws_url: str = "wss://stream.binance.com:9443/stream"
    coinbase_ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    # Falhas consecutivas que abrem o circuito; segundos em OPEN antes de sondar.
    circuit_failure_threshold: int = 5
    circuit_reset_timeout: float = 30.0
    # Backoff de reconnect (exponencial com teto e jitter), em segundos.
    reconnect_backoff_base: float = 1.0
    reconnect_backoff_max: float = 30.0
    # Tamanho maximo de frame WebSocket. Snapshots de order book (Coinbase level2)
    # passam de 1 MiB; o default da lib (1 MiB) derruba a conexao com 1009.
    ws_max_message_bytes: int = 16 * 1024 * 1024

    # --- Serving (FastAPI + dbt, Marco 4) ---
    # Backend de historico servido pela API: as marts dbt vivem ou no DuckDB de dev
    # (mesmo arquivo que o `dbt build` materializa) ou no Trino de prod — mesma SQL.
    serve_host: str = "0.0.0.0"
    serve_port: int = 8000  # API HTTP/WebSocket; /metrics no mesmo app (producer=8001, sink=8002).
    serve_history_backend: str = "duckdb"  # duckdb (dev) | trino (prod)
    serve_candle_topic: str = "candles.m1"  # topico que o push WebSocket retransmite
    # Catalogo/schema das marts dbt. DuckDB: catalogo = stem do arquivo .duckdb.
    dbt_duckdb_path: str = "dbt/pulso_lake.duckdb"
    dbt_lake_catalog: str = "pulso_lake"
    dbt_marts_schema: str = "analytics"
    # ksqlDB (estado live de janela aberta via pull query — ver ksqldb/README.md).
    ksqldb_url: str = "http://localhost:8088"
    # Trino (historico em prod; conector iceberg sobre o mesmo catalogo do sink).
    trino_host: str = "localhost"
    trino_port: int = 8085
    trino_user: str = "pulso"

    # --- Observabilidade ---
    metrics_port: int = 8001  # endpoint /metrics (Prometheus). Console=8080, Trino=8085.

    # --- Reprocessamento Kappa (Marco 6) ---
    # Consumer group de replay — grupo descartável, nunca conflita com o sink live.
    # O CLI adiciona sufixo de timestamp: `pulso-kappa-replay-<unix_ts>`.
    replay_consumer_group_prefix: str = "pulso-kappa-replay"

    # --- Governanca / Lineage (Marco 5) ---
    # URL vazia desliga o OpenLineage (no-op). Em dev com `make up-lineage`: http://localhost:5000.
    openlineage_url: str = ""
    openlineage_namespace: str = "pulso"
    # SLO de freshness: maximo de segundos entre o evento mais recente e agora.
    freshness_slo_seconds: int = 60
    # Pushgateway Prometheus para o freshness-emitter standalone. Vazio = so imprime.
    pushgateway_url: str = ""

    # --- Ambiente ---
    env: str = "dev"  # dev | prod
    log_level: str = "INFO"
    log_json: bool = False  # True em prod (Cloud Logging)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Settings cacheadas (singleton de processo)."""
    return Settings()
