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

    # --- Observabilidade ---
    metrics_port: int = 8001  # endpoint /metrics (Prometheus). Console=8080, Trino=8085.

    # --- Ambiente ---
    env: str = "dev"  # dev | prod
    log_level: str = "INFO"
    log_json: bool = False  # True em prod (Cloud Logging)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Settings cacheadas (singleton de processo)."""
    return Settings()
