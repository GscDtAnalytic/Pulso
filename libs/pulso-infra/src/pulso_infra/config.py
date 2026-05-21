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

    # --- Ambiente ---
    env: str = "dev"  # dev | prod
    log_level: str = "INFO"
    log_json: bool = False  # True em prod (Cloud Logging)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Settings cacheadas (singleton de processo)."""
    return Settings()
