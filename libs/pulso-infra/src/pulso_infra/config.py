"""Configuracao tipada via variaveis de ambiente (12-factor).

Todos os endpoints (Kafka, Schema Registry, object storage) vem do ambiente.
Em dev, `.env` aponta para o docker-compose local; em prod (GCP), as mesmas
chaves apontam para Redpanda self-hosted/Cloud e GCS.
"""

from __future__ import annotations

import tempfile
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


@lru_cache(maxsize=1)
def _materialize_ca(pem: str) -> str:
    """Grava o CA cert (conteudo PEM vindo por env) num arquivo temporario.

    librdkafka e o SchemaRegistryClient querem um caminho de arquivo
    (`ssl.ca.location`). Em prod o PEM chega como secret env var; aqui ele
    vira arquivo uma vez por processo.
    """
    path = Path(tempfile.gettempdir()) / "pulso-kafka-ca.crt"
    path.write_text(pem)
    return str(path)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PULSO_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Kafka / Redpanda ---
    kafka_bootstrap: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"

    # --- Seguranca do barramento (prod) ---
    # Dev: PLAINTEXT (docker-compose). Prod: SASL_SSL + SCRAM-SHA-256 contra o
    # Redpanda self-hosted. Ver kafka_security_config()/schema_registry_config().
    kafka_security_protocol: str = "PLAINTEXT"  # PLAINTEXT | SASL_SSL
    kafka_sasl_mechanism: str = "SCRAM-SHA-256"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    # CA cert que valida o TLS do broker e do Schema Registry. Em dev/local
    # aponta-se um arquivo (`_location`); em prod o conteudo PEM chega por env
    # (`_pem`, secret) e e materializado em arquivo por `_ca_path()`.
    kafka_ssl_ca_location: str = ""
    kafka_ssl_ca_pem: str = ""

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
    # data-stream.binance.vision: endpoint público de market-data da Binance, mesmo
    # formato de combined stream que stream.binance.com mas SEM geo-block (este último
    # responde HTTP 451 a IPs de regiões restritas, incl. GCP US). Override: PULSO_BINANCE_WS_URL.
    binance_ws_url: str = "wss://data-stream.binance.vision:9443/stream"
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

    # --- Eixo B / LLM (Marco 7) ---
    # Chave vazia desliga o LLM explainer (servico recusa startup). PULSO_ANTHROPIC_API_KEY.
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-haiku-4-5-20251001"
    topic_anomaly: str = "events.anomaly"
    anomaly_detector_metrics_port: int = 8003
    llm_explainer_metrics_port: int = 8004
    anomaly_consumer_group: str = "pulso-anomaly-detector"
    llm_consumer_group: str = "pulso-llm-explainer"
    # Rolling window de candles para calcular o baseline de deteccao.
    anomaly_window_size: int = 20
    anomaly_min_samples: int = 5  # minimo para evitar falsos positivos no warm-up
    anomaly_volume_zscore_threshold: float = 3.0
    anomaly_price_pct_threshold: float = 2.0  # variacao percentual (2 = 2%)
    anomaly_volatility_zscore_threshold: float = 3.0
    # Arquivo DuckDB que armazena anomalias + explicacoes. Separado do lake dbt.
    anomaly_duckdb_path: str = "anomaly_explanations.duckdb"
    # Feed RSS de noticias para contexto do LLM. Falha de fetch e silenciosa.
    news_rss_url: str = "https://cryptopanic.com/news/rss/"
    news_fetch_timeout: float = 5.0
    news_max_headlines: int = 5

    # --- Cloud GCP (Marco 8) ---
    # Em prod (Cloud Run), o warehouse usa gs:// e ADC resolve as credenciais.
    # gcs_project_id é opcional quando rodando com uma service account no GCP.
    gcs_project_id: str = ""

    # --- Ambiente ---
    env: str = "dev"  # dev | prod
    log_level: str = "INFO"
    log_json: bool = False  # True em prod (Cloud Logging)

    def _ca_path(self) -> str:
        """Caminho do CA cert. Prefere o arquivo explicito; senao materializa
        o PEM vindo por env. String vazia quando nao ha CA configurado."""
        if self.kafka_ssl_ca_location:
            return self.kafka_ssl_ca_location
        if self.kafka_ssl_ca_pem:
            return _materialize_ca(self.kafka_ssl_ca_pem)
        return ""

    def kafka_security_config(self) -> dict[str, str]:
        """Config de seguranca do cliente Kafka (librdkafka).

        Vazio em dev (PLAINTEXT). Em prod, SASL_SSL + SCRAM-SHA-256 + CA cert.
        Cada call site faz `Consumer({**base, **settings.kafka_security_config()})`.
        """
        if self.kafka_security_protocol == "PLAINTEXT":
            return {}
        cfg = {
            "security.protocol": self.kafka_security_protocol,
            "sasl.mechanism": self.kafka_sasl_mechanism,
            "sasl.username": self.kafka_sasl_username,
            "sasl.password": self.kafka_sasl_password,
        }
        ca = self._ca_path()
        if ca:
            cfg["ssl.ca.location"] = ca
        return cfg

    def schema_registry_config(self) -> dict[str, str]:
        """Config do SchemaRegistryClient — URL + CA cert quando em HTTPS."""
        cfg: dict[str, str] = {"url": self.schema_registry_url}
        ca = self._ca_path()
        if ca and self.schema_registry_url.startswith("https"):
            cfg["ssl.ca.location"] = ca
        return cfg


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Settings cacheadas (singleton de processo)."""
    return Settings()
