"""Lakehouse Iceberg do Pulso (Marco 3).

Sink idempotente Kafka -> Iceberg (MERGE por chave de negocio => efeito
exactly-once), particionamento bronze/silver, manutencao e time-travel para
backtest reproduzivel.

- `catalog`     — constroi o catalogo SQL Iceberg (Postgres em dev/prod, sqlite em teste).
- `tables`      — schemas e particionamento de `bronze.trades` / `silver.candles`.
- `sink`        — `IcebergSink`: MERGE idempotente + offsets Kafka no snapshot.
- `consumer`    — consumer Kafka em batch (assign explicito, Avro via Schema Registry).
- `pipelines`   — rotas topico -> tabela.
- `maintenance` — `expire_snapshots`.
- `timetravel`  — leitura de snapshots passados (backtest reproduzivel).
- `duckdb_mirror` — espelho Iceberg -> DuckDB para o dbt de dev (Marco 4).
"""

from pulso_storage.catalog import build_catalog
from pulso_storage.duckdb_mirror import mirror_to_duckdb
from pulso_storage.sink import IcebergSink, WriteResult, read_committed_offsets
from pulso_storage.tables import CANDLES, TRADES, TableSpec, ensure_table

__all__ = [
    "CANDLES",
    "TRADES",
    "IcebergSink",
    "TableSpec",
    "WriteResult",
    "build_catalog",
    "ensure_table",
    "mirror_to_duckdb",
    "read_committed_offsets",
]
