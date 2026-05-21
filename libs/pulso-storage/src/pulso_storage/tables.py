"""Definicao das tabelas Iceberg do lakehouse (Marco 3).

Duas camadas medallion sobre o object storage:

- **bronze.trades**  — copia fiel e imutavel de `trades.raw` (fonte da verdade no
  lake; reprocessar custa zero). Espelha `contracts/trade.avsc`.
- **silver.candles** — candles OHLCV selados vindos de `candles.m1/m5/h1`. Espelha
  `contracts/candle.avsc` + `symbol` (que no barramento e a chave Kafka, nao o value).

Particionamento por `day(event_time)` + `symbol`: poda de particao no time-travel
e no upsert (o anti-join do MERGE so varre as particoes do batch). `interval` entra
na particao do silver porque toda query de candle filtra por intervalo.

`event_time`/`window_*` viram `timestamptz` no Iceberg (no barramento sao epoch-millis
`long`): o lake fica consultavel com timestamps reais e o `day()` da particao funciona.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Propriedades padrao das tabelas: Parquet zstd (consistente com o producer) e
# algumas retentativas de commit (o sink e o unico writer, mas o catalogo SQL
# pode competir com a manutencao agendada).
_TABLE_PROPERTIES = {
    "write.parquet.compression-codec": "zstd",
    "commit.retry.num-retries": "4",
}


@dataclass(frozen=True, slots=True)
class TableSpec:
    """Tudo que o sink e a manutencao precisam saber sobre uma tabela do lake.

    `dedup_keys` e a chave de negocio do MERGE idempotente (ver `sink.py`):
    trades = `(exchange, symbol, trade_id)` — `trade_id` e local a exchange/simbolo,
    nao global; candles = `(symbol, interval, window_start)`.
    """

    namespace: str
    name: str
    schema: Schema
    partition_spec: PartitionSpec
    dedup_keys: tuple[str, ...]

    @property
    def identifier(self) -> str:
        return f"{self.namespace}.{self.name}"


# --- bronze.trades — espelho de contracts/trade.avsc -----------------------------
_TRADES_SCHEMA = Schema(
    NestedField(1, "trade_id", StringType(), required=True),
    NestedField(2, "exchange", StringType(), required=True),
    NestedField(3, "symbol", StringType(), required=True),
    NestedField(4, "price", DoubleType(), required=True),
    NestedField(5, "quantity", DoubleType(), required=True),
    NestedField(6, "side", StringType(), required=True),
    NestedField(7, "event_time", TimestamptzType(), required=True),
    NestedField(8, "ingest_time", TimestamptzType(), required=True),
)
_TRADES_PARTITION = PartitionSpec(
    PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="event_day"),
    PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="symbol"),
)
TRADES = TableSpec(
    namespace="bronze",
    name="trades",
    schema=_TRADES_SCHEMA,
    partition_spec=_TRADES_PARTITION,
    dedup_keys=("exchange", "symbol", "trade_id"),
)

# --- silver.candles — espelho de contracts/candle.avsc + symbol ------------------
_CANDLES_SCHEMA = Schema(
    NestedField(1, "symbol", StringType(), required=True),
    NestedField(2, "interval", StringType(), required=True),
    NestedField(3, "window_start", TimestamptzType(), required=True),
    NestedField(4, "window_end", TimestamptzType(), required=True),
    NestedField(5, "open", DoubleType(), required=True),
    NestedField(6, "high", DoubleType(), required=True),
    NestedField(7, "low", DoubleType(), required=True),
    NestedField(8, "close", DoubleType(), required=True),
    NestedField(9, "volume", DoubleType(), required=True),
    NestedField(10, "vwap", DoubleType(), required=True),
    NestedField(11, "trade_count", LongType(), required=True),
    NestedField(12, "is_final", BooleanType(), required=True),
)
_CANDLES_PARTITION = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="window_day"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="interval"),
)
CANDLES = TableSpec(
    namespace="silver",
    name="candles",
    schema=_CANDLES_SCHEMA,
    partition_spec=_CANDLES_PARTITION,
    dedup_keys=("symbol", "interval", "window_start"),
)

ALL_SPECS: tuple[TableSpec, ...] = (TRADES, CANDLES)


def ensure_table(catalog: Catalog, spec: TableSpec) -> Table:
    """Cria namespace e tabela se ainda nao existem; devolve a `Table` carregada.

    Idempotente — seguro chamar no startup do sink toda vez.
    """
    catalog.create_namespace_if_not_exists(spec.namespace)
    if not catalog.table_exists(spec.identifier):
        catalog.create_table(
            identifier=spec.identifier,
            schema=spec.schema,
            partition_spec=spec.partition_spec,
            properties=_TABLE_PROPERTIES,
        )
    return catalog.load_table(spec.identifier)
