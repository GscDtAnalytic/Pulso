"""Sink idempotente Kafka -> Iceberg — o pilar exactly-once do lake (Marco 3).

Idempotencia por **MERGE na chave de negocio** (`spec.dedup_keys`), nao por sorte:

    table.upsert(df, join_cols=dedup_keys,
                 when_matched_update_all=False,   # linha ja existe -> nao faz nada
                 when_not_matched_insert_all=True) # linha nova     -> insere

Reentregar o mesmo batch (consumer reinicia, fonte manda o trade duas vezes) e
inofensivo: as linhas ja estao la, o MERGE insere zero. Efeito exactly-once no lake
mesmo com entrega at-least-once no barramento.

**Resume sem replay infinito.** Os offsets Kafka consumidos vao para as
`snapshot_properties` do mesmo commit Iceberg que grava os dados — append e avanco
de offset sao *um* commit atomico. No restart, o sink le os offsets do snapshot
(`read_committed_offsets`) e da `seek`: crash no meio de um batch nao reprocessa do
inicio dos tempos, e o que reprocessar o MERGE deduplica. Os dois mecanismos se
reforcam — cinto e suspensorio, ambos honestos sobre o que cobrem.

> Ressalva (mesmo tom da nota Flink/ksql-test do projeto): o `upsert` do PyIceberg
> varre as particoes tocadas pelo batch para o anti-join. Particionar por
> `day + symbol` poda essa varredura; batches pequenos a mantem barata. Em volume
> alto, o MERGE do Trino ou o Kafka Connect Iceberg sink escalam melhor — caminho
> de evolucao, nao limitacao de desenho.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pyarrow as pa
from loguru import logger
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

from pulso_storage import metrics
from pulso_storage.tables import TableSpec

if TYPE_CHECKING:
    from pulso_infra.lineage import OpenLineageEmitter

# Prefixo das snapshot properties que carregam o offset Kafka commitado.
# Uma chave por (topico, particao): `pulso.kafka.offset.<topic>.<partition>`.
_OFFSET_PREFIX = "pulso.kafka.offset."

# Offset = (topico, particao) -> offset do *proximo* registro a consumir.
Offsets = dict[tuple[str, int], int]


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Resultado de um batch: quantas linhas entraram e quantas o MERGE ignorou."""

    received: int
    inserted: int
    skipped: int  # duplicatas (no batch ou ja no lake) — o efeito idempotente


def _offset_key(topic: str, partition: int) -> str:
    return f"{_OFFSET_PREFIX}{topic}.{partition}"


def read_committed_offsets(table: Table) -> Offsets:
    """Le do snapshot atual os offsets Kafka ja commitados nesta tabela.

    Tabela nova / sem snapshot => dict vazio (o consumer comeca do inicio).
    """
    snapshot = table.current_snapshot()
    if snapshot is None:
        return {}
    offsets: Offsets = {}
    for key, value in snapshot.summary.additional_properties.items():
        if not key.startswith(_OFFSET_PREFIX):
            continue
        topic, _, partition = key[len(_OFFSET_PREFIX) :].rpartition(".")
        offsets[(topic, int(partition))] = int(value)
    return offsets


def _dedup_keep_last(records: list[dict], keys: tuple[str, ...]) -> list[dict]:
    """Remove duplicatas dentro do batch pela chave de negocio, mantendo a ultima.

    O `upsert` do PyIceberg aborta se a fonte tem chaves repetidas; alem disso, a
    ultima ocorrencia e a mais recente (maior offset) — a versao que queremos.
    """
    by_key: dict[tuple, dict] = {}
    for rec in records:
        by_key[tuple(rec[k] for k in keys)] = rec
    return list(by_key.values())


class IcebergSink:
    """Escreve batches numa tabela Iceberg de forma idempotente e atomica."""

    def __init__(
        self,
        table: Table,
        spec: TableSpec,
        emitter: OpenLineageEmitter | None = None,
    ) -> None:
        self._table = table
        self._spec = spec
        self._arrow_schema = schema_to_pyarrow(table.schema())
        self._emitter = emitter

    @property
    def table(self) -> Table:
        return self._table

    def write(self, records: list[dict], offsets: Offsets) -> WriteResult:
        """MERGE de `records` na tabela + grava `offsets` no mesmo commit.

        `records` sao dicts ja no formato do schema (timestamps como `datetime`
        tz-aware). Batch vazio => no-op (nada a commitar, offsets nao avancam).
        """
        if not records:
            return WriteResult(received=0, inserted=0, skipped=0)

        deduped = _dedup_keep_last(records, self._spec.dedup_keys)
        df = pa.Table.from_pylist(deduped, schema=self._arrow_schema)
        snapshot_props = {
            _offset_key(topic, partition): str(offset)
            for (topic, partition), offset in offsets.items()
        }

        label = self._spec.identifier
        try:
            with metrics.commit_seconds.labels(label).time():
                result = self._table.upsert(
                    df,
                    join_cols=list(self._spec.dedup_keys),
                    when_matched_update_all=False,  # evento imutavel: nunca atualiza
                    when_not_matched_insert_all=True,
                    snapshot_properties=snapshot_props,
                )
        except Exception:
            metrics.commit_errors.labels(label).inc()
            logger.error("Falha ao commitar batch em {} ({} linhas)", label, len(deduped))
            raise

        inserted = int(result.rows_inserted)
        skipped = len(records) - inserted
        metrics.records_written.labels(label).inc(inserted)
        metrics.duplicates_skipped.labels(label).inc(max(skipped, 0))
        metrics.batches_committed.labels(label).inc()
        self._observe_freshness(deduped)
        logger.info(
            "{}: batch {} recebido(s) -> {} inserido(s), {} duplicata(s) ignorada(s)",
            label,
            len(records),
            inserted,
            max(skipped, 0),
        )
        if self._emitter is not None:
            # offsets: {(topic, partition): next_offset} — extrair topics unicos
            topic_list = ", ".join(sorted({topic for topic, _part in offsets}))
            self._emitter.emit_sink_run(
                job_name=f"sink-{self._spec.name}",
                input_dataset=topic_list or self._spec.identifier,
                output_dataset=self._spec.identifier,
                records=inserted,
            )
        return WriteResult(received=len(records), inserted=inserted, skipped=max(skipped, 0))

    def _observe_freshness(self, records: list[dict]) -> None:
        """Freshness = now - max(event_time) do batch. Fail-loud (principio #4)."""
        field = "event_time" if "event_time" in records[0] else "window_end"
        newest: datetime = max(rec[field] for rec in records)
        lag = (datetime.now(UTC) - newest).total_seconds()
        metrics.freshness_seconds.labels(self._spec.identifier).set(lag)
