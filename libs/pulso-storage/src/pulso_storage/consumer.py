"""Consumer Kafka em batch que alimenta o sink Iceberg (Marco 3).

Faz **assign explicito** das particoes (nao `subscribe`): o sink e um writer unico,
sem rebalance, e o ponto de retomada e o offset gravado no snapshot Iceberg, nao o
offset commitado no Kafka. `enable.auto.commit=false` de proposito — a verdade do
progresso mora no lake (ver `sink.py`).

No startup, cada particao da `seek` para o offset lido do snapshot da tabela
(`initial_offsets`); particao sem offset registrado comeca do inicio do log
(`earliest` — o lake e a fonte da verdade, reprocessar do zero e barato).

Valores em Avro sao desserializados via Schema Registry (o payload carrega so o
schema ID). `timestamp-millis` volta como `datetime` tz-aware — ja no formato que o
schema Iceberg (`timestamptz`) espera.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer
from loguru import logger
from pulso_infra import Settings

from pulso_storage import metrics

# Um registro decodificado: (chave Kafka — symbol — pode ser None, value Avro dict).
DecodeFn = Callable[[str | None, dict], dict]
Offsets = dict[tuple[str, int], int]


class BatchingConsumer:
    """Consome topicos Kafka e entrega registros decodificados em batches."""

    def __init__(
        self,
        settings: Settings,
        topics: tuple[str, ...],
        decode: DecodeFn,
        initial_offsets: Offsets,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._settings = settings
        self._topics = topics
        self._decode = decode
        self._stop = stop_event or threading.Event()
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": settings.sink_consumer_group,
                "enable.auto.commit": False,  # progresso vive no snapshot Iceberg
                "auto.offset.reset": "earliest",
            }
        )
        sr = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._avro = AvroDeserializer(sr)
        self._key_de = StringDeserializer("utf_8")
        self._assign(initial_offsets)

    def _assign(self, initial_offsets: Offsets) -> None:
        """Assign explicito de todas as particoes dos topicos, no offset de retomada."""
        assignment: list[TopicPartition] = []
        for topic in self._topics:
            meta = self._consumer.list_topics(topic, timeout=10).topics[topic]
            if meta.error is not None:
                raise RuntimeError(f"Topico ausente no broker: {topic} ({meta.error})")
            for partition in meta.partitions:
                offset = initial_offsets.get((topic, partition), OFFSET_BEGINNING)
                assignment.append(TopicPartition(topic, partition, offset))
        self._consumer.assign(assignment)
        logger.info(
            "Sink assign | topicos={} | particoes={} | retomando de offsets gravados={}",
            list(self._topics),
            len(assignment),
            {f"{t}/{p}": o for (t, p), o in initial_offsets.items()},
        )

    def batches(self) -> Iterator[tuple[list[dict], Offsets]]:
        """Gera `(registros, offsets)` — `offsets` = proximo offset por particao.

        Fecha o batch quando atinge `sink_batch_max_records` ou
        `sink_batch_max_seconds`. Batch vazio (so timeout) nao e entregue, mas as
        metricas de lag sao atualizadas a cada ciclo.
        """
        records: list[dict] = []
        offsets: Offsets = {}
        deadline = time.monotonic() + self._settings.sink_batch_max_seconds
        while not self._stop.is_set():
            msg = self._consumer.poll(0.5)
            now = time.monotonic()
            if msg is not None and msg.error() is None:
                records.append(self._decode_msg(msg))
                # Offset a retomar = proximo registro apos o consumido.
                offsets[(msg.topic(), msg.partition())] = msg.offset() + 1
            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error("Erro de consumo Kafka: {}", msg.error())

            full = len(records) >= self._settings.sink_batch_max_records
            timed_out = now >= deadline
            if records and (full or timed_out):
                self._update_lag(offsets)
                yield records, offsets
                records, offsets = [], {}
                deadline = now + self._settings.sink_batch_max_seconds
            elif timed_out:
                self._update_lag(offsets)
                deadline = now + self._settings.sink_batch_max_seconds

    def _decode_msg(self, msg) -> dict:  # noqa: ANN001 (tipo ditado pela lib)
        topic = msg.topic()
        key = self._key_de(msg.key()) if msg.key() is not None else None
        value = self._avro(msg.value(), SerializationContext(topic, MessageField.VALUE))
        return self._decode(key, value)

    def _update_lag(self, offsets: Offsets) -> None:
        """Atualiza o gauge de consumer lag (metrica #1 de streaming)."""
        for (topic, partition), next_offset in offsets.items():
            _, high = self._consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=5, cached=True
            )
            metrics.consumer_lag.labels(topic, str(partition)).set(max(high - next_offset, 0))

    def close(self) -> None:
        self._consumer.close()
