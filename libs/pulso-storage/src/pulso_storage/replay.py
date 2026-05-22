"""Replay Kappa — consumidor limitado ao HWM do startup (Marco 6).

Pilar #2 da arquitetura: "reprocessar = replay do log". Em vez de uma camada
batch separada (Lambda), o Kappa replica o mesmo pipeline de stream sobre o log
histórico. O resultado é idempotente porque o sink já usa MERGE por chave de
negócio.

`BoundedConsumer` implementa o replay controlado:

1. No startup, registra o HWM (high-water mark) de cada partição — snapshot do
   estado do log naquele instante.
2. Consome mensagens a partir do ponto configurado (beginning, offset fixo ou
   timestamp).
3. Para quando `is_caught_up` retorna True para todas as partições: processa
   exatamente os dados que existiam quando o replay foi iniciado, sem competição
   com escrita simultânea do producer live.

O consumer group é descartável (prefixo + timestamp): cada replay nasce sem
estado persistido no broker e não interfere no grupo do sink live.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from loguru import logger
from pulso_infra import Settings

from pulso_storage.consumer import DecodeFn, KeyDecodeFn, decode_key_utf8

# Offsets: (tópico, partição) -> próximo offset a consumir.
Offsets = dict[tuple[str, int], int]


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    """Estatísticas de um replay completo."""

    received: int
    inserted: int
    skipped: int
    duration_seconds: float

    @property
    def throughput_rps(self) -> float:
        """Registros por segundo processados."""
        return self.received / self.duration_seconds if self.duration_seconds > 0 else 0.0


def is_caught_up(current: Offsets, hwm: Offsets) -> bool:
    """True se `current` alcançou o HWM em todas as partições não-vazias.

    Partição com HWM == 0 está vazia — considerada "caught up" desde o início.
    Função pura: testável sem broker.
    """
    for key, hwm_offset in hwm.items():
        if hwm_offset <= 0:
            continue  # partição vazia, nada a consumir
        if current.get(key, 0) < hwm_offset:
            return False
    return True


def _snapshot_hwm(settings: Settings, topics: tuple[str, ...]) -> Offsets:
    """Registra o HWM de cada partição usando um consumer temporário.

    Chamado antes de criar o consumer principal para garantir que o HWM
    capturado é o estado do log no momento do startup do replay.
    """
    tmp = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "__pulso-hwm-probe__",
            "enable.auto.commit": False,
            **settings.kafka_security_config(),
        }
    )
    hwm: Offsets = {}
    try:
        for topic in topics:
            meta = tmp.list_topics(topic, timeout=10)
            if meta.topics[topic].error:
                raise RuntimeError(f"Tópico ausente no broker: {topic}")
            for pid in meta.topics[topic].partitions:
                tp = TopicPartition(topic, pid)
                _, high = tmp.get_watermark_offsets(tp, timeout=5)
                hwm[(topic, pid)] = high
    finally:
        tmp.close()
    logger.info(
        "HWM do replay: {}",
        {f"{t}/{p}": o for (t, p), o in hwm.items()},
    )
    return hwm


def _offsets_for_timestamp(
    settings: Settings,
    topics: tuple[str, ...],
    ts: datetime,
) -> Offsets:
    """Resolve os offsets do primeiro registro >= `ts` em cada partição.

    Usa `offsets_for_times` da API Kafka. Partições sem registro após `ts`
    retornam OFFSET_BEGINNING (processa do início, o MERGE garante idempotência).
    """
    ts_ms = int(ts.timestamp() * 1000)
    tmp = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "__pulso-ts-probe__",
            "enable.auto.commit": False,
            **settings.kafka_security_config(),
        }
    )
    result: Offsets = {}
    try:
        for topic in topics:
            meta = tmp.list_topics(topic, timeout=10)
            tps = [
                TopicPartition(topic, pid, ts_ms)
                for pid in meta.topics[topic].partitions
            ]
            resolved = tmp.offsets_for_times(tps, timeout=10)
            for tp in resolved:
                offset = tp.offset if tp.offset >= 0 else OFFSET_BEGINNING
                result[(topic, tp.partition)] = offset
    finally:
        tmp.close()
    logger.info(
        "Offsets para {}: {}",
        ts.isoformat(),
        {f"{t}/{p}": o for (t, p), o in result.items()},
    )
    return result


class BoundedConsumer:
    """Consumer Kafka que para no HWM registrado no startup do replay.

    Usa assign explícito (não subscribe): sem rebalance, sem estado persistente
    no broker entre replays. O grupo descartável garante isolamento do sink live.
    """

    def __init__(
        self,
        settings: Settings,
        topics: tuple[str, ...],
        decode: DecodeFn,
        group_id: str,
        start_offsets: Offsets | None = None,
        from_timestamp: datetime | None = None,
        key_decode: KeyDecodeFn = decode_key_utf8,
    ) -> None:
        """
        Args:
            start_offsets: offsets iniciais por (tópico, partição).
                           None ou ausente de uma partição => OFFSET_BEGINNING.
            from_timestamp: se fornecido, sobrepõe `start_offsets` resolvendo
                            o primeiro offset >= o timestamp.
            key_decode: como extrair o symbol da chave Kafka crua (candles vêm
                        de TABLEs janeladas do ksqlDB — chave não é string simples).
        """
        self._settings = settings
        self._topics = topics
        self._decode = decode
        self._key_decode = key_decode

        # 1. HWM antes de criar o consumer (snapshot do log agora).
        self._hwm = _snapshot_hwm(settings, topics)
        self._current: Offsets = {}

        # 2. Resolver offsets de início.
        if from_timestamp is not None:
            if from_timestamp.tzinfo is None:
                from_timestamp = from_timestamp.replace(tzinfo=UTC)
            resolved = _offsets_for_timestamp(settings, topics, from_timestamp)
        else:
            resolved = start_offsets or {}

        # 3. Consumer principal.
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                **settings.kafka_security_config(),
            }
        )
        sr = SchemaRegistryClient(settings.schema_registry_config())
        self._avro = AvroDeserializer(sr)
        self._assign(resolved)

    def _assign(self, start_offsets: Offsets) -> None:
        assignment: list[TopicPartition] = []
        for topic in self._topics:
            meta = self._consumer.list_topics(topic, timeout=10)
            for pid in meta.topics[topic].partitions:
                offset = start_offsets.get((topic, pid), OFFSET_BEGINNING)
                assignment.append(TopicPartition(topic, pid, offset))
        self._consumer.assign(assignment)
        logger.info(
            "Replay assign | topics={} | partições={} | start={}",
            list(self._topics),
            len(assignment),
            {f"{t}/{p}": o for (t, p), o in start_offsets.items()} or "beginning",
        )

    def batches(
        self,
        batch_size: int = 500,
        timeout_s: float = 5.0,
    ) -> Iterator[tuple[list[dict], Offsets]]:
        """Itera batches até alcançar o HWM em todas as partições.

        Flushes o batch quando `batch_size` é atingido, quando o timeout
        expira, ou quando `is_caught_up` se torna True — para garantir que
        o último batch parcial seja entregue antes de parar.
        """
        if all(h <= 0 for h in self._hwm.values()):
            logger.info("Todos os tópicos estão vazios — replay trivial.")
            return

        records: list[dict] = []
        offsets: Offsets = {}
        deadline = time.monotonic() + timeout_s

        while not is_caught_up(self._current, self._hwm):
            msg = self._consumer.poll(0.5)
            now = time.monotonic()

            if msg is not None and msg.error() is None:
                key = self._key_decode(msg.key())
                value = self._avro(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
                records.append(self._decode(key, value))
                next_off = msg.offset() + 1
                offsets[(msg.topic(), msg.partition())] = next_off
                self._current[(msg.topic(), msg.partition())] = next_off

            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error("Erro Kafka durante replay: {}", msg.error())

            full = len(records) >= batch_size
            timed_out = now >= deadline
            caught = is_caught_up(self._current, self._hwm)

            if records and (full or timed_out or caught):
                yield records, offsets
                records, offsets = [], {}
                deadline = now + timeout_s
            elif timed_out:
                deadline = now + timeout_s

        # Flush residual após loop terminar.
        if records:
            yield records, offsets

    def close(self) -> None:
        self._consumer.close()
