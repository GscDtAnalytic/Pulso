"""Producer idempotente Kafka + Avro (Schema Registry).

Garante exactly-once *producer->broker* (ver wiki: conceitos/exactly-once):

    enable.idempotence=true  -> broker descarta reenvios duplicados (PID + seq).
    acks=all                 -> espera todas as ISR; nao perde em failover.

A chave de particionamento e o **simbolo canonico**: ordering por ativo dentro
da particao (decisao de design da wiki: tecnologias/kafka). Valor serializado em
Avro via Schema Registry — o payload carrega so o schema ID, nunca JSON livre.

A entrega e assincrona: `produce()` enfileira e o delivery callback atualiza as
metricas de sucesso/erro. Sempre chame `flush()` no encerramento.
"""

from __future__ import annotations

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from loguru import logger
from pulso_infra import Settings

from pulso_ingest import metrics
from pulso_ingest.models import OrderBookDeltaEvent, TradeEvent
from pulso_ingest.schemas import load_schema_str


class MarketDataProducer:
    """Produz `TradeEvent`/`OrderBookDeltaEvent` para seus topicos, idempotente."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "client.id": settings.producer_client_id,
                # Exactly-once producer->broker. Implica retries, acks=all e
                # max.in.flight<=5 automaticamente, mas tornamos explicito o intent.
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 5,
                **settings.kafka_security_config(),
            }
        )
        sr = SchemaRegistryClient(settings.schema_registry_config())
        self._key_ser = StringSerializer("utf_8")
        self._trade_ser = AvroSerializer(sr, load_schema_str("trade.avsc"))
        self._ob_ser = AvroSerializer(sr, load_schema_str("orderbook_delta.avsc"))

    def produce_trade(self, event: TradeEvent) -> None:
        topic = self._settings.topic_trades
        self._producer.produce(
            topic=topic,
            key=self._key_ser(event.symbol, SerializationContext(topic, MessageField.KEY)),
            value=self._trade_ser(
                event.to_avro(), SerializationContext(topic, MessageField.VALUE)
            ),
            on_delivery=_make_delivery_cb(topic),
        )
        metrics.trades_produced.labels(event.exchange, event.symbol).inc()
        metrics.event_skew_ms.labels(event.exchange).observe(event.skew_ms)
        self._producer.poll(0)

    def produce_orderbook_delta(self, event: OrderBookDeltaEvent) -> None:
        topic = self._settings.topic_orderbook
        self._producer.produce(
            topic=topic,
            key=self._key_ser(event.symbol, SerializationContext(topic, MessageField.KEY)),
            value=self._ob_ser(event.to_avro(), SerializationContext(topic, MessageField.VALUE)),
            on_delivery=_make_delivery_cb(topic),
        )
        metrics.orderbook_deltas_produced.labels(event.exchange, event.symbol).inc()
        metrics.event_skew_ms.labels(event.exchange).observe(event.skew_ms)
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Bloqueia ate entregar o que esta na fila. Retorna msgs ainda pendentes."""
        return self._producer.flush(timeout)


def _make_delivery_cb(topic: str):
    """Delivery report: fail-loud em erro (log + metrica), silencioso no sucesso."""

    def _cb(err, msg) -> None:  # noqa: ANN001 (assinatura ditada pela lib)
        if err is not None:
            metrics.produce_errors.labels(topic).inc()
            logger.error("Falha de entrega em {}: {}", topic, err)

    return _cb
