"""Detector de anomalias de mercado (Marco 7 — Eixo B).

Consome `candles.m1` (janelas seladas via EMIT FINAL) e detecta tres tipos de
anomalia por rolling window estatistico:

- PRICE_SPIKE   : variacao % do close vs candle anterior > threshold.
- VOLUME_SPIKE  : z-score do volume > threshold.
- VOLATILITY_SPIKE : z-score do range (high - low) > threshold.

Publica eventos em `events.anomaly` (Avro, Schema Registry). O `llm_explainer.py`
consome esse topico e gera a explicacao em linguagem natural via Claude.

Design:
- `RollingWindow` e `AnomalyDetector` sao puros (sem I/O) — testados offline.
- `run()` faz a cola Kafka (consume + produce).
- Prometheus /metrics na porta `anomaly_detector_metrics_port` (default 8003).

Uso:
    uv run python services/anomaly_detector.py
    PULSO_KAFKA_BOOTSTRAP=... uv run python services/anomaly_detector.py
"""

from __future__ import annotations

import signal
import statistics
import uuid
from collections import deque
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)
from loguru import logger
from prometheus_client import Counter, Gauge, start_http_server
from pulso_infra import Settings, get_settings, setup_logging
from pulso_ingest.schemas import load_schema_str

# ---------------------------------------------------------------------------
# Metricas Prometheus
# ---------------------------------------------------------------------------

_candles_processed = Counter(
    "anomaly_detector_candles_processed_total",
    "Candles processados pelo detector",
    ["symbol"],
)
_anomalies_detected = Counter(
    "anomaly_detector_anomalies_detected_total",
    "Anomalias detectadas e publicadas",
    ["symbol", "anomaly_type"],
)
_window_size_gauge = Gauge(
    "anomaly_detector_window_size",
    "Tamanho atual do rolling window por simbolo",
    ["symbol"],
)

# ---------------------------------------------------------------------------
# Logica pura (testavel offline)
# ---------------------------------------------------------------------------


class RollingWindow:
    """Historico de candles por simbolo para calculo do baseline."""

    def __init__(self, maxlen: int) -> None:
        self.volumes: deque[float] = deque(maxlen=maxlen)
        self.ranges: deque[float] = deque(maxlen=maxlen)
        self.last_close: float | None = None

    def update(self, close: float, volume: float, high: float, low: float) -> None:
        self.volumes.append(volume)
        self.ranges.append(high - low)
        self.last_close = close

    def __len__(self) -> int:
        return len(self.volumes)


def _z_score(values: deque, current: float) -> float:
    """Z-score amostral. Retorna 0.0 se stdev == 0 (serie constante)."""
    mean = statistics.mean(values)
    try:
        stdev = statistics.stdev(values)
    except statistics.StatisticsError:
        return 0.0
    return (current - mean) / stdev if stdev > 0 else 0.0


def _mean(values: deque) -> float:
    return statistics.mean(values) if values else 0.0


class AnomalyDetector:
    """Detecta anomalias num stream de candles por rolling window estatistico."""

    def __init__(self, settings: Settings) -> None:
        self._win_size = settings.anomaly_window_size
        self._min_samples = settings.anomaly_min_samples
        self._vol_threshold = settings.anomaly_volume_zscore_threshold
        self._price_threshold = settings.anomaly_price_pct_threshold
        self._range_threshold = settings.anomaly_volatility_zscore_threshold
        self._windows: dict[str, RollingWindow] = {}

    def process(self, symbol: str, candle: dict[str, Any]) -> list[dict[str, Any]]:
        """Processa um candle e retorna lista de anomalias (vazia se nenhuma).

        O candle atual e comparado contra o historico *antes* de ser adicionado
        ao rolling window — o baseline sempre e historico puro.
        """
        win = self._windows.setdefault(symbol, RollingWindow(self._win_size))
        anomalies: list[dict[str, Any]] = []
        now_ms = int(datetime.now(UTC).timestamp() * 1000)

        close = candle["close"]
        volume = candle["volume"]
        high = candle["high"]
        low = candle["low"]
        window_start = candle.get("window_start", 0)
        interval = candle.get("interval", "M1")

        if len(win) >= self._min_samples:
            # --- PRICE_SPIKE ---
            if win.last_close is not None and win.last_close > 0:
                pct = abs(close - win.last_close) / win.last_close * 100
                if pct >= self._price_threshold:
                    anomalies.append(
                        _make_anomaly(
                            symbol=symbol,
                            detected_at=now_ms,
                            anomaly_type="PRICE_SPIKE",
                            severity=round(pct / 100, 6),
                            current_value=close,
                            baseline_value=win.last_close,
                            window_start=window_start,
                            interval=interval,
                        )
                    )

            # --- VOLUME_SPIKE ---
            if win.volumes:
                vol_z = _z_score(win.volumes, volume)
                if vol_z >= self._vol_threshold:
                    anomalies.append(
                        _make_anomaly(
                            symbol=symbol,
                            detected_at=now_ms,
                            anomaly_type="VOLUME_SPIKE",
                            severity=round(vol_z, 4),
                            current_value=volume,
                            baseline_value=_mean(win.volumes),
                            window_start=window_start,
                            interval=interval,
                        )
                    )

            # --- VOLATILITY_SPIKE ---
            if win.ranges:
                cur_range = high - low
                range_z = _z_score(win.ranges, cur_range)
                if range_z >= self._range_threshold:
                    anomalies.append(
                        _make_anomaly(
                            symbol=symbol,
                            detected_at=now_ms,
                            anomaly_type="VOLATILITY_SPIKE",
                            severity=round(range_z, 4),
                            current_value=cur_range,
                            baseline_value=_mean(win.ranges),
                            window_start=window_start,
                            interval=interval,
                        )
                    )

        # Atualiza o historico apos a deteccao.
        win.update(close, volume, high, low)
        return anomalies


def _make_anomaly(
    *,
    symbol: str,
    detected_at: int,
    anomaly_type: str,
    severity: float,
    current_value: float,
    baseline_value: float,
    window_start: int,
    interval: str,
) -> dict[str, Any]:
    return {
        "anomaly_id": str(uuid.uuid4()),
        "symbol": symbol,
        "detected_at": detected_at,
        "anomaly_type": anomaly_type,
        "severity": severity,
        "current_value": current_value,
        "baseline_value": baseline_value,
        "candle_window_start": window_start,
        "candle_interval": interval,
    }


# ---------------------------------------------------------------------------
# Servico (I/O: Kafka)
# ---------------------------------------------------------------------------


def run(settings: Settings | None = None) -> None:  # noqa: C901
    settings = settings or get_settings()
    setup_logging(settings.log_level, settings.log_json)
    start_http_server(settings.anomaly_detector_metrics_port)
    logger.info("Anomaly detector iniciado | metrics=:{}", settings.anomaly_detector_metrics_port)

    sr = SchemaRegistryClient(settings.schema_registry_config())
    candle_de = AvroDeserializer(sr)
    anomaly_ser = AvroSerializer(sr, load_schema_str("anomaly.avsc"))
    key_de = StringDeserializer("utf_8")
    key_ser = StringSerializer("utf_8")

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.anomaly_consumer_group,
            "enable.auto.commit": True,
            "auto.offset.reset": "latest",  # so anomalias novas; replay via kappa_replay
            **settings.kafka_security_config(),
        }
    )
    consumer.subscribe([settings.serve_candle_topic])  # candles.m1

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            **settings.kafka_security_config(),
        }
    )

    detector = AnomalyDetector(settings)
    stop = False

    def _handle_stop(sig, _frame):
        nonlocal stop
        logger.info("Sinal {} recebido — encerrando detector.", sig)
        stop = True

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    try:
        while not stop:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Erro Kafka: {}", msg.error())
                continue

            symbol = key_de(msg.key()) if msg.key() else "UNKNOWN"
            candle = candle_de(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if candle is None:
                continue

            anomalies = detector.process(symbol, candle)
            _candles_processed.labels(symbol).inc()
            _window_size_gauge.labels(symbol).set(
                len(detector._windows.get(symbol, RollingWindow(0)))  # noqa: SLF001
            )

            for anomaly in anomalies:
                topic = settings.topic_anomaly
                producer.produce(
                    topic=topic,
                    key=key_ser(symbol, SerializationContext(topic, MessageField.KEY)),
                    value=anomaly_ser(anomaly, SerializationContext(topic, MessageField.VALUE)),
                )
                producer.poll(0)
                _anomalies_detected.labels(symbol, anomaly["anomaly_type"]).inc()
                logger.info(
                    "Anomalia | symbol={} type={} severity={:.4f}",
                    symbol,
                    anomaly["anomaly_type"],
                    anomaly["severity"],
                )
    finally:
        producer.flush()
        consumer.close()
        logger.info("Anomaly detector encerrado.")


if __name__ == "__main__":
    run()
