"""Testes offline do Marco 7 — Eixo B LLM.

Cobrem quatro propriedades:
1. Rolling window e calculo de z-score (puro, sem I/O).
2. Deteccao de anomalias (PRICE_SPIKE, VOLUME_SPIKE, VOLATILITY_SPIKE).
3. AnomalyStore (DuckDB via tmp_path — sem Kafka, sem Claude).
4. Funcoes puras do llm_explainer (build_prompt, parse_llm_response).
5. Schema Avro anomaly.avsc e valido (offline).
6. API /api/anomalies retorna 200 com store mockado.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# Garante que services/ esta no path (scripts standalone).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services"))

from anomaly_detector import AnomalyDetector, RollingWindow, _z_score
from llm_explainer import build_prompt, fetch_news_headlines, parse_llm_response
from pulso_infra import get_settings
from pulso_serve.anomaly_store import build_anomaly_store, pending_record

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings():
    return get_settings()


@pytest.fixture()
def detector(settings):
    return AnomalyDetector(settings)


@pytest.fixture()
def store(tmp_path):
    return build_anomaly_store(str(tmp_path / "test_anomalies.duckdb"))


@pytest.fixture()
def sample_anomaly():
    return {
        "anomaly_id": "test-uuid-1234",
        "symbol": "BTC-USD",
        "detected_at": int(datetime.now(UTC).timestamp() * 1000),
        "anomaly_type": "VOLUME_SPIKE",
        "severity": 3.5,
        "current_value": 1500.0,
        "baseline_value": 300.0,
        "candle_window_start": int(datetime.now(UTC).timestamp() * 1000),
        "candle_interval": "M1",
    }


# ---------------------------------------------------------------------------
# 1. Rolling window e z-score
# ---------------------------------------------------------------------------


def test_rolling_window_len():
    win = RollingWindow(maxlen=5)
    assert len(win) == 0
    win.update(50000.0, 100.0, 50100.0, 49900.0)
    assert len(win) == 1


def test_rolling_window_respects_maxlen():
    win = RollingWindow(maxlen=3)
    for i in range(10):
        win.update(float(i), float(i), float(i), float(i))
    assert len(win) == 3


def test_z_score_detects_outlier():
    from collections import deque

    # Serie com variacao para que stdev > 0.
    values = deque([10.0, 10.5, 9.5, 10.2, 9.8, 10.1, 9.9, 10.3, 9.7, 10.4])
    z = _z_score(values, 100.0)
    assert z > 3.0


def test_z_score_normal_value():
    from collections import deque

    values = deque([10.0, 11.0, 9.0, 10.5, 9.5, 10.2, 9.8, 10.1, 10.3, 9.9])
    z = _z_score(values, 10.0)
    assert abs(z) < 1.0


def test_z_score_constant_returns_zero():
    from collections import deque

    values = deque([5.0] * 10)
    z = _z_score(values, 5.0)
    assert z == 0.0


# ---------------------------------------------------------------------------
# 2. Deteccao de anomalias
# ---------------------------------------------------------------------------


def _make_candle(
    close: float,
    volume: float = 100.0,
    high: float | None = None,
    low: float | None = None,
    interval: str = "M1",
) -> dict[str, Any]:
    return {
        "close": close,
        "open": close * 0.999,
        "high": high if high is not None else close * 1.001,
        "low": low if low is not None else close * 0.999,
        "volume": volume,
        "vwap": close,
        "trade_count": 10,
        "window_start": int(datetime.now(UTC).timestamp() * 1000),
        "interval": interval,
    }


def test_no_anomaly_during_warmup(detector):
    """Nenhuma anomalia antes de atingir min_samples."""
    symbol = "BTC-USD"
    for i in range(detector._min_samples - 1):
        result = detector.process(symbol, _make_candle(50000.0 + i))
        assert result == [], f"Anomalia prematura no candle {i}"


def test_volume_spike_detected(detector):
    """VOLUME_SPIKE apos warm-up com volume muito acima da media."""
    symbol = "ETH-USD"
    # Warm-up com volume variando levemente para stdev > 0.
    import random

    rng = random.Random(42)
    for _ in range(detector._min_samples + 5):
        detector.process(symbol, _make_candle(3000.0, volume=100.0 + rng.uniform(-5, 5)))
    # Agora spike de volume (muito acima da media baseline).
    result = detector.process(symbol, _make_candle(3000.0, volume=100_000.0))
    types = [a["anomaly_type"] for a in result]
    assert "VOLUME_SPIKE" in types


def test_price_spike_detected(detector):
    """PRICE_SPIKE apos warm-up com variacao percentual acima do threshold."""
    symbol = "SOL-USD"
    price = 100.0
    for _ in range(detector._min_samples + 5):
        detector.process(symbol, _make_candle(price))
    # Spike de preco > 2%.
    result = detector.process(symbol, _make_candle(price * 1.10))
    types = [a["anomaly_type"] for a in result]
    assert "PRICE_SPIKE" in types


def test_volatility_spike_detected(detector):
    """VOLATILITY_SPIKE apos warm-up com range muito acima da media."""
    symbol = "XRP-USD"
    import random

    rng = random.Random(7)
    for _ in range(detector._min_samples + 5):
        # Range pequeno com variacao para stdev > 0.
        d = rng.uniform(0.0005, 0.002)
        detector.process(symbol, _make_candle(1.0, high=1.0 + d, low=1.0 - d))
    # Range gigante (100x maior que a media).
    result = detector.process(symbol, _make_candle(1.0, high=2.0, low=0.1))
    types = [a["anomaly_type"] for a in result]
    assert "VOLATILITY_SPIKE" in types


def test_no_false_positives_on_steady_stream(detector):
    """Stream constante nao deve gerar anomalias apos o warm-up."""
    symbol = "DOGE-USD"
    results = []
    for _ in range(50):
        candle = _make_candle(0.30, volume=500.0, high=0.301, low=0.299)
        results.extend(detector.process(symbol, candle))
    assert results == [], f"Falsos positivos: {results}"


# ---------------------------------------------------------------------------
# 3. AnomalyStore (DuckDB)
# ---------------------------------------------------------------------------


def test_store_save_and_recent(store, sample_anomaly):
    record = pending_record(sample_anomaly)
    store.save(record)
    rows = store.recent(limit=10)
    assert len(rows) == 1
    assert rows[0]["anomaly_id"] == sample_anomaly["anomaly_id"]
    assert rows[0]["symbol"] == "BTC-USD"
    assert rows[0]["anomaly_type"] == "VOLUME_SPIKE"


def test_store_upsert_updates_explanation(store, sample_anomaly):
    record = pending_record(sample_anomaly)
    store.save(record)
    # Atualiza com a explicacao.
    updated = {
        **record,
        "explanation": "Pico de volume causado por noticia de mercado.",
        "key_factors": ["Noticia X", "Liquidez Y"],
    }
    store.save(updated)
    rows = store.recent()
    assert rows[0]["explanation"] == "Pico de volume causado por noticia de mercado."
    assert "Noticia X" in rows[0]["key_factors"]


def test_store_filter_by_symbol(store, sample_anomaly):
    """recent(symbol=X) so retorna anomalias daquele simbolo."""
    btc = pending_record(sample_anomaly)
    store.save(btc)
    eth_anomaly = {**sample_anomaly, "anomaly_id": "eth-uuid", "symbol": "ETH-USD"}
    store.save(pending_record(eth_anomaly))

    btc_rows = store.recent(symbol="BTC-USD")
    eth_rows = store.recent(symbol="ETH-USD")
    assert all(r["symbol"] == "BTC-USD" for r in btc_rows)
    assert all(r["symbol"] == "ETH-USD" for r in eth_rows)


def test_store_recent_empty(store):
    assert store.recent() == []


# ---------------------------------------------------------------------------
# 4. Funcoes puras do llm_explainer
# ---------------------------------------------------------------------------


def test_build_prompt_contains_symbol_and_type(sample_anomaly):
    prompt = build_prompt(sample_anomaly, news=[])
    assert "BTC-USD" in prompt
    assert "VOLUME_SPIKE" in prompt


def test_build_prompt_includes_news(sample_anomaly):
    news = ["Bitcoin sobe 5% apos aprovacao de ETF", "Whale move 10k BTC"]
    prompt = build_prompt(sample_anomaly, news=news)
    assert "Bitcoin sobe 5%" in prompt
    assert "Whale move 10k BTC" in prompt


def test_parse_llm_response_valid_json():
    raw = json.dumps({
        "explanation": "Pico causado por liquidacao em cascata.",
        "key_factors": ["alavancagem", "stop-loss"],
    })
    result = parse_llm_response(raw)
    assert result["explanation"] == "Pico causado por liquidacao em cascata."
    assert "alavancagem" in result["key_factors"]


def test_parse_llm_response_with_markdown_fence():
    inner = json.dumps({"explanation": "Anomalia de volume.", "key_factors": ["mercado illiquido"]})
    raw = f"```json\n{inner}\n```"
    result = parse_llm_response(raw)
    assert result["explanation"] == "Anomalia de volume."


def test_parse_llm_response_fallback_on_invalid_json():
    raw = "Nao foi possivel identificar uma causa clara para esta anomalia."
    result = parse_llm_response(raw)
    assert "causa clara" in result["explanation"]
    assert result["key_factors"] == []


def test_fetch_news_returns_empty_on_failure():
    """Falha de rede nao propaga — retorna lista vazia."""
    headlines = fetch_news_headlines(
        "BTC-USD", "http://localhost:9999/nonexistent", max_headlines=3, timeout=0.5
    )
    assert headlines == []


# ---------------------------------------------------------------------------
# 5. Schema Avro anomaly.avsc valido offline
# ---------------------------------------------------------------------------


def test_anomaly_avsc_is_valid():
    """O schema deve ser JSON valido com os campos obrigatorios."""
    contracts_dir = Path(__file__).resolve().parent.parent / "contracts"
    avsc_path = contracts_dir / "anomaly.avsc"
    assert avsc_path.exists(), "contracts/anomaly.avsc nao encontrado"

    schema = json.loads(avsc_path.read_text())
    assert schema["type"] == "record"
    assert schema["name"] == "AnomalyEvent"
    field_names = {f["name"] for f in schema["fields"]}
    required = {"anomaly_id", "symbol", "detected_at", "anomaly_type", "severity"}
    assert required <= field_names, f"Campos ausentes: {required - field_names}"


def test_anomaly_avsc_backward_compat():
    """Campos sem default indicam breaking change — todos os novos devem ter default."""
    contracts_dir = Path(__file__).resolve().parent.parent / "contracts"
    schema = json.loads((contracts_dir / "anomaly.avsc").read_text())
    # anomaly_id, symbol, detected_at, anomaly_type, severity, current_value, baseline_value,
    # candle_window_start sao campos originais (sem default obrigatorio no schema v1).
    # candle_interval tem default="M1" — verificamos isso.
    interval_field = next(f for f in schema["fields"] if f["name"] == "candle_interval")
    assert "default" in interval_field, "candle_interval deve ter default para BACKWARD compat"


# ---------------------------------------------------------------------------
# 6. API /api/anomalies com store mockado
# ---------------------------------------------------------------------------


def test_api_anomalies_returns_200(tmp_path):
    """GET /api/anomalies retorna 200 com lista (pode ser vazia) quando store existe."""
    from fastapi.testclient import TestClient
    from pulso_serve.anomaly_store import build_anomaly_store
    from pulso_serve.app import create_app
    from pulso_serve.store import MarketStore

    store_path = str(tmp_path / "anomalies_api_test.duckdb")
    anomaly_store = build_anomaly_store(store_path)

    # Mocks minimos para create_app.
    mock_store = MagicMock(spec=MarketStore)
    mock_ksql = MagicMock()
    mock_stream = MagicMock()

    # Patch do broadcaster para nao tentar conexao Kafka.
    with patch("pulso_serve.live.CandleBroadcaster") as _mock_broadcaster:
        _mock_broadcaster.return_value.run = MagicMock(return_value=None)
        app = create_app(
            settings=get_settings(),
            store=mock_store,
            ksql=mock_ksql,
            stream=mock_stream,
            anomaly_store=anomaly_store,
        )

    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/api/anomalies")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_api_anomalies_503_without_store():
    """GET /api/anomalies retorna 503 quando o store nao esta configurado."""
    from fastapi.testclient import TestClient
    from pulso_serve.app import create_app
    from pulso_serve.store import MarketStore

    mock_store = MagicMock(spec=MarketStore)
    mock_ksql = MagicMock()
    mock_stream = MagicMock()

    with patch("pulso_serve.live.CandleBroadcaster") as _mock_broadcaster:
        _mock_broadcaster.return_value.run = MagicMock(return_value=None)
        app = create_app(
            settings=get_settings(),
            store=mock_store,
            ksql=mock_ksql,
            stream=mock_stream,
            anomaly_store=None,  # store ausente
        )

    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/api/anomalies")
    assert resp.status_code == 503
