"""Explicador LLM de anomalias de mercado (Marco 7 — Eixo B).

Consome `events.anomaly`, busca titulos de noticias recentes (best-effort via RSS),
e chama o Claude API para gerar uma explicacao em linguagem natural por evento.

Design:
- Uma chamada LLM por evento de anomalia (nunca por trade — Eixo B isolado).
- Prompt caching no system prompt (invariante entre chamadas) — economiza tokens.
- `build_prompt` / `parse_llm_response` sao funcoes puras — testadas offline.
- Escrita no `anomaly_store` (DuckDB) compartilhado com a API FastAPI.
- Prometheus /metrics na porta `llm_explainer_metrics_port` (default 8004).

Requer:
    PULSO_ANTHROPIC_API_KEY=sk-ant-...

Uso:
    uv run python services/llm_explainer.py
"""

from __future__ import annotations

import json
import re
import signal
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from typing import Any

import anthropic
import httpx
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer
from loguru import logger
from prometheus_client import Counter, Histogram, start_http_server
from pulso_infra import Settings, get_settings, setup_logging
from pulso_serve.anomaly_store import AnomalyStore, build_anomaly_store, pending_record

# ---------------------------------------------------------------------------
# Metricas Prometheus
# ---------------------------------------------------------------------------

_explanations_total = Counter(
    "llm_explainer_explanations_total",
    "Explicacoes LLM geradas",
    ["symbol", "anomaly_type"],
)
_llm_errors_total = Counter(
    "llm_explainer_errors_total",
    "Erros na chamada ao Claude API",
    ["kind"],
)
_llm_call_seconds = Histogram(
    "llm_explainer_call_seconds",
    "Duracao da chamada ao Claude API",
    buckets=[0.5, 1, 2, 5, 10, 20, 30],
)
_news_fetch_errors = Counter(
    "llm_explainer_news_fetch_errors_total",
    "Falhas ao buscar noticias (best-effort)",
)

# ---------------------------------------------------------------------------
# System prompt — cacheado no Claude API (ephemeral cache)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """Voce e um analista quantitativo de mercado de criptomoedas.
Sua tarefa e explicar anomalias estatisticas detectadas automaticamente no stream
de candles em tempo real.

Regras:
1. Seja objetivo e direto — maximo 3 paragrafos curtos.
2. Explique a anomalia em termos de mercado (nao de algoritmo).
3. Liste ate 3 fatores possiveis (key_factors) como array JSON.
4. Se houver noticias relevantes no contexto, mencione-as.
5. Reconheca incerteza quando nao houver causa clara.
6. Responda sempre em portugues.

Formato de resposta (JSON puro, sem markdown):
{
  "explanation": "<texto explicativo>",
  "key_factors": ["<fator 1>", "<fator 2>", "<fator 3>"]
}"""

# ---------------------------------------------------------------------------
# Funcoes puras (testaveis offline)
# ---------------------------------------------------------------------------


def _fmt_ts(ms: int | None) -> str:
    if ms is None:
        return "N/A"
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def build_prompt(anomaly: dict[str, Any], news: list[str]) -> str:
    """Constroi o prompt do usuario para o Claude."""
    ts = _fmt_ts(anomaly.get("detected_at"))
    atype = anomaly.get("anomaly_type", "?")
    symbol = anomaly.get("symbol", "?")
    severity = anomaly.get("severity", 0)
    current = anomaly.get("current_value", 0)
    baseline = anomaly.get("baseline_value", 0)
    interval = anomaly.get("candle_interval", "M1")

    # Traduz o tipo para linguagem de negocio.
    type_labels = {
        "PRICE_SPIKE": (
            f"variacao de preco de {severity * 100:.2f}%"
            f" (close={current:.4f}, anterior={baseline:.4f})"
        ),
        "VOLUME_SPIKE": (
            f"pico de volume z-score={severity:.2f}"
            f" (volume={current:.2f}, media={baseline:.2f})"
        ),
        "VOLATILITY_SPIKE": (
            f"pico de volatilidade z-score={severity:.2f}"
            f" (range={current:.4f}, media={baseline:.4f})"
        ),
    }
    descricao = type_labels.get(atype, f"anomalia do tipo {atype}, severity={severity:.4f}")

    linhas = [
        f"Anomalia detectada em {symbol} ({interval}) as {ts}:",
        f"Tipo: {atype} — {descricao}",
    ]

    if news:
        linhas.append("\nNoticias recentes relacionadas:")
        for i, headline in enumerate(news, 1):
            linhas.append(f"  {i}. {headline}")

    linhas.append("\nExplique esta anomalia conforme as instrucoes.")
    return "\n".join(linhas)


def parse_llm_response(text: str) -> dict[str, Any]:
    """Extrai explanation e key_factors do JSON da resposta do Claude.

    Tolera JSON embrulhado em markdown code fences.
    Retorna defaults seguros se o parse falhar.
    """
    # Remove marcadores de code fence se presentes.
    cleaned = re.sub(r"```(?:json)?", "", text).strip()
    try:
        data = json.loads(cleaned)
        return {
            "explanation": str(data.get("explanation", "")),
            "key_factors": [str(f) for f in data.get("key_factors", [])],
        }
    except (json.JSONDecodeError, TypeError):
        # Fallback: trata a resposta inteira como explicacao.
        return {"explanation": text.strip(), "key_factors": []}


# ---------------------------------------------------------------------------
# Busca de noticias (best-effort)
# ---------------------------------------------------------------------------


def fetch_news_headlines(
    symbol: str,
    rss_url: str,
    max_headlines: int,
    timeout: float,
) -> list[str]:
    """Busca titulos recentes do feed RSS. Retorna lista vazia em qualquer falha."""
    base = symbol.split("-")[0].upper()  # BTC-USD -> BTC
    try:
        resp = httpx.get(f"{rss_url}?currencies={base}", timeout=timeout, follow_redirects=True)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        titles = []
        for item in root.iter("item"):
            title_el = item.find("title")
            if title_el is not None and title_el.text:
                titles.append(title_el.text.strip())
                if len(titles) >= max_headlines:
                    break
        return titles
    except Exception as exc:  # noqa: BLE001 — best-effort, sem propagar
        logger.debug("Falha ao buscar noticias para {}: {}", symbol, exc)
        _news_fetch_errors.inc()
        return []


# ---------------------------------------------------------------------------
# Chamada Claude API (com prompt caching)
# ---------------------------------------------------------------------------


def call_claude(
    client: anthropic.Anthropic,
    model: str,
    prompt: str,
) -> tuple[str, int, int]:
    """Chama o Claude e retorna (texto, prompt_tokens, completion_tokens).

    O system prompt e cacheado via `cache_control: ephemeral` — reutilizado entre
    chamadas consecutivas (TTL ~5 min). Reduz custo em streams com anomalias frequentes.
    """
    with _llm_call_seconds.time():
        response = client.messages.create(
            model=model,
            max_tokens=512,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": prompt}],
        )

    text = response.content[0].text if response.content else ""
    usage = response.usage
    prompt_tokens = getattr(usage, "input_tokens", 0)
    completion_tokens = getattr(usage, "output_tokens", 0)
    return text, prompt_tokens, completion_tokens


# ---------------------------------------------------------------------------
# Servico (I/O: Kafka + Claude API + DuckDB)
# ---------------------------------------------------------------------------


def run(settings: Settings | None = None) -> None:  # noqa: C901
    settings = settings or get_settings()
    setup_logging(settings.log_level, settings.log_json)

    if not settings.anthropic_api_key:
        raise SystemExit(
            "PULSO_ANTHROPIC_API_KEY nao configurado. "
            "O llm_explainer requer uma chave valida."
        )

    start_http_server(settings.llm_explainer_metrics_port)
    logger.info(
        "LLM explainer iniciado | model={} | metrics=:{}",
        settings.anthropic_model,
        settings.llm_explainer_metrics_port,
    )

    store: AnomalyStore = build_anomaly_store(settings.anomaly_duckdb_path)
    llm_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    sr = SchemaRegistryClient(settings.schema_registry_config())
    anomaly_de = AvroDeserializer(sr)
    key_de = StringDeserializer("utf_8")

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.llm_consumer_group,
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
            **settings.kafka_security_config(),
        }
    )
    consumer.subscribe([settings.topic_anomaly])

    stop = False

    def _handle_stop(sig, _frame):
        nonlocal stop
        logger.info("Sinal {} recebido — encerrando explainer.", sig)
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

            anomaly = anomaly_de(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )
            if anomaly is None:
                continue

            symbol = key_de(msg.key()) if msg.key() else anomaly.get("symbol", "")
            anomaly_id = anomaly.get("anomaly_id", "?")
            atype = anomaly.get("anomaly_type", "?")

            # Salva registro pendente (sem explicacao) imediatamente.
            store.save(pending_record(anomaly))

            # Busca noticias (best-effort, nao bloqueia o fluxo).
            news = fetch_news_headlines(
                symbol,
                settings.news_rss_url,
                settings.news_max_headlines,
                settings.news_fetch_timeout,
            )

            # Chama o Claude.
            try:
                prompt = build_prompt(anomaly, news)
                raw_text, pt, ct = call_claude(llm_client, settings.anthropic_model, prompt)
                parsed = parse_llm_response(raw_text)
                store.save(
                    {
                        **pending_record(anomaly),
                        "explanation": parsed["explanation"],
                        "key_factors": parsed["key_factors"],
                        "news_headlines": news,
                        "model_used": settings.anthropic_model,
                        "explained_at": datetime.now(UTC),
                        "prompt_tokens": pt,
                        "completion_tokens": ct,
                    }
                )
                _explanations_total.labels(symbol, atype).inc()
                logger.info(
                    "Explicacao gerada | id={} symbol={} type={} tokens={}/{}",
                    anomaly_id, symbol, atype, pt, ct,
                )
            except anthropic.APIError as exc:
                _llm_errors_total.labels("api_error").inc()
                logger.error("Erro Claude API para anomalia {}: {}", anomaly_id, exc)
            except Exception as exc:  # noqa: BLE001
                _llm_errors_total.labels("unexpected").inc()
                logger.error("Erro inesperado ao explicar {}: {}", anomaly_id, exc)

    finally:
        consumer.close()
        logger.info("LLM explainer encerrado.")


if __name__ == "__main__":
    run()
