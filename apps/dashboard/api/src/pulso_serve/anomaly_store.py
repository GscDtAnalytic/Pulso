"""Armazenamento de anomalias e explicacoes LLM (Marco 7).

DuckDB local — arquivo separado do lake dbt para nao disputar o lock de escrita
com o `dbt build`. Mesmo padrao de conexao por operacao do `store.py` das marts.

O `llm_explainer.py` escreve; a API le em read_only. DuckDB suporta multiplos
leitores read_only concorrentes com um unico escritor, desde que as conexoes
nao fiquem abertas de forma persistente — dai a abertura por operacao.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import duckdb

_DDL = """
CREATE TABLE IF NOT EXISTS anomaly_explanations (
    anomaly_id        TEXT PRIMARY KEY,
    symbol            TEXT NOT NULL,
    detected_at       TIMESTAMPTZ NOT NULL,
    anomaly_type      TEXT NOT NULL,
    severity          DOUBLE,
    current_value     DOUBLE,
    baseline_value    DOUBLE,
    candle_window_start TIMESTAMPTZ,
    candle_interval   TEXT,
    explanation       TEXT,
    key_factors       TEXT,   -- JSON array de strings
    news_headlines    TEXT,   -- JSON array de strings
    model_used        TEXT,
    explained_at      TIMESTAMPTZ,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER
)
"""

_INSERT = """
INSERT INTO anomaly_explanations (
    anomaly_id, symbol, detected_at, anomaly_type, severity,
    current_value, baseline_value, candle_window_start, candle_interval,
    explanation, key_factors, news_headlines, model_used, explained_at,
    prompt_tokens, completion_tokens
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (anomaly_id) DO UPDATE SET
    explanation       = excluded.explanation,
    key_factors       = excluded.key_factors,
    news_headlines    = excluded.news_headlines,
    model_used        = excluded.model_used,
    explained_at      = excluded.explained_at,
    prompt_tokens     = excluded.prompt_tokens,
    completion_tokens = excluded.completion_tokens
"""

MAX_LIMIT = 500


class AnomalyStore:
    """Leitura e escrita da tabela `anomaly_explanations` em DuckDB."""

    def __init__(self, path: str, *, read_only: bool = False) -> None:
        self._path = path
        self._read_only = read_only
        if read_only:
            # Leitor (pulso-serve): o arquivo DuckDB é montado read-only (volume GCS).
            # A tabela já foi criada pelo escritor (llm_explainer); abrir em modo
            # escrita para rodar o DDL falharia no filesystem read-only.
            return
        # Escritor (llm_explainer): cria a tabela na primeira vez; idempotente.
        with duckdb.connect(path) as con:
            con.execute(_DDL)

    def save(self, record: dict[str, Any]) -> None:
        """Insere ou atualiza uma anomalia (upsert por anomaly_id)."""
        with duckdb.connect(self._path) as con:
            con.execute(
                _INSERT,
                [
                    record["anomaly_id"],
                    record["symbol"],
                    record["detected_at"],
                    record["anomaly_type"],
                    record.get("severity"),
                    record.get("current_value"),
                    record.get("baseline_value"),
                    record.get("candle_window_start"),
                    record.get("candle_interval"),
                    record.get("explanation"),
                    json.dumps(record.get("key_factors") or []),
                    json.dumps(record.get("news_headlines") or []),
                    record.get("model_used"),
                    record.get("explained_at"),
                    record.get("prompt_tokens"),
                    record.get("completion_tokens"),
                ],
            )

    def recent(self, symbol: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        """Anomalias recentes, ordenadas por detected_at desc. Filtra por symbol se fornecido."""
        limit = max(1, min(int(limit), MAX_LIMIT))
        where = "WHERE symbol = ?" if symbol else ""
        params: list[Any] = [symbol] if symbol else []
        with duckdb.connect(self._path, read_only=True) as con:
            cur = con.execute(
                f"""
                SELECT anomaly_id, symbol, detected_at, anomaly_type, severity,
                       current_value, baseline_value, candle_window_start, candle_interval,
                       explanation, key_factors, news_headlines, model_used, explained_at,
                       prompt_tokens, completion_tokens
                FROM anomaly_explanations
                {where}
                ORDER BY detected_at DESC
                LIMIT {limit}
                """,
                params,
            )
            cols = [d[0] for d in cur.description]
            rows = []
            for row in cur.fetchall():
                r = dict(zip(cols, row, strict=True))
                r["key_factors"] = json.loads(r["key_factors"] or "[]")
                r["news_headlines"] = json.loads(r["news_headlines"] or "[]")
                rows.append(r)
        return rows


def build_anomaly_store(path: str, *, read_only: bool = False) -> AnomalyStore:
    return AnomalyStore(path, read_only=read_only)


# Sentinel de "ainda nao explicado" — gravado quando o detector publica a anomalia
# mas o explainer ainda nao rodou.
def pending_record(anomaly: dict[str, Any]) -> dict[str, Any]:
    """Registro inicial sem explicacao — inserido pelo detector ao detectar."""
    return {
        "anomaly_id": anomaly["anomaly_id"],
        "symbol": anomaly["symbol"],
        "detected_at": _millis_to_dt(anomaly["detected_at"]),
        "anomaly_type": anomaly["anomaly_type"],
        "severity": anomaly["severity"],
        "current_value": anomaly["current_value"],
        "baseline_value": anomaly["baseline_value"],
        "candle_window_start": _millis_to_dt(anomaly["candle_window_start"]),
        "candle_interval": anomaly.get("candle_interval", "M1"),
        "explanation": None,
        "key_factors": [],
        "news_headlines": [],
        "model_used": None,
        "explained_at": None,
        "prompt_tokens": None,
        "completion_tokens": None,
    }


def _millis_to_dt(ms: int | datetime | None) -> datetime | None:
    if ms is None:
        return None
    if isinstance(ms, datetime):
        return ms if ms.tzinfo else ms.replace(tzinfo=UTC)
    return datetime.fromtimestamp(ms / 1000, tz=UTC)
