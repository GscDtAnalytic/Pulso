"""Modelos de resposta da API (Marco 4).

Pydantic = o contrato da API, como o Avro e o contrato do barramento. Os nomes
batem com as colunas das marts dbt (`fct_candle`, `fct_symbol_daily`, `dim_symbol`)
e com o que o dashboard React consome.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class Symbol(BaseModel):
    """Um simbolo negociado — espelha `dim_symbol` / o seed do pulso-domain."""

    symbol: str
    base_asset: str
    quote_asset: str
    is_active: bool


class Candle(BaseModel):
    """Candle OHLCV selado — uma linha de `fct_candle`."""

    symbol: str
    interval: str
    window_start: datetime
    window_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trade_count: int
    direction: str
    return_pct: float


class DailyStat(BaseModel):
    """Resumo diario por simbolo — uma linha de `fct_symbol_daily`."""

    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trade_count: int
    candle_count: int


class AnomalyExplanation(BaseModel):
    """Anomalia de mercado com explicacao LLM — uma linha do `anomaly_explanations`."""

    anomaly_id: str
    symbol: str
    detected_at: datetime
    anomaly_type: str
    severity: float | None = None
    current_value: float | None = None
    baseline_value: float | None = None
    candle_window_start: datetime | None = None
    candle_interval: str | None = None
    explanation: str | None = None
    key_factors: list[str] = []
    news_headlines: list[str] = []
    model_used: str | None = None
    explained_at: datetime | None = None


class LiveCandle(BaseModel):
    """Estado parcial da janela ABERTA — vem de pull query no ksqlDB, nao do lake.

    Difere de `Candle`: `is_final = false` (a janela ainda recebe trades) e nao ha
    garantia de selamento. Ver ksqldb/README.md (pull queries).
    """

    symbol: str
    interval: str
    window_start: datetime
    window_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trade_count: int
    is_final: bool = False
