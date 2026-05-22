// Espelha os modelos pydantic de apps/dashboard/api/src/pulso_serve/models.py.
// Mantê-los em sincronia é o contrato da camada de serving.

export interface Symbol {
  symbol: string;
  base_asset: string;
  quote_asset: string;
  is_active: boolean;
}

export interface Candle {
  symbol: string;
  interval: string;
  window_start: string;
  window_end: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  vwap: number;
  trade_count: number;
  direction: "BULLISH" | "BEARISH" | "FLAT" | string;
  return_pct: number;
}

export interface LiveCandle {
  symbol: string;
  interval: string;
  window_start: string;
  window_end: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  vwap: number;
  trade_count: number;
  is_final: boolean;
}

export interface DailyStat {
  symbol: string;
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  vwap: number;
  trade_count: number;
  candle_count: number;
}

export interface AnomalyExplanation {
  anomaly_id: string;
  symbol: string;
  detected_at: string;
  anomaly_type: string;
  severity: number | null;
  current_value: number | null;
  baseline_value: number | null;
  candle_window_start: string | null;
  candle_interval: string | null;
  explanation: string | null;
  key_factors: string[];
  news_headlines: string[];
  model_used: string | null;
  explained_at: string | null;
}

export type Interval = "M1" | "M5" | "H1";

export const INTERVALS: Interval[] = ["M1", "M5", "H1"];
