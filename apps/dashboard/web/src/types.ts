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
  direction: string;
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

export type Interval = "M1" | "M5" | "H1";
