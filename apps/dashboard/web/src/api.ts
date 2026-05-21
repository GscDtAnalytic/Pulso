import type { Candle, DailyStat, Interval, LiveCandle, Symbol } from "./types";

const BASE = "/api";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export const api = {
  symbols: (): Promise<Symbol[]> => get("/symbols"),

  candles: (symbol: string, interval: Interval, limit = 200): Promise<Candle[]> =>
    get(`/candles?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${limit}`),

  liveCandle: (symbol: string, interval: Interval): Promise<LiveCandle | null> =>
    fetch(`${BASE}/candles/live?symbol=${encodeURIComponent(symbol)}&interval=${interval}`).then(
      (res) => {
        if (res.status === 204) return null;
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.json() as Promise<LiveCandle>;
      },
    ),

  daily: (symbol: string, limit = 90): Promise<DailyStat[]> =>
    get(`/symbols/${encodeURIComponent(symbol)}/daily?limit=${limit}`),
};
