import type {
  AnomalyExplanation,
  Candle,
  DailyStat,
  Interval,
  LiveCandle,
  Symbol,
} from "./types";

// Base da API. Em prod fica "" (mesma origem do FastAPI que serve a UI), então
// as chamadas viram paths relativos /api/*. Em dev, NEXT_PUBLIC_API_URL aponta
// para o Cloud Run de prod (CORS liberado).
export const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${path}`);
  return res.json() as Promise<T>;
}

export const api = {
  symbols: (): Promise<Symbol[]> => get("/api/symbols"),

  candles: (symbol: string, interval: Interval, limit = 200): Promise<Candle[]> =>
    get(`/api/candles?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${limit}`),

  liveCandle: async (symbol: string, interval: Interval): Promise<LiveCandle | null> => {
    const res = await fetch(
      `${API_BASE}/api/candles/live?symbol=${encodeURIComponent(symbol)}&interval=${interval}`,
      { cache: "no-store" },
    );
    if (res.status === 204) return null;
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return res.json() as Promise<LiveCandle>;
  },

  daily: (symbol: string, limit = 30): Promise<DailyStat[]> =>
    get(`/api/symbols/${encodeURIComponent(symbol)}/daily?limit=${limit}`),

  anomalies: async (limit = 30): Promise<AnomalyExplanation[]> => {
    // O store de anomalias é opcional (Marco 7). 503 = explainer nunca rodou;
    // tratamos como "sem anomalias" em vez de quebrar o dashboard inteiro.
    const res = await fetch(`${API_BASE}/api/anomalies?limit=${limit}`, { cache: "no-store" });
    if (res.status === 503) return [];
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return res.json() as Promise<AnomalyExplanation[]>;
  },
};

// URL do WebSocket de candles selados. Em prod usa o mesmo host (wss//ws);
// em dev deriva do NEXT_PUBLIC_API_URL.
export function candlesWsUrl(symbol: string): string {
  const qs = `?symbol=${encodeURIComponent(symbol)}`;
  if (API_BASE) {
    return `${API_BASE.replace(/^http/, "ws")}/ws/candles${qs}`;
  }
  const proto = typeof window !== "undefined" && window.location.protocol === "https:" ? "wss" : "ws";
  const host = typeof window !== "undefined" ? window.location.host : "";
  return `${proto}://${host}/ws/candles${qs}`;
}
