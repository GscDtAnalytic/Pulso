"use client";

import { useEffect, useRef, useState } from "react";
import { api, candlesWsUrl } from "./api";
import type { AnomalyExplanation, Candle, DailyStat, Interval, LiveCandle, Symbol } from "./types";

// ── símbolos (uma vez) ──────────────────────────────────────────────
export function useSymbols(): Symbol[] {
  const [symbols, setSymbols] = useState<Symbol[]>([]);
  useEffect(() => {
    api.symbols().then(setSymbols).catch(() => setSymbols([]));
  }, []);
  return symbols;
}

// ── histórico de candles (REST, refetch ao trocar símbolo/intervalo) ─
interface CandlesState {
  candles: Candle[];
  loading: boolean;
  error: string | null;
}

export function useCandles(symbol: string, interval: Interval, limit = 200): CandlesState {
  const [state, setState] = useState<CandlesState>({ candles: [], loading: true, error: null });
  useEffect(() => {
    if (!symbol) return;
    let alive = true;
    setState((s) => ({ ...s, loading: true, error: null }));
    api
      .candles(symbol, interval, limit)
      .then((candles) => alive && setState({ candles, loading: false, error: null }))
      .catch((err: unknown) => alive && setState({ candles: [], loading: false, error: String(err) }));
    return () => {
      alive = false;
    };
  }, [symbol, interval, limit]);
  return state;
}

// ── candle ao vivo (janela aberta, pull query ksqlDB) — poll 4s ──────
export function useLiveCandle(symbol: string, interval: Interval, ms = 4000): LiveCandle | null {
  const [live, setLive] = useState<LiveCandle | null>(null);
  useEffect(() => {
    if (!symbol) return;
    let alive = true;
    const poll = () =>
      api
        .liveCandle(symbol, interval)
        .then((c) => alive && setLive(c))
        .catch(() => alive && setLive(null));
    poll();
    const id = setInterval(poll, ms);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [symbol, interval, ms]);
  return live;
}

// ── candles selados via WebSocket (push) ─────────────────────────────
export function useLiveCandlesWs(symbol: string, interval: Interval): {
  candles: Candle[];
  connected: boolean;
} {
  const [candles, setCandles] = useState<Candle[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    if (!symbol) return;
    setCandles([]);
    const ws = new WebSocket(candlesWsUrl(symbol));
    wsRef.current = ws;
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onerror = () => setConnected(false);
    ws.onmessage = (ev) => {
      try {
        const candle = JSON.parse(ev.data as string) as Candle;
        if (candle.interval !== interval) return;
        setCandles((prev) => {
          const idx = prev.findIndex((c) => c.window_start === candle.window_start);
          if (idx >= 0) {
            const next = [...prev];
            next[idx] = candle;
            return next;
          }
          return [...prev, candle];
        });
      } catch {
        // mensagem malformada — fail-soft no cliente
      }
    };
    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [symbol, interval]);

  return { candles, connected };
}

// ── resumo diário (contexto: máxima/mínima/variação do dia) ──────────
export function useDaily(symbol: string, limit = 30): DailyStat[] {
  const [daily, setDaily] = useState<DailyStat[]>([]);
  useEffect(() => {
    if (!symbol) return;
    let alive = true;
    api
      .daily(symbol, limit)
      .then((d) => alive && setDaily(d))
      .catch(() => alive && setDaily([]));
    return () => {
      alive = false;
    };
  }, [symbol, limit]);
  return daily;
}

// ── anomalias com explicação LLM — poll 12s ──────────────────────────
export function useAnomalies(ms = 12000): AnomalyExplanation[] {
  const [items, setItems] = useState<AnomalyExplanation[]>([]);
  useEffect(() => {
    let alive = true;
    const poll = () =>
      api
        .anomalies(40)
        .then((a) => alive && setItems(a))
        .catch(() => {});
    poll();
    const id = setInterval(poll, ms);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [ms]);
  return items;
}

// ── visão de mercado: 1 linha por símbolo (preço, variação do dia, spark) ─
export interface MarketRow {
  symbol: string;
  base: string;
  last: number | null;
  changePct: number | null; // variação do dia (close vs open de hoje)
  volume24h: number | null;
  trades24h: number | null;
  spark: number[]; // closes recentes p/ sparkline
  updatedAt: string | null; // window_end do último candle
}

export function useMarketRows(symbols: Symbol[], ms = 20000): MarketRow[] {
  const [rows, setRows] = useState<MarketRow[]>([]);

  useEffect(() => {
    if (symbols.length === 0) return;
    let alive = true;

    const load = async () => {
      const built = await Promise.all(
        symbols.map(async (s): Promise<MarketRow> => {
          const base: MarketRow = {
            symbol: s.symbol,
            base: s.base_asset,
            last: null,
            changePct: null,
            volume24h: null,
            trades24h: null,
            spark: [],
            updatedAt: null,
          };
          try {
            const [candles, daily] = await Promise.all([
              api.candles(s.symbol, "M1", 60),
              api.daily(s.symbol, 1),
            ]);
            const lastCandle = candles.at(-1) ?? null;
            const today = daily.at(-1) ?? null;
            return {
              ...base,
              last: lastCandle?.close ?? today?.close ?? null,
              changePct:
                today && today.open ? ((today.close - today.open) / today.open) * 100 : null,
              volume24h: today?.volume ?? null,
              trades24h: today?.trade_count ?? null,
              spark: candles.map((c) => c.close),
              updatedAt: lastCandle?.window_end ?? null,
            };
          } catch {
            return base;
          }
        }),
      );
      if (alive) setRows(built);
    };

    load();
    const id = setInterval(load, ms);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [symbols, ms]);

  return rows;
}

// ── relógio que tica a cada segundo (para freshness/relativos) ───────
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(id);
  }, [ms]);
  return now;
}
