import { useEffect, useMemo, useState } from "react";
import { api } from "./api";
import { CandleChart } from "./components/CandleChart";
import { StatsPanel } from "./components/StatsPanel";
import { SymbolPicker } from "./components/SymbolPicker";
import { useCandles } from "./hooks/useCandles";
import { useLiveCandles } from "./hooks/useLiveCandles";
import type { Candle, Interval, LiveCandle } from "./types";

export function App() {
  const [symbol, setSymbol] = useState("BTC-USD");
  const [interval, setInterval] = useState<Interval>("M1");
  const [liveCandle, setLiveCandle] = useState<LiveCandle | null>(null);

  const { candles, loading, error } = useCandles(symbol, interval);
  const wsCandles = useLiveCandles(symbol, interval);

  // Candles completos: histórico + os que chegaram via WebSocket (deduplicados por window_start)
  const allCandles = useMemo<Candle[]>(() => {
    const map = new Map(candles.map((c) => [c.window_start, c]));
    for (const c of wsCandles) map.set(c.window_start, c);
    return [...map.values()].sort(
      (a, b) => new Date(a.window_start).getTime() - new Date(b.window_start).getTime(),
    );
  }, [candles, wsCandles]);

  const latest = allCandles.at(-1) ?? null;

  // Poll do estado live (janela aberta) a cada 5 s
  useEffect(() => {
    const poll = () =>
      api
        .liveCandle(symbol, interval)
        .then(setLiveCandle)
        .catch(() => setLiveCandle(null));
    poll();
    const id = setInterval(poll, 5000);
    return () => clearInterval(id);
  }, [symbol, interval]);

  return (
    <div style={{ maxWidth: 960, margin: "0 auto", padding: 16, fontFamily: "sans-serif" }}>
      <h1 style={{ fontSize: 20, marginBottom: 8 }}>Pulso — Market Analytics</h1>
      <SymbolPicker
        symbol={symbol}
        interval={interval}
        onSymbol={setSymbol}
        onInterval={setInterval}
      />
      {loading && <p style={{ color: "#888" }}>Carregando…</p>}
      {error && <p style={{ color: "#e57373" }}>Erro: {error}</p>}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 240px", gap: 16, marginTop: 8 }}>
        <CandleChart candles={allCandles} />
        <StatsPanel latest={latest} live={liveCandle} />
      </div>
    </div>
  );
}
