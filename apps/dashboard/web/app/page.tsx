"use client";

import { useMemo, useState } from "react";
import { AnomalyFeed } from "@/components/AnomalyFeed";
import { Header } from "@/components/Header";
import { LivePanel } from "@/components/LivePanel";
import { MarketOverview } from "@/components/MarketOverview";
import { PriceChart } from "@/components/PriceChart";
import { StatsGrid } from "@/components/StatsGrid";
import {
  useAnomalies,
  useCandles,
  useDaily,
  useLiveCandle,
  useLiveCandlesWs,
  useMarketRows,
  useNow,
  useSymbols,
} from "@/lib/hooks";
import type { Candle, Interval } from "@/lib/types";

export default function Page() {
  const [symbol, setSymbol] = useState("BTC-USD");
  const [interval, setSelectedInterval] = useState<Interval>("M1");
  const now = useNow();

  const symbols = useSymbols();
  const rows = useMarketRows(symbols);
  const { candles } = useCandles(symbol, interval, 200);
  const { candles: wsCandles, connected } = useLiveCandlesWs(symbol, interval);
  const live = useLiveCandle(symbol, interval);
  const daily = useDaily(symbol, 1);
  const anomalies = useAnomalies();

  const base = symbols.find((s) => s.symbol === symbol)?.base_asset ?? symbol.split("-")[0];
  const row = rows.find((r) => r.symbol === symbol);
  const today = daily.at(-1) ?? null;

  // Histórico + candles selados do WebSocket, deduplicados por window_start.
  // O candle ao vivo (janela aberta) entra como última barra em formação.
  const allCandles = useMemo<Candle[]>(() => {
    const map = new Map<string, Candle>();
    for (const c of candles) map.set(c.window_start, c);
    for (const c of wsCandles) map.set(c.window_start, c);
    if (live && !map.has(live.window_start)) {
      map.set(live.window_start, {
        symbol: live.symbol,
        interval: live.interval,
        window_start: live.window_start,
        window_end: live.window_end,
        open: live.open,
        high: live.high,
        low: live.low,
        close: live.close,
        volume: live.volume,
        vwap: live.vwap,
        trade_count: live.trade_count,
        direction: live.close >= live.open ? "BULLISH" : "BEARISH",
        return_pct: 0,
      });
    }
    return [...map.values()].sort(
      (a, b) => new Date(a.window_start).getTime() - new Date(b.window_start).getTime(),
    );
  }, [candles, wsCandles, live]);

  const sealed = useMemo(() => allCandles.filter((c) => c.window_start !== live?.window_start), [
    allCandles,
    live,
  ]);
  const latestSealed = sealed.at(-1) ?? null;
  const lastPrice = live?.close ?? latestSealed?.close ?? row?.last ?? null;
  const changePct = row?.changePct ?? null;

  return (
    <div className="min-h-screen">
      <Header updatedAt={latestSealed?.window_end ?? null} connected={connected} now={now} />

      <main className="mx-auto flex max-w-[1400px] flex-col gap-5 px-5 py-5">
        <MarketOverview rows={rows} selected={symbol} onSelect={setSymbol} />

        <div className="grid grid-cols-1 gap-5 lg:grid-cols-3">
          {/* Coluna principal: gráfico + contexto */}
          <div className="flex flex-col gap-5 lg:col-span-2">
            <PriceChart
              candles={allCandles}
              interval={interval}
              onInterval={setSelectedInterval}
              symbol={symbol}
              base={base}
              last={lastPrice}
              changePct={changePct}
            />
            <div className="grid grid-cols-1 gap-5 sm:grid-cols-2">
              <LivePanel live={live} interval={interval} now={now} />
              <StatsGrid latest={latestSealed} today={today} />
            </div>
          </div>

          {/* Coluna lateral: feed de anomalias com explicação LLM */}
          <div className="lg:col-span-1">
            <div className="lg:sticky lg:top-[68px] lg:h-[calc(100vh-88px)]">
              <AnomalyFeed
                anomalies={anomalies}
                selectedSymbol={symbol}
                now={now}
                onSelectSymbol={setSymbol}
              />
            </div>
          </div>
        </div>

        <footer className="border-t border-line py-4 text-center text-[11px] text-ink-faint">
          Pulso · pipeline Kafka → ksqlDB (exactly-once) → Iceberg → dbt → FastAPI · dados de
          mercado em tempo real, anomalias explicadas por LLM
        </footer>
      </main>
    </div>
  );
}
