"use client";

import {
  ColorType,
  createChart,
  CrosshairMode,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";
import clsx from "clsx";
import { useEffect, useRef } from "react";
import { fmtPct, fmtUsd } from "@/lib/format";
import { INTERVALS, type Candle, type Interval } from "@/lib/types";

interface Props {
  candles: Candle[];
  interval: Interval;
  onInterval: (i: Interval) => void;
  symbol: string;
  base: string;
  last: number | null;
  changePct: number | null;
}

const INTERVAL_LABEL: Record<Interval, string> = { M1: "1m", M5: "5m", H1: "1h" };

export function PriceChart({
  candles,
  interval,
  onInterval,
  symbol,
  base,
  last,
  changePct,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  // Cria o gráfico uma vez.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      width: el.clientWidth,
      height: 420,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#9aa7b4",
        fontFamily: "var(--font-mono)",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "rgba(31,38,48,0.5)" },
        horzLines: { color: "rgba(31,38,48,0.5)" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#1f2630", scaleMargins: { top: 0.08, bottom: 0.28 } },
      timeScale: { borderColor: "#1f2630", timeVisible: true, secondsVisible: false },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#26d196",
      downColor: "#ff5d6c",
      borderVisible: false,
      wickUpColor: "#26d196",
      wickDownColor: "#ff5d6c",
    });

    const volSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
    });
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volSeriesRef.current = volSeries;

    const observer = new ResizeObserver(() => chart.applyOptions({ width: el.clientWidth }));
    observer.observe(el);

    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  // Atualiza os dados quando os candles mudam.
  useEffect(() => {
    const candleSeries = candleSeriesRef.current;
    const volSeries = volSeriesRef.current;
    if (!candleSeries || !volSeries) return;

    const sorted = [...candles].sort(
      (a, b) => new Date(a.window_start).getTime() - new Date(b.window_start).getTime(),
    );

    candleSeries.setData(
      sorted.map((c) => ({
        time: (new Date(c.window_start).getTime() / 1000) as UTCTimestamp,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      })),
    );
    volSeries.setData(
      sorted.map((c) => ({
        time: (new Date(c.window_start).getTime() / 1000) as UTCTimestamp,
        value: c.volume,
        color: c.close >= c.open ? "rgba(38,209,150,0.45)" : "rgba(255,93,108,0.45)",
      })),
    );
  }, [candles]);

  const up = (changePct ?? 0) >= 0;

  return (
    <div className="rounded-xl border border-line bg-bg-soft p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-baseline gap-3">
          <div className="flex items-baseline gap-1.5">
            <span className="text-base font-semibold text-ink">{base}</span>
            <span className="text-xs text-ink-faint">{symbol}</span>
          </div>
          <span className="tnum text-xl font-semibold text-ink">
            {last != null ? fmtUsd(last) : "—"}
          </span>
          {changePct != null && (
            <span className={clsx("tnum text-sm font-medium", up ? "text-up" : "text-down")}>
              {fmtPct(changePct)}
            </span>
          )}
        </div>

        <div className="flex items-center gap-1 rounded-lg border border-line bg-bg p-0.5">
          {INTERVALS.map((i) => (
            <button
              key={i}
              onClick={() => onInterval(i)}
              className={clsx(
                "rounded-md px-2.5 py-1 text-xs font-medium transition",
                i === interval
                  ? "bg-accent/20 text-accent"
                  : "text-ink-dim hover:text-ink",
              )}
            >
              {INTERVAL_LABEL[i]}
            </button>
          ))}
        </div>
      </div>

      <div ref={containerRef} className="w-full" />

      {candles.length === 0 && (
        <div className="-mt-[420px] flex h-[420px] items-center justify-center text-sm text-ink-faint">
          Sem candles para {symbol} ({interval})
        </div>
      )}
    </div>
  );
}
