import { createChart, ColorType } from "lightweight-charts";
import { useEffect, useRef } from "react";
import type { Candle } from "../types";

interface Props {
  candles: Candle[];
}

export function CandleChart({ candles }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || candles.length === 0) return;

    const chart = createChart(el, {
      width: el.clientWidth,
      height: 360,
      layout: { background: { type: ColorType.Solid, color: "#111" }, textColor: "#ddd" },
      grid: { vertLines: { color: "#333" }, horzLines: { color: "#333" } },
      timeScale: { timeVisible: true, secondsVisible: false },
    });

    const series = chart.addCandlestickSeries({
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
    });

    series.setData(
      candles.map((c) => ({
        time: Math.floor(new Date(c.window_start).getTime() / 1000) as unknown as string,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      })),
    );

    chart.timeScale().fitContent();

    const observer = new ResizeObserver(() => {
      chart.applyOptions({ width: el.clientWidth });
    });
    observer.observe(el);

    return () => {
      observer.disconnect();
      chart.remove();
    };
  }, [candles]);

  if (candles.length === 0) {
    return <div style={{ height: 360, display: "flex", alignItems: "center", justifyContent: "center", color: "#666" }}>Sem dados</div>;
  }

  return <div ref={containerRef} style={{ width: "100%" }} />;
}
