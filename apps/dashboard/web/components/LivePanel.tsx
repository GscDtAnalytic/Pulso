"use client";

import clsx from "clsx";
import { fmtPrice, fmtUsd, fmtVolume } from "@/lib/format";
import type { Interval, LiveCandle } from "@/lib/types";

interface Props {
  live: LiveCandle | null;
  interval: Interval;
  now: number;
}

// Progresso dentro da janela aberta (0..1) — quanto falta para o candle selar.
function windowProgress(live: LiveCandle, now: number): number {
  const start = new Date(live.window_start).getTime();
  const end = new Date(live.window_end).getTime();
  if (end <= start) return 0;
  return Math.min(1, Math.max(0, (now - start) / (end - start)));
}

export function LivePanel({ live, interval, now }: Props) {
  if (!live) {
    return (
      <div className="rounded-xl border border-line bg-bg-soft p-4">
        <div className="mb-1 text-xs font-semibold uppercase tracking-wider text-ink-dim">
          Janela ao vivo
        </div>
        <p className="text-sm text-ink-faint">Sem janela aberta no ksqlDB ({interval}).</p>
      </div>
    );
  }

  const up = live.close >= live.open;
  const progress = windowProgress(live, now);

  return (
    <div className="rounded-xl border border-line bg-bg-soft p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-xs font-semibold uppercase tracking-wider text-ink-dim">
          Janela ao vivo · {interval}
        </div>
        <span className="flex items-center gap-1.5 rounded-full bg-up/10 px-2 py-0.5 text-[10px] font-semibold text-up">
          <span className="h-1.5 w-1.5 animate-pulseDot rounded-full bg-up" />
          AO VIVO
        </span>
      </div>

      <div className="flex items-end justify-between">
        <div className={clsx("tnum text-2xl font-bold", up ? "text-up" : "text-down")}>
          {fmtUsd(live.close)}
        </div>
        <div className="text-right text-[11px] text-ink-dim">
          <div className="tnum">{live.trade_count.toLocaleString("en-US")} trades</div>
          <div className="tnum">vol {fmtVolume(live.volume)}</div>
        </div>
      </div>

      {/* Barra de progresso da janela até selar */}
      <div className="mt-3">
        <div className="mb-1 flex justify-between text-[10px] text-ink-faint">
          <span>janela formando</span>
          <span className="tnum">{Math.round(progress * 100)}%</span>
        </div>
        <div className="h-1 overflow-hidden rounded-full bg-bg-inset">
          <div
            className="h-full rounded-full bg-accent transition-all duration-700"
            style={{ width: `${progress * 100}%` }}
          />
        </div>
      </div>

      <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
        <Pair label="Abertura" value={fmtPrice(live.open)} />
        <Pair label="VWAP" value={fmtPrice(live.vwap)} />
        <Pair label="Máxima" value={fmtPrice(live.high)} tone="up" />
        <Pair label="Mínima" value={fmtPrice(live.low)} tone="down" />
      </div>
    </div>
  );
}

function Pair({ label, value, tone }: { label: string; value: string; tone?: "up" | "down" }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-ink-dim">{label}</span>
      <span
        className={clsx(
          "tnum",
          tone === "up" ? "text-up" : tone === "down" ? "text-down" : "text-ink",
        )}
      >
        {value}
      </span>
    </div>
  );
}
