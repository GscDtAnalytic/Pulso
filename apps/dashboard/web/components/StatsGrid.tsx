"use client";

import clsx from "clsx";
import { fmtCount, fmtPct, fmtPrice, fmtUsd, fmtVolume } from "@/lib/format";
import type { Candle, DailyStat } from "@/lib/types";

interface Props {
  latest: Candle | null;
  today: DailyStat | null;
}

// Contexto > número solto (princípio da wiki/conceitos/dashboard-design):
// onde o preço atual está dentro da faixa do dia, prêmio/desconto vs VWAP, etc.
export function StatsGrid({ latest, today }: Props) {
  const price = latest?.close ?? today?.close ?? null;

  // Posição na faixa do dia (0 = mínima, 1 = máxima).
  let rangePos: number | null = null;
  if (today && price != null && today.high > today.low) {
    rangePos = Math.min(1, Math.max(0, (price - today.low) / (today.high - today.low)));
  }

  // Prêmio/desconto vs VWAP do dia.
  const vwapDelta =
    today && price != null && today.vwap ? ((price - today.vwap) / today.vwap) * 100 : null;

  return (
    <div className="rounded-xl border border-line bg-bg-soft p-4">
      <div className="mb-3 text-xs font-semibold uppercase tracking-wider text-ink-dim">
        Contexto do dia
      </div>

      {today ? (
        <>
          {/* Faixa do dia com marcador de preço atual */}
          <div className="mb-1 flex justify-between text-[11px]">
            <span className="text-ink-dim">Mínima</span>
            <span className="text-ink-dim">Máxima</span>
          </div>
          <div className="relative h-1.5 rounded-full bg-gradient-to-r from-down/40 via-bg-inset to-up/40">
            {rangePos != null && (
              <div
                className="absolute top-1/2 h-3 w-3 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-bg bg-accent shadow-glow"
                style={{ left: `${rangePos * 100}%` }}
              />
            )}
          </div>
          <div className="mb-3 mt-1 flex justify-between text-[11px]">
            <span className="tnum text-down">{fmtPrice(today.low)}</span>
            <span className="tnum text-up">{fmtPrice(today.high)}</span>
          </div>

          <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
            <Stat label="Preço" value={price != null ? fmtUsd(price) : "—"} strong />
            <Stat
              label="vs VWAP"
              value={vwapDelta != null ? fmtPct(vwapDelta) : "—"}
              tone={vwapDelta != null ? (vwapDelta >= 0 ? "up" : "down") : undefined}
            />
            <Stat label="Abertura" value={fmtPrice(today.open)} />
            <Stat label="VWAP dia" value={fmtPrice(today.vwap)} />
            <Stat label="Volume" value={fmtVolume(today.volume)} />
            <Stat label="Trades" value={fmtCount(today.trade_count)} />
          </div>

          {latest && latest.return_pct != null && (
            <div className="mt-3 flex items-center justify-between border-t border-line/60 pt-3 text-xs">
              <span className="text-ink-dim">Último candle selado</span>
              <span
                className={clsx(
                  "tnum font-medium",
                  latest.return_pct >= 0 ? "text-up" : "text-down",
                )}
              >
                {fmtPct(latest.return_pct)}
              </span>
            </div>
          )}
        </>
      ) : (
        <p className="text-sm text-ink-faint">Sem resumo diário disponível.</p>
      )}
    </div>
  );
}

function Stat({
  label,
  value,
  tone,
  strong,
}: {
  label: string;
  value: string;
  tone?: "up" | "down";
  strong?: boolean;
}) {
  return (
    <div>
      <div className="text-[11px] text-ink-dim">{label}</div>
      <div
        className={clsx(
          "tnum",
          strong ? "text-sm font-semibold text-ink" : "text-[13px]",
          tone === "up" ? "text-up" : tone === "down" ? "text-down" : !strong && "text-ink",
        )}
      >
        {value}
      </div>
    </div>
  );
}
