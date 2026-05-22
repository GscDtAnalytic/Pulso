"use client";

import clsx from "clsx";
import { fmtPct, fmtUsd, fmtVolume } from "@/lib/format";
import type { MarketRow } from "@/lib/hooks";
import { Sparkline } from "./Sparkline";

interface Props {
  rows: MarketRow[];
  selected: string;
  onSelect: (symbol: string) => void;
}

function Card({
  row,
  active,
  onClick,
}: {
  row: MarketRow;
  active: boolean;
  onClick: () => void;
}) {
  const up = (row.changePct ?? 0) >= 0;
  const hasChange = row.changePct != null;

  return (
    <button
      onClick={onClick}
      className={clsx(
        "group flex min-w-[200px] flex-1 flex-col gap-2 rounded-xl border bg-bg-soft p-4 text-left transition",
        active
          ? "border-accent/60 shadow-glow"
          : "border-line hover:border-line/80 hover:bg-bg-inset/60",
      )}
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold text-ink">{row.base}</span>
          <span className="text-[11px] text-ink-faint">{row.symbol}</span>
        </div>
        {active && <span className="h-1.5 w-1.5 rounded-full bg-accent" />}
      </div>

      <div className="flex items-end justify-between gap-2">
        <div>
          <div className="tnum text-lg font-semibold text-ink">
            {row.last != null ? fmtUsd(row.last) : "—"}
          </div>
          <div
            className={clsx(
              "tnum text-xs font-medium",
              !hasChange ? "text-ink-faint" : up ? "text-up" : "text-down",
            )}
          >
            {hasChange ? `${fmtPct(row.changePct as number)} hoje` : "—"}
          </div>
        </div>
        <Sparkline data={row.spark} width={96} height={34} />
      </div>

      <div className="flex items-center justify-between border-t border-line/60 pt-2 text-[11px] text-ink-dim">
        <span>Vol {row.volume24h != null ? fmtVolume(row.volume24h) : "—"}</span>
        <span className="tnum">
          {row.trades24h != null ? `${fmtVolume(row.trades24h)} trades` : ""}
        </span>
      </div>
    </button>
  );
}

export function MarketOverview({ rows, selected, onSelect }: Props) {
  return (
    <section>
      <div className="mb-2 flex items-center justify-between">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-ink-dim">
          Visão de mercado
        </h2>
        <span className="text-[11px] text-ink-faint">variação desde a abertura do dia (UTC)</span>
      </div>
      <div className="flex gap-3 overflow-x-auto pb-1">
        {rows.length === 0
          ? Array.from({ length: 5 }).map((_, i) => (
              <div
                key={i}
                className="h-[120px] min-w-[200px] flex-1 animate-pulse rounded-xl border border-line bg-bg-soft"
              />
            ))
          : rows.map((row) => (
              <Card
                key={row.symbol}
                row={row}
                active={row.symbol === selected}
                onClick={() => onSelect(row.symbol)}
              />
            ))}
      </div>
    </section>
  );
}
