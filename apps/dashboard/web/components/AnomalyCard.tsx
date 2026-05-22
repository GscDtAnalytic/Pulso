"use client";

import clsx from "clsx";
import { ChevronDown, Sparkles, TrendingUp } from "lucide-react";
import { useState } from "react";
import { anomalyTypeLabel, severityTier, TIER_STYLE } from "@/lib/anomaly";
import { fmtMultiple, fmtRelative } from "@/lib/format";
import type { AnomalyExplanation } from "@/lib/types";

interface Props {
  anomaly: AnomalyExplanation;
  highlighted: boolean;
  now: number;
  onSelectSymbol: (symbol: string) => void;
}

export function AnomalyCard({ anomaly, highlighted, now, onSelectSymbol }: Props) {
  const [open, setOpen] = useState(false);
  const tier = severityTier(anomaly.severity);
  const style = TIER_STYLE[tier];
  const multiple =
    anomaly.current_value != null && anomaly.baseline_value != null
      ? fmtMultiple(anomaly.current_value, anomaly.baseline_value)
      : null;

  return (
    <div
      className={clsx(
        "rounded-xl border bg-bg-soft p-3 ring-1 transition",
        highlighted ? "border-accent/40 ring-accent/20" : "border-line ring-transparent",
      )}
    >
      <div className="flex items-start gap-3">
        <span className={clsx("mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg", style.bg)}>
          <TrendingUp className={clsx("h-3.5 w-3.5", style.text)} />
        </span>

        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <button
                onClick={() => onSelectSymbol(anomaly.symbol)}
                className="text-sm font-semibold text-ink hover:text-accent"
              >
                {anomaly.symbol}
              </button>
              <span className="text-xs text-ink-dim">{anomalyTypeLabel(anomaly.anomaly_type)}</span>
            </div>
            <span className="shrink-0 text-[11px] text-ink-faint">
              {fmtRelative(anomaly.detected_at, now)}
            </span>
          </div>

          <div className="mt-1.5 flex flex-wrap items-center gap-2">
            <span
              className={clsx(
                "rounded-full px-2 py-0.5 text-[11px] font-medium ring-1",
                style.bg,
                style.text,
                style.ring,
              )}
            >
              {style.label}
              {anomaly.severity != null && (
                <span className="tnum ml-1 opacity-80">z={anomaly.severity.toFixed(1)}</span>
              )}
            </span>
            {multiple && (
              <span className="tnum rounded-full bg-bg-inset px-2 py-0.5 text-[11px] text-ink">
                {multiple} acima da média
              </span>
            )}
          </div>

          {anomaly.explanation && (
            <>
              <p
                className={clsx(
                  "mt-2 text-[13px] leading-relaxed text-ink-dim",
                  !open && "line-clamp-2",
                )}
              >
                {anomaly.explanation}
              </p>
              <button
                onClick={() => setOpen((v) => !v)}
                className="mt-1 flex items-center gap-1 text-[11px] font-medium text-accent hover:underline"
              >
                {open ? "menos" : "ler explicação"}
                <ChevronDown className={clsx("h-3 w-3 transition", open && "rotate-180")} />
              </button>
            </>
          )}

          {open && (
            <div className="mt-2 space-y-2">
              {anomaly.key_factors.length > 0 && (
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-ink-faint">
                    Fatores prováveis
                  </div>
                  <ul className="space-y-1">
                    {anomaly.key_factors.map((f, i) => (
                      <li key={i} className="flex gap-1.5 text-[12px] text-ink-dim">
                        <span className="text-accent">•</span>
                        <span>{f}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {anomaly.news_headlines.length > 0 && (
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-ink-faint">
                    Manchetes
                  </div>
                  <ul className="space-y-1">
                    {anomaly.news_headlines.map((h, i) => (
                      <li key={i} className="text-[12px] text-ink-dim">
                        {h}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {anomaly.model_used && (
                <div className="flex items-center gap-1 text-[10px] text-ink-faint">
                  <Sparkles className="h-3 w-3" />
                  explicado por {anomaly.model_used}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
