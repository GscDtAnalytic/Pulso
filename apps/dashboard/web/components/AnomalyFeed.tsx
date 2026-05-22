"use client";

import { Sparkles } from "lucide-react";
import type { AnomalyExplanation } from "@/lib/types";
import { AnomalyCard } from "./AnomalyCard";

interface Props {
  anomalies: AnomalyExplanation[];
  selectedSymbol: string;
  now: number;
  onSelectSymbol: (symbol: string) => void;
}

export function AnomalyFeed({ anomalies, selectedSymbol, now, onSelectSymbol }: Props) {
  return (
    <section className="flex h-full flex-col rounded-xl border border-line bg-bg-soft/40 p-4">
      <div className="mb-1 flex items-center gap-2">
        <span className="flex h-6 w-6 items-center justify-center rounded-lg bg-accent/15">
          <Sparkles className="h-3.5 w-3.5 text-accent" />
        </span>
        <h2 className="text-sm font-semibold text-ink">Anomalias detectadas</h2>
        {anomalies.length > 0 && (
          <span className="tnum ml-auto rounded-full bg-bg-inset px-2 py-0.5 text-[11px] text-ink-dim">
            {anomalies.length}
          </span>
        )}
      </div>
      <p className="mb-3 text-[11px] text-ink-faint">
        Detector estatístico (z-score) sobre o stream · explicação gerada por LLM, uma chamada por
        evento
      </p>

      <div className="flex-1 space-y-2 overflow-y-auto pr-1">
        {anomalies.length === 0 ? (
          <div className="flex h-32 flex-col items-center justify-center gap-2 text-center text-ink-faint">
            <Sparkles className="h-5 w-5 opacity-40" />
            <p className="text-sm">Nenhuma anomalia recente.</p>
            <p className="text-[11px]">O mercado está dentro do padrão estatístico.</p>
          </div>
        ) : (
          anomalies.map((a) => (
            <AnomalyCard
              key={a.anomaly_id}
              anomaly={a}
              highlighted={a.symbol === selectedSymbol}
              now={now}
              onSelectSymbol={onSelectSymbol}
            />
          ))
        )}
      </div>
    </section>
  );
}
