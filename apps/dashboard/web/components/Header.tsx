"use client";

import clsx from "clsx";
import { Activity, Github } from "lucide-react";
import { ageSeconds, fmtRelative } from "@/lib/format";

interface Props {
  updatedAt: string | null;
  connected: boolean;
  now: number;
}

// Semáforo de freshness (princípio da wiki: freshness sempre visível).
// verde < 90s, âmbar < 5min, vermelho acima — o pipeline é tempo-real.
function freshness(updatedAt: string | null, now: number) {
  if (!updatedAt) return { color: "bg-ink-faint", text: "text-ink-faint", label: "sem dados" };
  const age = ageSeconds(updatedAt, now);
  if (age < 90) return { color: "bg-up", text: "text-up", label: fmtRelative(updatedAt, now) };
  if (age < 300) return { color: "bg-warn", text: "text-warn", label: fmtRelative(updatedAt, now) };
  return { color: "bg-down", text: "text-down", label: fmtRelative(updatedAt, now) };
}

export function Header({ updatedAt, connected, now }: Props) {
  const f = freshness(updatedAt, now);

  return (
    <header className="sticky top-0 z-20 border-b border-line bg-bg/85 backdrop-blur-md">
      <div className="mx-auto flex max-w-[1400px] items-center justify-between gap-4 px-5 py-3">
        <div className="flex items-center gap-3">
          <span className="relative flex h-8 w-8 items-center justify-center rounded-lg bg-accent/15 ring-1 ring-accent/30">
            <Activity className="h-4 w-4 text-accent" strokeWidth={2.5} />
            <span className="absolute -right-0.5 -top-0.5 h-2 w-2 animate-pulseDot rounded-full bg-up" />
          </span>
          <div className="leading-tight">
            <h1 className="text-[15px] font-semibold tracking-tight text-ink">Pulso</h1>
            <p className="hidden text-[11px] text-ink-dim sm:block">
              Analytics de mercado cripto em tempo real
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2 sm:gap-3">
          {/* Trilha do pipeline — conta a história da arquitetura de uma olhada */}
          <div className="hidden items-center gap-1.5 rounded-full border border-line bg-bg-soft px-3 py-1 text-[11px] text-ink-dim lg:flex">
            <span className="text-ink-dim">Kafka</span>
            <span className="text-line">→</span>
            <span className="text-ink-dim">ksqlDB</span>
            <span className="text-line">→</span>
            <span className="text-ink-dim">Iceberg</span>
            <span className="text-line">→</span>
            <span className="text-ink-dim">dbt</span>
            <span className="text-line">→</span>
            <span className="text-accent">FastAPI</span>
          </div>

          <div
            className={clsx(
              "flex items-center gap-1.5 rounded-full border border-line bg-bg-soft px-3 py-1 text-[11px]",
              connected ? "text-up" : "text-ink-dim",
            )}
            title={connected ? "WebSocket conectado" : "WebSocket desconectado"}
          >
            <span
              className={clsx(
                "h-1.5 w-1.5 rounded-full",
                connected ? "animate-pulseDot bg-up" : "bg-ink-faint",
              )}
            />
            {connected ? "AO VIVO" : "offline"}
          </div>

          <div
            className="flex items-center gap-1.5 rounded-full border border-line bg-bg-soft px-3 py-1 text-[11px] text-ink-dim"
            title="Idade do último candle selado"
          >
            <span className={clsx("h-1.5 w-1.5 rounded-full", f.color)} />
            <span className="text-ink-dim">atualizado</span>
            <span className={clsx("tnum", f.text)}>{f.label}</span>
          </div>

          <a
            href="https://github.com/GscDtAnalytic/Pulso"
            target="_blank"
            rel="noreferrer"
            className="hidden rounded-lg border border-line bg-bg-soft p-1.5 text-ink-dim transition hover:text-ink sm:block"
            title="Código no GitHub"
          >
            <Github className="h-4 w-4" />
          </a>
        </div>
      </div>
    </header>
  );
}
