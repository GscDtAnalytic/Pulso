// Formatação consistente em todo o dashboard. Números monoespaçados, casas
// decimais adaptadas à escala do ativo (BTC ~77k vs DOGE ~0.15).

export function priceDecimals(value: number): number {
  if (value >= 1000) return 2;
  if (value >= 1) return 3;
  if (value >= 0.01) return 5;
  return 7;
}

export function fmtPrice(value: number, decimals?: number): string {
  const d = decimals ?? priceDecimals(value);
  return value.toLocaleString("en-US", {
    minimumFractionDigits: d,
    maximumFractionDigits: d,
  });
}

export function fmtUsd(value: number): string {
  return `$${fmtPrice(value)}`;
}

export function fmtVolume(value: number): string {
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
  if (value >= 1) return value.toFixed(2);
  return value.toFixed(4);
}

export function fmtCount(value: number): string {
  return value.toLocaleString("en-US");
}

export function fmtPct(value: number, digits = 2): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}%`;
}

export function fmtMultiple(current: number, baseline: number): string {
  if (!baseline) return "—";
  return `${(current / baseline).toFixed(1)}x`;
}

export function fmtTime(iso: string): string {
  return new Date(iso).toLocaleTimeString("pt-BR", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function fmtDateTime(iso: string): string {
  return new Date(iso).toLocaleString("pt-BR", { dateStyle: "short", timeStyle: "short" });
}

// "há 12s", "há 3min", "há 2h" — para freshness e timestamps de anomalia.
export function fmtRelative(iso: string, now = Date.now()): string {
  const diffMs = now - new Date(iso).getTime();
  const s = Math.max(0, Math.round(diffMs / 1000));
  if (s < 60) return `há ${s}s`;
  const m = Math.round(s / 60);
  if (m < 60) return `há ${m}min`;
  const h = Math.round(m / 60);
  if (h < 24) return `há ${h}h`;
  const d = Math.round(h / 24);
  return `há ${d}d`;
}

// Idade em segundos de um timestamp ISO — usado para o semáforo de freshness.
export function ageSeconds(iso: string, now = Date.now()): number {
  return Math.max(0, (now - new Date(iso).getTime()) / 1000);
}
