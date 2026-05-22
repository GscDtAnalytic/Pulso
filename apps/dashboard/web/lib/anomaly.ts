// Mapeamento de severidade (z-score) → faixa visual, e rótulos PT-BR dos tipos.

export type SeverityTier = "info" | "moderate" | "high" | "critical";

export function severityTier(severity: number | null): SeverityTier {
  if (severity == null) return "info";
  if (severity >= 10) return "critical";
  if (severity >= 5) return "high";
  if (severity >= 3) return "moderate";
  return "info";
}

export const TIER_STYLE: Record<
  SeverityTier,
  { label: string; text: string; bg: string; ring: string; dot: string }
> = {
  critical: {
    label: "Severa",
    text: "text-crit",
    bg: "bg-crit/10",
    ring: "ring-crit/40",
    dot: "bg-crit",
  },
  high: {
    label: "Alta",
    text: "text-down",
    bg: "bg-down/10",
    ring: "ring-down/30",
    dot: "bg-down",
  },
  moderate: {
    label: "Moderada",
    text: "text-warn",
    bg: "bg-warn/10",
    ring: "ring-warn/30",
    dot: "bg-warn",
  },
  info: {
    label: "Leve",
    text: "text-ink-dim",
    bg: "bg-bg-inset",
    ring: "ring-line",
    dot: "bg-ink-dim",
  },
};

export const ANOMALY_TYPE_LABEL: Record<string, string> = {
  VOLUME_SPIKE: "Pico de volume",
  PRICE_SPIKE: "Salto de preço",
  PRICE_DROP: "Queda de preço",
  VOLATILITY_SPIKE: "Pico de volatilidade",
  SPREAD_WIDENING: "Alargamento de spread",
};

export function anomalyTypeLabel(type: string): string {
  return ANOMALY_TYPE_LABEL[type] ?? type.replaceAll("_", " ").toLowerCase();
}
