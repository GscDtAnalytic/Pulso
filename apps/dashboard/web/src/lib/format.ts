export function fmtPrice(value: number, decimals = 2): string {
  return value.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export function fmtVolume(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
  return value.toFixed(4);
}

export function fmtPct(value: number): string {
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

export function fmtDate(isoString: string): string {
  return new Date(isoString).toLocaleString("pt-BR", {
    dateStyle: "short",
    timeStyle: "short",
  });
}
