import { fmtDate, fmtPct, fmtPrice, fmtVolume } from "../lib/format";
import type { Candle, LiveCandle } from "../types";

interface Props {
  latest: Candle | null;
  live: LiveCandle | null;
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <tr>
      <td style={{ color: "#888", paddingRight: 16 }}>{label}</td>
      <td style={{ textAlign: "right", fontFamily: "monospace" }}>{value}</td>
    </tr>
  );
}

export function StatsPanel({ latest, live }: Props) {
  const candle = live ?? latest;
  if (!candle) return <p style={{ color: "#666" }}>Sem dados de estatísticas.</p>;

  const retPct = "return_pct" in candle ? (candle as Candle).return_pct : null;

  return (
    <table style={{ borderCollapse: "collapse", width: "100%" }}>
      <tbody>
        <Row label="Abertura" value={fmtPrice(candle.open)} />
        <Row label="Máxima" value={fmtPrice(candle.high)} />
        <Row label="Mínima" value={fmtPrice(candle.low)} />
        <Row label="Fechamento" value={fmtPrice(candle.close)} />
        <Row label="Volume" value={fmtVolume(candle.volume)} />
        <Row label="VWAP" value={fmtPrice(candle.vwap)} />
        <Row label="Trades" value={String(candle.trade_count)} />
        {retPct !== null && <Row label="Retorno" value={fmtPct(retPct)} />}
        <Row label="Início" value={fmtDate(candle.window_start)} />
        <Row label="Fim" value={fmtDate(candle.window_end)} />
        {"is_final" in candle && (
          <Row label="Status" value={candle.is_final ? "Selado" : "Ao vivo"} />
        )}
      </tbody>
    </table>
  );
}
