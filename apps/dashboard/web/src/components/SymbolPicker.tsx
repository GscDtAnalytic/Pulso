import { useEffect, useState } from "react";
import { api } from "../api";
import type { Interval, Symbol } from "../types";

interface Props {
  symbol: string;
  interval: Interval;
  onSymbol: (s: string) => void;
  onInterval: (i: Interval) => void;
}

const INTERVALS: Interval[] = ["M1", "M5", "H1"];

export function SymbolPicker({ symbol, interval, onSymbol, onInterval }: Props) {
  const [symbols, setSymbols] = useState<Symbol[]>([]);

  useEffect(() => {
    api.symbols().then(setSymbols).catch(console.error);
  }, []);

  return (
    <div style={{ display: "flex", gap: 12, alignItems: "center", padding: "8px 0" }}>
      <label>
        Símbolo{" "}
        <select value={symbol} onChange={(e) => onSymbol(e.target.value)}>
          {symbols
            .filter((s) => s.is_active)
            .map((s) => (
              <option key={s.symbol} value={s.symbol}>
                {s.symbol}
              </option>
            ))}
        </select>
      </label>
      <label>
        Intervalo{" "}
        <select value={interval} onChange={(e) => onInterval(e.target.value as Interval)}>
          {INTERVALS.map((i) => (
            <option key={i} value={i}>
              {i}
            </option>
          ))}
        </select>
      </label>
    </div>
  );
}
