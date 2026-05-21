import { useEffect, useState } from "react";
import { api } from "../api";
import type { Candle, Interval } from "../types";

interface State {
  candles: Candle[];
  loading: boolean;
  error: string | null;
}

export function useCandles(symbol: string, interval: Interval, limit = 200): State {
  const [state, setState] = useState<State>({ candles: [], loading: true, error: null });

  useEffect(() => {
    if (!symbol) return;
    setState((s) => ({ ...s, loading: true, error: null }));

    api
      .candles(symbol, interval, limit)
      .then((candles) => setState({ candles, loading: false, error: null }))
      .catch((err: unknown) =>
        setState({ candles: [], loading: false, error: String(err) }),
      );
  }, [symbol, interval, limit]);

  return state;
}
