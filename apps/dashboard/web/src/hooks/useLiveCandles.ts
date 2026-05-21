import { useEffect, useRef, useState } from "react";
import type { Candle, Interval } from "../types";

// Recebe candles selados do WebSocket (/ws/candles) e os acumula em memória.
// O histórico persiste enquanto o símbolo não muda — cobre reconexões curtas.
export function useLiveCandles(symbol: string, interval: Interval): Candle[] {
  const [live, setLive] = useState<Candle[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    if (!symbol) return;
    setLive([]);

    const url = `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}/ws/candles?symbol=${encodeURIComponent(symbol)}`;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onmessage = (ev) => {
      try {
        const candle = JSON.parse(ev.data as string) as Candle;
        if (candle.interval !== interval) return;
        setLive((prev) => {
          // Substitui se a janela já existe (re-emissão); senão anexa.
          const idx = prev.findIndex((c) => c.window_start === candle.window_start);
          if (idx >= 0) {
            const next = [...prev];
            next[idx] = candle;
            return next;
          }
          return [...prev, candle];
        });
      } catch {
        // Ignora mensagens malformadas — fail-soft no cliente.
      }
    };

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [symbol, interval]);

  return live;
}
