import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Pulso — Analytics de Mercado Cripto em Tempo Real",
  description:
    "Dashboard de mercado cripto em tempo real: candles OHLCV, estado ao vivo via ksqlDB e detecção de anomalias explicadas por LLM. Pipeline Kafka → Iceberg → dbt → FastAPI.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR">
      <body className="min-h-screen bg-bg text-ink antialiased">{children}</body>
    </html>
  );
}
