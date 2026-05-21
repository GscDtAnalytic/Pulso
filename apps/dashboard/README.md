# Dashboard — API + Web (Marco 4)

Camada de serving do Pulso: expõe os dados do lakehouse Iceberg/dbt ao usuário final.

## Estrutura

```
apps/dashboard/
├── api/          # FastAPI — histórico (marts dbt) + live (ksqlDB) + push (WebSocket)
│   └── src/pulso_serve/
│       ├── app.py        # factory + entrypoint (uvicorn)
│       ├── routes.py     # /health, /api/symbols, /api/candles, /ws/candles
│       ├── store.py      # MarketStore — DuckDB (dev) / Trino (prod)
│       ├── ksql.py       # KsqlClient — pull query janela aberta
│       ├── live.py       # ConnectionManager + CandleBroadcaster (WebSocket)
│       ├── models.py     # Pydantic: Symbol, Candle, DailyStat, LiveCandle
│       └── metrics.py    # Prometheus: http_requests, live_query, ws_connections
└── web/          # React + Vite + TypeScript — dashboard de candles
    └── src/
        ├── api.ts            # cliente HTTP tipado
        ├── types.ts          # tipos compartilhados
        ├── App.tsx           # layout principal (picker + chart + stats)
        ├── hooks/
        │   ├── useCandles.ts      # histórico via REST
        │   └── useLiveCandles.ts  # candles selados via WebSocket
        ├── components/
        │   ├── SymbolPicker.tsx   # dropdowns símbolo/intervalo
        │   ├── CandleChart.tsx    # gráfico OHLCV (lightweight-charts)
        │   └── StatsPanel.tsx     # tabela de estatísticas do candle atual
        └── lib/format.ts          # formatadores de preço/volume/percentual
```

## Rodar localmente

```bash
# 1. Sobe a stack (Kafka, ksqlDB, MinIO, Trino, Postgres)
make up

# 2. Constrói os modelos dbt (espelha Iceberg → DuckDB → marts)
make dbt-build

# 3. API FastAPI em :8000
make serve

# 4. Frontend (em outro terminal, após npm install)
cd apps/dashboard/web
npm install   # apenas na primeira vez / depois de pull
npm run dev   # http://localhost:5173 (proxy → :8000)
```

## Endpoints da API

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/health` | Liveness + backend ativo |
| GET | `/api/symbols` | Lista de símbolos do seed |
| GET | `/api/candles` | Histórico OHLCV (`fct_candle`) |
| GET | `/api/candles/live` | Estado da janela aberta (ksqlDB pull query) |
| GET | `/api/symbols/{symbol}/daily` | Resumo diário (`fct_symbol_daily`) |
| WS | `/ws/candles` | Push de candles selados via WebSocket |
| GET | `/metrics` | Prometheus scrape endpoint |

## Dois backends de histórico

| Variável | Valor | Fonte |
|----------|-------|-------|
| `PULSO_SERVE_HISTORY_BACKEND=duckdb` | dev (padrão) | arquivo `dbt/pulso_lake.duckdb` |
| `PULSO_SERVE_HISTORY_BACKEND=trino` | prod | Trino sobre Iceberg no MinIO/GCS |
