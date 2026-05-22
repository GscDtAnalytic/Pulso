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
└── web/          # Next.js 14 (App Router, static export) + TypeScript + Tailwind
    ├── app/
    │   ├── layout.tsx        # shell + metadata
    │   ├── page.tsx          # orquestra dados e layout (client component)
    │   └── globals.css       # Tailwind + tema dark
    ├── components/
    │   ├── Header.tsx        # marca + semáforo de freshness + estado AO VIVO
    │   ├── MarketOverview.tsx# cards dos 5 símbolos (preço, var. dia, sparkline)
    │   ├── Sparkline.tsx     # mini-gráfico SVG
    │   ├── PriceChart.tsx    # candlestick + volume (lightweight-charts) + toggle M1/M5/H1
    │   ├── LivePanel.tsx     # janela aberta ao vivo (ksqlDB) + progresso até selar
    │   ├── StatsGrid.tsx     # contexto do dia (faixa, VWAP, volume) — não número solto
    │   ├── AnomalyFeed.tsx   # feed de anomalias com explicação LLM
    │   └── AnomalyCard.tsx   # card expansível: severidade, fatores, modelo
    └── lib/
        ├── api.ts            # cliente HTTP/WS tipado (path relativo em prod)
        ├── types.ts          # tipos espelhando os modelos pydantic
        ├── hooks.ts          # hooks de dados (REST poll + WebSocket)
        ├── anomaly.ts        # severidade (z-score) → faixa visual; rótulos PT-BR
        └── format.ts         # formatadores de preço/volume/percentual/freshness
```

### Design (segue `wiki/conceitos/dashboard-design`)

Dashboard **operacional** (tempo real, at-a-glance) com tema dark "trading-desk":

- **Visão de mercado** — 5 cards, variação do dia + sparkline; clique seleciona o símbolo.
- **Gráfico** — candlestick OHLCV + histograma de volume, candle ao vivo como barra em formação.
- **Janela ao vivo** — estado parcial da janela aberta no ksqlDB, com barra de progresso até selar.
- **Contexto do dia** — preço dentro da faixa min/max, prêmio/desconto vs VWAP (contexto, não número solto).
- **Anomalias explicadas por LLM** — o diferenciador: feed em tempo real do detector de z-score com a explicação gerada por LLM, fatores e severidade.

Cor tem significado (verde alta / vermelho baixa / âmbar-rosa severidade); freshness sempre visível.

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
npm run dev   # http://localhost:3000 — fala com a API via NEXT_PUBLIC_API_URL (.env.local)
```

Em dev o `.env.local` aponta `NEXT_PUBLIC_API_URL` para a API (prod ou `http://localhost:8000`);
a API libera CORS. No build de prod o `.env.local` é removido (ver Dockerfile) e as chamadas
ficam **relativas** (`/api`, `/ws`) — o mesmo container FastAPI serve a UI estática (`out/`).

## Endpoints da API

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/health` | Liveness + backend ativo |
| GET | `/api/symbols` | Lista de símbolos do seed |
| GET | `/api/candles` | Histórico OHLCV (`fct_candle`) |
| GET | `/api/candles/live` | Estado da janela aberta (ksqlDB pull query) |
| GET | `/api/symbols/{symbol}/daily` | Resumo diário (`fct_symbol_daily`) |
| GET | `/api/anomalies` | Anomalias com explicação LLM (Marco 7; 503 → tratado como vazio) |
| WS | `/ws/candles` | Push de candles selados via WebSocket |
| GET | `/metrics` | Prometheus scrape endpoint |

## Dois backends de histórico

| Variável | Valor | Fonte |
|----------|-------|-------|
| `PULSO_SERVE_HISTORY_BACKEND=duckdb` | dev (padrão) | arquivo `dbt/pulso_lake.duckdb` |
| `PULSO_SERVE_HISTORY_BACKEND=trino` | prod | Trino sobre Iceberg no MinIO/GCS |
