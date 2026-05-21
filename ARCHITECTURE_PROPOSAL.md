# Pulso — Proposta de Arquitetura

Plataforma de analytics de mercado cripto em tempo real. Este documento detalha as decisões e o roadmap. O `README.md` é o resumo; aqui mora o porquê.

## Contexto

Segunda peça de portfólio depois do **Mapear-RN**. O Mapear provou batch maduro (cron, BigQuery, dbt, Terraform, observabilidade, < R$5/mês). O Pulso ataca a fronteira que falta e mais valorizada em 2026: **streaming em tempo real com correção formal** — exactly-once, event-time, watermarks, late data, lakehouse aberto e reprocessamento (Kappa). Domínio: mercado cripto, o caso canônico de streaming (firehose real, gratuito, alto volume).

**Risco a neutralizar:** "dashboard de cripto" é o tutorial mais batido do mundo. O que torna isto sênior está em quatro pilares de correção e numa camada de governança.

**Decisão sobre LLM:** fora do MVP. Entra como Eixo B isolado (Marco 7), como *explicador* de eventos anômalos — uma chamada por evento, nunca por trade. Espelha o padrão "determinístico no núcleo, LLM só na explicação" do Mapear.

## Princípios

1. **Correção antes de features** — garantia de entrega e event-time são requisito.
2. **Log durável é a fonte da verdade** — reprocessar = replay do log (Kappa).
3. **Lake antes do serving** — trades brutos imutáveis no Iceberg; reprocessar custa zero.
4. **Schema é contrato versionado e enforçado em CI** — sem JSON livre no barramento.
5. **Fail-loud por padrão** — consumer lag e freshness são cidadãos de primeira classe (lição do incidente de abril do Mapear).
6. **Vendor-neutral e barato** — roda 100% local a custo zero; cloud (GCP) opcional, com custo medido.

## Os 4 pilares anti-toy

1. **Exactly-once de verdade** — idempotent producer (`enable.idempotence=true`, `acks=all`) + ksqlDB `exactly_once_v2` + sink Iceberg idempotente (MERGE por `trade_id`).
2. **Correção sob desordem** — candles fecham por watermark/grace; late trades vão para `*.dlq`. Grace calibrado pelo p99 do skew medido.
3. **Reprodutibilidade** — backtest sobre o estado exato de uma data via Iceberg time-travel.
4. **Resiliência a gaps** — reconnect + circuit breaker + detecção de gap de sequência (`update_id`) no order book.

## Stack e justificativas

### Barramento de eventos
- **Redpanda** (API Kafka-compatível) — single binary, leve, custo zero local; Schema Registry embutido (API Confluent-compatível). Alternativa Kafka+KRaft é mais pesada para dev solo.
- **Avro** + Schema Registry — schema evolution first-class; payload carrega só o schema ID. Política **BACKWARD** por subject; `.avsc` em `contracts/`, validado no CI.
- Tópicos: `trades.raw`, `orderbook.delta`, `candles.m1/m5/h1`, `events.anomaly`, `*.dlq`. **Partition key = símbolo** (ordering por ativo; atenção a hot partition em BTC/ETH).

### Ingestão (producer) — Marco 1
- **Python** (`websockets`, `confluent-kafka`) consumindo WebSocket de Binance + Coinbase.
- Idempotent producer; reconnect + circuit breaker; detecção de gap de sequência no order book.
- Normaliza tickers de exchange → símbolo canônico via `pulso-domain` (seed CSV).

### Stream processing — Marco 2
- **ksqlDB** (sobre Kafka Streams) — SQL declarativo; janelamento + grace nativos; queries versionadas em `ksqldb/`.
- `processing.guarantee=exactly_once_v2`.
- OHLC candles (**tumbling** 1m/5m/1h), VWAP, volatilidade rolling (**hopping**), por **event time**.
- Late data: grace period + side output para `*.dlq`. Pull queries servem o estado live.
- Escape hatch: Kafka Streams (Java) para lógica que ksqlDB não expressa.

> Honestidade técnica: para event-time/state complexo, **Flink** é mais robusto. Como o escopo (candles em janela) cabe em ksqlDB e escolhemos Kafka, ksqlDB é o pragmático, com Flink SQL como caminho de evolução.

### Lakehouse e serving — Marcos 3-4
- **Apache Iceberg** sobre object storage (MinIO local / **GCS** prod) — ACID, time-travel, schema evolution, multi-engine.
- Sink Kafka→Iceberg via **PyIceberg** (mesma stack do producer, testável offline; Kafka Connect ficou como caminho de evolução para alta escala de escrita), **idempotente** — MERGE por chave de negócio + offset Kafka no snapshot.
- Manutenção: `expire_snapshots` (PyIceberg) + `rewrite_data_files` (compactação via Trino) agendados.
- **Trino** (histórico/BI) + **DuckDB** (dev) sobre o mesmo dado.
- **dbt** (Trino/prod, DuckDB/dev) → `fct_trade`, `fct_candle`, `dim_symbol` + testes.
- **FastAPI** (live via pull queries + histórico via Trino, push por WebSocket) + **React/Vite/TS**.

### Governança — Marco 5 (o diferenciador)
- **Data contracts** — Avro + Schema Registry, compat check no CI bloqueia quebra.
- **Qualidade** — dbt tests + Great Expectations/Soda: preço>0, OHLC consistente (low ≤ open,close ≤ high), sem gap de minuto.
- **Lineage** — OpenLineage → Marquez, end-to-end (producer → Kafka → Iceberg → dbt → API).
- **Observability (5 pilares)** — freshness, volume, distribution, schema, lineage. + freshness emitter (espelha o Mapear).
- **Métrica #1 de streaming** — consumer lag + throughput + latência e2e (event-time→served) via Prometheus.
- **SLO/SLI** — freshness SLO, lag error budget, latência p99.

### Infra, IaC, CI/CD
- Local: **docker-compose** (Redpanda + Console + ksqlDB + MinIO + Trino + Marquez + Postgres).
- Cloud: **Terraform** / **GCP** (GCS para Iceberg, Cloud Run, Artifact Registry).
- **GitHub Actions**: schema compat (offline no CI; live local), ruff, import-linter, dbt test.
- **uv** monorepo + import-linter (camadas `domain ◄ infra ◄ {ingest,storage}`).

## Camadas de dados (medallion sobre streaming)

```
trades.raw / orderbook.delta   (Kafka/Avro, fonte da verdade — replay)
        │  ksqlDB (exactly_once_v2, event-time, grace)
        ▼
candles.m1/m5/h1, vwap, vol     (Kafka; pull queries servem o estado live)
        │  sink idempotente (MERGE por trade_id)
        ▼
Iceberg: bronze (trades) → silver (candles) → gold (dbt: fct_/dim_)
        │  Trino / DuckDB
        ▼
FastAPI (live: ksqlDB pull · histórico: Trino)  →  React dashboard
```

## Roadmap por marcos

Critério de "feito" = roda no docker-compose + tem teste + tem o item de observabilidade correspondente. Check-in ao fim de cada marco.

- **Marco 0 — Fundação** ✅: exchanges/símbolos (seed CSV), uv workspace + import-linter, docker-compose, contratos Avro + CI compat check, README + esta proposta.
- **Marco 1 — Ingestão**: producer WebSocket idempotente, reconnect/circuit breaker/gap detection, métricas Prometheus.
- **Marco 2 — Stream processing** ✅: ksqlDB OHLC/VWAP/vol, event-time, grace + DLQ, exactly_once_v2, pull queries, testes de topologia (`ksql-test-runner`). SQL versionado em `ksqldb/`; candles via `EMIT FINAL` (só janelas seladas no tópico); volatilidade e estado live de janela aberta via pull query.
- **Marco 3 — Lakehouse** ✅: sink idempotente PyIceberg → Iceberg `bronze.trades`/`silver.candles`, MERGE por chave de negócio + offset Kafka no snapshot (exactly-once), particionamento `day + symbol/interval`, `expire_snapshots`, time-travel. Catálogo SQL (Postgres) multi-engine. SQL/código versionado em `libs/pulso-storage/`; testes offline via catálogo sqlite (`pytest`).
- **Marco 4 — Modelagem e serving** ✅: dbt marts (staging/intermediate/marts, DuckDB dev + Trino prod), espelho Iceberg→DuckDB (`mirror_to_duckdb`), FastAPI em `apps/dashboard/api/` (histórico + live pull-query ksqlDB + push WebSocket, `/metrics` Prometheus), dashboard React Vite+TS em `apps/dashboard/web/`. Rodar: `make dbt-build && make serve`.
- **Marco 5 — Governança**: contracts completos no CI, GE/Soda, OpenLineage→Marquez, 5 pilares + freshness emitter + alertas + SLO de lag.
- **Marco 6 — Reprocessamento (Kappa)**: replay com nova lógica, backfill, backtest reprodutível, runbook.
- **Marco 7 — Eixo B (LLM)**: ingestão de notícias + detector de anomalia → explicação LLM por evento.
- **Marco 8 — Cloud (GCP)**: Terraform, GCS, Cloud Run, análise de custo explícita.

## Estrutura do repositório

```
Pulso/
├── contracts/        # .avsc versionados (data contracts) + compat check
├── libs/
│   ├── pulso-domain/   # símbolos, exchanges, seed CSV (fonte de verdade)
│   ├── pulso-infra/    # config, logging, retry, circuit breaker, métricas
│   ├── pulso-ingest/   # WebSocket clients, producer idempotente, gap detection (M1)
│   └── pulso-storage/  # writers Iceberg, dedup/MERGE, time-travel (M3)
├── ksqldb/           # queries .sql versionadas (M2)
├── dbt/              # staging → intermediate → marts (M4)
├── apps/dashboard/   # FastAPI + React/Vite (M4)
├── governance/       # GE/Soda, OpenLineage, SLOs (M5)
├── services/         # freshness-emitter, anomaly-detector, llm-explainer (M5/M7)
├── infra/            # Terraform GCP (M8)
├── tools/            # check_schema_compat.py
├── .github/workflows/
└── docker-compose.yml
```

## Decisões travadas

- **Nome:** Pulso. **Cloud:** GCP. **Exchanges:** Binance + Coinbase. **Símbolos:** BTC, ETH, SOL, XRP, DOGE (ver `libs/pulso-domain/seeds/symbols.csv`).
- **LLM:** fora do MVP (Marco 7).
