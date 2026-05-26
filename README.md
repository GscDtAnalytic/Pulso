# Pulso

> Uma plataforma que acompanha o mercado de criptomoedas **em tempo real** — captando cada negócio assim que ele acontece, transformando esse fluxo em informação confiável e explicando, em linguagem humana, os momentos em que o mercado se move fora do normal.

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)]() [![uv workspace](https://img.shields.io/badge/uv-workspace-purple)]() [![Kafka](https://img.shields.io/badge/bus-Redpanda%2FKafka-black)]() [![ksqlDB](https://img.shields.io/badge/stream-ksqlDB-orange)]() [![Iceberg](https://img.shields.io/badge/lakehouse-Iceberg-blue)]() [![GCP](https://img.shields.io/badge/cloud-GCP-4285F4)]() [![License](https://img.shields.io/badge/license-MIT-green)]()

---

## A ideia, em linguagem simples

Imagine que, a cada segundo, milhares de pessoas compram e vendem Bitcoin, Ethereum e outras moedas digitais em bolsas espalhadas pelo mundo. Esse vai-e-vem gera um **rio contínuo de informação** — rápido demais para qualquer pessoa acompanhar a olho nu.

O **Pulso** se conecta a esse rio. Ele:

1. **Escuta** cada negócio no instante em que acontece, direto nas bolsas (Binance e Coinbase).
2. **Organiza** essa enxurrada em resumos úteis — por exemplo, o preço de abertura, de fechamento, o máximo e o mínimo de cada minuto (os famosos "candles" dos gráficos financeiros).
3. **Guarda** tudo de forma segura e auditável, para que se possa voltar no tempo e revisitar exatamente como o mercado estava em qualquer momento do passado.
4. **Mostra** o resultado num painel ao vivo, que atualiza sozinho conforme o mercado se move.
5. **Avisa e explica** quando algo estranho acontece — um salto de preço, uma explosão de volume — usando inteligência artificial para escrever, em poucas frases, o que aconteceu e o possível contexto.

O resultado é uma sala de controle do mercado cripto: você abre o painel e enxerga o pulso do mercado batendo em tempo real.

---

## Que valor isso entrega

| Para quem | O que ganha |
|---|---|
| **Analista / trader** | Candles e indicadores ao vivo, sem depender de um terminal de terceiros, e a capacidade de testar uma estratégia sobre dados históricos *exatos*. |
| **Time de dados** | Um exemplo completo de pipeline de streaming feito do jeito certo — dados que nunca se perdem, nunca se duplicam e sempre podem ser reprocessados. |
| **Quem toma decisão** | Alertas explicados em linguagem natural: em vez de "z-score 4.2 no volume", lê "o volume de SOL triplicou nos últimos 3 minutos, possivelmente ligado a [notícia]". |
| **Custo** | Roda 100% na sua máquina de graça para desenvolvimento; na nuvem, opera por cerca de **US$ 40/mês**. |

### O que torna o Pulso diferente de um "dashboard de cripto qualquer"

A internet está cheia de tutoriais que pegam um preço e desenham um gráfico. O Pulso foi construído para resolver os problemas difíceis que aparecem quando os dados são *de verdade*:

- **Nenhum negócio é contado duas vezes, nem some.** Mesmo que a conexão caia ou o sistema reinicie, o número final é o mesmo. (Em linguagem técnica: *exactly-once*.)
- **A ordem caótica do mundo real é tratada.** Negócios que chegam atrasados não são ignorados em silêncio — são identificados e tratados à parte.
- **Dá para voltar no tempo.** Qualquer análise pode ser refeita sobre a "fotografia" exata dos dados de uma data passada.
- **Quando algo quebra, o sistema grita.** Atrasos e falhas viram alarmes, não surpresas descobertas tarde demais.

---

## Como funciona, passo a passo

```
   As bolsas (Binance, Coinbase)
   transmitem cada negócio ao vivo
              │
              ▼
   [1] CAPTAÇÃO      → um "ouvinte" se conecta e recebe cada trade
              │
              ▼
   [2] FILA SEGURA   → tudo entra numa esteira ordenada e à prova de perda
              │
              ▼
   [3] RESUMO AO VIVO → o fluxo bruto vira candles (1min, 5min, 1h),
                        volume e volatilidade — no instante certo
              │
              ▼
   [4] ARQUIVO        → tudo é gravado num "lago de dados" que permite
                        voltar no tempo e auditar
              │
              ▼
   [5] PAINEL + IA    → gráficos ao vivo na tela + alertas explicados por IA
```

Cada etapa é desenhada para que, se a anterior falhar e voltar, nada se perca e nada se duplique.

---

## A parte técnica

A partir daqui, o foco é em **como** isso foi construído e nas decisões de engenharia.

### Em uma linha

Pipeline de dados *streaming-first* que consome trades e order book de exchanges cripto via WebSocket, agrega candles OHLCV por **event time** com correção formal (exactly-once, watermarks, late data → DLQ), materializa um **lakehouse Iceberg** auditável com time-travel, serve via **API + dashboard ao vivo** e fecha com uma camada de **governança** (data contracts, lineage, quality, SLOs) e um **explicador LLM** de anomalias.

> É a peça de *streaming* do portfólio, irmã do [Mapear-RN](https://github.com/Mapear-Data/Mapear-RN), que cobre o lado *batch*.

### Os 4 pilares de correção

| Pilar | Como é garantido |
|---|---|
| **Exactly-once de verdade** | Producer idempotente + ksqlDB `exactly_once_v2` + sink Iceberg idempotente (MERGE por `trade_id`). Não é "at-least-once e reza". |
| **Correção sob desordem** | Candles fecham por **watermark/grace**; trades atrasados vão para DLQ (`*.dlq`) e podem reprocessar — nunca subcontam em silêncio. Grace calibrado pelo p99 do skew medido. |
| **Reprodutibilidade** | Backtest roda sobre o estado *exato* de uma data via **Iceberg time-travel**. |
| **Resiliência a gaps** | Reconnect + circuit breaker + **detecção de gap de sequência** (`update_id`) no order book. |

### Arquitetura

```
Binance / Coinbase WebSocket (trades + order book, grátis)
        │  producer Python (idempotente, gap detection)            [pulso-ingest]
        ▼
   Redpanda (Kafka API) + Schema Registry (Avro = data contract)
        │
        ▼
   ksqlDB → candles OHLCV (tumbling 1m/5m/1h), VWAP, volatilidade   [ksqldb/]
        │     event-time + grace + DLQ + exactly_once_v2 + EMIT FINAL
        ▼
   Iceberg lakehouse (bronze trades · silver candles · time-travel) [pulso-storage]
        │  Trino (prod) / DuckDB (dev) — dbt: fct_/dim_
        ▼
   FastAPI (live: ksqlDB pull · histórico: Trino · push: WebSocket) [pulso-serve]
        │
        ▼
   Dashboard Next.js + Tailwind  ·  alertas explicados por LLM
```

**Camada de governança** (transversal): data contracts (Avro/Schema Registry, validados em CI), qualidade (dbt tests + Soda), lineage (OpenLineage → Marquez), observability dos 5 pilares + consumer lag, SLOs com alert rules Prometheus.

**Eixo B — LLM** (isolado do caminho quente): detector de anomalias por rolling window publica em `events.anomaly`; um explicador chama a Claude API (Haiku, com *prompt caching*) **uma vez por evento, nunca por trade**, e grava a explicação para a API servir.

### Stack

| Camada | Tecnologia | Por quê |
|---|---|---|
| Barramento | **Redpanda** (Kafka API) | Single binary, custo zero local, Schema Registry embutido. |
| Contratos | **Avro + Schema Registry** | Schema = contrato versionado em git; compat BACKWARD no CI. |
| Ingestão | **Python** (websockets, confluent-kafka) | Producer idempotente; gap detection. |
| Stream | **ksqlDB** (sobre Kafka Streams) | Janelamento + grace em SQL; `exactly_once_v2`. |
| Lakehouse | **Apache Iceberg** (MinIO/GCS) | ACID, time-travel, schema evolution, multi-engine. |
| Query | **Trino** + **DuckDB** | Histórico (BI) e dev local sobre o mesmo dado. |
| Transformação | **dbt** | marts + testes; dois targets (Trino/DuckDB). |
| Serving | **FastAPI** + **Next.js/Tailwind/TS** | Candles ao vivo + histórico; storytelling de mercado. |
| Governança | **OpenLineage/Marquez, Soda, Prometheus** | Lineage, qualidade, observability, SLOs. |
| LLM | **Claude API (Haiku, prompt caching)** | Explicador de anomalias, 1 chamada/evento. |
| Infra | **docker-compose** (dev) · **Terraform/GCP** (prod) | Local custo zero; cloud ~US$40/mês. |

### Status — projeto completo

Todos os marcos entregues. Critério de "feito" = roda no docker-compose + tem teste + tem o item de observabilidade correspondente.

| Marco | Escopo | Status |
|---|---|---|
| **0 — Fundação** | uv workspace, docker-compose, contratos Avro, CI compat check, seed de símbolos | ✅ |
| **1 — Ingestão** | producer WebSocket idempotente, gap detection, métricas Prometheus | ✅ |
| **2 — Stream processing** | ksqlDB OHLCV/VWAP/volatilidade, event-time, grace + DLQ, exactly-once, pull queries | ✅ |
| **3 — Lakehouse** | sink idempotente → Iceberg bronze/silver, particionamento, time-travel | ✅ |
| **4 — Modelagem & serving** | dbt marts, FastAPI, dashboard | ✅ |
| **5 — Governança** | data contracts, Soda, lineage (Marquez), observability, SLOs | ✅ |
| **6 — Reprocessamento (Kappa)** | replay, backfill, backtest reprodutível, runbook | ✅ |
| **7 — Eixo B (LLM)** | detector de anomalias + explicador (1 chamada/evento) | ✅ |
| **8 — Cloud (GCP)** | Terraform, GCS, Cloud Run, Cloud SQL, análise de custo | ✅ |

### Como rodar localmente

**Pré-requisitos:** Docker + docker-compose, [`uv`](https://docs.astral.sh/uv/), `make`.

```bash
make up                 # sobe Redpanda + ksqlDB + MinIO + Trino + Marquez + Postgres
make install            # instala o workspace uv (um .venv para tudo)
make check              # lint + testes + validação de contratos (offline)
make schema-check       # valida compatibilidade Avro contra o Schema Registry ao vivo

# Ingestão — producer WebSocket → Kafka (idempotente)
uv run python -m pulso_ingest                    # Binance + Coinbase
uv run python -m pulso_ingest --exchange binance # uma exchange só

# Stream processing — ksqlDB (candles OHLCV, volatilidade, DLQ)
make ksql-test          # testes de topologia offline (ksql-test-runner)
make ksql-apply         # aplica ksqldb/*.sql na stack (após `make up`)

# Lakehouse — sink idempotente Kafka → Iceberg (bronze/silver)
make sink               # consome trades.raw + candles.* → Iceberg (MERGE idempotente)
make iceberg-maintain   # manutenção: expire_snapshots

# Serving — dbt marts + API + dashboard
make dbt-build          # marts dbt (DuckDB em dev)
make serve              # FastAPI :8000 (histórico, live, push WebSocket) + dashboard

# Governança e reprocessamento
make soda-check         # quality checks (OHLC invariant, gap de minuto, volume)
make freshness-check    # SLO de freshness (exit 1 se violado)
make replay             # reprocessamento Kappa (replay do log)
make backtest           # análise reprodutível via Iceberg time-travel

# Eixo B — LLM (requer PULSO_ANTHROPIC_API_KEY)
make anomaly-detector   # detecta PRICE/VOLUME/VOLATILITY_SPIKE → events.anomaly
make llm-explainer      # explica anomalias via Claude API → DuckDB → API
```

**UIs e métricas locais:** Redpanda Console `:8080` · MinIO `:9001` · Trino `:8085` · ksqlDB `:8088` · API/dashboard `:8000` · Marquez `:3000` (`make up-lineage`). Métricas Prometheus: producer `:8001`, sink `:8002`, anomaly-detector `:8003`, llm-explainer `:8004`.

Detalhes por componente: stream processing em [`ksqldb/README.md`](ksqldb/README.md); lakehouse (exactly-once, particionamento, time-travel) em [`libs/pulso-storage/README.md`](libs/pulso-storage/README.md); reprocessamento e rollback em [`RUNBOOK_KAPPA.md`](RUNBOOK_KAPPA.md); cloud em [`infra/terraform/README.md`](infra/terraform/README.md) e custos em [`infra/COST_ANALYSIS.md`](infra/COST_ANALYSIS.md).

### Estrutura do repositório

```
Pulso/
├── contracts/        # .avsc versionados (data contracts) + compat check
├── libs/
│   ├── pulso-domain/   # símbolos, exchanges, seed CSV (fonte de verdade)
│   ├── pulso-infra/    # config, logging, retry, circuit breaker, métricas
│   ├── pulso-ingest/   # WebSocket clients, producer idempotente, gap detection
│   ├── pulso-storage/  # writers Iceberg, dedup/MERGE, time-travel, replay
│   └── pulso-serve/    # API, anomaly store
├── ksqldb/           # queries .sql versionadas (candles, volatilidade)
├── dbt/              # staging → intermediate → marts
├── apps/dashboard/   # FastAPI + Next.js/Tailwind
├── governance/       # Soda, OpenLineage, SLOs + Prometheus rules
├── services/         # freshness-emitter, kappa_replay, backtest, anomaly-detector, llm-explainer
├── infra/            # Terraform GCP
├── tools/            # check_schema_compat.py
├── .github/workflows/
└── docker-compose.yml
```

---

## Licença

MIT.
