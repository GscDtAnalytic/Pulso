# Pulso

> Plataforma de **analytics de mercado cripto em tempo real** — streaming-first, exactly-once, lakehouse aberto e governança. A peça de *streaming* do portfólio, irmã do [Mapear-RN](https://github.com/Mapear-Data/Mapear-RN) (batch).

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)]() [![uv workspace](https://img.shields.io/badge/uv-workspace-purple)]() [![Kafka](https://img.shields.io/badge/bus-Redpanda%2FKafka-black)]() [![ksqlDB](https://img.shields.io/badge/stream-ksqlDB-orange)]() [![Iceberg](https://img.shields.io/badge/lakehouse-Iceberg-blue)]() [![GCP](https://img.shields.io/badge/cloud-GCP-4285F4)]() [![License](https://img.shields.io/badge/license-MIT-green)]()

---

## O que é, em uma linha

Um pipeline de dados **em tempo real** que consome trades e order book de exchanges de cripto (Binance, Coinbase) via WebSocket, agrega candles OHLCV por *event time* com correção formal (exactly-once, watermarks, late data), materializa um lakehouse Iceberg auditável e serve via API + dashboard ao vivo.

> **Para quem é este README:** tech leads e recrutadores que querem entender em 3-5 minutos **o quê foi construído, com quais tecnologias e por quê** — com foco honesto nas decisões de correção que separam isto de um "dashboard de cripto de tutorial".

---

## Por que este projeto existe

O [Mapear-RN](https://github.com/Mapear-Data/Mapear-RN) já prova **batch maduro** (cron, BigQuery, dbt, Terraform, observabilidade pós-incidente, < R$5/mês). O Pulso ataca o hemisfério que falta — **streaming em tempo real com correção formal** — sobre o caso canônico de streaming: mercado financeiro.

**O que torna isto sênior, e não brinquedo** (os 4 pilares anti-toy):

| Pilar | Como |
|---|---|
| **Exactly-once de verdade** | idempotent producer + ksqlDB `exactly_once_v2` + sink Iceberg idempotente (MERGE por `trade_id`). Não "at-least-once e reza". |
| **Correção sob desordem** | candles fecham por **watermark/grace**; trades atrasados vão para DLQ e reprocessam — nunca subcontam em silêncio. |
| **Reprodutibilidade** | backtest roda sobre o estado exato de uma data via **Iceberg time-travel**. |
| **Resiliência a gaps** | reconnect + circuit breaker + **detecção de gap de sequência** no order book. O "incidente" é desenhado, não sofrido. |

---

## Arquitetura em 30 segundos

```
Binance / Coinbase WebSocket (trades + order book, grátis)
        │  producer Python (idempotente, gap detection)
        ▼
   Redpanda  (Kafka-compatível) + Schema Registry (Avro = data contract)
        │
        ▼
   ksqlDB  → OHLC candles (tumbling 1m/5m/1h), VWAP, volatilidade
        │     event-time + grace + DLQ + exactly_once_v2
        ▼
   Iceberg lakehouse  (bronze trades · silver candles · time-travel)
        │  Trino / DuckDB
        ▼
   FastAPI (live: ksqlDB pull · histórico: Trino)  →  React dashboard
```

Camada de **governança** transversal: data contracts (Avro/Schema Registry, validados em CI), qualidade (dbt tests + Great Expectations), lineage (OpenLineage → Marquez), observability dos 5 pilares + consumer lag.

---

## Stack

| Camada | Tecnologia | Por quê (resumo) |
|---|---|---|
| Barramento | **Redpanda** (Kafka API) | Single binary, custo zero local, Schema Registry embutido. |
| Contratos | **Avro + Schema Registry** | Schema = contrato versionado em git, compat BACKWARD no CI. |
| Ingestão | **Python** (websockets, confluent-kafka) | Producer idempotente; continuidade com o Mapear. |
| Stream | **ksqlDB** (sobre Kafka Streams) | Janelamento + grace em SQL; `exactly_once_v2`. |
| Lakehouse | **Apache Iceberg** (MinIO/GCS) | ACID, time-travel, schema evolution, multi-engine. |
| Query | **Trino** + **DuckDB** | Histórico (BI) e dev local sobre o mesmo dado. |
| Transformação | **dbt** | gold/marts + testes; dois targets (Trino/DuckDB). |
| Serving | **FastAPI** + **React/Vite/TS** | Candles ao vivo + histórico; heatmap de order book. |
| Governança | **Schema Registry, OpenLineage/Marquez, Great Expectations, Prometheus** | Contracts, lineage, qualidade, observability. |
| Infra | **docker-compose** (dev) · **Terraform/GCP** (prod) | Local custo zero; cloud com custo medido. |

Justificativa completa de cada escolha em [`ARCHITECTURE_PROPOSAL.md`](ARCHITECTURE_PROPOSAL.md).

---

## Status e roadmap

| Marco | Escopo | Status |
|---|---|---|
| **0 — Fundação** | uv workspace, docker-compose, contratos Avro, CI compat check, seed de símbolos | ✅ Em andamento |
| **1 — Ingestão** | producer WebSocket idempotente, gap detection, métricas | ⬜ |
| **2 — Stream processing** | ksqlDB OHLC/VWAP, event-time, grace + DLQ, exactly-once | ⬜ |
| **3 — Lakehouse** | sink idempotente → Iceberg, particionamento, time-travel | ⬜ |
| **4 — Modelagem & serving** | dbt marts, FastAPI, dashboard React | ⬜ |
| **5 — Governança** | data contracts, GE/Soda, lineage, observability, SLOs | ⬜ |
| **6 — Reprocessamento (Kappa)** | replay, backfill, backtest reprodutível | ⬜ |
| **7 — Eixo B (LLM)** | explicador de eventos anômalos (1 chamada/evento) | ⬜ |
| **8 — Cloud (GCP)** | Terraform, GCS, análise de custo | ⬜ |

---

## Como rodar localmente

**Pré-requisitos:** Docker + docker-compose, [`uv`](https://docs.astral.sh/uv/), `make`.

```bash
make up                 # sobe Redpanda + ksqlDB + MinIO + Trino + Marquez + Postgres
make install            # instala o workspace uv (um .venv para tudo)
make check              # lint + testes + validação de contratos (offline)
make schema-check       # valida compatibilidade Avro contra o Schema Registry ao vivo
```

UIs locais: Redpanda Console `:8080` · MinIO `:9001` · Trino `:8085` · ksqlDB `:8088`. Lineage (Marquez `:3000`) sobe sob demanda no Marco 5: `docker compose --profile lineage up -d`.

---

## Sobre o desenvolvimento

Construído em pair-programming com Claude Code (declarado abertamente). As decisões arquiteturais e os trade-offs são humanos e defendidos — o conhecimento de base vem de uma wiki de engenharia de dados destilada de ~68 livros técnicos.

## Licença

MIT.
