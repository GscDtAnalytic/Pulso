# Pulso

> 🌐 **Português** · [English](README.md)

> Uma plataforma que acompanha o mercado de criptomoedas **em tempo real**. Ela capta cada negócio assim que acontece, transforma esse fluxo em informação confiável e explica, em linguagem simples, os momentos em que o mercado se move fora do normal.

[![CI](https://github.com/GscDtAnalytic/Pulso/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GscDtAnalytic/Pulso/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/GscDtAnalytic/Pulso/branch/main/graph/badge.svg)](https://codecov.io/gh/GscDtAnalytic/Pulso) [![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)]() [![uv workspace](https://img.shields.io/badge/uv-workspace-purple)]() [![Kafka](https://img.shields.io/badge/bus-Redpanda%2FKafka-black)]() [![ksqlDB](https://img.shields.io/badge/stream-ksqlDB-orange)]() [![Iceberg](https://img.shields.io/badge/lakehouse-Iceberg-blue)]() [![GCP](https://img.shields.io/badge/cloud-GCP-4285F4)]() [![License](https://img.shields.io/badge/license-MIT-green)]()

---

## A ideia, em linguagem simples

A cada segundo, milhares de pessoas compram e vendem Bitcoin, Ethereum e outras moedas digitais em bolsas espalhadas pelo mundo. Essa atividade gera um fluxo contínuo de informação, rápido demais para qualquer pessoa acompanhar a olho nu.

O **Pulso** se conecta a esse rio. Ele:

1. **Escuta** cada negócio no instante em que acontece, direto nas bolsas (Binance e Coinbase).
2. **Organiza** esse volume em resumos úteis, como o preço de abertura, de fechamento, o máximo e o mínimo de cada minuto (os "candles" dos gráficos financeiros).
3. **Guarda** tudo de forma segura e auditável, para que se possa voltar no tempo e revisitar exatamente como o mercado estava em qualquer momento do passado.
4. **Mostra** o resultado num painel ao vivo, que atualiza sozinho conforme o mercado se move.
5. **Avisa e explica** quando algo estranho acontece, como um salto de preço ou um pico de volume, usando inteligência artificial para escrever em poucas frases o que aconteceu e o possível contexto.

O resultado é uma sala de controle do mercado cripto: você abre o painel e vê o estado atual do mercado em tempo real.

---

## Que valor isso entrega

| Para quem | O que ganha |
|---|---|
| **Analista / trader** | Candles e indicadores ao vivo, sem depender de um terminal de terceiros, e a capacidade de testar uma estratégia sobre dados históricos *exatos*. |
| **Time de dados** | Uma referência completa de pipeline de streaming em que os dados não se perdem nem se duplicam e sempre podem ser reprocessados. |
| **Quem toma decisão** | Alertas explicados em linguagem natural: em vez de "z-score 4.2 no volume", lê "o volume de SOL triplicou nos últimos 3 minutos, possivelmente ligado a [notícia]". |
| **Custo** | Roda 100% na sua máquina de graça para desenvolvimento; na nuvem, opera por cerca de **US$ 40/mês**. |

### O que torna o Pulso diferente de um "dashboard de cripto qualquer"

Muitos tutoriais apenas pegam um preço e desenham um gráfico. O Pulso foi construído para tratar os problemas mais difíceis que aparecem com dados de produção:

- **Nenhum negócio é contado duas vezes nem se perde.** Mesmo que a conexão caia ou o sistema reinicie, o número final permanece o mesmo (tecnicamente, exactly-once).
- **Dados fora de ordem são tratados.** Negócios que chegam atrasados não são ignorados em silêncio; são identificados e processados à parte.
- **O histórico pode ser reprocessado.** Qualquer análise pode ser refeita sobre o snapshot exato dos dados de uma data passada.
- **Falhas são visíveis.** Atrasos e erros são sinalizados como alertas, em vez de descobertos tarde.

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
                        volume e volatilidade, no instante certo
              │
              ▼
   [4] ARQUIVO        → tudo é gravado num "lago de dados" que permite
                        voltar no tempo e auditar
              │
              ▼
   [5] PAINEL + IA    → gráficos ao vivo na tela + alertas explicados por IA
```

Cada etapa é desenhada para que, se a anterior falhar e voltar, nenhum dado se perca ou se duplique.

---

## A parte técnica

A partir daqui, o foco é em como isso foi construído e nas decisões de engenharia por trás.

### Em uma linha

Pipeline de dados streaming-first que consome trades e order book de exchanges cripto via WebSocket, agrega candles OHLCV por **event time** com correção formal (exactly-once, watermarks, late data roteado para uma DLQ), materializa um lakehouse Iceberg auditável com time-travel e serve via API e dashboard ao vivo. Uma camada de governança (data contracts, lineage, quality, SLOs) e um explicador LLM de anomalias ficam ao lado do pipeline.

> É a contraparte de streaming do [Mapear-RN](https://github.com/Mapear-Data/Mapear-RN), que cobre o lado batch do portfólio.

### Os 4 pilares de correção

| Pilar | Como é garantido |
|---|---|
| **Exactly-once** | Producer idempotente + ksqlDB `exactly_once_v2` + sink Iceberg idempotente (MERGE por `trade_id`). |
| **Correção sob dados fora de ordem** | Candles fecham por watermark/grace; trades atrasados vão para DLQ (`*.dlq`) e podem reprocessar. O grace é calibrado pelo p99 do skew medido. |
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

### Decisões de engenharia (o "por quê")

| Decisão | Por que esta, e não a alternativa |
|---|---|
| **ksqlDB** em vez de Flink | A carga é agregação por janela (OHLCV), não um DAG arbitrário com estado. O ksqlDB oferece janelamento com grace em SQL e `exactly_once_v2` de fábrica, com bem menos operação que um cluster Flink. O trade-off é menos flexibilidade para processamento de eventos complexo; se a carga crescer além do SQL, Flink é o alvo de migração. |
| **Apache Iceberg** em vez de Delta/Hudi/Parquet puro | Formato de tabela aberto e multi-engine: as mesmas tabelas são lidas por Trino, DuckDB e PyIceberg sem cópia. Snapshots e time-travel tornam o backtest reprodutível, e o formato traz schema evolution, hidden partitioning e ausência de lock de fornecedor. O MERGE pela chave de negócio dá o sink idempotente. |
| **Avro + Schema Registry** em vez de JSON livre | O schema é um contrato versionado em git, com compat BACKWARD enforçada no CI, o que evita a deriva silenciosa entre producer e consumer. Nenhuma mensagem chega ao barramento sem schema registrado. |
| **Redpanda** em vez de Apache Kafka (dev) | Binário único, sem ZooKeeper/KRaft para operar, Schema Registry embutido. Compatível com a Kafka API, então prod troca para MSK/Confluent sem mudar código. |
| **Arquitetura Kappa** em vez de Lambda | Um único caminho de código de stream serve tempo real e reprocessamento, usando replay do log e time-travel Iceberg em vez de uma camada batch paralela. |
| **LLM fora do caminho quente** | A detecção de anomalia é determinística (z-score em rolling window). O LLM só explica eventos já selados, com uma chamada por evento e prompt caching, então fica fora do caminho de dados e tem custo limitado. |

### Metas de design e capacidade

> ℹ️ **Metas de design / capacidade arquitetural.** Descrevem o que o sistema foi construído para garantir, não benchmarks medidos em produção.

| Dimensão | Meta de design |
|---|---|
| **Correção** | 0 duplicatas e 0 perdas por construção (producer idempotente + `exactly_once_v2` + sink MERGE). |
| **Tolerância a late data (grace da janela)** | Calibrada por intervalo pelo p99 do skew medido — `m1 = 10 s`, `m5 = 30 s`, `h1 = 60 s` ([`ksqldb/10_candles.sql`](ksqldb/10_candles.sql)). Cada candle é selado e emitido uma vez via `EMIT FINAL`. |
| **SLOs de freshness / latência** | `bronze.trades` < 60 s atrás do event-time · end-to-end (event-time → candle servido) < 5 s · p99 da API < 2 s · consumer lag < 10 k registros/partição — codificados em [`governance/slo.yml`](governance/slo.yml), alertados em [`governance/prometheus_rules.yml`](governance/prometheus_rules.yml). |
| **Escala horizontal** | Particionamento por símbolo canônico (partition key); throughput escala adicionando partições/brokers. Em dev, um único broker Redpanda atende o stream público dos pares do seed. |
| **Custo em nuvem** | ~US$ 40/mês na GCP (5 Cloud Run, GCS, Cloud SQL) — ver [`infra/COST_ANALYSIS.md`](infra/COST_ANALYSIS.md). |
| **Custo de LLM** | Limitado a uma chamada Claude por evento de anomalia (não por trade), com prompt caching no system prompt. |

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

Todos os marcos entregues. Um marco é considerado feito quando roda no docker-compose, tem teste e tem o item de observabilidade correspondente.

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
