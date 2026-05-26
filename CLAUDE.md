# Pulso — guia para o agente

Plataforma de analytics de mercado cripto em **tempo real**. Streaming-first. Leia `README.md` para o resumo e o desenho completo.

## Princípios não-negociáveis

1. **Correção antes de features.** Toda agregação é por **event time** com watermark/grace e política de late data explícita. Nunca janelar por processing time onde correção importa.
2. **Exactly-once é desenhado, não assumido.** Producer idempotente + ksqlDB `exactly_once_v2` + sink idempotente (MERGE por `trade_id`). Defina a garantia por pipeline.
3. **Schema é contrato.** Mudou um `.avsc`? Tem que passar no `check_schema_compat.py` (BACKWARD). Campo novo sempre com `default`. Nunca JSON livre no barramento.
4. **Fail-loud.** Carga com zero linhas onde se espera N é erro, não sucesso. Consumer lag e freshness são métricas de primeira classe.
5. **Símbolos vêm do seed.** `libs/pulso-domain/seeds/symbols.csv` é a fonte de verdade única. Não hardcode tickers no código.

## Arquitetura de camadas (import-linter enforça)

```
pulso_ingest | pulso_storage | pulso_serve   (topo — orquestram)
        ▲
pulso_infra                    (config, logging, retry, métricas)
        ▲
pulso_domain                   (símbolos, exchanges — sem deps de infra)
```

Quebrar essa ordem faz `make lint` (import-linter) falhar. É proposital.

## Convenções

- **uv workspace**: um `.venv` para tudo. `make install` = `uv sync`. Rode comandos com `uv run`.
- **Python 3.11+**, type hints, ruff (E,F,I,UP,B,SIM), line-length 100.
- **dbt**: convenções `stg_<fonte>__<entidade>`, `int_<dominio>__<desc>`, `fct_<fato>`/`dim_<dim>`. Dois targets: DuckDB (dev), Trino (prod).
- **Tópicos Kafka**: `trades.raw`, `orderbook.delta`, `candles.m1/m5/h1`, `events.anomaly`, `<topic>.dlq`. Partition key = símbolo canônico.
- **Cloud = GCP** (GCS, Cloud Run, Artifact Registry). Em dev, MinIO substitui GCS (S3-compatível); a config (`PULSO_S3_ENDPOINT`) alterna.

## Antes de commitar

`make check` (lint + testes + contratos offline). Para validar compat ao vivo: `make up` e `make schema-check`.

## Status

Marco 3 (lakehouse) concluído: sink idempotente PyIceberg em `libs/pulso-storage/` —
consome `trades.raw`/`candles.m1/m5/h1` → Iceberg `bronze.trades`/`silver.candles`.
Exactly-once por **MERGE na chave de negócio** (`Table.upsert`, insert-if-not-exists)
+ offset Kafka gravado no snapshot Iceberg (resume sem replay). Particionamento
`day + symbol/interval`, `expire_snapshots`, time-travel. Catálogo SQL (Postgres em dev,
sqlite nos testes). Rodar: `make sink` (`/metrics` em `:8002`); manutenção:
`make iceberg-maintain`. Testes offline no `pytest`/`make check`. Ressalva: `upsert`
varre as partições do batch; compactação `rewrite_data_files` fica para o Trino (M4) —
ver `libs/pulso-storage/README.md`. Marco 2 (stream processing) concluído: SQL ksqlDB
versionado em `ksqldb/` — candles OHLCV m1/m5/h1 por event-time com `EMIT FINAL` (tópico
só carrega janelas seladas), volatilidade HOPPING e estado live de janela aberta via pull
query, DLQ de late data em `trades.raw.dlq`. Testes de topologia offline via
`ksql-test-runner` (`make ksql-test`); aplicar na stack: `make ksql-apply`. Ressalva: o
`ksql-test-runner` standalone não dispara `EMIT FINAL` — ver `ksqldb/README.md`. Marco 1
(ingestão): producer WebSocket idempotente (Binance+Coinbase) → `trades.raw`/`orderbook.delta`,
reconnect + circuit breaker + gap detection, métricas Prometheus em `:8001/metrics`
(`uv run python -m pulso_ingest`). **Marco 4 (modelagem dbt + serving) concluído**: dbt com
targets DuckDB (dev) e Trino (prod) — modelos `stg_*`, `int_*`, `fct_*`, `dim_*` em `dbt/`;
espelho Iceberg→DuckDB via `make lake-mirror` / `python -m pulso_storage mirror`; API FastAPI
em `apps/dashboard/api/` — histórico (`/api/candles`), estado live (`/api/candles/live` pull
query ksqlDB), push WebSocket (`/ws/candles`), `/metrics` Prometheus — rodar: `make serve`
(`:8000`). Dashboard React Vite+TS em `apps/dashboard/web/`. **Marco 5 (governança)
concluído**: OpenLineage→Marquez end-to-end (sink + producer; `make up-lineage`); Soda
quality checks em `governance/soda/` — OHLC invariant, gap de minuto M1, volume (`make
soda-check`); SLO YAML + Prometheus alerting rules em `governance/`; freshness emitter
standalone em `services/freshness_emitter.py` (`make freshness-check`; exit 1 se SLO
violado); CI `governance-check` valida YAML offline. 5 pilares: freshness ✅ volume ✅
distribution ✅ schema ✅ lineage ✅. **Marco 6 (reprocessamento Kappa) concluído**:
`services/kappa_replay.py` — `BoundedConsumer` para ao HWM do startup, grupo descartável,
`--from-beginning | --from-timestamp ISO | --pipeline trades|candles|all | --dry-run`;
`services/backtest.py` — time-travel Iceberg→DuckDB, analytics + invariantes OHLC/preço,
`--as-of | --snapshot-id | --list-snapshots`; `libs/pulso-storage/src/pulso_storage/replay.py`
— `is_caught_up` (puro, testável offline) + `ReplaySummary`; `RUNBOOK_KAPPA.md` — replay,
backfill, migração ksqlDB, rollback via snapshot Iceberg, checklist operacional; `make replay`
/ `make backtest`. 14 testes offline cobrem `is_caught_up`, throughput, reproducibilidade
do time-travel e idempotência do replay (cerne do Kappa). **Marco 7 (Eixo B LLM)
concluído**: `services/anomaly_detector.py` — rolling window por símbolo, detecta
PRICE_SPIKE/VOLUME_SPIKE/VOLATILITY_SPIKE (z-score + % change), publica em
`events.anomaly` (Avro), /metrics :8003 (`make anomaly-detector`);
`services/llm_explainer.py` — consome `events.anomaly`, busca notícias RSS (best-effort),
chama Claude API (Haiku, **prompt caching** no system prompt — uma chamada por evento,
nunca por trade), armazena em DuckDB `anomaly_explanations`, /metrics :8004 (`make
llm-explainer`); `contracts/anomaly.avsc` (BACKWARD enforçado em CI);
`pulso_serve.anomaly_store` compartilhado entre explainer e API; API `GET /api/anomalies`
(filter por symbol, 503 se store ausente — fail-loud); 24 testes offline cobrem rolling
window, detecção, AnomalyStore e funções puras; requer `PULSO_ANTHROPIC_API_KEY`.
**Marco 8 (Cloud GCP) concluído**: Terraform em `infra/terraform/` — GCS (lakehouse Iceberg +
bucket anomaly-db montado via GCS volume no Cloud Run v2), Artifact Registry, Cloud SQL Postgres
(catálogo JDBC Iceberg compartilhado com Trino), Secret Manager, IAM service accounts por serviço,
Workload Identity Federation para GitHub Actions (sem chave JSON); 5 Cloud Run v2 services
(`pulso-ingest`, `pulso-sink`, `pulso-anomaly-detector`, `pulso-llm-explainer`, `pulso-serve`);
`Dockerfile` multi-stage uv workspace (imagem única, CMD sobrescrito por serviço no Cloud Run);
`.github/workflows/deploy.yml` (dispara após ci.yml, build+push AR+update Cloud Run via gcloud);
`infra/COST_ANALYSIS.md` (~$40/mês); `pyiceberg[gcs]` + `gs://` branch no `catalog.py` (ADC);
`PORT` env var no `pulso-serve`; `make tf-init | tf-plan | tf-apply | docker-build | docker-push`.
Ver `infra/terraform/README.md`. **Projeto completo — todos os marcos entregues.**

## O que NÃO fazer

- Não hardcodar símbolos/exchanges fora do seed.
- Não introduzir LLM no caminho de dados quente — LLM é só Eixo B (Marco 7), explicador, uma chamada por evento.
- Não janelar por processing time em agregação analítica.
- Não escrever no barramento sem schema Avro registrado.
- Não quebrar a ordem de camadas do import-linter.
