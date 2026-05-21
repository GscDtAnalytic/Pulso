# Pulso — guia para o agente

Plataforma de analytics de mercado cripto em **tempo real**. Streaming-first. Leia `ARCHITECTURE_PROPOSAL.md` para o desenho completo e `README.md` para o resumo.

## Princípios não-negociáveis

1. **Correção antes de features.** Toda agregação é por **event time** com watermark/grace e política de late data explícita. Nunca janelar por processing time onde correção importa.
2. **Exactly-once é desenhado, não assumido.** Producer idempotente + ksqlDB `exactly_once_v2` + sink idempotente (MERGE por `trade_id`). Defina a garantia por pipeline.
3. **Schema é contrato.** Mudou um `.avsc`? Tem que passar no `check_schema_compat.py` (BACKWARD). Campo novo sempre com `default`. Nunca JSON livre no barramento.
4. **Fail-loud.** Carga com zero linhas onde se espera N é erro, não sucesso. Consumer lag e freshness são métricas de primeira classe.
5. **Símbolos vêm do seed.** `libs/pulso-domain/seeds/symbols.csv` é a fonte de verdade única. Não hardcode tickers no código.

## Arquitetura de camadas (import-linter enforça)

```
pulso_ingest | pulso_storage   (topo — orquestram)
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
(`uv run python -m pulso_ingest`). Próximo: Marco 4 (modelagem dbt + serving FastAPI/React).
Ver roadmap em `ARCHITECTURE_PROPOSAL.md`.

## O que NÃO fazer

- Não hardcodar símbolos/exchanges fora do seed.
- Não introduzir LLM no caminho de dados quente — LLM é só Eixo B (Marco 7), explicador, uma chamada por evento.
- Não janelar por processing time em agregação analítica.
- Não escrever no barramento sem schema Avro registrado.
- Não quebrar a ordem de camadas do import-linter.
