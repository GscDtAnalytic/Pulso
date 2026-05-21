.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help up up-lineage down logs install lint test schema-check schema-check-offline check ksql-test ksql-apply sink iceberg-maintain lake-mirror dbt-seed-sync dbt-build dbt-test serve soda-check freshness-check replay backtest anomaly-detector llm-explainer

help: ## Lista os targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

up: ## Sobe a stack local (Redpanda+ksqlDB+MinIO+Trino+Postgres)
	docker compose up -d

up-lineage: ## Sobe stack + Marquez/OpenLineage (UI :3000, API :5000)
	docker compose --profile lineage up -d

down: ## Derruba a stack local
	docker compose down

logs: ## Tail dos logs da stack
	docker compose logs -f --tail=100

install: ## Instala o workspace uv (um .venv para tudo)
	uv sync --all-extras --dev

lint: ## ruff + import-linter (contratos de camada)
	uv run ruff check .
	uv run lint-imports

test: ## Testes
	uv run pytest

schema-check: ## Valida contratos Avro contra o Schema Registry (BACKWARD)
	uv run python tools/check_schema_compat.py

schema-check-offline: ## Valida so a parseabilidade dos .avsc (sem SR)
	uv run python tools/check_schema_compat.py --offline

check: lint test schema-check-offline ## Suite local rapida (pre-commit/CI)

ksql-test: ## Testes de topologia ksqlDB (offline, via docker — precisa de docker)
	bash ksqldb/tests/run.sh

ksql-apply: ## Aplica os ksqldb/*.sql no ksqldb da stack local (precisa de `make up`)
	cat ksqldb/[0-9]*.sql | docker compose exec -T ksqldb ksql http://localhost:8088

sink: ## Roda o sink idempotente Kafka -> Iceberg bronze/silver (precisa de `make up`)
	uv run python -m pulso_storage

iceberg-maintain: ## Manutencao Iceberg: expire_snapshots nas tabelas do lake
	uv run python -m pulso_storage maintain

lake-mirror: ## Espelha o Iceberg para .duckdb (bridge dbt dev)
	uv run python -m pulso_storage mirror

dbt-seed-sync: ## Copia o seed de dominio para dbt/seeds/symbols.csv
	cp libs/pulso-domain/seeds/symbols.csv dbt/seeds/symbols.csv

dbt-build: lake-mirror dbt-seed-sync ## Espelha o lake e roda dbt build (dev target)
	uv run dbt build --project-dir dbt --profiles-dir dbt --target dev

dbt-test: ## Roda apenas os testes dbt (sem re-build dos modelos)
	uv run dbt test --project-dir dbt --profiles-dir dbt --target dev

serve: ## Sobe a API de serving (historico+live+WebSocket) em :8000
	uv run python -m pulso_serve

soda-check: ## Checks de qualidade Soda contra o lake DuckDB (requer make dbt-build)
	uv run soda scan -d pulso_dev -c governance/soda/datasource_dev.yml governance/soda/

freshness-check: ## Verifica SLO de freshness do lake (exit 1 se violado)
	uv run python services/freshness_emitter.py

replay: ## Kappa replay: reprocessa trades.raw+candles desde o início (precisa de `make up`)
	uv run python services/kappa_replay.py --from-beginning

backtest: ## Lista snapshots Iceberg e prova reproducibilidade via time-travel
	uv run python services/backtest.py --list-snapshots

anomaly-detector: ## Sobe o detector de anomalias em candles.m1 (precisa de `make up`)
	uv run python services/anomaly_detector.py

llm-explainer: ## Sobe o explicador LLM (requer PULSO_ANTHROPIC_API_KEY e `make anomaly-detector`)
	uv run python services/llm_explainer.py
