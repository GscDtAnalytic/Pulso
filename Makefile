.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help up down logs install lint test schema-check schema-check-offline check

help: ## Lista os targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

up: ## Sobe a stack local (Redpanda+ksqlDB+MinIO+Trino+Marquez+Postgres)
	docker compose up -d

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
