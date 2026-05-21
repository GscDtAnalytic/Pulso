# Handoff — Marco 4 (Modelagem & Serving) — EM ANDAMENTO

Sessão interrompida em ~93% de uso. Marco 4 = **dbt marts + FastAPI + dashboard React**.
A camada dbt e a maior parte da API já existem; falta fechar a API, fazer o React,
o cabeamento de workspace/infra, os testes e a documentação.

## ✅ O que já foi implementado nesta sessão

### Config & storage
- `libs/pulso-infra/.../config.py` — bloco de config de serving (Marco 4): `serve_host`,
  `serve_port`, `serve_history_backend`, `serve_candle_topic`, `dbt_duckdb_path`,
  `dbt_lake_catalog`, `dbt_marts_schema`, `ksqldb_url`, `trino_host/port/user`.
- `libs/pulso-storage/.../duckdb_mirror.py` **(novo)** — `mirror_to_duckdb()`: espelha as
  tabelas Iceberg para um arquivo `.duckdb` (ponte para o dbt de dev rodar offline).
- `libs/pulso-storage/.../__init__.py` — exporta `mirror_to_duckdb`.
- `libs/pulso-storage/.../app.py` — subcomando `mirror` (`python -m pulso_storage mirror`).

### dbt (`dbt/`) — completo
- `dbt_project.yml`, `profiles.yml` (dev=DuckDB, prod=Trino), `README.md`.
- `seeds/symbols.csv` (cópia do seed de domínio) + `seeds/_seeds.yml`.
- `macros/test_unique_combination.sql` (teste genérico próprio — chave composta).
- staging: `stg_bronze__trades.sql`, `stg_silver__candles.sql`, `_staging__sources.yml`,
  `_staging__models.yml`.
- intermediate: `int_market__candles_enriched.sql` (ephemeral) + `_intermediate__models.yml`.
- marts: `dim_symbol.sql`, `fct_trade.sql`, `fct_candle.sql`, `fct_symbol_daily.sql` +
  `_marts__models.yml`.
- testes singulares: `tests/assert_fct_candle_ohlc_consistent.sql`,
  `assert_fct_trade_positive_values.sql`, `assert_fct_symbol_daily_ohlc_consistent.sql`.

### FastAPI (`apps/dashboard/api/`) — parcial
Pacote `pulso-serve` em `apps/dashboard/api/src/pulso_serve/`. Prontos:
- `pyproject.toml`, `models.py`, `metrics.py`, `store.py`, `ksql.py`, `live.py`, `routes.py`.

## ⛔ O que FALTA (ordem sugerida)

### 1. Fechar a API
- `apps/dashboard/api/src/pulso_serve/__init__.py` (docstring + exports).
- `apps/dashboard/api/src/pulso_serve/__main__.py` (`from pulso_serve.app import main`).
- `apps/dashboard/api/src/pulso_serve/app.py` — `create_app(settings, store, ksql, stream)`:
  - lifespan que cria `asyncio.create_task(CandleBroadcaster(manager, stream).run())` e
    cancela no shutdown;
  - `app.state.{settings,store,ksql,manager}`;
  - `CORSMiddleware` (allow `*`);
  - middleware HTTP de métricas — usar `request.scope.get("route").path` como label
    `route` (baixa cardinalidade, não a URL crua);
  - `app.include_router(router)`; `app.mount("/metrics", make_asgi_app())`;
  - `main()` monta deps reais (`build_store`, `KsqlClient`, `kafka_candle_stream`) e
    `uvicorn.run`.

### 2. Cabeamento de workspace / infra
- Root `pyproject.toml`: incluir `apps/dashboard/api` em `[tool.uv.workspace] members`;
  adicionar `pulso-serve` em `dependencies` e `[tool.uv.sources]`. Adicionar ao
  **dev group**: `dbt-core`, `dbt-duckdb` (e opcional `dbt-trino`) — o teste de marts
  roda `dbt build` no `pytest`.
- `.importlinter`: adicionar `pulso_serve` em `root_packages` e na camada de topo
  (`pulso_ingest | pulso_storage | pulso_serve`). `pulso_serve` só importa
  `pulso_domain` + `pulso_infra`.
- `infra/trino/catalog/iceberg.properties` **(novo)** — conector `iceberg`,
  `iceberg.catalog.type=jdbc` apontando para o Postgres do compose + MinIO.
- `docker-compose.yml` — montar esse arquivo no serviço `trino`
  (`/etc/trino/catalog/iceberg.properties`).
- `Makefile` — alvos: `lake-mirror`, `dbt-build` (mirror → `dbt build`), `dbt-test`,
  `dbt-seed-sync` (copia o seed de domínio), `serve` (`uv run python -m pulso_serve`).
- `.gitignore` — `dbt/target/`, `dbt/logs/`, `dbt/dbt_packages/`, `dbt/pulso_lake.duckdb`,
  `node_modules/`, `dist/`.

### 3. Dashboard React (`apps/dashboard/web/`) — NÃO iniciado
Vite + React + TS. `package.json`, `tsconfig*.json`, `vite.config.ts` (proxy `/api` e
`/ws` → `:8000`), `index.html`, `src/` (main, App, `api.ts`, `types.ts`, hooks
`useCandles`/`useLiveCandles` (WebSocket), componentes `SymbolPicker`/`CandleChart`
(usar `lightweight-charts`)/`StatsPanel`), `src/lib/format.ts` (+ teste vitest),
`README.md`. Não rodar `npm install` (sem rede garantida) — entregar o código e
documentar. `apps/dashboard/README.md` com visão geral API+web.

### 4. Testes (`tests/`)
- `test_dbt_marts.py`: monta lake Iceberg minúsculo (fixture `iceberg_catalog` de
  `conftest.py` + `IcebergSink`), `mirror_to_duckdb` para path tmp, `PULSO_DBT_DUCKDB_PATH`
  **absoluto**, `dbt.cli.main.dbtRunner().invoke(["build","--project-dir","dbt",
  "--profiles-dir","dbt","--target","dev"])`, assert `res.success`. Mais
  `test_dbt_seed_matches_domain_seed` (seed dbt == seed de domínio).
- `test_serve_store.py`: duckdb tmp com `analytics.fct_candle`/`fct_symbol_daily`
  fixados via SQL; `MarketStore`; checar ordenação cronológica e `StoreUnavailable`.
- `test_serve_api.py`: `TestClient` com store/ksql falsos em `app.state`; checar
  endpoints, 404 (símbolo), 503 (backend fora).
- `test_serve_ksql.py`: `KsqlClient` com `httpx.MockTransport` simulando o array v1.
- `test_serve_live.py`: `ConnectionManager.broadcast` com WebSockets falsos;
  `CandleBroadcaster` com stream falso.

### 5. Documentação
- `CLAUDE.md` (seção Status), `README.md` (tabela de roadmap: Marco 4 → ✅),
  `ARCHITECTURE_PROPOSAL.md` (roadmap: Marco 4 ✅).

### 6. Validar
`make check` (lint + test + schema-offline). Depois `make dbt-build` e `make serve`
com a stack de pé, se possível.

## ⚠️ Decisões e riscos a lembrar

- **dbt offline:** sem pacotes externos (sem `dbt_utils`) → sem `dbt deps` → `dbt build`
  roda 100% offline no `pytest`. O teste genérico de chave composta é a macro própria
  `unique_combination`.
- **Dois targets, uma SQL:** dev=DuckDB lê o `.duckdb` espelhado; prod=Trino lê o
  Iceberg direto. Var `lake_catalog` (default `pulso_lake`; prod:
  `--vars '{lake_catalog: iceberg}'`). Tudo materializa no schema `analytics`.
- **RISCO — coluna `interval`:** `interval` é palavra reservada de tipo no DuckDB.
  Os modelos dbt hoje referenciam `interval` sem aspas (`stg_silver__candles.sql`,
  `int_market__candles_enriched.sql`, `fct_*`). **Verificar no primeiro `dbt build`**;
  se quebrar, citar como `"interval"`. O ksqlDB já usa crases para o mesmo motivo.
- **`pulso_serve`** só depende de `pulso_domain` + `pulso_infra` (camada de topo,
  irmã de ingest/storage). `/api/symbols` vem do seed de domínio, não da mart —
  sempre disponível.
- **API:** `limit` entra na SQL como inteiro validado (não `?`) porque o Trino não
  aceita `LIMIT ?`; `symbol`/`interval` são parâmetros. Falha de backend → 503.
- **`/metrics`** montado no próprio app FastAPI (`:8000`), não em porta separada
  (producer=8001, sink=8002).
- `make check` passará a depender de `dbt-core`+`dbt-duckdb` instalados — rodar
  `uv sync` após editar o `pyproject.toml`.

## Roadmap dos modelos dbt (referência)

```
bronze.trades  → stg_bronze__trades  → fct_trade
silver.candles → stg_silver__candles → int_market__candles_enriched → fct_candle
                                                                    → fct_symbol_daily
seed symbols   → dim_symbol
```
