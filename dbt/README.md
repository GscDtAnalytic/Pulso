# dbt — modelagem analítica (Marco 4)

Transforma o lakehouse Iceberg (bronze/silver, Marco 3) na camada **gold**: marts
testadas que a API e o dashboard servem. Um modelo, **dois engines** — DuckDB em
dev, Trino em prod — sobre o mesmo dado (princípio multi-engine do projeto).

## Camadas

```
sources (Iceberg)        staging (view)              marts (table)
bronze.trades   ──▶  stg_bronze__trades   ──┐
                                            ├─▶ fct_trade
silver.candles  ──▶  stg_silver__candles ──┐│
                                           ││   int_market__candles_enriched
seed: symbols   ──────────────────────────┘│        │  (ephemeral)
                                            │        ├─▶ fct_candle
                              dim_symbol ◀──┘        └─▶ fct_symbol_daily
```

| Modelo | Camada | Grão | Materialização |
|---|---|---|---|
| `stg_bronze__trades` | staging | trade | view |
| `stg_silver__candles` | staging | candle selado | view |
| `int_market__candles_enriched` | intermediate | candle + métricas | ephemeral |
| `dim_symbol` | mart | símbolo | table |
| `fct_trade` | mart | trade | table |
| `fct_candle` | mart | candle `(symbol, interval, window_start)` | table |
| `fct_symbol_daily` | mart | `(symbol, trade_date)` | table |

Convenções de nome: `stg_<fonte>__<entidade>`, `int_<domínio>__<desc>`, `fct_`/`dim_`
(CLAUDE.md).

## Os dois targets

| Target | Engine | Sources lidas de | Quando |
|---|---|---|---|
| `dev` (default) | **DuckDB** | arquivo `.duckdb` espelhado do lake | dev local + testes |
| `prod` | **Trino** | conector `iceberg` (catálogo SQL do sink) | stack/cloud |

DuckDB não fala o catálogo SQL do Iceberg, então em dev o lake é **espelhado** para
um arquivo `.duckdb` antes do build (`python -m pulso_storage mirror` — uma cópia
descartável, refeita a cada run; a verdade continua no Iceberg). Em prod o Trino lê
o Iceberg direto. A SQL dos modelos é idêntica nos dois.

## Rodar

```bash
make dbt-build          # dev: espelha o lake -> DuckDB, depois `dbt build`
make dbt-test           # dev: só os testes (dbt test)

# prod (precisa de `make up` — Trino + catálogo iceberg):
cd dbt && dbt build --target prod --vars '{lake_catalog: iceberg}'
```

`dbt build` roda seed + modelos + **todos os testes** numa passada. O target `dev`
roda 100% offline (sem broker, sem MinIO) e é exercido no `pytest`/`make check` —
mesma filosofia do catálogo sqlite do Marco 3.

## Testes de dados

- **Genéricos** (nos `.yml`): `not_null`, `unique`, `accepted_values`,
  `relationships` (FKs `fct_*` → `dim_symbol`) e `unique_combination` (chave de
  negócio composta — macro própria em `macros/`, sem pacotes externos).
- **Singulares** (`tests/`): invariante OHLC (`low ≤ open,close ≤ high`) em
  `fct_candle` e `fct_symbol_daily`; `price/quantity > 0` em `fct_trade`.

A qualidade mais formal (Great Expectations/Soda, freshness) é o Marco 5; os
contratos de invariante já moram aqui.

## Sincronia do seed

`seeds/symbols.csv` é cópia fiel de `libs/pulso-domain/seeds/symbols.csv` (fonte de
verdade única — CLAUDE.md #5). `make dbt-seed-sync` recopia; o teste
`test_dbt_seed_matches_domain_seed` falha-loud se divergirem.
