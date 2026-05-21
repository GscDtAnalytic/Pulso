"""Espelho Iceberg -> DuckDB — ponte para o dbt rodar offline em dev (Marco 4).

O dbt do Pulso tem dois targets (`dbt/profiles.yml`):

- **prod** — Trino le as tabelas Iceberg direto do mesmo catalogo SQL do sink.
- **dev**  — DuckDB. DuckDB nao fala o catalogo SQL Iceberg; este modulo materializa
  `bronze.trades` / `silver.candles` num arquivo `.duckdb` que o dbt abre como
  *sources*. O `dbt build` de dev (e o teste offline no `pytest`) roda 100% local,
  sem broker nem MinIO — mesma filosofia do catalogo sqlite do Marco 3.

Nao e um segundo lugar de verdade: e uma copia descartavel, refeita a cada
`make dbt-build`. A fonte da verdade continua sendo o lake Iceberg.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from loguru import logger
from pyiceberg.catalog import Catalog

from pulso_storage.tables import ALL_SPECS, TableSpec


def mirror_to_duckdb(
    catalog: Catalog,
    db_path: str,
    specs: tuple[TableSpec, ...] = ALL_SPECS,
) -> dict[str, int]:
    """Copia cada tabela Iceberg para `db_path` como `<namespace>.<name>`.

    Devolve `{identifier: num_linhas}`. Tabela do lake ainda inexistente e
    ignorada (warn) — em dev o sink pode nao ter rodado para as duas camadas.
    `CREATE OR REPLACE`: o espelho e idempotente e sempre reflete o estado atual.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    con = duckdb.connect(db_path)
    try:
        for spec in specs:
            if not catalog.table_exists(spec.identifier):
                logger.warning("Tabela {} ausente no lake — pulada no espelho.", spec.identifier)
                continue
            arrow = catalog.load_table(spec.identifier).scan().to_arrow()
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{spec.namespace}"')
            con.register("_mirror_src", arrow)
            con.execute(
                f'CREATE OR REPLACE TABLE "{spec.namespace}"."{spec.name}" AS '
                "SELECT * FROM _mirror_src"
            )
            con.unregister("_mirror_src")
            counts[spec.identifier] = arrow.num_rows
            logger.info("Espelhado {} -> {} ({} linhas).", spec.identifier, db_path, arrow.num_rows)
    finally:
        con.close()
    return counts
