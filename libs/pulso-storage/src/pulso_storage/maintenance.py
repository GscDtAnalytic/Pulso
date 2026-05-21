"""Manutencao das tabelas Iceberg (Marco 3).

O streaming gera muitos snapshots e arquivos pequenos; sem manutencao, o metadata
incha e a leitura degrada. `ARCHITECTURE_PROPOSAL.md` pede `rewrite_data_files` +
`expire_snapshots` agendados.

- **expire_snapshots** — implementado aqui via PyIceberg: descarta snapshots antigos
  (e os arquivos orfaos), mantendo a janela de time-travel util. Roda por `make
  iceberg-maintain` ou agendado (Cloud Scheduler no Marco 8).
- **rewrite_data_files** (compactacao bin-pack) — o PyIceberg 0.11 ainda nao expoe
  compactacao de dados. Honestidade tecnica (mesmo tom da nota Flink/ksql-test do
  projeto): a compactacao roda pelo Trino — `ALTER TABLE ... EXECUTE optimize` —
  quando o Trino entrar como engine de query no Marco 4. Nao e limitacao de desenho,
  e divisao de trabalho entre engines sobre o mesmo dado Iceberg.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from loguru import logger
from pyiceberg.catalog import Catalog

from pulso_storage.tables import ALL_SPECS


def expire_snapshots(catalog: Catalog, *, retain_hours: float = 168.0) -> None:
    """Expira snapshots mais antigos que `retain_hours` em todas as tabelas do lake.

    Default = 168h (7 dias): janela de time-travel suficiente para backtest recente
    sem deixar o metadata crescer sem limite. O snapshot corrente nunca e expirado.
    """
    cutoff = datetime.now(UTC) - timedelta(hours=retain_hours)
    for spec in ALL_SPECS:
        if not catalog.table_exists(spec.identifier):
            logger.info("{}: tabela ainda nao existe — nada a expirar.", spec.identifier)
            continue
        table = catalog.load_table(spec.identifier)
        before = len(table.snapshots())
        table.maintenance.expire_snapshots().older_than(cutoff).commit()
        after = len(catalog.load_table(spec.identifier).snapshots())
        logger.info(
            "{}: expire_snapshots (corte {}) — {} -> {} snapshot(s).",
            spec.identifier,
            cutoff.isoformat(timespec="seconds"),
            before,
            after,
        )
