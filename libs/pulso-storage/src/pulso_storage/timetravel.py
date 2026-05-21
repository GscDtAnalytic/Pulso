"""Time-travel sobre as tabelas Iceberg — backtest reproduzivel (pilar #3).

Iceberg versiona cada commit como um snapshot imutavel. Ler o estado *exato* de
uma data passada e so escolher o snapshot certo — nenhum dado e sobrescrito. E o
que torna um backtest reproduzivel: rodar a mesma estrategia hoje e daqui a um mes
sobre `as_of(ontem)` da o mesmo resultado.

`scan_as_of` resolve o snapshot por id ou por timestamp e devolve um `DataScan`;
`to_duckdb` materializa esse scan numa conexao DuckDB para consulta SQL ad-hoc.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb
from pyiceberg.table import DataScan, Table


@dataclass(frozen=True, slots=True)
class SnapshotInfo:
    """Resumo legivel de um snapshot — uma versao da tabela."""

    snapshot_id: int
    timestamp: datetime
    operation: str


def list_snapshots(table: Table) -> list[SnapshotInfo]:
    """Historico de versoes da tabela, da mais antiga para a mais recente."""
    return [
        SnapshotInfo(
            snapshot_id=s.snapshot_id,
            timestamp=datetime.fromtimestamp(s.timestamp_ms / 1000, tz=UTC),
            operation=s.summary.operation if s.summary else "unknown",
        )
        for s in table.snapshots()
    ]


def scan_as_of(
    table: Table,
    *,
    snapshot_id: int | None = None,
    as_of: datetime | None = None,
) -> DataScan:
    """Devolve um `DataScan` da tabela como estava num snapshot/instante passado.

    Informe exatamente um de `snapshot_id` ou `as_of`. `as_of` resolve para o
    ultimo snapshot com timestamp <= o instante dado.
    """
    if (snapshot_id is None) == (as_of is None):
        raise ValueError("Informe exatamente um: snapshot_id OU as_of.")
    if as_of is not None:
        snapshot = table.snapshot_as_of_timestamp(int(as_of.timestamp() * 1000))
        if snapshot is None:
            raise ValueError(f"Nenhum snapshot em {table.name()} ate {as_of.isoformat()}.")
        snapshot_id = snapshot.snapshot_id
    return table.scan(snapshot_id=snapshot_id)


def to_duckdb(scan: DataScan, table_name: str = "snapshot") -> duckdb.DuckDBPyConnection:
    """Materializa um scan numa conexao DuckDB in-memory para consulta SQL local."""
    return scan.to_duckdb(table_name)
