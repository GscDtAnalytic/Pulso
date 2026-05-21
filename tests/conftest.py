"""Fixtures compartilhadas dos testes."""

from __future__ import annotations

import pytest
from pulso_infra import get_settings
from pulso_storage.catalog import build_catalog


@pytest.fixture
def iceberg_catalog(tmp_path):
    """Catalogo Iceberg offline: sqlite + warehouse local temporario.

    Sem broker nem MinIO — o sink e a manutencao sao testaveis 100% offline,
    como a topologia ksqlDB do Marco 2.
    """
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return build_catalog(
        get_settings(),
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
