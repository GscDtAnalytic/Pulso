"""Testes offline de governanca (Marco 5).

Valida sem broker, sem Iceberg ao vivo e sem DuckDB populado:
  - OpenLineageEmitter e no-op quando URL vazia
  - Arquivos YAML de governanca sao parseáveis
  - freshness_emitter sai com erro em DuckDB vazio/ausente
"""

from __future__ import annotations

import pathlib
import sys
import tempfile

import duckdb
import pytest
import yaml

# ---------------------------------------------------------------------------
# OpenLineage emitter — no-op quando URL vazia
# ---------------------------------------------------------------------------


def test_openlineage_emitter_noop_when_no_url():
    """Emitter sem URL deve ser instanciavel e todos os metodos sao no-op."""
    from pulso_infra.lineage import OpenLineageEmitter

    emitter = OpenLineageEmitter(url="")
    assert emitter._client is None

    # Nenhum metodo deve levantar excecao
    emitter.emit_sink_run("job", "input", "output", records=10)
    run_id = emitter.emit_ingest_start("job", "output")
    assert isinstance(run_id, str)
    emitter.emit_ingest_complete("job", "output", run_id, records=0)


def test_openlineage_emitter_from_settings_noop():
    """from_settings() com URL vazia deve gerar emitter no-op."""
    from pulso_infra.config import Settings
    from pulso_infra.lineage import OpenLineageEmitter

    settings = Settings(openlineage_url="", openlineage_namespace="test")
    emitter = OpenLineageEmitter.from_settings(settings)
    assert emitter._client is None


# ---------------------------------------------------------------------------
# YAML de governanca — parseabilidade
# ---------------------------------------------------------------------------


def _governance_yamls() -> list[pathlib.Path]:
    root = pathlib.Path(__file__).parent.parent / "governance"
    return list(root.rglob("*.yml"))


@pytest.mark.parametrize("yml_path", _governance_yamls(), ids=lambda p: p.name)
def test_governance_yaml_parseable(yml_path: pathlib.Path):
    """Cada arquivo YAML de governanca deve ser parseavel sem erros."""
    content = yaml.safe_load(yml_path.read_text())
    assert content is not None


def test_slo_yaml_has_required_keys():
    """governance/slo.yml deve ter slos com threshold e metric."""
    slo_path = pathlib.Path(__file__).parent.parent / "governance" / "slo.yml"
    data = yaml.safe_load(slo_path.read_text())
    assert "slos" in data
    for name, slo in data["slos"].items():
        assert "metric" in slo, f"SLO '{name}' sem campo 'metric'"
        assert "alert_name" in slo, f"SLO '{name}' sem campo 'alert_name'"


def test_prometheus_rules_yaml_structure():
    """governance/prometheus_rules.yml deve ter estrutura de grupos Prometheus."""
    rules_path = pathlib.Path(__file__).parent.parent / "governance" / "prometheus_rules.yml"
    data = yaml.safe_load(rules_path.read_text())
    assert "groups" in data
    for group in data["groups"]:
        assert "name" in group
        assert "rules" in group
        for rule in group["rules"]:
            assert "alert" in rule
            assert "expr" in rule


# ---------------------------------------------------------------------------
# freshness_emitter — fail-loud em DuckDB vazio
# ---------------------------------------------------------------------------


def test_freshness_emitter_fails_on_empty_db():
    """check_freshness() deve levancar RuntimeError em DuckDB sem bronze.trades."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "services"))
    from freshness_emitter import check_freshness

    with (
        tempfile.NamedTemporaryFile(suffix=".duckdb") as f,
        pytest.raises((RuntimeError, Exception)),
    ):
        check_freshness(f.name, slo_seconds=60)


def _make_trades_db(event_time_offset_seconds: int) -> str:
    """Cria DuckDB temporario com bronze.trades e um registro no tempo dado."""
    from datetime import UTC, datetime, timedelta

    tmp_dir = tempfile.mkdtemp()
    db_path = str(pathlib.Path(tmp_dir) / "test.duckdb")
    event_time = datetime.now(UTC) - timedelta(seconds=event_time_offset_seconds)
    con = duckdb.connect(db_path)
    con.execute('CREATE SCHEMA "bronze"')
    con.execute(
        'CREATE TABLE "bronze"."trades" (event_time TIMESTAMPTZ); '
        f"INSERT INTO \"bronze\".\"trades\" VALUES (TIMESTAMPTZ '{event_time.isoformat()}')"
    )
    con.close()
    return db_path


def test_freshness_emitter_ok_when_fresh():
    """check_freshness() deve retornar (freshness, True) com dado recente."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "services"))
    from freshness_emitter import check_freshness

    db_path = _make_trades_db(event_time_offset_seconds=5)
    try:
        freshness, ok = check_freshness(db_path, slo_seconds=60)
        assert ok is True
        assert 0 <= freshness < 60
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_freshness_emitter_violated_when_stale():
    """check_freshness() deve retornar (freshness, False) com dado antigo."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "services"))
    from freshness_emitter import check_freshness

    db_path = _make_trades_db(event_time_offset_seconds=120)
    try:
        freshness, ok = check_freshness(db_path, slo_seconds=60)
        assert ok is False
        assert freshness > 60
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)
