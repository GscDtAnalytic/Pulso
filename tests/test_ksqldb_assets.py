"""Validacao offline (sem docker) dos assets ksqlDB do Marco 2.

`make ksql-test` roda os testes de topologia de verdade, mas precisa de docker e
nao entra no `make check`. Estes testes leves rodam no pytest/CI normal e garantem
que os arquivos do diretorio `ksqldb/` ficam coerentes entre si — em especial que
cada caso de teste so espera topicos que algum statement de fato cria. Assim um
`ksqldb/tests/<caso>/expected.json` com topico errado quebra ja no `make check`,
sem esperar o job de docker.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

KSQL_DIR = Path(__file__).resolve().parent.parent / "ksqldb"
TESTS_DIR = KSQL_DIR / "tests"

# .sql de producao (numerados); os de teste vivem em tests/<caso>/ e nao contam aqui.
PROD_SQL = sorted(KSQL_DIR.glob("[0-9]*.sql"))
CASE_DIRS = sorted(p.parent for p in TESTS_DIR.glob("*/sources.txt"))

_TOPIC_RE = re.compile(r"KAFKA_TOPIC\s*=\s*'([^']+)'", re.IGNORECASE)


def _topics_criados(sql: str) -> set[str]:
    """Topicos que algum CREATE STREAM/TABLE ... WITH (KAFKA_TOPIC='...') declara."""
    return set(_TOPIC_RE.findall(sql))


def test_existem_sql_de_producao():
    assert PROD_SQL, "esperado pelo menos um ksqldb/NN_*.sql de producao"


@pytest.mark.parametrize("sql_path", PROD_SQL, ids=lambda p: p.name)
def test_sql_de_producao_nao_vazio(sql_path: Path):
    conteudo = sql_path.read_text(encoding="utf-8").strip()
    assert conteudo, f"{sql_path.name} esta vazio"
    assert ";" in conteudo, f"{sql_path.name} nao tem nenhum statement terminado"


def test_existem_casos_de_teste():
    assert CASE_DIRS, "esperado pelo menos um ksqldb/tests/<caso>/ com sources.txt"


@pytest.mark.parametrize("case_dir", CASE_DIRS, ids=lambda p: p.name)
def test_caso_de_teste_coerente(case_dir: Path):
    # sources.txt aponta para .sql de producao existentes.
    fontes = [
        ln.strip()
        for ln in (case_dir / "sources.txt").read_text(encoding="utf-8").splitlines()
        if ln.strip()
    ]
    assert fontes, f"{case_dir.name}/sources.txt vazio"
    sql_total = ""
    for nome in fontes:
        fpath = KSQL_DIR / nome
        assert fpath.exists(), f"{case_dir.name}/sources.txt aponta para {nome} inexistente"
        sql_total += fpath.read_text(encoding="utf-8") + "\n"

    # input.json e expected.json sao JSON validos com a forma esperada.
    entrada = json.loads((case_dir / "input.json").read_text(encoding="utf-8"))
    esperado = json.loads((case_dir / "expected.json").read_text(encoding="utf-8"))
    assert entrada.get("inputs"), f"{case_dir.name}/input.json sem 'inputs'"
    saidas = esperado.get("outputs")
    assert saidas, f"{case_dir.name}/expected.json sem 'outputs'"

    # Todo topico esperado na saida e criado por algum statement das fontes.
    criados = _topics_criados(sql_total)
    for rec in saidas:
        assert "value" in rec, f"{case_dir.name}: registro de saida sem 'value'"
        topico = rec["topic"]
        assert topico in criados, (
            f"{case_dir.name}: expected.json espera o topico '{topico}', "
            f"que nenhum statement de {fontes} cria (criados: {sorted(criados)})"
        )
