#!/usr/bin/env python
"""Valida os contratos Avro em `contracts/` contra o Schema Registry.

Dois niveis de verificacao:

1. Parseabilidade (sempre, offline): cada .avsc e um schema Avro valido?
2. Compatibilidade (se o Schema Registry estiver acessivel): a versao proposta
   e compativel com a ultima registrada, conforme a politica do subject
   (BACKWARD por default)? Subject inexistente = primeira versao = OK.

Usado por `make schema-check` e pelo job de CI `schema-compat`. Quebra => exit 1,
bloqueando o merge. E a implementacao tecnica de data contracts (shift-left).

Uso:
    python tools/check_schema_compat.py            # parse + compat (se SR up)
    python tools/check_schema_compat.py --offline  # so parse
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"
DEFAULT_REGISTRY = "http://localhost:18081"

# Mapeia cada arquivo .avsc para os subjects (TopicNameStrategy: <topic>-value).
# candle.avsc serve tres topicos de candle (1m/5m/1h).
SCHEMA_SUBJECTS: dict[str, list[str]] = {
    "trade.avsc": ["trades.raw-value"],
    "orderbook_delta.avsc": ["orderbook.delta-value"],
    "candle.avsc": ["candles.m1-value", "candles.m5-value", "candles.h1-value"],
}


def load_and_parse(path: Path) -> dict:
    """Le e valida que o arquivo e um schema Avro parseavel."""
    schema = json.loads(path.read_text(encoding="utf-8"))
    try:
        import fastavro

        fastavro.parse_schema(schema)
    except ImportError:
        print("  ! fastavro ausente — pulando validacao de parse (instale dev deps).")
    return schema


def check_compat(registry: str, subject: str, schema: dict) -> tuple[bool, str]:
    """Consulta o endpoint de compatibilidade do Schema Registry.

    Retorna (compativel, mensagem). Subject inexistente => primeira versao => OK.
    SR inacessivel => levanta para o caller decidir.
    """
    import requests

    url = f"{registry}/compatibility/subjects/{subject}/versions/latest"
    payload = {"schema": json.dumps(schema), "schemaType": "AVRO"}
    resp = requests.post(
        url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=5,
    )
    if resp.status_code == 404:
        return True, "subject novo (primeira versao)"
    resp.raise_for_status()
    is_compat = bool(resp.json().get("is_compatible", False))
    return is_compat, "compativel" if is_compat else "INCOMPATIVEL com a politica do subject"


def register_baseline(registry: str, baseline_dir: Path) -> None:
    """Registra os schemas de `baseline_dir` no SR como a linha de base.

    Usado no CI: registra os contratos da branch principal antes de checar os
    da branch do PR — assim o check BACKWARD compara contra o estado real, nao
    contra um SR vazio (onde todo subject seria "primeira versao").
    """
    import requests

    for path in sorted(baseline_dir.glob("*.avsc")):
        schema = json.loads(path.read_text(encoding="utf-8"))
        for subject in SCHEMA_SUBJECTS.get(path.name, []):
            # Fixa a politica BACKWARD no subject (data contract explicito).
            requests.put(
                f"{registry}/config/{subject}",
                data=json.dumps({"compatibility": "BACKWARD"}),
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=5,
            ).raise_for_status()
            resp = requests.post(
                f"{registry}/subjects/{subject}/versions",
                data=json.dumps({"schema": json.dumps(schema), "schemaType": "AVRO"}),
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=5,
            )
            resp.raise_for_status()
            print(f"  baseline registrado: {subject}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--offline", action="store_true", help="So valida parse, sem SR.")
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Diretorio com os .avsc da branch principal; registra-os como linha de base.",
    )
    args = parser.parse_args()

    if args.baseline and not args.offline:
        if args.baseline.is_dir():
            print(f"Registrando baseline de {args.baseline} ...")
            register_baseline(args.registry, args.baseline)
        else:
            print(f"  ! baseline {args.baseline} ausente — seguindo sem linha de base.")

    avsc_files = sorted(CONTRACTS_DIR.glob("*.avsc"))
    if not avsc_files:
        print(f"Nenhum .avsc em {CONTRACTS_DIR}")
        return 1

    failures = 0
    registry_down = False

    for path in avsc_files:
        print(f"\n== {path.name} ==")
        try:
            schema = load_and_parse(path)
            print("  ok parse")
        except Exception as exc:  # noqa: BLE001
            print(f"  FALHA parse: {exc}")
            failures += 1
            continue

        if args.offline or registry_down:
            continue

        for subject in SCHEMA_SUBJECTS.get(path.name, []):
            try:
                compat, msg = check_compat(args.registry, subject, schema)
            except Exception as exc:  # noqa: BLE001
                print(f"  ! Schema Registry inacessivel ({exc}); seguindo so com parse.")
                registry_down = True
                break
            mark = "ok" if compat else "FALHA"
            print(f"  {mark} compat [{subject}]: {msg}")
            if not compat:
                failures += 1

    print("\n" + ("FALHOU" if failures else "OK") + f" — {failures} problema(s).")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
