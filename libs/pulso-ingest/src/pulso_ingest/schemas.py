"""Carrega os contratos Avro de `contracts/` para o serializer do producer.

Os `.avsc` sao versionados no repo e validados em CI (`check_schema_compat.py`).
O producer precisa do schema-string para registrar/serializar via Schema Registry.
Em dev e em container o repo esta presente, entao localizamos o diretorio
`contracts/` subindo a arvore a partir do CWD e do proprio pacote.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def contracts_dir() -> Path:
    """Acha o diretorio `contracts/` (que contem `trade.avsc`). Cacheado."""
    seeds = (Path.cwd(), Path(__file__).resolve())
    for seed in seeds:
        for parent in (seed, *seed.parents):
            candidate = parent / "contracts"
            if (candidate / "trade.avsc").is_file():
                return candidate
    raise FileNotFoundError(
        "Diretorio 'contracts/' nao encontrado a partir do CWD nem do pacote. "
        "Rode a partir da raiz do repo Pulso."
    )


def load_schema_str(name: str) -> str:
    """Le um `.avsc` (ex.: 'trade.avsc') como string crua para o AvroSerializer."""
    return (contracts_dir() / name).read_text(encoding="utf-8")
