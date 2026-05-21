"""Simbolos negociados, carregados do seed CSV (fonte de verdade unica).

Espelha o padrao do Mapear-RN, onde os 167 municipios vinham de um seed CSV.
Aqui, o universo de ativos e suas traducoes por exchange vivem em
`seeds/symbols.csv` e nada no codigo hardcoda simbolos.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files

from pulso_domain.exchanges import Exchange


@dataclass(frozen=True, slots=True)
class Symbol:
    """Um ativo negociavel e suas traducoes por exchange.

    `canonical` e a identidade estavel usada em todo o pipeline (topicos,
    Iceberg, dbt). Cada exchange tem seu proprio ticker (ex.: BTC-USD ->
    BTCUSDT na Binance, BTC-USD na Coinbase).
    """

    canonical: str
    base: str
    quote: str
    binance_symbol: str
    coinbase_symbol: str
    active: bool

    def exchange_symbol(self, exchange: Exchange) -> str:
        """Ticker desta moeda na exchange dada."""
        return getattr(self, exchange.symbol_column)


@lru_cache(maxsize=1)
def load_symbols() -> tuple[Symbol, ...]:
    """Le `seeds/symbols.csv` empacotado com o pacote. Cacheado."""
    seed = files("pulso_domain").joinpath("seeds/symbols.csv")
    with seed.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return tuple(
            Symbol(
                canonical=row["canonical"],
                base=row["base"],
                quote=row["quote"],
                binance_symbol=row["binance_symbol"],
                coinbase_symbol=row["coinbase_symbol"],
                active=row["active"].strip().lower() == "true",
            )
            for row in reader
        )


def active_symbols() -> tuple[Symbol, ...]:
    """Apenas os simbolos marcados como ativos."""
    return tuple(s for s in load_symbols() if s.active)


def resolve_canonical(exchange: Exchange, exchange_symbol: str) -> str | None:
    """Traduz um ticker especifico de exchange para o simbolo canonico.

    Retorna None se o ticker nao pertence ao universo configurado.
    """
    for symbol in load_symbols():
        if symbol.exchange_symbol(exchange) == exchange_symbol:
            return symbol.canonical
    return None
