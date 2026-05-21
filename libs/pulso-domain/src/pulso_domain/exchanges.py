"""Exchanges suportadas pelo Pulso."""

from __future__ import annotations

from enum import StrEnum


class Exchange(StrEnum):
    """Exchanges de origem. O valor casa com a coluna `exchange` dos contratos Avro."""

    BINANCE = "binance"
    COINBASE = "coinbase"

    @property
    def symbol_column(self) -> str:
        """Nome da coluna no seed que mapeia o simbolo canonico -> simbolo da exchange."""
        return f"{self.value}_symbol"
