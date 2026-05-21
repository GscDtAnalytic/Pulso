"""Utilitarios pequenos da ingestao (tempo)."""

from __future__ import annotations

import time
from datetime import datetime


def now_ms() -> int:
    """Processing time atual em ms desde epoch (UTC)."""
    return int(time.time() * 1000)


def iso_to_ms(value: str) -> int:
    """Converte timestamp ISO-8601 (ex.: Coinbase '2026-05-21T12:00:00.123Z') em ms."""
    # fromisoformat (3.11+) aceita o sufixo 'Z' e fracoes de segundo.
    return int(datetime.fromisoformat(value).timestamp() * 1000)
