"""Pipelines do sink: liga topico(s) do barramento -> tabela Iceberg + decode.

Dois pipelines, um por camada medallion:

- **trades**  — `trades.raw` -> `bronze.trades`. O value Avro ja tem todos os campos
  do `contracts/trade.avsc`; decode e identidade.
- **candles** — `candles.m1/m5/h1` -> `silver.candles`. O `symbol` NAO esta no value
  (e a chave Kafka — ver `ksqldb/README.md`); o decode o injeta a partir da chave.

Os tres topicos de candle compartilham `contracts/candle.avsc` e a mesma tabela
silver — `interval` (no value) distingue a granularidade.
"""

from __future__ import annotations

from dataclasses import dataclass

from pulso_infra import Settings

from pulso_storage.consumer import DecodeFn, KeyDecodeFn, decode_key_utf8
from pulso_storage.tables import CANDLES, TRADES, TableSpec

# Topicos de candle: lista canonica e fixa (CLAUDE.md — "Topicos Kafka").
CANDLE_TOPICS: tuple[str, ...] = ("candles.m1", "candles.m5", "candles.h1")

# Sufixo binario que o ksqlDB anexa a chave de uma TABLE janelada: o window-start
# como long big-endian (8 bytes). Ver ksqldb/README.md.
_KSQL_WINDOW_SUFFIX = 8


@dataclass(frozen=True, slots=True)
class Pipeline:
    """Uma rota topico(s) -> tabela do lake, com decode de value e de chave."""

    name: str
    spec: TableSpec
    topics: tuple[str, ...]
    decode: DecodeFn
    key_decode: KeyDecodeFn = decode_key_utf8


def _decode_windowed_symbol_key(raw: bytes | None) -> str | None:
    """Extrai o `symbol` da chave janelada do ksqlDB (`symbol + window-start 8B`).

    Decodificar a chave inteira como UTF-8 quebra: os 8 bytes finais sao um long
    binario (timestamp), nao texto. Fatiamos o sufixo e decodificamos o prefixo.
    """
    if raw is None:
        return None
    if len(raw) <= _KSQL_WINDOW_SUFFIX:
        raise ValueError(f"Chave de candle curta demais para ser janelada: {len(raw)} bytes")
    return raw[:-_KSQL_WINDOW_SUFFIX].decode("utf-8")


def _decode_trade(_key: str | None, value: dict) -> dict:
    """Value de `trades.raw` ja casa 1:1 com o schema de `bronze.trades`."""
    return value


def _decode_candle(key: str | None, value: dict) -> dict:
    """Candle: injeta `symbol` (chave Kafka) no registro de `silver.candles`."""
    if key is None:
        raise ValueError("Mensagem de candle sem chave Kafka — symbol indeterminado.")
    return {**value, "symbol": key}


def build_pipelines(settings: Settings) -> tuple[Pipeline, ...]:
    """Os pipelines ativos do sink."""
    return (
        Pipeline("trades", TRADES, (settings.topic_trades,), _decode_trade),
        Pipeline(
            "candles",
            CANDLES,
            CANDLE_TOPICS,
            _decode_candle,
            key_decode=_decode_windowed_symbol_key,
        ),
    )
