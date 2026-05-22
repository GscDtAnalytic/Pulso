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

from pulso_infra import Settings, decode_key_utf8, decode_ksql_windowed_key

from pulso_storage.consumer import DecodeFn, KeyDecodeFn
from pulso_storage.tables import CANDLES, TRADES, TableSpec

# Topicos de candle: lista canonica e fixa (CLAUDE.md — "Topicos Kafka").
CANDLE_TOPICS: tuple[str, ...] = ("candles.m1", "candles.m5", "candles.h1")


@dataclass(frozen=True, slots=True)
class Pipeline:
    """Uma rota topico(s) -> tabela do lake, com decode de value e de chave."""

    name: str
    spec: TableSpec
    topics: tuple[str, ...]
    decode: DecodeFn
    key_decode: KeyDecodeFn = decode_key_utf8


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
            key_decode=decode_ksql_windowed_key,
        ),
    )
