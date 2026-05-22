"""Decodificadores de chave Kafka compartilhados entre consumidores.

O formato da chave depende do produtor:

- Produtores do Marco 1 (`trades.raw`, `events.anomaly`) usam a chave como o
  **symbol** numa string UTF-8 simples.
- TABLEs **janeladas** do ksqlDB (`candles.m1/m5/h1`, volatilidade) serializam a
  chave como `symbol + window-start`, onde o window-start e um long big-endian de
  8 bytes anexado ao fim. Decodificar a chave inteira como UTF-8 quebra nos bytes
  binarios do timestamp — fatiamos o sufixo e decodificamos so o prefixo.

Qualquer consumidor de um topico janelado do ksqlDB (sink, anomaly_detector) tem
que usar `decode_ksql_windowed_key` em vez do decode UTF-8 plano.
"""

from __future__ import annotations

# Sufixo binario que o ksqlDB anexa a chave de uma TABLE janelada: o window-start
# como long big-endian (8 bytes). Ver ksqldb/README.md.
KSQL_WINDOW_SUFFIX_LEN = 8


def decode_key_utf8(raw: bytes | None) -> str | None:
    """Chave Kafka como string UTF-8 simples (produtores do Marco 1)."""
    return raw.decode("utf-8") if raw is not None else None


def decode_ksql_windowed_key(raw: bytes | None) -> str | None:
    """Extrai o `symbol` da chave janelada do ksqlDB (`symbol + window-start 8B`)."""
    if raw is None:
        return None
    if len(raw) <= KSQL_WINDOW_SUFFIX_LEN:
        raise ValueError(f"Chave janelada curta demais: {len(raw)} bytes")
    return raw[:-KSQL_WINDOW_SUFFIX_LEN].decode("utf-8")
