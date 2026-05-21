"""Ingestao streaming (Marco 1 — a implementar).

Planejado:
- clients WebSocket (Binance, Coinbase) -> normalizam para os contratos Avro;
- producer idempotente (enable.idempotence=true, acks=all);
- reconnect + circuit breaker + deteccao de gap de sequencia no order book.
"""
