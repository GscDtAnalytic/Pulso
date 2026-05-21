"""Ingestao streaming (Marco 1).

- clients WebSocket (Binance, Coinbase) que normalizam para os contratos Avro;
- producer idempotente (enable.idempotence=true, acks=all);
- reconnect + circuit breaker + deteccao de gap de sequencia no order book;
- metricas Prometheus (throughput, skew, gaps, saude da conexao).

Entrada: `python -m pulso_ingest`. Os submodulos que tocam rede/broker
(`producer`, `exchanges`) importam `confluent_kafka`/`websockets` localmente, nao
neste `__init__`, mantendo o import do pacote (e de `metrics`) leve.
"""
