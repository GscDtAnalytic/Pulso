"""Lakehouse Iceberg (Marco 3 — a implementar).

Planejado:
- escrita idempotente Kafka -> Iceberg (MERGE por trade_id => exactly-once effect);
- particionamento (dia + simbolo), manutencao (rewrite_data_files, expire_snapshots);
- helpers de time-travel para backtest reproduzivel.
"""
