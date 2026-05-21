-- Pulso — Marco 2 — DLQ de trades atrasados (late data)
--
-- ksqlDB nao tem side-output nativo de late records (limitacao vs Flink, anotada
-- no ARCHITECTURE_PROPOSAL.md). Politica de late data explicita (principio #1):
-- todo trade cujo skew (ingest_time - event_time) ultrapassa o grace do candle m1
-- (10s) e roteado para a DLQ `trades.raw.dlq` — fail-loud, nada e descartado em
-- silencio. O sinal e o skew JA medido pelo producer no Marco 1.
--
-- Limiar = 10000 ms = grace do candle m1 (00_streams + 10_candles). Trade dentro
-- do prazo NAO aparece aqui. Ajustar junto com o grace do m1 se for recalibrado.

CREATE STREAM trades_late WITH (
  KAFKA_TOPIC  = 'trades.raw.dlq',
  KEY_FORMAT   = 'KAFKA',
  VALUE_FORMAT = 'AVRO'
) AS
  SELECT *
  FROM trades_stream
  WHERE (ingest_time - event_time) > 10000
  EMIT CHANGES;
