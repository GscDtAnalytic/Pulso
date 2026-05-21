-- Pulso — Marco 2 — stream de origem
--
-- trades_stream le o topico do barramento `trades.raw` produzido pelo Marco 1.
-- Convencao fixada no producer (libs/pulso-ingest/.../producer.py):
--   key   = simbolo canonico, StringSerializer  -> KEY_FORMAT='KAFKA', symbol VARCHAR KEY
--   value = Avro via Schema Registry            -> VALUE_FORMAT='AVRO'
-- TIMESTAMP='event_time': TODA agregacao a jusante e por EVENT TIME (principio
-- nao-negociavel #1). `event_time`/`ingest_time` sao epoch-millis (timestamp-millis).

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS trades_stream (
  symbol     VARCHAR KEY,
  trade_id   VARCHAR,
  exchange   VARCHAR,
  price      DOUBLE,
  quantity   DOUBLE,
  side       VARCHAR,
  event_time BIGINT,
  ingest_time BIGINT
) WITH (
  KAFKA_TOPIC  = 'trades.raw',
  KEY_FORMAT   = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  TIMESTAMP    = 'event_time'
);
