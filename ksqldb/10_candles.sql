-- Pulso — Marco 2 — candles OHLCV (event-time, EMIT FINAL)
--
-- Tres TABLEs agregadas por janela TUMBLING de event time, uma por intervalo.
-- O topico `candles.m1/m5/h1` carrega APENAS janelas seladas: EMIT FINAL emite
-- uma vez por janela, depois do grace period (is_final sempre true). Estado live
-- de janelas abertas e servido por PULL QUERY na TABLE materializada (ver README).
--
-- Casing: o schema Avro registrado tem que bater com contracts/candle.avsc, que usa
-- nomes minusculos. Alias sem aspas vira MAIUSCULO no Avro -> TODO alias leva crases.
-- `interval` e palavra reservada -> crases obrigatorias de qualquer forma.
--
-- `symbol` NAO vai para o value: e a group key = chave Kafka da mensagem (idiomatico
-- ksqlDB). candle.avsc nao tem o campo `symbol` de proposito.
--
-- Ressalva honesta (mesmo tom da nota Flink no ARCHITECTURE_PROPOSAL.md):
-- open/close usam EARLIEST/LATEST_BY_OFFSET = ordem de OFFSET dentro da janela.
-- ksqlDB nao tem "first/last by event-time"; o grace period cobre o reordenamento
-- antes do fechamento. VWAP = sum(price*qty)/sum(qty); sum(qty) nunca e 0 numa
-- janela emitida (quantity > 0 e a janela so fecha com >= 1 trade).
--
-- Grace por intervalo: m1=10s, m5=30s, h1=60s. Calibrar pelo p99 do skew
-- (ingest_time - event_time) medido no producer; valores iniciais conservadores.

CREATE TABLE candles_m1 WITH (
  KAFKA_TOPIC  = 'candles.m1',
  KEY_FORMAT   = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  RETENTION_MS = 86400000
) AS
  SELECT
    symbol,
    'M1'                       AS `interval`,
    FROM_UNIXTIME(WINDOWSTART) AS `window_start`,
    FROM_UNIXTIME(WINDOWEND)   AS `window_end`,
    EARLIEST_BY_OFFSET(price)  AS `open`,
    MAX(price)                 AS `high`,
    MIN(price)                 AS `low`,
    LATEST_BY_OFFSET(price)    AS `close`,
    SUM(quantity)              AS `volume`,
    (SUM(price * quantity) / SUM(quantity)) AS `vwap`,
    COUNT(*)                   AS `trade_count`,
    true                       AS `is_final`
  FROM trades_stream
  WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 10 SECONDS)
  GROUP BY symbol
  EMIT FINAL;

CREATE TABLE candles_m5 WITH (
  KAFKA_TOPIC  = 'candles.m5',
  KEY_FORMAT   = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  RETENTION_MS = 86400000
) AS
  SELECT
    symbol,
    'M5'                       AS `interval`,
    FROM_UNIXTIME(WINDOWSTART) AS `window_start`,
    FROM_UNIXTIME(WINDOWEND)   AS `window_end`,
    EARLIEST_BY_OFFSET(price)  AS `open`,
    MAX(price)                 AS `high`,
    MIN(price)                 AS `low`,
    LATEST_BY_OFFSET(price)    AS `close`,
    SUM(quantity)              AS `volume`,
    (SUM(price * quantity) / SUM(quantity)) AS `vwap`,
    COUNT(*)                   AS `trade_count`,
    true                       AS `is_final`
  FROM trades_stream
  WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 30 SECONDS)
  GROUP BY symbol
  EMIT FINAL;

CREATE TABLE candles_h1 WITH (
  KAFKA_TOPIC  = 'candles.h1',
  KEY_FORMAT   = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  RETENTION_MS = 86400000
) AS
  SELECT
    symbol,
    'H1'                       AS `interval`,
    FROM_UNIXTIME(WINDOWSTART) AS `window_start`,
    FROM_UNIXTIME(WINDOWEND)   AS `window_end`,
    EARLIEST_BY_OFFSET(price)  AS `open`,
    MAX(price)                 AS `high`,
    MIN(price)                 AS `low`,
    LATEST_BY_OFFSET(price)    AS `close`,
    SUM(quantity)              AS `volume`,
    (SUM(price * quantity) / SUM(quantity)) AS `vwap`,
    COUNT(*)                   AS `trade_count`,
    true                       AS `is_final`
  FROM trades_stream
  WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 60 SECONDS)
  GROUP BY symbol
  EMIT FINAL;
