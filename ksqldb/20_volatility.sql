-- Pulso — Marco 2 — volatilidade de preco (tabela pull-served)
--
-- Desvio-padrao do preco numa janela HOPPING (SIZE 5 MIN, ADVANCE 1 MIN): a cada
-- minuto, a stddev dos ultimos 5 minutos. Variancia pela forma de um passo:
--   Var = E[price^2] - E[price]^2 = SUM(p^2)/n - (SUM(p)/n)^2
-- stddev = SQRT(Var). Clampado em 0 via MAX(...,0) por seguranca numerica
-- (erro de arredondamento pode dar Var levemente negativa com precos quase iguais).
--
-- NAO e topico do barramento: a lista canonica de topicos (CLAUDE.md) e fixa.
-- Esta TABLE existe so para ser consultada por PULL QUERY (estado live) — sem
-- EMIT FINAL. Ver README para exemplos de pull query.
-- O VALUE_FORMAT e AVRO mas o schema NAO precisa casar com nenhum .avsc (nao ha
-- contrato de barramento aqui); ainda assim usamos crases por consistencia.

CREATE TABLE volatility_5m WITH (
  KAFKA_TOPIC  = 'pulso.volatility.5m',
  KEY_FORMAT   = 'KAFKA',
  VALUE_FORMAT = 'AVRO'
) AS
  SELECT
    symbol,
    FROM_UNIXTIME(WINDOWSTART) AS `window_start`,
    FROM_UNIXTIME(WINDOWEND)   AS `window_end`,
    COUNT(*)                   AS `trade_count`,
    AVG(price)                 AS `mean_price`,
    SQRT(
      GREATEST(
        SUM(price * price) / COUNT(*) - (SUM(price) / COUNT(*)) * (SUM(price) / COUNT(*)),
        0.0
      )
    )                          AS `volatility`
  FROM trades_stream
  WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
  GROUP BY symbol
  EMIT CHANGES;
