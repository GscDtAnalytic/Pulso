-- Fato: resumo diario por simbolo. Grao = (symbol, trade_date).
-- Rollup dos candles M1 (grao mais fino) — OHLC do dia, volume, n. de trades.
-- min_by/max_by pegam open/close pela ordem temporal da janela (cross-engine:
-- mesma funcao em DuckDB e Trino).

with m1 as (

    select * from {{ ref('int_market__candles_enriched') }}
    where interval = 'M1'

)

select
    symbol,
    window_date                          as trade_date,
    count(*)                             as candle_count,
    sum(trade_count)                     as trade_count,
    sum(volume)                          as volume,
    min_by(open_price, window_start)     as open_price,
    max(high_price)                      as high_price,
    min(low_price)                       as low_price,
    max_by(close_price, window_start)    as close_price,
    -- VWAP do dia: media dos VWAPs de minuto ponderada pelo volume do minuto.
    case when sum(volume) > 0
         then sum(vwap * volume) / sum(volume)
         else avg(vwap) end              as vwap

from m1
group by symbol, window_date
