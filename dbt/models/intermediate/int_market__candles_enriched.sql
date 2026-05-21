-- Candles com metricas derivadas de analise tecnica. Ephemeral: inlinado nos
-- modelos que o consomem (fct_candle, fct_symbol_daily) — sem objeto no banco.

with candles as (

    select * from {{ ref('stg_silver__candles') }}

)

select
    symbol,
    interval,
    window_start,
    window_end,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    trade_count,

    -- Metricas de candle (vocabulario de analise tecnica).
    high_price - low_price                          as price_range,
    close_price - open_price                        as body,
    case when close_price >= open_price
         then 'BULLISH' else 'BEARISH' end           as direction,
    case when open_price > 0
         then (close_price - open_price) / open_price
         else 0.0 end                                as return_pct,

    cast(window_start as date)                       as window_date

from candles
