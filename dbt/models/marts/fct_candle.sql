-- Fato: um candle OHLCV selado. Grao = (symbol, interval, window_start).
-- Base do historico servido pela API (apps/dashboard) e dos graficos do dashboard.

with candles as (

    select * from {{ ref('int_market__candles_enriched') }}

)

select
    symbol,
    interval,
    window_start,
    window_end,
    window_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    trade_count,
    price_range,
    body,
    direction,
    return_pct

from candles
