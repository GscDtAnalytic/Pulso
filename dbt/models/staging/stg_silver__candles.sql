-- Staging de silver.candles: tipagem e nomes consistentes, sem regra de negocio.

with source as (

    select * from {{ source('silver', 'candles') }}

)

select
    symbol,
    interval,
    window_start,
    window_end,
    cast(open as double)    as open_price,
    cast(high as double)    as high_price,
    cast(low as double)     as low_price,
    cast(close as double)   as close_price,
    cast(volume as double)  as volume,
    cast(vwap as double)    as vwap,
    cast(trade_count as bigint) as trade_count,
    is_final

from source
