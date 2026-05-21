-- Fato: um trade executado. Grao = um trade por (exchange, symbol, trade_id).
-- Espelha bronze.trades com o notional pre-computado para consumo analitico.

with trades as (

    select * from {{ ref('stg_bronze__trades') }}

)

select
    trade_id,
    exchange,
    symbol,
    side,
    price,
    quantity,
    price * quantity            as notional,
    ingest_skew_ms,
    event_time,
    ingest_time,
    cast(event_time as date)    as event_date

from trades
