-- Staging de bronze.trades: limpeza leve, sem regra de negocio.
-- View (nao materializa) — a fonte ja e imutavel no lake.

with source as (

    select * from {{ source('bronze', 'trades') }}

)

select
    trade_id,
    lower(exchange)                  as exchange,
    symbol,
    cast(price as double)            as price,
    cast(quantity as double)         as quantity,
    upper(side)                      as side,
    event_time,
    ingest_time,
    -- skew event-time -> ingest (atraso de captura). Negativo = relogio da
    -- exchange adiantado; cabe ao consumo decidir o que fazer com isso.
    {{ dbt.datediff('event_time', 'ingest_time', 'millisecond') }} as ingest_skew_ms

from source
