-- Dimensao de simbolos negociados. Origem: o seed (copia da fonte de verdade
-- unica, libs/pulso-domain/seeds/symbols.csv — CLAUDE.md principio #5).

with symbols as (

    select * from {{ ref('symbols') }}

)

select
    canonical        as symbol,
    base             as base_asset,
    quote            as quote_asset,
    binance_symbol,
    coinbase_symbol,
    active           as is_active
from symbols
