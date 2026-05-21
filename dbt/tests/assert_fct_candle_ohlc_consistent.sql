-- Invariante do contrato (contracts/candle.avsc): low <= open,close <= high
-- e high >= low. Um candle que viola isso e dado corrompido — falha o build.
-- Espelha a checagem de qualidade prometida no ARCHITECTURE_PROPOSAL.md (Marco 5),
-- ja exercida aqui sobre a mart.

select
    symbol,
    interval,
    window_start,
    open_price,
    high_price,
    low_price,
    close_price
from {{ ref('fct_candle') }}
where not (
        low_price  <= open_price
    and low_price  <= close_price
    and high_price >= open_price
    and high_price >= close_price
    and high_price >= low_price
)
