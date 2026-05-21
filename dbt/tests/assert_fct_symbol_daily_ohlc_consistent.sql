-- O rollup diario tem que preservar a invariante OHLC dos candles que agrega:
-- low <= open,close <= high. Pega erro de agregacao (ex.: min_by/max_by trocados).

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price
from {{ ref('fct_symbol_daily') }}
where not (
        low_price  <= open_price
    and low_price  <= close_price
    and high_price >= open_price
    and high_price >= close_price
    and high_price >= low_price
)
