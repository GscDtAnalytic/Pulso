-- Invariante de qualidade (contracts/trade.avsc): price > 0 e quantity > 0.
-- Trade com preco/quantidade nao-positivos e corrupcao — falha o build.

select
    trade_id,
    exchange,
    symbol,
    price,
    quantity
from {{ ref('fct_trade') }}
where price <= 0 or quantity <= 0
