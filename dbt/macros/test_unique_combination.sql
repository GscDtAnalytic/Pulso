{#
  Teste generico: a combinacao de colunas e unica (chave de negocio composta).

  Os fatos do Pulso tem chave natural composta — fct_candle = (symbol, interval,
  window_start), fct_trade = (exchange, symbol, trade_id). O `unique` nativo do dbt
  so cobre coluna unica; este cobre a combinacao. SQL puro, identica em DuckDB e
  Trino — sem depender de pacotes externos (o `dbt build` roda offline no pytest).
#}
{% test unique_combination(model, columns) %}

with grouped as (
    select
        {{ columns | join(", ") }},
        count(*) as n_rows
    from {{ model }}
    group by {{ columns | join(", ") }}
)

select *
from grouped
where n_rows > 1

{% endtest %}
