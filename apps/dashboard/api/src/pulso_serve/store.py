"""Acesso ao historico — as marts dbt servidas pela API (Marco 4).

`MarketStore` fala SQL DB-API 2.0 contra **a mart, nao o lake cru**: `fct_candle`
e `fct_symbol_daily` (Marco 4, dbt). A mesma SQL roda nos dois engines —
princpio multi-engine do `ARCHITECTURE_PROPOSAL.md`:

- **dev**  — DuckDB, lendo o arquivo `.duckdb` que o `dbt build` materializou.
- **prod** — Trino, lendo as marts Iceberg.

A diferenca mora so na *connection factory* (`build_store`); as queries sao
identicas. `limit` entra na SQL como inteiro validado (nao parametro) porque nem
todo engine aceita `LIMIT ?`; `symbol`/`interval` sao sempre parametros.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pulso_infra import Settings

# Uma connection factory: chamada a cada query, devolve uma conexao DB-API 2.0
# (DuckDB ou Trino). Conexao por query mantem o acesso thread-safe sob o
# threadpool do FastAPI sem um pool dedicado.
ConnectFn = Callable[[], Any]

# Teto de linhas por resposta — protege a API e o navegador de um range gigante.
MAX_LIMIT = 5000


class StoreUnavailable(RuntimeError):
    """As marts ainda nao existem / o backend nao respondeu. Vira HTTP 503."""


class MarketStore:
    """Consultas de leitura sobre as marts dbt (`fct_candle`, `fct_symbol_daily`)."""

    def __init__(self, connect: ConnectFn, schema: str) -> None:
        self._connect = connect
        self._schema = schema

    def _rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        try:
            con = self._connect()
        except Exception as exc:  # noqa: BLE001 — qualquer falha de conexao -> 503
            raise StoreUnavailable(f"Backend de historico indisponivel: {exc}") from exc
        try:
            cur = con.cursor()
            cur.execute(sql, params)
            columns = [d[0] for d in cur.description]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001 — marts nao materializadas, etc.
            raise StoreUnavailable(f"Falha ao consultar as marts: {exc}") from exc
        finally:
            con.close()

    @staticmethod
    def _clamp(limit: int) -> int:
        return max(1, min(int(limit), MAX_LIMIT))

    def candles(self, symbol: str, interval: str, limit: int = 200) -> list[dict[str, Any]]:
        """Candles de `fct_candle`, em ordem cronologica (antigo -> recente)."""
        rows = self._rows(
            f"""
            SELECT symbol, interval, window_start, window_end,
                   open_price  AS open,  high_price AS high,
                   low_price   AS low,   close_price AS close,
                   volume, vwap, trade_count, direction, return_pct
            FROM {self._schema}.fct_candle
            WHERE symbol = ? AND interval = ?
            ORDER BY window_start DESC
            LIMIT {self._clamp(limit)}
            """,
            (symbol, interval),
        )
        rows.reverse()  # query desc (mais recentes) -> entrega asc para o grafico
        return rows

    def daily(self, symbol: str, limit: int = 90) -> list[dict[str, Any]]:
        """Resumo diario de `fct_symbol_daily`, em ordem cronologica."""
        rows = self._rows(
            f"""
            SELECT symbol, trade_date,
                   open_price  AS open,  high_price AS high,
                   low_price   AS low,   close_price AS close,
                   volume, vwap, trade_count, candle_count
            FROM {self._schema}.fct_symbol_daily
            WHERE symbol = ?
            ORDER BY trade_date DESC
            LIMIT {self._clamp(limit)}
            """,
            (symbol,),
        )
        rows.reverse()
        return rows


def build_store(settings: Settings) -> MarketStore:
    """Constroi o `MarketStore` do backend configurado (`PULSO_SERVE_HISTORY_BACKEND`)."""
    backend = settings.serve_history_backend.lower()
    schema = settings.dbt_marts_schema

    if backend == "duckdb":
        import duckdb

        path = settings.dbt_duckdb_path

        def connect() -> Any:
            # read_only: a API so le; o `dbt build` e o unico escritor do arquivo.
            return duckdb.connect(path, read_only=True)

        return MarketStore(connect, schema)

    if backend == "trino":
        import trino

        def connect() -> Any:
            return trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog="iceberg",
                schema=schema,
                http_scheme="http",
            )

        return MarketStore(connect, schema)

    raise ValueError(f"serve_history_backend invalido: {backend!r} (use duckdb|trino).")
