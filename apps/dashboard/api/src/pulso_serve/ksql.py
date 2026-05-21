"""Cliente de pull query do ksqlDB — o estado LIVE da janela aberta (Marco 4).

A janela de candle ainda aberta nao vai para topico nenhum (`EMIT FINAL` so emite
janelas seladas — ver ksqldb/README.md). O estado parcial dela vive na TABLE
materializada do ksqlDB e e lido por **pull query** via REST. E o que a API entrega
no endpoint `/api/candles/live`: o minuto corrente, ainda contando trades.

Projecao explicita das colunas (nao `SELECT *`): a ordem do resultado fica
determinada pela query, sem depender de pseudo-colunas de janela do ksqlDB.
"""

from __future__ import annotations

import httpx

# interval canonico (contracts/candle.avsc) -> TABLE materializada (ksqldb/10_candles.sql).
_TABLE_BY_INTERVAL = {"M1": "candles_m1", "M5": "candles_m5", "H1": "candles_h1"}

# Colunas de valor projetadas, na ordem em que voltam no resultado.
_FIELDS = (
    "interval",
    "window_start",
    "window_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
)


class KsqlUnavailable(RuntimeError):
    """O ksqlDB nao respondeu / erro de pull query. Vira HTTP 503."""


class KsqlClient:
    """Faz pull queries de candle ao ksqlDB. `http_client` injetavel (testes)."""

    def __init__(
        self,
        url: str,
        http_client: httpx.Client | None = None,
        *,
        timeout: float = 5.0,
    ) -> None:
        self._url = url.rstrip("/")
        self._client = http_client or httpx.Client(timeout=timeout)

    def live_candle(self, symbol: str, interval: str) -> dict | None:
        """Estado atual da janela aberta de `symbol`/`interval`, ou None se nao ha.

        `symbol` entra como literal SQL — o chamador (rota) deve valida-lo contra
        os simbolos conhecidos do `pulso-domain` antes de chamar.
        """
        table = _TABLE_BY_INTERVAL.get(interval)
        if table is None:
            raise ValueError(f"interval invalido: {interval!r} (use M1|M5|H1).")

        columns = ", ".join(f"`{field}`" for field in _FIELDS)
        sql = f"SELECT {columns} FROM {table} WHERE symbol = '{symbol}';"
        rows = self._pull(sql)
        if not rows:
            return None
        return dict(zip(_FIELDS, rows[0], strict=True))

    def _pull(self, sql: str) -> list[list]:
        """POST /query no ksqlDB; devolve as linhas (lista de colunas) do resultado."""
        try:
            response = self._client.post(
                f"{self._url}/query",
                json={"ksql": sql, "streamsProperties": {}},
                headers={
                    "Content-Type": "application/vnd.ksql.v1+json",
                    "Accept": "application/vnd.ksql.v1+json",
                },
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001 — rede/HTTP/JSON -> indisponivel
            raise KsqlUnavailable(f"Pull query ao ksqlDB falhou: {exc}") from exc

        # Resposta v1 = array JSON: um header com o schema, depois um item por linha.
        rows: list[list] = []
        for item in payload:
            if isinstance(item, dict) and item.get("row"):
                rows.append(item["row"]["columns"])
        return rows

    def close(self) -> None:
        self._client.close()
