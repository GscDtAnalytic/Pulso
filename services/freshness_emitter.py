"""Freshness emitter — verificador de SLO standalone (Marco 5).

Le o espelho DuckDB (gerado por `make lake-mirror`) e verifica se o dado mais
recente de `bronze.trades` esta dentro do SLO de freshness definido em
`governance/slo.yml` (default: 60 segundos).

Util em tres contextos:
  1. Pos-batch / pos-dbt — verifica que o lake esta fresco antes de servir.
  2. Cron — monitora freshness quando o sink esta parado.
  3. CI — falha o pipeline se os dados estiverem velhos.

Exit code 0 = SLO OK; exit code 1 = SLO violado ou erro de leitura (fail-loud).

Uso:
    uv run python services/freshness_emitter.py
    uv run python services/freshness_emitter.py --db dbt/pulso_lake.duckdb --slo 120
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

import duckdb
from loguru import logger


def check_freshness(db_path: str, slo_seconds: int) -> tuple[float, bool]:
    """Le max(event_time) do bronze.trades e calcula freshness.

    Devolve (freshness_seconds, slo_ok). Lanca RuntimeError se a tabela
    nao existir (fail-loud: DuckDB vazio e erro, nao sucesso silencioso).
    """
    con = duckdb.connect(db_path, read_only=True)
    try:
        result = con.execute(
            'SELECT MAX(event_time) FROM "bronze"."trades"'
        ).fetchone()
    finally:
        con.close()

    if result is None or result[0] is None:
        raise RuntimeError(
            f"bronze.trades vazio em {db_path} — lake nao espelhado ou sink nunca rodou."
        )

    max_event_time = result[0]
    # DuckDB devolve timestamptz como datetime aware; normaliza para UTC.
    if max_event_time.tzinfo is None:
        max_event_time = max_event_time.replace(tzinfo=UTC)
    freshness = (datetime.now(UTC) - max_event_time).total_seconds()
    return freshness, freshness <= slo_seconds


def push_to_gateway(url: str, freshness: float, slo_ok: bool) -> None:
    """Envia metrica para o Prometheus Pushgateway (opcional)."""
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        g_freshness = Gauge(
            "pulso_freshness_slo_freshness_seconds",
            "Freshness atual do bronze.trades (now - max event_time)",
            registry=registry,
        )
        g_violated = Gauge(
            "pulso_freshness_slo_violated",
            "1 se SLO de freshness foi violado, 0 caso contrario",
            registry=registry,
        )
        g_freshness.set(freshness)
        g_violated.set(0 if slo_ok else 1)
        push_to_gateway(url, job="pulso-freshness-emitter", registry=registry)
        logger.info("Metricas enviadas ao Pushgateway: {}", url)
    except Exception as exc:
        logger.warning("Falha ao enviar ao Pushgateway (nao critico): {}", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verifica SLO de freshness do lake DuckDB.")
    parser.add_argument(
        "--db",
        default="dbt/pulso_lake.duckdb",
        help="Caminho do arquivo DuckDB espelho (default: dbt/pulso_lake.duckdb).",
    )
    parser.add_argument(
        "--slo",
        type=int,
        default=60,
        help="SLO de freshness em segundos (default: 60).",
    )
    parser.add_argument(
        "--pushgateway",
        default="",
        help="URL do Prometheus Pushgateway. Vazio = so imprime (default).",
    )
    args = parser.parse_args()

    try:
        freshness, slo_ok = check_freshness(args.db, args.slo)
    except Exception as exc:
        logger.error("Erro ao verificar freshness: {}", exc)
        sys.exit(1)

    status = "OK" if slo_ok else "VIOLADO"
    logger.info(
        "Freshness: {:.1f}s | SLO: {}s | Status: {}",
        freshness,
        args.slo,
        status,
    )

    if args.pushgateway:
        push_to_gateway(args.pushgateway, freshness, slo_ok)

    if not slo_ok:
        logger.error(
            "SLO DE FRESHNESS VIOLADO: {:.1f}s > {}s. "
            "Verifique `make sink` e `make lake-mirror`.",
            freshness,
            args.slo,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
