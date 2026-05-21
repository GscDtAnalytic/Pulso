"""Exposicao de metricas Prometheus (modelo pull).

Prometheus faz scraping de um endpoint `/metrics` HTTP (ver wiki:
tecnologias/prometheus). Esta funcao sobe esse endpoint no processo do producer.
Os objetos de metrica em si (counters/gauges/histogramas) vivem na camada que os
emite — `pulso_ingest.metrics` —, mantendo a infra agnostica ao dominio.
"""

from __future__ import annotations

from prometheus_client import start_http_server


def start_metrics_server(port: int, addr: str = "0.0.0.0") -> None:
    """Sobe o endpoint HTTP `/metrics` para o Prometheus fazer scraping.

    Idempotente do ponto de vista do chamador: chame uma vez no startup. Usa o
    registry global default do `prometheus_client`, onde as metricas do
    `pulso_ingest` se registram na importacao.
    """
    start_http_server(port, addr=addr)
