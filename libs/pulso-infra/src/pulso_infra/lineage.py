"""Wrapper OpenLineage para emissao de lineage end-to-end (Marco 5).

Ativo apenas quando PULSO_OPENLINEAGE_URL esta definida; caso contrario, todos
os metodos sao no-op para nao bloquear o caminho critico de dados.

Uso tipico:
    emitter = OpenLineageEmitter.from_settings(settings)
    emitter.emit_sink_run("sink-trades", "trades.raw", "bronze.trades", records=42)

O Marquez roda em dev via `make up-lineage` (portas 5000/3000). O dbt envia
eventos automaticamente quando OPENLINEAGE_URL esta no ambiente (dbt-core >= 1.8).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from pulso_infra.config import Settings


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class OpenLineageEmitter:
    """Emite RunEvents para o Marquez/qualquer backend OpenLineage.

    Se `url` for vazio ou o pacote nao estiver disponivel, todos os metodos
    sao no-op silencioso — o pipeline nao para por falta de lineage.
    """

    def __init__(self, url: str, namespace: str = "pulso") -> None:
        self._namespace = namespace
        self._client = None
        if not url:
            return
        try:
            from openlineage.client import OpenLineageClient

            self._client = OpenLineageClient(url=url)
            logger.info("OpenLineage ativo | url={} | namespace={}", url, namespace)
        except Exception as exc:
            logger.warning("OpenLineage indisponivel (nao critico): {}", exc)

    @classmethod
    def from_settings(cls, settings: Settings) -> OpenLineageEmitter:
        return cls(url=settings.openlineage_url, namespace=settings.openlineage_namespace)

    # ------------------------------------------------------------------
    # Sink (Kafka -> Iceberg): emite COMPLETE apos cada batch commitado.
    # ------------------------------------------------------------------

    def emit_sink_run(
        self,
        job_name: str,
        input_dataset: str,
        output_dataset: str,
        records: int,
    ) -> None:
        """COMPLETE apos batch bem-sucedido: input=topico, output=tabela Iceberg."""
        if self._client is None:
            return
        self._emit(
            job_name=job_name,
            inputs=[input_dataset],
            outputs=[output_dataset],
            state="COMPLETE",
            extra={"records": str(records)},
        )

    # ------------------------------------------------------------------
    # Ingest (producer WebSocket): START ao conectar, COMPLETE no flush.
    # ------------------------------------------------------------------

    def emit_ingest_start(self, job_name: str, output_dataset: str) -> str:
        """START ao conectar; devolve run_id para usar no COMPLETE."""
        run_id = str(uuid.uuid4())
        if self._client is not None:
            self._emit(job_name, [], [output_dataset], "START", run_id=run_id)
        return run_id

    def emit_ingest_complete(
        self, job_name: str, output_dataset: str, run_id: str, records: int
    ) -> None:
        """COMPLETE no flush final do producer."""
        if self._client is None:
            return
        self._emit(job_name, [], [output_dataset], "COMPLETE", {"records": str(records)}, run_id)

    # ------------------------------------------------------------------
    # Interno
    # ------------------------------------------------------------------

    def _emit(
        self,
        job_name: str,
        inputs: list[str],
        outputs: list[str],
        state: str,
        extra: dict[str, str] | None = None,
        run_id: str | None = None,
    ) -> None:
        try:
            from openlineage.client.run import (
                InputDataset,
                Job,
                OutputDataset,
                Run,
                RunEvent,
                RunState,
            )

            event = RunEvent(
                eventType=RunState[state],
                eventTime=_now_iso(),
                run=Run(runId=run_id or str(uuid.uuid4())),
                job=Job(namespace=self._namespace, name=job_name),
                inputs=[InputDataset(namespace=self._namespace, name=n) for n in inputs],
                outputs=[OutputDataset(namespace=self._namespace, name=n) for n in outputs],
            )
            self._client.emit(event)
            logger.debug("OpenLineage {} | job={} records={}", state, job_name, extra)
        except Exception as exc:
            logger.debug("OpenLineage emit falhou (nao critico): {}", exc)
