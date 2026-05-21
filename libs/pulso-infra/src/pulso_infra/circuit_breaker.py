"""Circuit breaker para conexoes instaveis (WebSocket de exchange).

Padrao herdado do `mapear-infra`. Evita o anti-padrao de martelar uma fonte
que esta caindo: depois de N falhas consecutivas o circuito **abre** e as
tentativas sao recusadas por um periodo de cooldown; passado o cooldown ele
vai a **half-open** e deixa *uma* tentativa passar para sondar a recuperacao.

Tres estados (classico):

    CLOSED      -- normal; tentativas passam. Falhas acumulam.
    OPEN        -- recusando tentativas; espera `reset_timeout`.
    HALF_OPEN   -- sondando; um sucesso fecha, uma falha reabre.

E deliberadamente sincrono e sem I/O: o caller (loop de reconnect em
`pulso_ingest`) consulta `allow()` e reporta `record_success()`/`record_failure()`.
A nocao de tempo e injetavel (`time_source`) para os testes nao dormirem.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import IntEnum


class CircuitState(IntEnum):
    """Estado do circuito. Valor inteiro casa direto com a metrica Prometheus."""

    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


@dataclass
class CircuitBreaker:
    """Circuit breaker de falhas consecutivas.

    Args:
        failure_threshold: falhas consecutivas que abrem o circuito.
        reset_timeout: segundos em OPEN antes de permitir uma sonda (HALF_OPEN).
        time_source: relogio monotonico injetavel (default `time.monotonic`).
    """

    failure_threshold: int = 5
    reset_timeout: float = 30.0
    time_source: Callable[[], float] = time.monotonic

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _consecutive_failures: int = field(default=0, init=False)
    _opened_at: float = field(default=0.0, init=False)

    @property
    def state(self) -> CircuitState:
        """Estado atual, ja resolvendo a transicao OPEN -> HALF_OPEN por tempo."""
        if self._state is CircuitState.OPEN and self._cooldown_elapsed():
            self._state = CircuitState.HALF_OPEN
        return self._state

    def allow(self) -> bool:
        """True se uma tentativa pode ser feita agora.

        CLOSED e HALF_OPEN permitem; OPEN recusa ate o cooldown expirar (quando
        `state` ja o promove a HALF_OPEN e libera exatamente uma sonda).
        """
        return self.state is not CircuitState.OPEN

    def record_success(self) -> None:
        """Sucesso: zera o contador e fecha o circuito."""
        self._consecutive_failures = 0
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        """Falha: em HALF_OPEN reabre na hora; em CLOSED abre ao atingir o limiar."""
        self._consecutive_failures += 1
        half_open = self._state is CircuitState.HALF_OPEN
        if half_open or self._consecutive_failures >= self.failure_threshold:
            self._open()

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self.time_source()

    def _cooldown_elapsed(self) -> bool:
        return self.time_source() - self._opened_at >= self.reset_timeout
