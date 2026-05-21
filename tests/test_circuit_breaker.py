"""Circuit breaker: transicoes CLOSED -> OPEN -> HALF_OPEN -> CLOSED/OPEN."""

from pulso_infra import CircuitBreaker, CircuitState


class _Clock:
    """Relogio fake injetavel para nao dormir nos testes."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_abre_apos_falhas_consecutivas():
    cb = CircuitBreaker(failure_threshold=3, reset_timeout=10, time_source=_Clock())
    assert cb.allow()
    cb.record_failure()
    cb.record_failure()
    assert cb.state is CircuitState.CLOSED  # ainda nao atingiu o limiar
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    assert not cb.allow()


def test_sucesso_reseta_contador():
    cb = CircuitBreaker(failure_threshold=2, time_source=_Clock())
    cb.record_failure()
    cb.record_success()
    cb.record_failure()
    assert cb.state is CircuitState.CLOSED  # o sucesso zerou o streak


def test_half_open_apos_cooldown_e_fecha_no_sucesso():
    clock = _Clock()
    cb = CircuitBreaker(failure_threshold=1, reset_timeout=10, time_source=clock)
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    clock.now = 10
    assert cb.state is CircuitState.HALF_OPEN
    assert cb.allow()
    cb.record_success()
    assert cb.state is CircuitState.CLOSED


def test_half_open_reabre_no_fracasso_da_sonda():
    clock = _Clock()
    cb = CircuitBreaker(failure_threshold=1, reset_timeout=10, time_source=clock)
    cb.record_failure()
    clock.now = 10
    assert cb.state is CircuitState.HALF_OPEN
    cb.record_failure()  # a sonda falhou
    assert cb.state is CircuitState.OPEN
