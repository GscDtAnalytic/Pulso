"""Deteccao de gap de sequencia no order book."""

from pulso_ingest.gap import OrderBookGapDetector


def test_primeira_observacao_nao_e_gap():
    d = OrderBookGapDetector()
    check = d.observe("binance:BTC-USD", 100, 110)
    assert check.first_seen
    assert not check.is_gap


def test_sequencia_continua_sem_gap():
    d = OrderBookGapDetector()
    d.observe("k", 100, 110)
    check = d.observe("k", 111, 120)  # 110 + 1 == 111
    assert not check.is_gap
    assert check.missing == 0


def test_gap_conta_update_ids_perdidos():
    d = OrderBookGapDetector()
    d.observe("k", 100, 110)
    check = d.observe("k", 114, 120)  # esperado 111; faltam 111,112,113
    assert check.is_gap
    assert check.missing == 3


def test_reenvio_sobreposto_nao_e_gap_nem_regride():
    d = OrderBookGapDetector()
    d.observe("k", 100, 110)
    check = d.observe("k", 105, 110)  # intervalo ja visto (reenvio idempotente)
    assert not check.is_gap
    # estado nao regride: a proxima continuidade ainda parte de 110
    nxt = d.observe("k", 111, 115)
    assert not nxt.is_gap


def test_streams_sao_independentes():
    d = OrderBookGapDetector()
    d.observe("binance:BTC-USD", 100, 110)
    check = d.observe("binance:ETH-USD", 500, 510)  # outra stream => first_seen
    assert check.first_seen
    assert not check.is_gap


def test_reset_zera_a_stream():
    d = OrderBookGapDetector()
    d.observe("k", 100, 110)
    d.reset("k")
    check = d.observe("k", 999, 1000)
    assert check.first_seen
    assert not check.is_gap
