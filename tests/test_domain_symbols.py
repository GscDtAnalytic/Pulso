"""Testes do dominio: o seed CSV e a fonte de verdade unica dos simbolos."""

from pulso_domain import Exchange, active_symbols, load_symbols, resolve_canonical


def test_seed_carrega_simbolos():
    symbols = load_symbols()
    assert len(symbols) >= 5
    canonicos = {s.canonical for s in symbols}
    assert {"BTC-USD", "ETH-USD", "SOL-USD"} <= canonicos


def test_todos_ativos_no_seed_inicial():
    assert len(active_symbols()) == len(load_symbols())


def test_traducao_por_exchange():
    btc = next(s for s in load_symbols() if s.canonical == "BTC-USD")
    assert btc.exchange_symbol(Exchange.BINANCE) == "BTCUSDT"
    assert btc.exchange_symbol(Exchange.COINBASE) == "BTC-USD"


def test_resolve_canonical_ida_e_volta():
    assert resolve_canonical(Exchange.BINANCE, "ETHUSDT") == "ETH-USD"
    assert resolve_canonical(Exchange.COINBASE, "SOL-USD") == "SOL-USD"


def test_resolve_canonical_desconhecido_retorna_none():
    assert resolve_canonical(Exchange.BINANCE, "FOOBAR") is None
