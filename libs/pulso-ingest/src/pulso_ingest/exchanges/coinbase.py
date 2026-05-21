"""Client WebSocket da Coinbase (Advanced Trade — canais publicos, sem auth).

Canais: `market_trades` (trades) e `level2` (snapshot + updates do order book).
O envelope traz `sequence_num` monotonico **por conexao** (nao por simbolo).

Honestidade tecnica: a Coinbase nao expoe update_ids por simbolo como a Binance.
O `sequence_num` e por-conexao e reinicia a cada reconnect, o que torna a deteccao
de gap por-simbolo nao confiavel — entao `gap_stream_key` retorna None e a
deteccao de gap fica como feature exclusiva do client da Binance (o caso canonico).
Guardamos o `sequence_num` em first/final_update_id apenas como metadado de ordem.

Order book: produzimos so os `updates` incrementais; o `snapshot` inicial (book
inteiro, >1 MiB) e ignorado — bootstrap de book e responsabilidade do consumidor
(mesmo padrao do @depth da Binance, que tambem so entrega diffs).
"""

from __future__ import annotations

from pulso_domain import Exchange

from pulso_ingest import metrics
from pulso_ingest.exchanges.base import ExchangeClient
from pulso_ingest.models import (
    SIDE_BUY,
    SIDE_SELL,
    SIDE_UNKNOWN,
    MarketEvent,
    OrderBookDeltaEvent,
    PriceLevel,
    TradeEvent,
)
from pulso_ingest.util import iso_to_ms


class CoinbaseClient(ExchangeClient):
    exchange = Exchange.COINBASE

    def __init__(self, settings, symbols) -> None:  # noqa: ANN001
        super().__init__(settings, symbols)
        # product_id da Coinbase (ex.: BTC-USD) -> simbolo canonico.
        self._canonical = {s.coinbase_symbol: s.canonical for s in symbols}

    def ws_url(self) -> str:
        return self.settings.coinbase_ws_url

    def subscribe_messages(self) -> list[dict]:
        product_ids = [s.coinbase_symbol for s in self.symbols]
        return [
            {"type": "subscribe", "product_ids": product_ids, "channel": "market_trades"},
            {"type": "subscribe", "product_ids": product_ids, "channel": "level2"},
        ]

    def gap_stream_key(self, event: OrderBookDeltaEvent) -> str | None:
        return None  # sem deteccao de gap confiavel na Coinbase (ver docstring)

    def parse(self, raw: dict, ingest_ms: int) -> list[MarketEvent]:
        channel = raw.get("channel")
        if channel == "market_trades":
            return self._parse_trades(raw, ingest_ms)
        if channel == "l2_data":
            return self._parse_l2(raw, ingest_ms)
        return []  # 'subscriptions' ack, heartbeats, etc.

    def _parse_trades(self, raw: dict, ingest_ms: int) -> list[MarketEvent]:
        out: list[MarketEvent] = []
        for ev in raw.get("events", []):
            if ev.get("type") == "snapshot":
                continue  # lote de trades recentes reenviado a cada reconnect
            for t in ev.get("trades", []):
                canonical = self._canonical.get(t.get("product_id"))
                if canonical is None:
                    metrics.events_dropped.labels("coinbase", "unknown_symbol").inc()
                    continue
                side_raw = str(t.get("side", "")).upper()
                side = side_raw if side_raw in (SIDE_BUY, SIDE_SELL) else SIDE_UNKNOWN
                out.append(
                    TradeEvent(
                        trade_id=str(t["trade_id"]),
                        exchange="coinbase",
                        symbol=canonical,
                        price=float(t["price"]),
                        quantity=float(t["size"]),
                        side=side,
                        event_time=iso_to_ms(t["time"]),
                        ingest_time=ingest_ms,
                    )
                )
        return out

    def _parse_l2(self, raw: dict, ingest_ms: int) -> list[MarketEvent]:
        seq = int(raw.get("sequence_num", 0))
        ts = raw.get("timestamp")
        event_time = iso_to_ms(ts) if ts else ingest_ms
        out: list[MarketEvent] = []
        for ev in raw.get("events", []):
            if ev.get("type") == "snapshot":
                continue  # book inteiro (>1 MiB); bootstrap e do consumidor
            canonical = self._canonical.get(ev.get("product_id"))
            if canonical is None:
                metrics.events_dropped.labels("coinbase", "unknown_symbol").inc()
                continue
            bids: list[PriceLevel] = []
            asks: list[PriceLevel] = []
            for u in ev.get("updates", []):
                level = PriceLevel(float(u["price_level"]), float(u["new_quantity"]))
                (bids if u["side"] == "bid" else asks).append(level)
            out.append(
                OrderBookDeltaEvent(
                    exchange="coinbase",
                    symbol=canonical,
                    first_update_id=seq,
                    final_update_id=seq,
                    bids=tuple(bids),
                    asks=tuple(asks),
                    event_time=event_time,
                    ingest_time=ingest_ms,
                )
            )
        return out
