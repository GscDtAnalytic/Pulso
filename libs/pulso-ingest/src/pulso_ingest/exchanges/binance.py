"""Client WebSocket da Binance (combined streams).

Canais por simbolo: `<sym>@trade` (trades) e `<sym>@depth` (diff. depth do order
book). O endpoint combinado entrega `{"stream": "...", "data": {...}}`; mensagens
de controle (ack de subscribe) sao ignoradas.

Gap detection: o diff stream carrega `U` (firstUpdateId) e `u` (finalUpdateId)
por simbolo — o caso canonico de deteccao de gap. A continuidade e por simbolo.
Docs: Binance Spot WebSocket Streams (formato estavel ha anos).
"""

from __future__ import annotations

from pulso_domain import Exchange

from pulso_ingest import metrics
from pulso_ingest.exchanges.base import ExchangeClient
from pulso_ingest.models import (
    SIDE_BUY,
    SIDE_SELL,
    MarketEvent,
    OrderBookDeltaEvent,
    PriceLevel,
    TradeEvent,
)


class BinanceClient(ExchangeClient):
    exchange = Exchange.BINANCE

    def __init__(self, settings, symbols) -> None:  # noqa: ANN001
        super().__init__(settings, symbols)
        # ticker da Binance (UPPER, como vem em 'data.s') -> simbolo canonico.
        self._canonical = {s.binance_symbol: s.canonical for s in symbols}

    def ws_url(self) -> str:
        return self.settings.binance_ws_url

    def subscribe_messages(self) -> list[dict]:
        params: list[str] = []
        for s in self.symbols:
            low = s.binance_symbol.lower()
            params.append(f"{low}@trade")
            params.append(f"{low}@depth")
        return [{"method": "SUBSCRIBE", "params": params, "id": 1}]

    def gap_stream_key(self, event: OrderBookDeltaEvent) -> str:
        # Sequencia e por simbolo na Binance.
        return f"binance:{event.symbol}"

    def parse(self, raw: dict, ingest_ms: int) -> list[MarketEvent]:
        data = raw.get("data")
        if not isinstance(data, dict):
            return []  # ack de subscribe ou frame de controle
        kind = data.get("e")
        if kind == "trade":
            return self._parse_trade(data, ingest_ms)
        if kind == "depthUpdate":
            return self._parse_depth(data, ingest_ms)
        return []

    def _parse_trade(self, d: dict, ingest_ms: int) -> list[MarketEvent]:
        canonical = self._canonical.get(d["s"])
        if canonical is None:
            metrics.events_dropped.labels("binance", "unknown_symbol").inc()
            return []
        # m=True: comprador e o maker (passivo) => agressor vendeu => SELL.
        side = SIDE_SELL if d["m"] else SIDE_BUY
        return [
            TradeEvent(
                trade_id=str(d["t"]),
                exchange="binance",
                symbol=canonical,
                price=float(d["p"]),
                quantity=float(d["q"]),
                side=side,
                event_time=int(d["T"]),
                ingest_time=ingest_ms,
            )
        ]

    def _parse_depth(self, d: dict, ingest_ms: int) -> list[MarketEvent]:
        canonical = self._canonical.get(d["s"])
        if canonical is None:
            metrics.events_dropped.labels("binance", "unknown_symbol").inc()
            return []
        return [
            OrderBookDeltaEvent(
                exchange="binance",
                symbol=canonical,
                first_update_id=int(d["U"]),
                final_update_id=int(d["u"]),
                bids=tuple(PriceLevel(float(p), float(q)) for p, q in d.get("b", [])),
                asks=tuple(PriceLevel(float(p), float(q)) for p, q in d.get("a", [])),
                event_time=int(d["E"]),
                ingest_time=ingest_ms,
            )
        ]
