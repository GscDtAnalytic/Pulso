"""Eventos normalizados de mercado — espelho 1:1 dos contratos Avro.

`trade.avsc` e `orderbook_delta.avsc` sao a fonte de verdade do formato; estas
dataclasses sao a representacao em memoria entre o parsing do WebSocket e a
serializacao Avro. `to_avro()` produz exatamente o dict que o serializer espera
(timestamps como `long` em milissegundos, conforme `logicalType: timestamp-millis`).

`symbol` ja e o **simbolo canonico** (resolvido via `pulso_domain`), nunca o
ticker cru da exchange.
"""

from __future__ import annotations

from dataclasses import dataclass

# Lados validos do enum Side no contrato Avro.
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"
SIDE_UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class TradeEvent:
    """Um trade individual. Casa com `io.pulso.market.Trade`."""

    trade_id: str
    exchange: str
    symbol: str
    price: float
    quantity: float
    side: str
    event_time: int  # ms desde epoch (event time na exchange)
    ingest_time: int  # ms desde epoch (quando o producer recebeu)

    @property
    def skew_ms(self) -> int:
        """Atraso de chegada: ingest - event. Base p/ calibrar o grace do Marco 2."""
        return self.ingest_time - self.event_time

    def to_avro(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "side": self.side,
            "event_time": self.event_time,
            "ingest_time": self.ingest_time,
        }


@dataclass(frozen=True, slots=True)
class PriceLevel:
    """Nivel de preco alterado. `quantity == 0` significa remover o nivel."""

    price: float
    quantity: float

    def to_avro(self) -> dict:
        return {"price": self.price, "quantity": self.quantity}


@dataclass(frozen=True, slots=True)
class OrderBookDeltaEvent:
    """Delta incremental do order book. Casa com `io.pulso.market.OrderBookDelta`.

    `first_update_id`/`final_update_id` sao a sequencia usada para detectar gaps
    apos reconexao (ver `pulso_ingest.gap`).
    """

    exchange: str
    symbol: str
    first_update_id: int
    final_update_id: int
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    event_time: int
    ingest_time: int

    @property
    def skew_ms(self) -> int:
        return self.ingest_time - self.event_time

    def to_avro(self) -> dict:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "first_update_id": self.first_update_id,
            "final_update_id": self.final_update_id,
            "bids": [lvl.to_avro() for lvl in self.bids],
            "asks": [lvl.to_avro() for lvl in self.asks],
            "event_time": self.event_time,
            "ingest_time": self.ingest_time,
        }


# Uniao dos eventos que um client de exchange pode emitir.
MarketEvent = TradeEvent | OrderBookDeltaEvent
