"""pulso-serve — API de serving do Marco 4.

Expõe histórico (marts dbt), estado live (ksqlDB pull query) e push (WebSocket).
"""

from pulso_serve.app import create_app, main
from pulso_serve.ksql import KsqlClient, KsqlUnavailable
from pulso_serve.live import CandleBroadcaster, ConnectionManager, kafka_candle_stream
from pulso_serve.store import MarketStore, StoreUnavailable, build_store

__all__ = [
    "create_app",
    "main",
    "KsqlClient",
    "KsqlUnavailable",
    "CandleBroadcaster",
    "ConnectionManager",
    "kafka_candle_stream",
    "MarketStore",
    "StoreUnavailable",
    "build_store",
]
