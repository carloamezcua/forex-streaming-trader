from __future__ import annotations

from abc import ABC, abstractmethod

from common.contracts import CandleEvent, SignalEvent


class BaseStrategy(ABC):
    """Pure strategy interface shared by Kafka live runners and the backtesting engine."""

    id_algoritmo: str

    @abstractmethod
    def on_candle(self, candle: CandleEvent) -> SignalEvent | None:
        """Process one 1-minute candle and optionally emit a BUY/SELL SignalEvent."""

    @abstractmethod
    def reset(self) -> None:
        """Clear all internal state (useful between backtest runs)."""
