from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from common.contracts import CandleEvent, SignalEvent
from strategies.base import BaseStrategy


@dataclass
class _RsiState:
    prev_close: float | None = None
    avg_gain: float = 0.0
    avg_loss: float = 0.0
    deltas_seen: int = 0
    prev_rsi: float | None = None
    last_inicio: str | None = None


class RsiMeanReversionStrategy(BaseStrategy):
    """Mean-reversion strategy using Wilder's RSI threshold crossings."""

    def __init__(
        self,
        period: int = 14,
        oversold: float = 30.0,
        overbought: float = 70.0,
        id_algoritmo: str | None = None,
    ) -> None:
        if period < 2:
            raise ValueError("period debe ser >= 2")
        if not (0 < oversold < overbought < 100):
            raise ValueError("Se requiere 0 < oversold < overbought < 100")
        self.period = period
        self.oversold = oversold
        self.overbought = overbought
        self.id_algoritmo = id_algoritmo or f"rsi_reversion_{period}"
        self._states: dict[str, _RsiState] = {}

    def reset(self) -> None:
        self._states.clear()

    def on_candle(self, candle: CandleEvent) -> SignalEvent | None:
        state = self._states.setdefault(candle.symbol, _RsiState())

        if state.last_inicio is not None and candle.inicio <= state.last_inicio:
            return None
        state.last_inicio = candle.inicio

        price = candle.close
        if state.prev_close is None:
            state.prev_close = price
            return None

        delta = price - state.prev_close
        state.prev_close = price
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        state.deltas_seen += 1

        if state.deltas_seen < self.period:
            state.avg_gain += gain
            state.avg_loss += loss
            return None

        if state.deltas_seen == self.period:
            state.avg_gain = (state.avg_gain + gain) / self.period
            state.avg_loss = (state.avg_loss + loss) / self.period
        else:
            state.avg_gain = (state.avg_gain * (self.period - 1) + gain) / self.period
            state.avg_loss = (state.avg_loss * (self.period - 1) + loss) / self.period

        if state.avg_loss == 0.0:
            rsi = 100.0 if state.avg_gain > 0.0 else 50.0
        else:
            rs = state.avg_gain / state.avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

        prev_rsi = state.prev_rsi
        state.prev_rsi = rsi
        if prev_rsi is None:
            return None

        # Cruce de salida de sobreventa -> BUY; cruce de salida de sobrecompra -> SELL
        signal_side: Literal["BUY", "SELL"] | None = None
        if prev_rsi <= self.oversold and rsi > self.oversold:
            signal_side = "BUY"
        elif prev_rsi >= self.overbought and rsi < self.overbought:
            signal_side = "SELL"

        if signal_side is None:
            return None

        return SignalEvent(
            id_algoritmo=self.id_algoritmo,
            symbol=candle.symbol,
            signal=signal_side,
            precio_referencia=price,
            timestamp=candle.inicio,
        )
