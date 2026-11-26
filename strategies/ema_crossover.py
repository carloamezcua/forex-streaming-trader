from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from common.contracts import CandleEvent, SignalEvent
from strategies.base import BaseStrategy


@dataclass
class _EmaState:
    fast_ema: float | None = None
    slow_ema: float | None = None
    count: int = 0
    last_inicio: str | None = None


class EmaCrossoverStrategy(BaseStrategy):
    """Trend-following strategy triggered on fast/slow Exponential Moving Average crossovers."""

    def __init__(
        self,
        fast_period: int = 5,
        slow_period: int = 13,
        id_algoritmo: str | None = None,
    ) -> None:
        if fast_period <= 0 or slow_period <= 0 or fast_period >= slow_period:
            raise ValueError("Se requiere 0 < fast_period < slow_period")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self._alpha_fast = 2.0 / (fast_period + 1.0)
        self._alpha_slow = 2.0 / (slow_period + 1.0)
        self.id_algoritmo = id_algoritmo or f"ema_crossover_{fast_period}_{slow_period}"
        self._states: dict[str, _EmaState] = {}

    def reset(self) -> None:
        self._states.clear()

    def on_candle(self, candle: CandleEvent) -> SignalEvent | None:
        state = self._states.setdefault(candle.symbol, _EmaState())

        # Deduplicar velas repetidas (protección at-least-once)
        if state.last_inicio == candle.inicio:
            return None
        state.last_inicio = candle.inicio

        price = candle.close
        prev_fast = state.fast_ema
        prev_slow = state.slow_ema

        if state.fast_ema is None or state.slow_ema is None:
            state.fast_ema = price
            state.slow_ema = price
            state.count = 1
            return None

        state.fast_ema = self._alpha_fast * price + (1.0 - self._alpha_fast) * state.fast_ema
        state.slow_ema = self._alpha_slow * price + (1.0 - self._alpha_slow) * state.slow_ema
        state.count += 1

        # Esperar calentamiento mínimo antes de emitir señales
        if state.count < self.slow_period or prev_fast is None or prev_slow is None:
            return None

        signal_side: Literal["BUY", "SELL"] | None = None
        if prev_fast <= prev_slow and state.fast_ema > state.slow_ema:
            signal_side = "BUY"
        elif prev_fast >= prev_slow and state.fast_ema < state.slow_ema:
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
