from __future__ import annotations

from strategies.base import BaseStrategy
from strategies.ema_crossover import EmaCrossoverStrategy
from strategies.rsi_reversion import RsiMeanReversionStrategy


def create_strategy(name: str) -> BaseStrategy:
    """Instantiate a registered trading strategy by short CLI name."""
    key = name.strip().lower()
    if key == "ema":
        return EmaCrossoverStrategy(fast_period=5, slow_period=13)
    if key == "rsi":
        return RsiMeanReversionStrategy(period=14, oversold=30.0, overbought=70.0)
    raise ValueError(f"Estrategia desconocida '{name}'. Opciones válidas: ema, rsi")


def create_all_strategies() -> list[BaseStrategy]:
    """Return fresh instances of all registered strategies."""
    return [create_strategy("ema"), create_strategy("rsi")]
