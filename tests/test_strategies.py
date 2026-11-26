from datetime import datetime, timedelta, timezone

from common.contracts import CandleEvent
from strategies.ema_crossover import EmaCrossoverStrategy
from strategies.rsi_reversion import RsiMeanReversionStrategy


def _make_candles(prices: list[float], symbol: str = "EURUSD") -> list[CandleEvent]:
    start = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    candles = []
    for i, price in enumerate(prices):
        candles.append(
            CandleEvent(
                symbol=symbol,
                inicio=(start + timedelta(minutes=i)).isoformat(),
                open=price,
                high=price + 0.0005,
                low=price - 0.0005,
                close=price,
                num_ticks=20,
            )
        )
    return candles


def test_ema_crossover_emits_buy_and_sell_on_trend_reversals():
    strategy = EmaCrossoverStrategy(fast_period=3, slow_period=6)
    # Bajada sostenida -> subida sostenida (BUY) -> bajada sostenida (SELL)
    prices = [
        1.100, 1.098, 1.096, 1.094, 1.092, 1.090,
        1.091, 1.094, 1.098, 1.103, 1.108, 1.112,
        1.109, 1.104, 1.098, 1.092, 1.086, 1.080,
    ]
    signals = [s for c in _make_candles(prices) if (s := strategy.on_candle(c)) is not None]
    sides = [s.signal for s in signals]
    assert "BUY" in sides
    assert "SELL" in sides
    assert all(s.id_algoritmo == "ema_crossover_3_6" for s in signals)


def test_ema_deduplicates_same_candle_and_isolates_symbols():
    strategy = EmaCrossoverStrategy(fast_period=2, slow_period=4)
    c1 = _make_candles([1.10], symbol="EURUSD")[0]
    c2 = _make_candles([1.25], symbol="GBPUSD")[0]
    assert strategy.on_candle(c1) is None
    assert strategy.on_candle(c1) is None  # duplicada ignorada
    assert strategy.on_candle(c2) is None
    assert set(strategy._states.keys()) == {"EURUSD", "GBPUSD"}


def test_rsi_reversion_triggers_buy_and_sell():
    strategy = RsiMeanReversionStrategy(period=5, oversold=30.0, overbought=70.0)
    # Caída fuerte (RSI < 30) -> rebote (BUY) -> rally fuerte (RSI > 70) -> corrección (SELL)
    prices = [
        1.100, 1.095, 1.090, 1.085, 1.080, 1.075, 1.070,
        1.078, 1.085, 1.095, 1.105, 1.115, 1.125, 1.135,
        1.120, 1.110,
    ]
    signals = [s for c in _make_candles(prices) if (s := strategy.on_candle(c)) is not None]
    sides = [s.signal for s in signals]
    assert "BUY" in sides
    assert "SELL" in sides
