import pytest
from common.contracts import CandleEvent, SignalEvent, TickEvent, parse_timestamp


def test_tick_normalization_and_mid():
    tick = TickEvent.from_json(
        '{"symbol": "eur/usd", "bid": 1.0500, "ask": 1.0502, "timestamp": 1732315500000}'
    )
    assert tick.symbol == "EURUSD"
    assert tick.timestamp == "2024-11-22T22:45:00Z"
    assert tick.mid == pytest.approx(1.0501)


def test_invalid_tick_rejected():
    with pytest.raises(ValueError):
        TickEvent(symbol="EURUSD", bid=-1.0, ask=1.05, timestamp="2026-11-22T22:45:00Z")
    with pytest.raises(ValueError):
        TickEvent(symbol="EURUSD", bid=1.06, ask=1.05, timestamp="2026-11-22T22:45:00Z")


def test_candle_roundtrip():
    raw = (
        '{"symbol":"GBPUSD","inicio":"2026-11-22T12:00:00Z",'
        '"open":1.26,"high":1.265,"low":1.258,"close":1.262,"num_ticks":15}'
    )
    candle = CandleEvent.from_json(raw)
    assert CandleEvent.from_json(candle.to_json()) == candle


def test_signal_validation():
    sig = SignalEvent(
        id_algoritmo="ema_test",
        symbol="EURUSD",
        signal="buy",
        precio_referencia=1.055,
        timestamp="2026-11-22T12:01:00Z",
    )
    assert sig.signal == "BUY"
    with pytest.raises(ValueError):
        SignalEvent("ema", "EURUSD", "HOLD", 1.05, "2026-11-22T12:01:00Z")


def test_parse_timestamp_accepts_seconds_millis_and_iso():
    dt_iso = parse_timestamp("2024-11-22T22:45:00Z")
    dt_sec = parse_timestamp(1732315500)
    dt_ms = parse_timestamp(1732315500000)
    assert dt_iso == dt_sec == dt_ms
