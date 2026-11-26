import json

from common.contracts import CandleEvent, SignalEvent, TickEvent


def test_tick_contract_exact_keys_and_cross_team_formats():
    # Payload de otro equipo usando epoch en segundos y símbolo con barra
    foreign_payload = '{"symbol": "eur/usd", "bid": 1.0812, "ask": 1.0814, "timestamp": 1732315500}'
    tick = TickEvent.from_json(foreign_payload)
    serialized = json.loads(tick.to_json())
    assert set(serialized.keys()) == {"symbol", "bid", "ask", "timestamp"}
    assert serialized["symbol"] == "EURUSD"


def test_candle_contract_exact_keys():
    foreign_payload = (
        '{"symbol": "GBPUSD", "inicio": "2026-11-23T15:04:00Z", '
        '"open": 1.27, "high": 1.275, "low": 1.268, "close": 1.272, "num_ticks": 42}'
    )
    candle = CandleEvent.from_json(foreign_payload)
    serialized = json.loads(candle.to_json())
    assert set(serialized.keys()) == {"symbol", "inicio", "open", "high", "low", "close", "num_ticks"}


def test_signal_contract_exact_keys():
    sig = SignalEvent(
        id_algoritmo="equipo_externo_v1",
        symbol="EURUSD",
        signal="SELL",
        precio_referencia=1.0825,
        timestamp="2026-11-23T15:05:00Z",
    )
    serialized = json.loads(sig.to_json())
    assert set(serialized.keys()) == {"id_algoritmo", "symbol", "signal", "precio_referencia", "timestamp"}
    assert serialized["signal"] in ("BUY", "SELL")
