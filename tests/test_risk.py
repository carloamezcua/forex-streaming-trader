from datetime import datetime, timedelta, timezone

from common.contracts import SignalEvent
from execution.risk import RiskManager


def _sig(symbol="EURUSD", side="BUY", ts=None, algo="ema_5_13") -> SignalEvent:
    ts = ts or datetime(2026, 11, 23, 15, 0, 0, tzinfo=timezone.utc)
    return SignalEvent(
        id_algoritmo=algo,
        symbol=symbol,
        signal=side,
        precio_referencia=1.0850,
        timestamp=ts.isoformat(),
    )


def _rm(**overrides) -> RiskManager:
    defaults = dict(
        allowed_symbols=("EURUSD", "GBPUSD"),
        order_size_units=10000,
        max_position_units=20000,
        signal_cooldown_sec=60,
        max_signal_age_sec=120,
        max_daily_orders=3,
    )
    defaults.update(overrides)
    return RiskManager(**defaults)


def test_rejects_unauthorized_symbol():
    rm = _rm()
    now = datetime(2026, 11, 23, 15, 0, 5, tzinfo=timezone.utc)
    decision = rm.evaluate(_sig(symbol="USDJPY"), now=now)
    assert decision.approved is False
    assert "no autorizado" in decision.reason


def test_rejects_stale_signal():
    rm = _rm(max_signal_age_sec=60)
    sig_time = datetime(2026, 11, 23, 15, 0, 0, tzinfo=timezone.utc)
    now = sig_time + timedelta(seconds=61)
    decision = rm.evaluate(_sig(ts=sig_time), now=now)
    assert decision.approved is False
    assert "expirada" in decision.reason


def test_enforces_cooldown_and_max_position_and_allows_reduction():
    rm = _rm(order_size_units=10000, max_position_units=20000, signal_cooldown_sec=60, max_daily_orders=10)
    t0 = datetime(2026, 11, 23, 15, 0, 0, tzinfo=timezone.utc)

    # Orden 1: 0 -> +10000
    d1 = rm.evaluate(_sig(side="BUY", ts=t0), now=t0 + timedelta(seconds=1))
    assert d1.approved is True
    rm.record_execution(_sig(side="BUY", ts=t0), now=t0 + timedelta(seconds=1))

    # En cooldown (10s después): rechazada
    t1 = t0 + timedelta(seconds=11)
    assert rm.evaluate(_sig(side="BUY", ts=t1), now=t1).approved is False

    # Después del cooldown: +10000 -> +20000 (aprobada)
    t2 = t0 + timedelta(seconds=65)
    d2 = rm.evaluate(_sig(side="BUY", ts=t2), now=t2)
    assert d2.approved is True
    rm.record_execution(_sig(side="BUY", ts=t2), now=t2)

    # Intentar +30000 (excede límite de 20000): rechazada
    t3 = t2 + timedelta(seconds=65)
    d3 = rm.evaluate(_sig(side="BUY", ts=t3), now=t3)
    assert d3.approved is False
    assert "posición máxima" in d3.reason

    # Vender para reducir exposición (+20000 -> +10000): aprobada
    d4 = rm.evaluate(_sig(side="SELL", ts=t3), now=t3)
    assert d4.approved is True
    assert d4.projected_position == 10000


def test_daily_order_limit_resets_next_utc_day():
    rm = _rm(max_daily_orders=1, signal_cooldown_sec=0)
    t0 = datetime(2026, 11, 23, 23, 59, 0, tzinfo=timezone.utc)
    assert rm.evaluate(_sig(ts=t0), now=t0).approved is True
    rm.record_execution(_sig(ts=t0), now=t0)

    # Mismo día: rechazada
    assert rm.evaluate(_sig(ts=t0), now=t0 + timedelta(seconds=10)).approved is False

    # Día siguiente UTC: contador reiniciado -> aprobada
    t_next_day = datetime(2026, 11, 24, 0, 0, 5, tzinfo=timezone.utc)
    assert rm.evaluate(_sig(ts=t_next_day), now=t_next_day).approved is True
