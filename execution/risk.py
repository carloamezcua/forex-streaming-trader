from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from common.config import Settings
from common.contracts import SignalEvent, parse_timestamp


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str
    order_size: int = 0
    projected_position: int = 0


class RiskManager:
    """Stateful pre-trade risk controls for incoming SignalEvents."""

    def __init__(
        self,
        allowed_symbols: tuple[str, ...],
        order_size_units: int,
        max_position_units: int,
        signal_cooldown_sec: int,
        max_signal_age_sec: int,
        max_daily_orders: int,
        initial_positions: dict[str, int] | None = None,
    ) -> None:
        self.allowed_symbols = set(allowed_symbols)
        self.order_size_units = order_size_units
        self.max_position_units = max_position_units
        self.signal_cooldown_sec = signal_cooldown_sec
        self.max_signal_age_sec = max_signal_age_sec
        self.max_daily_orders = max_daily_orders

        self.positions: dict[str, int] = dict(initial_positions or {})
        self._last_execution_by_symbol: dict[str, datetime] = {}
        self._daily_orders_count: int = 0
        self._current_utc_day: date | None = None

    @classmethod
    def from_settings(
        cls,
        cfg: Settings,
        initial_positions: dict[str, int] | None = None,
    ) -> RiskManager:
        """Build a RiskManager configured from centralized Settings."""
        return cls(
            allowed_symbols=cfg.symbols,
            order_size_units=cfg.order_size_units,
            max_position_units=cfg.max_position_units,
            signal_cooldown_sec=cfg.signal_cooldown_sec,
            max_signal_age_sec=cfg.max_signal_age_sec,
            max_daily_orders=cfg.max_daily_orders,
            initial_positions=initial_positions,
        )

    def _rollover_day_if_needed(self, now: datetime) -> None:
        today = now.astimezone(timezone.utc).date()
        if self._current_utc_day != today:
            self._current_utc_day = today
            self._daily_orders_count = 0

    def evaluate(self, signal: SignalEvent, now: datetime | None = None) -> RiskDecision:
        """Evaluate all pre-trade risk rules for an incoming SignalEvent."""
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._rollover_day_if_needed(now_utc)

        # 1. Símbolo autorizado
        if signal.symbol not in self.allowed_symbols:
            return RiskDecision(False, f"Símbolo no autorizado: {signal.symbol}")

        # 2. Frescura de señal
        signal_time = parse_timestamp(signal.timestamp)
        age_sec = (now_utc - signal_time).total_seconds()
        if age_sec > self.max_signal_age_sec:
            return RiskDecision(
                False,
                f"Señal expirada ({age_sec:.1f}s > {self.max_signal_age_sec}s)",
            )
        if age_sec < -30.0:
            return RiskDecision(False, f"Timestamp en el futuro ({age_sec:.1f}s)")

        # 3. Límite diario de órdenes
        if self._daily_orders_count >= self.max_daily_orders:
            return RiskDecision(
                False,
                f"Límite diario de órdenes alcanzado ({self._daily_orders_count}/{self.max_daily_orders})",
            )

        # 4. Cooldown por símbolo
        last_exec = self._last_execution_by_symbol.get(signal.symbol)
        if last_exec is not None:
            elapsed = (now_utc - last_exec).total_seconds()
            if elapsed < self.signal_cooldown_sec:
                return RiskDecision(
                    False,
                    f"Cooldown activo para {signal.symbol} ({elapsed:.1f}s < {self.signal_cooldown_sec}s)",
                )

        # 5. Límite de posición neta máxima
        current_pos = self.positions.get(signal.symbol, 0)
        delta = self.order_size_units if signal.signal == "BUY" else -self.order_size_units
        projected_pos = current_pos + delta
        if abs(projected_pos) > self.max_position_units:
            return RiskDecision(
                False,
                f"Excede posición máxima para {signal.symbol}: proyectada={projected_pos}, max=±{self.max_position_units}",
                order_size=0,
                projected_position=projected_pos,
            )

        return RiskDecision(
            approved=True,
            reason="OK",
            order_size=self.order_size_units,
            projected_position=projected_pos,
        )

    def record_execution(self, signal: SignalEvent, now: datetime | None = None) -> None:
        """Update tracked position, cooldown timestamp, and daily order counter."""
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._rollover_day_if_needed(now_utc)
        delta = self.order_size_units if signal.signal == "BUY" else -self.order_size_units
        self.positions[signal.symbol] = self.positions.get(signal.symbol, 0) + delta
        self._last_execution_by_symbol[signal.symbol] = now_utc
        self._daily_orders_count += 1
