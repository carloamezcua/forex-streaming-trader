from __future__ import annotations

import csv
import math
from dataclasses import asdict, dataclass
from pathlib import Path

from common.contracts import CandleEvent
from strategies.base import BaseStrategy


@dataclass(frozen=True)
class BacktestConfig:
    initial_capital: float = 100_000.0
    order_size_units: int = 10_000
    spread_pips: float = 0.8
    pip_size: float = 0.0001


@dataclass(frozen=True)
class TradeRecord:
    symbol: str
    side: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    units: int
    pnl_usd: float


@dataclass(frozen=True)
class BacktestMetrics:
    id_algoritmo: str
    symbol: str
    candles_evaluated: int
    total_trades: int
    win_rate_pct: float
    total_pnl_usd: float
    return_pct: float
    profit_factor: float
    max_drawdown_pct: float
    sharpe_ratio: float
    avg_trade_pnl_usd: float

    def to_dict(self) -> dict:
        return asdict(self)


def load_candles_csv(csv_path: str | Path) -> list[CandleEvent]:
    """Load 1-minute CandleEvent instances from a CSV dataset."""
    candles: list[CandleEvent] = []
    with Path(csv_path).open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            candles.append(
                CandleEvent(
                    symbol=row["symbol"],
                    inicio=row["inicio"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    num_ticks=int(row["num_ticks"]),
                )
            )
    return candles


def run_backtest(
    strategy: BaseStrategy,
    candles: list[CandleEvent],
    config: BacktestConfig | None = None,
) -> tuple[BacktestMetrics, list[TradeRecord]]:
    """Evaluate a BaseStrategy over historical 1-minute candles with spread cost modeling."""
    if not candles:
        raise ValueError("La lista de velas no puede estar vacía")

    cfg = config or BacktestConfig()
    strategy.reset()
    half_spread = (cfg.spread_pips * cfg.pip_size) / 2.0

    position_side: str | None = None  # "BUY" (long) | "SELL" (short) | None (flat)
    entry_price: float = 0.0
    entry_time: str = ""
    realized_pnl: float = 0.0
    trades: list[TradeRecord] = []
    equity_curve: list[float] = []

    def _close_position(exit_mid: float, exit_time: str, symbol: str) -> None:
        nonlocal position_side, entry_price, entry_time, realized_pnl
        if position_side is None:
            return
        if position_side == "BUY":
            exit_exec = exit_mid - half_spread
            pnl = (exit_exec - entry_price) * cfg.order_size_units
        else:
            exit_exec = exit_mid + half_spread
            pnl = (entry_price - exit_exec) * cfg.order_size_units

        realized_pnl += pnl
        trades.append(
            TradeRecord(
                symbol=symbol,
                side=position_side,
                entry_time=entry_time,
                exit_time=exit_time,
                entry_price=round(entry_price, 6),
                exit_price=round(exit_exec, 6),
                units=cfg.order_size_units,
                pnl_usd=round(pnl, 4),
            )
        )
        position_side = None

    for candle in candles:
        sig = strategy.on_candle(candle)
        if sig is not None and sig.signal != position_side:
            if position_side is not None:
                _close_position(candle.close, candle.inicio, candle.symbol)
            position_side = sig.signal
            entry_price = (
                candle.close + half_spread if sig.signal == "BUY" else candle.close - half_spread
            )
            entry_time = candle.inicio

        # Valuación mark-to-market de la curva de capital
        unrealized = 0.0
        if position_side == "BUY":
            unrealized = ((candle.close - half_spread) - entry_price) * cfg.order_size_units
        elif position_side == "SELL":
            unrealized = (entry_price - (candle.close + half_spread)) * cfg.order_size_units
        equity_curve.append(cfg.initial_capital + realized_pnl + unrealized)

    # Cierre forzado al final del periodo histórico
    last_candle = candles[-1]
    if position_side is not None:
        _close_position(last_candle.close, last_candle.inicio, last_candle.symbol)
        equity_curve[-1] = cfg.initial_capital + realized_pnl

    total_trades = len(trades)
    wins = [t.pnl_usd for t in trades if t.pnl_usd > 0]
    losses = [abs(t.pnl_usd) for t in trades if t.pnl_usd < 0]
    win_rate = (len(wins) / total_trades * 100.0) if total_trades else 0.0
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    else:
        profit_factor = float("inf") if gross_profit > 0 else 0.0

    # Max Drawdown (%)
    peak = cfg.initial_capital
    max_dd = 0.0
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak * 100.0 if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Sharpe Ratio anualizado sobre retornos por vela de 1 minuto (~252 días * 1440 min)
    bar_returns = [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        for i in range(1, len(equity_curve))
        if equity_curve[i - 1] > 0
    ]
    sharpe = 0.0
    if len(bar_returns) > 1:
        mean_r = sum(bar_returns) / len(bar_returns)
        var_r = sum((r - mean_r) ** 2 for r in bar_returns) / (len(bar_returns) - 1)
        std_r = math.sqrt(var_r)
        if std_r > 0:
            sharpe = (mean_r / std_r) * math.sqrt(252 * 1440)

    metrics = BacktestMetrics(
        id_algoritmo=strategy.id_algoritmo,
        symbol=candles[0].symbol,
        candles_evaluated=len(candles),
        total_trades=total_trades,
        win_rate_pct=round(win_rate, 2),
        total_pnl_usd=round(realized_pnl, 2),
        return_pct=round(realized_pnl / cfg.initial_capital * 100.0, 4),
        profit_factor=round(profit_factor, 3) if math.isfinite(profit_factor) else 999.0,
        max_drawdown_pct=round(max_dd, 4),
        sharpe_ratio=round(sharpe, 3),
        avg_trade_pnl_usd=round(realized_pnl / total_trades, 2) if total_trades else 0.0,
    )
    return metrics, trades
