from __future__ import annotations

import argparse
import json
from pathlib import Path

from backtesting.engine import BacktestConfig, load_candles_csv, run_backtest
from common.config import settings
from strategies.registry import create_all_strategies


def main() -> None:
    """Compare all registered strategies over a historical CSV dataset and save JSON metrics."""
    parser = argparse.ArgumentParser(description="Ejecuta y compara estrategias en backtesting histórico")
    parser.add_argument("--csv", required=True, help="Ruta al archivo CSV de velas 1m")
    parser.add_argument("--spread-pips", type=float, default=0.8, help="Spread simulado en pips")
    parser.add_argument("--output", default="data/backtest_results.json", help="Archivo JSON de resultados")
    args = parser.parse_args()

    candles = load_candles_csv(args.csv)
    cfg = BacktestConfig(
        order_size_units=settings.order_size_units,
        spread_pips=args.spread_pips,
    )

    results = []
    print(f"\n=== BACKTEST ({candles[0].symbol} | {len(candles)} velas 1m | spread={cfg.spread_pips} pips) ===\n")
    header = f"{'Algoritmo':<24} {'Trades':>6} {'Win%':>7} {'PnL($)':>10} {'Ret%':>8} {'PF':>7} {'MaxDD%':>8} {'Sharpe':>8}"
    print(header)
    print("-" * len(header))

    for strategy in create_all_strategies():
        metrics, _ = run_backtest(strategy, candles, config=cfg)
        results.append(metrics.to_dict())
        print(
            f"{metrics.id_algoritmo:<24} "
            f"{metrics.total_trades:>6d} "
            f"{metrics.win_rate_pct:>6.2f}% "
            f"{metrics.total_pnl_usd:>10.2f} "
            f"{metrics.return_pct:>7.3f}% "
            f"{metrics.profit_factor:>7.2f} "
            f"{metrics.max_drawdown_pct:>7.3f}% "
            f"{metrics.sharpe_ratio:>8.2f}"
        )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nResultados guardados en: {out_path}\n")


if __name__ == "__main__":
    main()
