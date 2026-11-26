from backtesting.download_history import generate_synthetic_candles, save_candles_csv
from backtesting.engine import BacktestConfig, load_candles_csv, run_backtest
from strategies.ema_crossover import EmaCrossoverStrategy
from strategies.rsi_reversion import RsiMeanReversionStrategy


def test_csv_roundtrip_and_backtest_metrics(tmp_path):
    candles = generate_synthetic_candles("EURUSD", num_bars=300, seed=7)
    csv_file = tmp_path / "EURUSD_1m.csv"
    save_candles_csv(candles, csv_file)

    loaded = load_candles_csv(csv_file)
    assert len(loaded) == 300
    assert loaded[0] == candles[0]

    for strategy in (EmaCrossoverStrategy(5, 13), RsiMeanReversionStrategy(14)):
        metrics, trades = run_backtest(strategy, loaded, BacktestConfig(spread_pips=0.5))
        assert metrics.candles_evaluated == 300
        assert metrics.total_trades == len(trades)
        assert 0.0 <= metrics.win_rate_pct <= 100.0
        assert metrics.max_drawdown_pct >= 0.0
