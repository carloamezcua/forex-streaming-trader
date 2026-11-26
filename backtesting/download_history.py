from __future__ import annotations

import argparse
import csv
import logging
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.config import settings
from common.contracts import CandleEvent, format_timestamp, normalize_symbol, parse_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("backtesting.download")


def save_candles_csv(candles: list[CandleEvent], output_path: Path) -> None:
    """Write 1-minute CandleEvent rows to a CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["symbol", "inicio", "open", "high", "low", "close", "num_ticks"],
        )
        writer.writeheader()
        for c in candles:
            writer.writerow(
                {
                    "symbol": c.symbol,
                    "inicio": c.inicio,
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "num_ticks": c.num_ticks,
                }
            )
    logger.info("Guardadas %d velas en %s", len(candles), output_path)


def generate_synthetic_candles(
    symbol: str,
    num_bars: int = 2000,
    start_price: float = 1.0850,
    seed: int = 42,
) -> list[CandleEvent]:
    """Generate realistic synthetic 1-minute FOREX candles for offline backtesting."""
    sym = normalize_symbol(symbol)
    rng = random.Random(seed)
    start_dt = datetime(2026, 11, 16, 0, 0, tzinfo=timezone.utc)
    candles: list[CandleEvent] = []
    price = start_price

    for i in range(num_bars):
        # Combina ciclos suaves de tendencia + ruido intradía
        drift = 0.00012 * math.sin(i / 60.0) + 0.00005 * math.cos(i / 17.0)
        o = price
        c = max(0.05, o + drift + rng.gauss(0.0, 0.00035))
        wick_up = abs(rng.gauss(0.0, 0.00022))
        wick_down = abs(rng.gauss(0.0, 0.00022))
        h = max(o, c) + wick_up
        l = max(0.01, min(o, c) - wick_down)
        price = c

        candles.append(
            CandleEvent(
                symbol=sym,
                inicio=format_timestamp(start_dt + timedelta(minutes=i)),
                open=round(o, 6),
                high=round(h, 6),
                low=round(l, 6),
                close=round(c, 6),
                num_ticks=rng.randint(15, 90),
            )
        )
    return candles


def download_from_ibkr(symbol: str, duration: str = "5 D") -> list[CandleEvent]:
    """Download 1-minute historical midpoint bars from IBKR TWS/Gateway."""
    from ib_async import Forex, IB

    sym = normalize_symbol(symbol)
    ib = IB()
    ib.connect(
        host=settings.ib_host,
        port=settings.ib_port,
        clientId=settings.ib_client_id_history,
        readonly=True,
    )
    try:
        contract = Forex(sym)
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(f"No se pudo calificar contrato FOREX en IBKR: {sym}")
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting="1 min",
            whatToShow="MIDPOINT",
            useRTH=False,
            formatDate=2,
        )
        return [
            CandleEvent(
                symbol=sym,
                inicio=format_timestamp(parse_timestamp(bar.date)),
                open=round(float(bar.open), 6),
                high=round(float(bar.high), 6),
                low=round(float(bar.low), 6),
                close=round(float(bar.close), 6),
                num_ticks=max(1, int(getattr(bar, "barCount", 1) or 1)),
            )
            for bar in bars
        ]
    finally:
        if ib.isConnected():
            ib.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descarga o genera velas históricas 1m para backtesting")
    parser.add_argument("--symbol", default="EURUSD", help="Par FOREX (ej. EURUSD, GBPUSD)")
    parser.add_argument("--duration", default="5 D", help="Duración histórica IBKR (ej. '2 D', '5 D')")
    parser.add_argument("--synthetic", action="store_true", help="Generar dataset sintético sin conectar a IBKR")
    parser.add_argument("--bars", type=int, default=2000, help="Cantidad de velas en modo sintético")
    parser.add_argument("--output", default=None, help="Archivo CSV de salida")
    args = parser.parse_args()

    sym = normalize_symbol(args.symbol)
    out = Path(args.output or f"data/{sym}_1m.csv")
    if args.synthetic:
        data = generate_synthetic_candles(sym, num_bars=args.bars)
    else:
        data = download_from_ibkr(sym, duration=args.duration)
    save_candles_csv(data, out)
