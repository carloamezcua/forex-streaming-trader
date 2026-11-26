from __future__ import annotations

import argparse
import logging
import math
import random
import time
from datetime import datetime, timezone

from common.config import settings
from common.contracts import TickEvent, format_timestamp
from producer.kafka_publisher import TickKafkaPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("producer.mock")

DEFAULT_BASE_PRICES = {
    "EURUSD": 1.0850,
    "GBPUSD": 1.2720,
    "USDJPY": 153.50,
}


def generate_next_price(
    current_mid: float,
    step: int,
    volatility: float = 0.00018,
    trend_amplitude: float = 0.00009,
    rng: random.Random | None = None,
) -> tuple[float, float, float]:
    """Generate a realistic next (mid, bid, ask) with mild cyclical trend so strategies trigger signals."""
    gen = rng or random
    cyclical_drift = trend_amplitude * math.sin(step / 45.0)
    shock = gen.gauss(0.0, volatility)
    next_mid = max(0.01, current_mid + cyclical_drift + shock)
    half_spread = gen.uniform(0.00003, 0.00008)
    bid = round(next_mid - half_spread, 5)
    ask = round(next_mid + half_spread, 5)
    return next_mid, bid, ask


def run(interval_sec: float, max_ticks: int | None, seed: int | None) -> None:
    """Emit synthetic Forex ticks into Kafka at a fixed interval for 24/7 testing."""
    rng = random.Random(seed)
    publisher = TickKafkaPublisher()
    mids = {sym: DEFAULT_BASE_PRICES.get(sym, 1.1000) for sym in settings.symbols}
    step = 0
    emitted = 0

    logger.info(
        "Iniciando simulador de ticks para %s (intervalo=%.2fs, max_ticks=%s)",
        settings.symbols,
        interval_sec,
        max_ticks,
    )
    try:
        while max_ticks is None or emitted < max_ticks:
            step += 1
            now = datetime.now(timezone.utc)
            for symbol in settings.symbols:
                next_mid, bid, ask = generate_next_price(mids[symbol], step=step, rng=rng)
                mids[symbol] = next_mid
                tick = TickEvent(symbol=symbol, bid=bid, ask=ask, timestamp=format_timestamp(now))
                publisher.publish(tick)
                emitted += 1
                logger.info("MockTick #%d -> %s", emitted, tick.to_json())
                if max_ticks is not None and emitted >= max_ticks:
                    break
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        logger.info("Simulador detenido por el usuario.")
    finally:
        publisher.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulador de ticks FOREX hacia Kafka")
    parser.add_argument("--interval", type=float, default=0.5, help="Segundos entre rondas de ticks")
    parser.add_argument("--max-ticks", type=int, default=None, help="Número máximo de ticks a emitir")
    parser.add_argument("--seed", type=int, default=42, help="Semilla aleatoria reproducible")
    args = parser.parse_args()
    run(interval_sec=args.interval, max_ticks=args.max_ticks, seed=args.seed)
