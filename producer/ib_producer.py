from __future__ import annotations

import logging
import math
import signal
import sys
from datetime import datetime, timezone

from ib_async import Forex, IB, Ticker

from common.config import settings
from common.contracts import TickEvent, format_timestamp
from producer.kafka_publisher import TickKafkaPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("producer.ib")

PAPER_PORTS = {7497, 4002}


def is_valid_quote(bid: float | None, ask: float | None) -> bool:
    """Return True when both bid and ask are finite positive numbers with ask >= bid."""
    if bid is None or ask is None:
        return False
    return (
        math.isfinite(bid)
        and math.isfinite(ask)
        and bid > 0
        and ask > 0
        and ask >= bid
    )


def build_tick_from_ticker(symbol: str, ticker: Ticker) -> TickEvent | None:
    """Convert an IBKR Ticker update into a validated TickEvent, or None if invalid."""
    if not is_valid_quote(ticker.bid, ticker.ask):
        return None
    ts = ticker.time or datetime.now(timezone.utc)
    return TickEvent(
        symbol=symbol,
        bid=round(float(ticker.bid), 6),
        ask=round(float(ticker.ask), 6),
        timestamp=format_timestamp(ts),
    )


def assert_paper_connection(ib: IB, port: int) -> None:
    """Block execution unless connected to an IBKR Paper Trading port and DU* account."""
    if port not in PAPER_PORTS:
        raise RuntimeError(
            f"Puerto IBKR no permitido ({port}). Solo se permiten puertos Paper Trading: {PAPER_PORTS}."
        )
    accounts = ib.managedAccounts()
    if not accounts or not all(acc.startswith("DU") for acc in accounts):
        raise RuntimeError(
            f"Cuenta IBKR no reconocida como Paper Trading (prefijo 'DU' requerido): {accounts}"
        )
    logger.info("Conectado de forma segura a cuenta Paper Trading: %s", accounts)


def run() -> None:
    """Connect to TWS/IB Gateway Paper Trading and stream Forex ticks into Kafka."""
    ib = IB()
    publisher = TickKafkaPublisher()
    last_quotes: dict[str, tuple[float, float]] = {}
    running = True

    def _handle_stop(signum, frame) -> None:
        nonlocal running
        logger.info("Señal de apagado recibida (%s). Cerrando productor IBKR...", signum)
        running = False

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    logger.info(
        "Conectando a TWS/Gateway en %s:%s (clientId=%s)...",
        settings.ib_host,
        settings.ib_port,
        settings.ib_client_id_producer,
    )
    ib.connect(
        host=settings.ib_host,
        port=settings.ib_port,
        clientId=settings.ib_client_id_producer,
        readonly=True,
    )
    assert_paper_connection(ib, settings.ib_port)

    tickers_by_symbol: dict[str, Ticker] = {}
    for symbol in settings.symbols:
        contract = Forex(symbol)
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(f"No se pudo calificar contrato FOREX en IBKR: {symbol}")
        ticker = ib.reqMktData(
            contract,
            genericTickList="",
            snapshot=False,
            regulatorySnapshot=False,
        )
        tickers_by_symbol[symbol] = ticker
        logger.info("Suscrito a mercado FOREX: %s", symbol)

    def on_pending_tickers(tickers: set[Ticker]) -> None:
        for symbol, ticker in tickers_by_symbol.items():
            if ticker not in tickers:
                continue
            tick = build_tick_from_ticker(symbol, ticker)
            if tick is None:
                continue
            quote = (tick.bid, tick.ask)
            if last_quotes.get(symbol) == quote:
                continue
            last_quotes[symbol] = quote
            publisher.publish(tick)
            logger.info("Tick -> %s", tick.to_json())

    ib.pendingTickersEvent += on_pending_tickers

    try:
        while running and ib.isConnected():
            ib.sleep(0.5)
    finally:
        ib.pendingTickersEvent -= on_pending_tickers
        if ib.isConnected():
            for ticker in tickers_by_symbol.values():
                ib.cancelMktData(ticker.contract)
        publisher.flush()
        if ib.isConnected():
            ib.disconnect()
        logger.info("Productor IBKR detenido limpiamente.")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.exception("Error fatal en ib_producer: %s", exc)
        sys.exit(1)
