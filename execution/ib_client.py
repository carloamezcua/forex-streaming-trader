from __future__ import annotations

import logging
from dataclasses import dataclass

from ib_async import Forex, IB, MarketOrder

from common.config import settings

logger = logging.getLogger("execution.ib_client")

PAPER_PORTS = {7497, 4002}


@dataclass(frozen=True)
class OrderExecutionReport:
    order_id: int
    status: str
    filled: float
    avg_fill_price: float


class IBPaperExecutor:
    """Safe wrapper around ib_async enforcing Paper Trading only."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        client_id: int | None = None,
    ) -> None:
        self.host = host or settings.ib_host
        self.port = port or settings.ib_port
        self.client_id = client_id or settings.ib_client_id_executor
        self._ib = IB()

    def connect(self) -> None:
        """Connect to TWS/IB Gateway and verify Paper Trading port and DU* account."""
        if self.port not in PAPER_PORTS:
            raise RuntimeError(
                f"EJECUCIÓN BLOQUEADA: puerto {self.port} no es de Paper Trading ({PAPER_PORTS})."
            )
        self._ib.connect(self.host, self.port, clientId=self.client_id, readonly=False)
        accounts = self._ib.managedAccounts()
        if not accounts or not all(acc.startswith("DU") for acc in accounts):
            self._ib.disconnect()
            raise RuntimeError(
                f"EJECUCIÓN BLOQUEADA: cuenta no es Paper Trading (prefijo 'DU' requerido): {accounts}"
            )
        logger.info("IBPaperExecutor conectado de forma segura a cuenta Paper: %s", accounts)

    def disconnect(self) -> None:
        """Disconnect cleanly from TWS/IB Gateway if connected."""
        if self._ib.isConnected():
            self._ib.disconnect()

    def get_open_forex_positions(self, tracked_symbols: tuple[str, ...]) -> dict[str, int]:
        """Synchronize existing cash positions from IBKR Paper account."""
        positions = {sym: 0 for sym in tracked_symbols}
        for pos in self._ib.positions():
            if pos.contract.secType != "CASH":
                continue
            sym = f"{pos.contract.symbol}{pos.contract.currency}".upper()
            if sym in positions:
                positions[sym] = int(round(pos.position))
        return positions

    def place_market_order(self, symbol: str, side: str, quantity: int) -> OrderExecutionReport:
        """Submit a Forex market order to the Paper Trading account and await initial status."""
        contract = Forex(symbol)
        qualified = self._ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(f"No se pudo calificar contrato FOREX en IBKR: {symbol}")
        order = MarketOrder(action=side, totalQuantity=quantity)
        trade = self._ib.placeOrder(contract, order)

        # Esperar brevemente actualización inicial de estado
        for _ in range(20):
            self._ib.sleep(0.25)
            if trade.isDone():
                break

        return OrderExecutionReport(
            order_id=trade.order.orderId,
            status=trade.orderStatus.status,
            filled=float(trade.orderStatus.filled),
            avg_fill_price=float(trade.orderStatus.avgFillPrice),
        )
