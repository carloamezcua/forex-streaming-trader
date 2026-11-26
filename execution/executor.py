from __future__ import annotations

import argparse
import json
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

from common.config import settings
from common.contracts import SignalEvent
from execution.ib_client import IBPaperExecutor
from execution.risk import RiskManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("execution.executor")


def append_audit_record(audit_path: Path, record: dict) -> None:
    """Append a single JSONL audit record for every evaluated trading signal."""
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def run(dry_run: bool = False, audit_file: str = "data/execution_audit.jsonl") -> None:
    """Consume signals from Kafka, apply pre-trade risk controls, and route approved orders."""
    audit_path = Path(audit_file)
    ib_executor: IBPaperExecutor | None = None
    initial_positions: dict[str, int] = {}

    if not dry_run:
        ib_executor = IBPaperExecutor()
        ib_executor.connect()
        initial_positions = ib_executor.get_open_forex_positions(settings.symbols)
        logger.info("Posiciones iniciales sincronizadas desde IBKR Paper: %s", initial_positions)
    else:
        logger.info("Ejecutor iniciado en modo --dry-run (no enviará órdenes a IBKR).")

    risk_manager = RiskManager.from_settings(settings, initial_positions=initial_positions)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "execution-risk-service",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )

    running = True

    def _stop(signum, frame) -> None:
        nonlocal running
        logger.info("Deteniendo servicio de ejecución...")
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer.subscribe([settings.topic_signals])
    logger.info("Escuchando señales en tópico '%s'...", settings.topic_signals)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Error de Kafka consumer: %s", msg.error())
                continue

            try:
                sig = SignalEvent.from_json(msg.value())
            except Exception as exc:
                logger.warning("Señal inválida descartada: %s | payload=%r", exc, msg.value())
                continue

            now = datetime.now(timezone.utc)
            decision = risk_manager.evaluate(sig, now=now)

            audit = {
                "evaluated_at": now.isoformat().replace("+00:00", "Z"),
                "dry_run": dry_run,
                "signal": json.loads(sig.to_json()),
                "approved": decision.approved,
                "reason": decision.reason,
                "order_size": decision.order_size,
                "projected_position": decision.projected_position,
            }

            if not decision.approved:
                logger.warning(
                    "RECHAZADA [%s %s %s]: %s",
                    sig.id_algoritmo,
                    sig.symbol,
                    sig.signal,
                    decision.reason,
                )
                append_audit_record(audit_path, audit)
                continue

            if dry_run or ib_executor is None:
                risk_manager.record_execution(sig, now=now)
                audit["execution"] = {"status": "DRY_RUN_APPROVED"}
                logger.info(
                    "APROBADA (DRY-RUN) [%s] %s %d %s | pos=%d",
                    sig.id_algoritmo,
                    sig.signal,
                    decision.order_size,
                    sig.symbol,
                    risk_manager.positions[sig.symbol],
                )
            else:
                try:
                    report = ib_executor.place_market_order(sig.symbol, sig.signal, decision.order_size)
                    risk_manager.record_execution(sig, now=now)
                    audit["execution"] = {
                        "order_id": report.order_id,
                        "status": report.status,
                        "filled": report.filled,
                        "avg_fill_price": report.avg_fill_price,
                    }
                    logger.info(
                        "ORDEN ENVIADA A IBKR PAPER #%d | %s %d %s | status=%s | fill=%.5f",
                        report.order_id,
                        sig.signal,
                        decision.order_size,
                        sig.symbol,
                        report.status,
                        report.avg_fill_price,
                    )
                except Exception as exc:
                    audit["execution"] = {"status": "ERROR", "error": str(exc)}
                    logger.exception("Error enviando orden a IBKR Paper: %s", exc)

            append_audit_record(audit_path, audit)
    finally:
        consumer.close()
        if ib_executor is not None:
            ib_executor.disconnect()
        logger.info("Servicio de ejecución detenido limpiamente.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Servicio de riesgo y ejecución en IBKR Paper")
    parser.add_argument("--dry-run", action="store_true", help="Evaluar riesgo sin enviar órdenes reales a TWS")
    parser.add_argument("--audit-file", default="data/execution_audit.jsonl", help="Ruta del log de auditoría JSONL")
    args = parser.parse_args()
    run(dry_run=args.dry_run, audit_file=args.audit_file)
