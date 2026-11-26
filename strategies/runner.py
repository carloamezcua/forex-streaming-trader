from __future__ import annotations

import argparse
import logging
import signal
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

from common.config import settings
from common.contracts import CandleEvent, SignalEvent, format_timestamp
from strategies.registry import create_strategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("strategies.runner")


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Error publicando señal en Kafka: %s", err)


def run(strategy_name: str, from_beginning: bool = False) -> None:
    """Consume 1-minute candles from Kafka and emit strategy signals to 'signals'."""
    strategy = create_strategy(strategy_name)
    group_id = f"strategy-{strategy.id_algoritmo}"

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": True,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": f"producer-{strategy.id_algoritmo}",
            "acks": "1",
        }
    )

    running = True

    def _stop(signum, frame) -> None:
        nonlocal running
        logger.info("Deteniendo estrategia %s...", strategy.id_algoritmo)
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer.subscribe([settings.topic_velas])
    logger.info(
        "Estrategia '%s' escuchando '%s' (group.id=%s) -> publicando en '%s'",
        strategy.id_algoritmo,
        settings.topic_velas,
        group_id,
        settings.topic_signals,
    )

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
                candle = CandleEvent.from_json(msg.value())
            except Exception as exc:
                logger.warning("Vela inválida descartada: %s | payload=%r", exc, msg.value())
                continue

            sig = strategy.on_candle(candle)
            if sig is None:
                continue

            # En tiempo real sellamos la señal con el instante actual UTC para la validación de frescura
            live_signal = SignalEvent(
                id_algoritmo=sig.id_algoritmo,
                symbol=sig.symbol,
                signal=sig.signal,
                precio_referencia=sig.precio_referencia,
                timestamp=format_timestamp(datetime.now(timezone.utc)),
            )
            producer.produce(
                topic=settings.topic_signals,
                key=live_signal.symbol.encode("utf-8"),
                value=live_signal.to_json().encode("utf-8"),
                callback=_delivery_report,
            )
            producer.poll(0)
            logger.info("SEÑAL EMITIDA -> %s", live_signal.to_json())
    finally:
        producer.flush(5.0)
        consumer.close()
        logger.info("Estrategia '%s' cerrada.", strategy.id_algoritmo)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runner de estrategia algorítmica sobre Kafka")
    parser.add_argument("--strategy", required=True, choices=["ema", "rsi"], help="Nombre de la estrategia")
    parser.add_argument("--from-beginning", action="store_true", help="Leer velas desde el inicio del tópico")
    args = parser.parse_args()
    run(strategy_name=args.strategy, from_beginning=args.from_beginning)
