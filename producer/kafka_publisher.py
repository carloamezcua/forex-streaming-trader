from __future__ import annotations

import logging
from typing import Callable

from confluent_kafka import Producer

from common.config import settings
from common.contracts import TickEvent

logger = logging.getLogger(__name__)


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Error publicando tick en Kafka: %s", err)


class TickKafkaPublisher:
    """Publishes validated TickEvent payloads to the Kafka 'ticks' topic."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        producer_factory: Callable[[dict], Producer] | None = None,
    ) -> None:
        self.topic = topic or settings.topic_ticks
        conf = {
            "bootstrap.servers": bootstrap_servers or settings.kafka_bootstrap_servers,
            "client.id": "forex-tick-producer",
            "acks": "1",
            "linger.ms": 5,
        }
        factory = producer_factory or Producer
        self._producer = factory(conf)

    def publish(self, tick: TickEvent) -> None:
        """Serialize and asynchronously publish a TickEvent keyed by symbol."""
        payload = tick.to_json().encode("utf-8")
        key = tick.symbol.encode("utf-8")
        self._producer.produce(
            topic=self.topic,
            key=key,
            value=payload,
            callback=_delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> None:
        """Flush any buffered messages to Kafka before shutdown."""
        self._producer.flush(timeout)
