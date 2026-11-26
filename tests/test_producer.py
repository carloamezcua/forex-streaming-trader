import math
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.contracts import TickEvent
from producer.ib_producer import assert_paper_connection, build_tick_from_ticker, is_valid_quote
from producer.kafka_publisher import TickKafkaPublisher
from producer.mock_producer import generate_next_price


class FakeKafkaProducer:
    def __init__(self, conf):
        self.conf = conf
        self.messages = []
        self.flushed = False

    def produce(self, topic, key, value, callback=None):
        self.messages.append((topic, key, value))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        self.flushed = True


def test_is_valid_quote_filters_invalid_ib_values():
    assert is_valid_quote(1.0850, 1.0852) is True
    assert is_valid_quote(-1.0, 1.0852) is False
    assert is_valid_quote(math.nan, 1.0852) is False
    assert is_valid_quote(1.0860, 1.0850) is False
    assert is_valid_quote(None, 1.0850) is False


def test_build_tick_from_ticker_valid_and_invalid():
    valid_ticker = SimpleNamespace(
        bid=1.0851,
        ask=1.0853,
        time=datetime(2026, 11, 23, 15, 0, tzinfo=timezone.utc),
    )
    tick = build_tick_from_ticker("eur/usd", valid_ticker)
    assert tick is not None
    assert tick.symbol == "EURUSD"
    assert tick.bid == 1.0851
    assert tick.ask == 1.0853

    invalid_ticker = SimpleNamespace(bid=-1.0, ask=-1.0, time=None)
    assert build_tick_from_ticker("EURUSD", invalid_ticker) is None


def test_assert_paper_connection_blocks_live_port_and_live_account():
    fake_paper_ib = SimpleNamespace(managedAccounts=lambda: ["DU1234567"])
    assert_paper_connection(fake_paper_ib, 7497)

    with pytest.raises(RuntimeError, match="Paper Trading"):
        assert_paper_connection(fake_paper_ib, 7496)

    fake_live_ib = SimpleNamespace(managedAccounts=lambda: ["U9999999"])
    with pytest.raises(RuntimeError, match="DU"):
        assert_paper_connection(fake_live_ib, 7497)


def test_tick_kafka_publisher_serializes_with_symbol_key():
    fake = FakeKafkaProducer({})
    pub = TickKafkaPublisher(topic="ticks", producer_factory=lambda conf: fake)
    tick = TickEvent(symbol="EURUSD", bid=1.0850, ask=1.0852, timestamp="2026-11-23T15:00:00Z")
    pub.publish(tick)
    pub.flush()

    assert fake.flushed is True
    assert len(fake.messages) == 1
    topic, key, value = fake.messages[0]
    assert topic == "ticks"
    assert key == b"EURUSD"
    assert TickEvent.from_json(value) == tick


def test_mock_price_generator_produces_valid_spread():
    mid, bid, ask = generate_next_price(1.0850, step=10)
    assert mid > 0
    assert 0 < bid <= ask
