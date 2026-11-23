import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Centralized runtime configuration loaded from environment variables."""

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_ticks: str = os.getenv("TOPIC_TICKS", "ticks")
    topic_velas: str = os.getenv("TOPIC_VELAS", "velas")
    topic_signals: str = os.getenv("TOPIC_SIGNALS", "signals")

    ib_host: str = os.getenv("IB_HOST", "127.0.0.1")
    ib_port: int = int(os.getenv("IB_PORT", "7497"))
    ib_client_id_producer: int = int(os.getenv("IB_CLIENT_ID_PRODUCER", "1"))
    ib_client_id_executor: int = int(os.getenv("IB_CLIENT_ID_EXECUTOR", "2"))
    ib_client_id_history: int = int(os.getenv("IB_CLIENT_ID_HISTORY", "3"))

    symbols: tuple[str, ...] = tuple(
        s.strip().upper().replace("/", "")
        for s in os.getenv("TRADING_SYMBOLS", "EURUSD,GBPUSD").split(",")
        if s.strip()
    )
    order_size_units: int = int(os.getenv("ORDER_SIZE_UNITS", "10000"))
    max_position_units: int = int(os.getenv("MAX_POSITION_UNITS", "30000"))
    signal_cooldown_sec: int = int(os.getenv("SIGNAL_COOLDOWN_SEC", "60"))
    max_signal_age_sec: int = int(os.getenv("MAX_SIGNAL_AGE_SEC", "120"))
    max_daily_orders: int = int(os.getenv("MAX_DAILY_ORDERS", "20"))


settings = Settings()
