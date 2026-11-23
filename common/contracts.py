from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal


def normalize_symbol(symbol: str) -> str:
    """Normalize a Forex pair to uppercase 6-letter format (e.g. EURUSD)."""
    cleaned = symbol.strip().upper().replace("/", "").replace(".", "")
    if not cleaned or not cleaned.isalpha() or len(cleaned) != 6:
        raise ValueError(f"Símbolo inválido '{symbol}'. Debe ser formato EURUSD (6 letras).")
    return cleaned


def parse_timestamp(value: str | int | float | datetime) -> datetime:
    """Parse ISO-8601 strings or Unix timestamps (seconds or milliseconds) to UTC."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(value, str):
        raw = value.strip()
        if raw.replace(".", "", 1).isdigit():
            return parse_timestamp(float(raw))
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    raise ValueError(f"Formato de timestamp no soportado: {value!r}")


def format_timestamp(dt: datetime) -> str:
    """Format a datetime as an ISO-8601 UTC string with trailing 'Z'."""
    utc_dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return utc_dt.isoformat().replace("+00:00", "Z")


def _validate_positive_float(value: Any, field_name: str) -> float:
    num = float(value)
    if not math.isfinite(num) or num <= 0:
        raise ValueError(f"{field_name} debe ser un número finito positivo, recibido: {value!r}")
    return num


@dataclass(frozen=True)
class TickEvent:
    """Contract for events published to the 'ticks' Kafka topic."""

    symbol: str
    bid: float
    ask: float
    timestamp: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        bid_val = _validate_positive_float(self.bid, "bid")
        ask_val = _validate_positive_float(self.ask, "ask")
        if ask_val < bid_val:
            raise ValueError(f"ask ({ask_val}) no puede ser menor que bid ({bid_val})")
        object.__setattr__(self, "bid", bid_val)
        object.__setattr__(self, "ask", ask_val)
        object.__setattr__(self, "timestamp", format_timestamp(parse_timestamp(self.timestamp)))

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    def to_json(self) -> str:
        return json.dumps({
            "symbol": self.symbol,
            "bid": self.bid,
            "ask": self.ask,
            "timestamp": self.timestamp,
        })

    @classmethod
    def from_json(cls, payload: str | bytes) -> TickEvent:
        data = json.loads(payload)
        return cls(
            symbol=data["symbol"],
            bid=data["bid"],
            ask=data["ask"],
            timestamp=data["timestamp"],
        )


@dataclass(frozen=True)
class CandleEvent:
    """Contract for 1-minute OHLC candles published to the 'velas' Kafka topic."""

    symbol: str
    inicio: str
    open: float
    high: float
    low: float
    close: float
    num_ticks: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "inicio", format_timestamp(parse_timestamp(self.inicio)))
        o = _validate_positive_float(self.open, "open")
        h = _validate_positive_float(self.high, "high")
        l = _validate_positive_float(self.low, "low")
        c = _validate_positive_float(self.close, "close")
        if h < max(o, c, l) or l > min(o, c, h):
            raise ValueError(f"Inconsistencia OHLC: O={o}, H={h}, L={l}, C={c}")
        ticks = int(self.num_ticks)
        if ticks <= 0:
            raise ValueError("num_ticks debe ser >= 1")
        object.__setattr__(self, "open", o)
        object.__setattr__(self, "high", h)
        object.__setattr__(self, "low", l)
        object.__setattr__(self, "close", c)
        object.__setattr__(self, "num_ticks", ticks)

    def to_json(self) -> str:
        return json.dumps({
            "symbol": self.symbol,
            "inicio": self.inicio,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "num_ticks": self.num_ticks,
        })

    @classmethod
    def from_json(cls, payload: str | bytes) -> CandleEvent:
        data = json.loads(payload)
        return cls(
            symbol=data["symbol"],
            inicio=data["inicio"],
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            num_ticks=data["num_ticks"],
        )


@dataclass(frozen=True)
class SignalEvent:
    """Contract for trading signals published to the 'signals' Kafka topic."""

    id_algoritmo: str
    symbol: str
    signal: Literal["BUY", "SELL"]
    precio_referencia: float
    timestamp: str

    def __post_init__(self) -> None:
        algo = self.id_algoritmo.strip()
        if not algo:
            raise ValueError("id_algoritmo no puede estar vacío")
        sig = self.signal.strip().upper()
        if sig not in ("BUY", "SELL"):
            raise ValueError(f"signal debe ser BUY o SELL, recibido: {self.signal!r}")
        object.__setattr__(self, "id_algoritmo", algo)
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "signal", sig)
        object.__setattr__(
            self,
            "precio_referencia",
            _validate_positive_float(self.precio_referencia, "precio_referencia"),
        )
        object.__setattr__(self, "timestamp", format_timestamp(parse_timestamp(self.timestamp)))

    def to_json(self) -> str:
        return json.dumps({
            "id_algoritmo": self.id_algoritmo,
            "symbol": self.symbol,
            "signal": self.signal,
            "precio_referencia": self.precio_referencia,
            "timestamp": self.timestamp,
        })

    @classmethod
    def from_json(cls, payload: str | bytes) -> SignalEvent:
        data = json.loads(payload)
        return cls(
            id_algoritmo=data["id_algoritmo"],
            symbol=data["symbol"],
            signal=data["signal"],
            precio_referencia=data["precio_referencia"],
            timestamp=data["timestamp"],
        )
