from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


EVENT_TYPE_MARKET_QUOTE_V1 = "market.quote.v1"
EVENT_TYPE_MARKET_TRADE_V1 = "market.trade.v1"
SCHEMA_VERSION = "1.0.0"


class MarketQuote(BaseModel):
    """
    Normalized quote-like market data.

    This should represent the best available current quote/price state
    for a symbol at a point in time.

    For yfinance websocket payloads, you may not always have bid/ask.
    That is okay. Keep fields optional and preserve raw_payload_hash.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    symbol: str = Field(min_length=1, max_length=32)

    # Time the source says this quote/update happened.
    # If the source does not provide one, the collector can set this to collector_time.
    event_time: datetime

    # Time your worker received/normalized the event.
    collector_time: datetime

    price: Decimal | None = Field(default=None, ge=Decimal("0"))

    bid: Decimal | None = Field(default=None, ge=Decimal("0"))
    ask: Decimal | None = Field(default=None, ge=Decimal("0"))
    bid_size: Decimal | None = Field(default=None, ge=Decimal("0"))
    ask_size: Decimal | None = Field(default=None, ge=Decimal("0"))

    spread: Decimal | None = Field(default=None, ge=Decimal("0"))
    spread_bps: Decimal | None = Field(default=None, ge=Decimal("0"))

    day_volume: Decimal | None = Field(default=None, ge=Decimal("0"))
    volume: Decimal | None = Field(default=None, ge=Decimal("0"))

    exchange: str | None = None
    market_hours: str | None = None
    quote_type: str | None = None

    source_payload_ts_ms: int | None = Field(default=None, ge=0)
    collector_ts_ms: int | None = Field(default=None, ge=0)

    raw_payload_hash: str | None = None

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        return str(value).strip().upper()

    @field_validator("event_time", "collector_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime fields must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def populate_derived_fields(self) -> MarketQuote:
        if self.spread is None and self.bid is not None and self.ask is not None:
            if self.ask >= self.bid:
                self.spread = self.ask - self.bid

        if (
            self.spread_bps is None
            and self.spread is not None
            and self.bid is not None
            and self.ask is not None
        ):
            mid = (self.bid + self.ask) / Decimal("2")
            if mid > 0:
                self.spread_bps = (self.spread / mid) * Decimal("10000")

        if self.collector_ts_ms is None:
            self.collector_ts_ms = int(self.collector_time.timestamp() * 1000)

        return self


class MarketTrade(BaseModel):
    """
    Normalized trade-like market data.

    Use this when the upstream message represents an actual trade/last sale,
    rather than a quote/update snapshot.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    symbol: str = Field(min_length=1, max_length=32)

    event_time: datetime
    collector_time: datetime

    price: Decimal = Field(ge=Decimal("0"))
    size: Decimal | None = Field(default=None, ge=Decimal("0"))

    exchange: str | None = None
    trade_id: str | None = None
    conditions: list[str] | None = None

    source_payload_ts_ms: int | None = Field(default=None, ge=0)
    collector_ts_ms: int | None = Field(default=None, ge=0)

    raw_payload_hash: str | None = None

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        return str(value).strip().upper()

    @field_validator("event_time", "collector_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime fields must be timezone-aware")
        return value.astimezone(timezone.utc)

    @field_validator("conditions")
    @classmethod
    def normalize_conditions(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None

        normalized = sorted({str(item).strip() for item in value if str(item).strip()})
        return normalized or None

    @model_validator(mode="after")
    def populate_derived_fields(self) -> MarketTrade:
        if self.collector_ts_ms is None:
            self.collector_ts_ms = int(self.collector_time.timestamp() * 1000)

        return self


class MarketQuoteEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: Literal["market.quote.v1"] = EVENT_TYPE_MARKET_QUOTE_V1
    schema_version: Literal["1.0.0"] = SCHEMA_VERSION

    event_id: str
    source: str = Field(min_length=1)

    symbol: str
    event_time: datetime
    ingest_time: datetime

    quote: MarketQuote

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        return str(value).strip().upper()

    @field_validator("event_time", "ingest_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime fields must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def event_fields_must_match_quote(self) -> MarketQuoteEvent:
        if self.symbol != self.quote.symbol:
            raise ValueError("event.symbol must match quote.symbol")

        if self.event_time != self.quote.event_time:
            raise ValueError("event.event_time must match quote.event_time")

        return self

    @classmethod
    def from_quote(
        cls,
        quote: MarketQuote | dict[str, Any],
        *,
        source: str,
        ingest_time: datetime | None = None,
    ) -> MarketQuoteEvent:
        parsed_quote = (
            quote
            if isinstance(quote, MarketQuote)
            else MarketQuote.model_validate(quote)
        )

        parsed_ingest_time = ingest_time or datetime.now(timezone.utc)

        event_id = build_market_event_id(
            event_type=EVENT_TYPE_MARKET_QUOTE_V1,
            source=source,
            symbol=parsed_quote.symbol,
            event_time=parsed_quote.event_time,
            raw_payload_hash=parsed_quote.raw_payload_hash,
            discriminator={
                "price": parsed_quote.price,
                "bid": parsed_quote.bid,
                "ask": parsed_quote.ask,
                "day_volume": parsed_quote.day_volume,
            },
        )

        return cls(
            event_id=event_id,
            source=source,
            symbol=parsed_quote.symbol,
            event_time=parsed_quote.event_time,
            ingest_time=parsed_ingest_time,
            quote=parsed_quote,
        )


class MarketTradeEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: Literal["market.trade.v1"] = EVENT_TYPE_MARKET_TRADE_V1
    schema_version: Literal["1.0.0"] = SCHEMA_VERSION

    event_id: str
    source: str = Field(min_length=1)

    symbol: str
    event_time: datetime
    ingest_time: datetime

    trade: MarketTrade

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        return str(value).strip().upper()

    @field_validator("event_time", "ingest_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime fields must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def event_fields_must_match_trade(self) -> MarketTradeEvent:
        if self.symbol != self.trade.symbol:
            raise ValueError("event.symbol must match trade.symbol")

        if self.event_time != self.trade.event_time:
            raise ValueError("event.event_time must match trade.event_time")

        return self

    @classmethod
    def from_trade(
        cls,
        trade: MarketTrade | dict[str, Any],
        *,
        source: str,
        ingest_time: datetime | None = None,
    ) -> MarketTradeEvent:
        parsed_trade = (
            trade
            if isinstance(trade, MarketTrade)
            else MarketTrade.model_validate(trade)
        )

        parsed_ingest_time = ingest_time or datetime.now(timezone.utc)

        event_id = build_market_event_id(
            event_type=EVENT_TYPE_MARKET_TRADE_V1,
            source=source,
            symbol=parsed_trade.symbol,
            event_time=parsed_trade.event_time,
            raw_payload_hash=parsed_trade.raw_payload_hash,
            discriminator={
                "price": parsed_trade.price,
                "size": parsed_trade.size,
                "trade_id": parsed_trade.trade_id,
            },
        )

        return cls(
            event_id=event_id,
            source=source,
            symbol=parsed_trade.symbol,
            event_time=parsed_trade.event_time,
            ingest_time=parsed_ingest_time,
            trade=parsed_trade,
        )


def build_market_event_id(
    *,
    event_type: str,
    source: str,
    symbol: str,
    event_time: datetime,
    raw_payload_hash: str | None = None,
    discriminator: dict[str, Any] | None = None,
) -> str:
    """
    Build a deterministic event ID.

    The raw payload hash is preferred because two updates for the same symbol
    can share the same timestamp but still have different values.
    @param event_type: The type of the event, e.g. "market.quote.v1"
    @param source: The source of the data, e.g. "yfinance(YAHOO_FINANCE)_predefined_screens"
    @param symbol: The symbol this event is about
    @param event_time: The time the source says this event happened
    @param raw_payload_hash: An optional hash of the raw upstream payload for deduplication
    @param discriminator: An optional dict of additional fields to include in the hash to help differentiate
        events that might have the same timestamp and raw payload hash (e.g. multiple trades at the same ms).
    """

    normalized_time = event_time.astimezone(timezone.utc).isoformat()

    discriminator_json = json.dumps(
        _json_safe(discriminator or {}),
        sort_keys=True,
        separators=(",", ":"),
    )

    raw = "|".join(
        [
            event_type,
            source,
            symbol.upper(),
            normalized_time,
            raw_payload_hash or "",
            discriminator_json,
        ]
    )

    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"evt_{digest}"


def build_raw_payload_hash(payload: dict[str, Any]) -> str:
    """
    Hash the raw upstream payload so we can dedupe/debug without storing
    the whole raw payload inside every normalized event.
    """

    payload_json = json.dumps(
        _json_safe(payload),
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

# Helper to convert epoch ms timestamps to timezone-aware datetimes.
def datetime_from_epoch_ms(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


# Helper to convert values to JSON-safe formats for hashing. 
# We want to preserve as much info as possible in the hash, but also need to ensure consistent 
# formatting (e.g. datetimes should be in ISO format, Decimals should be strings, etc).
def _json_safe(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _json_safe(value.model_dump(mode="python", exclude_none=True))

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
            if item is not None
        }

    if isinstance(value, list):
        return [_json_safe(item) for item in value]

    return value