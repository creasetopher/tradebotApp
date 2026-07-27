from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from tradebot.events.market import (
    MarketQuote,
    MarketQuoteEvent,
    build_raw_payload_hash,
    datetime_from_epoch_ms,
)


YFINANCE_WEBSOCKET_SOURCE = "yfinance.websocket"


def quote_event_from_yfinance_message(
    message: Mapping[str, Any],
    *,
    collector_time: datetime | None = None,
    source: str = YFINANCE_WEBSOCKET_SOURCE,
) -> MarketQuoteEvent:
    """
    Convert a raw yfinance websocket message into the bot's normalized
    market.quote.v1 event.

    Keep this provider-specific mapping outside tradebot.events.market so the
    event schema remains provider-neutral.
    """

    raw_message = dict(message)
    collector_time_utc = _collector_time_or_now(collector_time)

    symbol = _symbol_from_message(raw_message)

    source_payload_ts_ms = _int_or_none(
        _first_present(raw_message, ("time", "timestamp"))
    )

    event_time = (
        datetime_from_epoch_ms(source_payload_ts_ms)
        if source_payload_ts_ms is not None
        else collector_time_utc
    )

    raw_payload_hash = build_raw_payload_hash(raw_message)

    quote = MarketQuote(
        symbol=symbol,
        event_time=event_time,
        collector_time=collector_time_utc,
        price=_decimal_or_none(
            _first_present(raw_message, ("price", "regularMarketPrice", "lastPrice"))
        ),
        bid=_decimal_or_none(_first_present(raw_message, ("bid",))),
        ask=_decimal_or_none(_first_present(raw_message, ("ask",))),
        bid_size=_decimal_or_none(_first_present(raw_message, ("bidSize",))),
        ask_size=_decimal_or_none(_first_present(raw_message, ("askSize",))),
        day_volume=_decimal_or_none(
            _first_present(raw_message, ("dayVolume", "regularMarketVolume"))
        ),
        volume=_decimal_or_none(_first_present(raw_message, ("volume",))),
        exchange=_str_or_none(raw_message.get("exchange")),
        market_hours=_str_or_none(raw_message.get("marketHours")),
        quote_type=_str_or_none(raw_message.get("quoteType")),
        source_payload_ts_ms=source_payload_ts_ms,
        raw_payload_hash=raw_payload_hash,
    )

    return MarketQuoteEvent.from_quote(
        quote,
        source=source,
        ingest_time=collector_time_utc,
    )


def _symbol_from_message(message: Mapping[str, Any]) -> str:
    value = _first_present(message, ("id", "symbol", "ticker"))

    if value is None:
        raise ValueError(
            f"Unable to determine symbol from yfinance message keys={list(message.keys())}"
        )

    symbol = str(value).strip().upper()

    if not symbol:
        raise ValueError("Unable to determine symbol from empty yfinance symbol field")

    return symbol


def _collector_time_or_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("collector_time must be timezone-aware")

    return value.astimezone(timezone.utc)


def _first_present(message: Mapping[str, Any], keys: tuple[str, ...]) -> Any | None:
    for key in keys:
        if key in message and message[key] is not None:
            return message[key]

    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None

    if isinstance(value, str) and not value.strip():
        return None

    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal value from yfinance payload: {value!r}") from exc

    if not decimal_value.is_finite():
        return None

    return decimal_value


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, str) and not value.strip():
        return None

    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid integer value from yfinance payload: {value!r}") from exc


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None