from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tradebot.events.market import build_raw_payload_hash, datetime_from_epoch_ms
from tradebot.providers.yfinance import quote_event_from_yfinance_message


def test_yfinance_websocket_message_maps_to_market_quote_event() -> None:
    source_ts_ms = 1781035759950
    collector_time = datetime(2026, 6, 9, 20, 10, 0, tzinfo=timezone.utc)

    message = {
        "id": "spce",
        "price": 4.58,
        "dayVolume": 54329445,
        "time": source_ts_ms,
        "exchange": "NYQ",
        "quoteType": "EQUITY",
        "marketHours": "REGULAR_MARKET",
    }

    event = quote_event_from_yfinance_message(
        message,
        collector_time=collector_time,
    )

    assert event.event_type == "market.quote.v1"
    assert event.schema_version == "1.0.0"
    assert event.source == "yfinance.websocket"
    assert event.symbol == "SPCE"
    assert event.event_time == datetime_from_epoch_ms(source_ts_ms)
    assert event.ingest_time == collector_time

    assert event.quote.symbol == "SPCE"
    assert event.quote.event_time == datetime_from_epoch_ms(source_ts_ms)
    assert event.quote.collector_time == collector_time
    assert event.quote.price == Decimal("4.58")
    assert event.quote.day_volume == Decimal("54329445")
    assert event.quote.exchange == "NYQ"
    assert event.quote.quote_type == "EQUITY"
    assert event.quote.market_hours == "REGULAR_MARKET"
    assert event.quote.source_payload_ts_ms == source_ts_ms
    assert event.quote.raw_payload_hash == build_raw_payload_hash(message)


def test_yfinance_mapping_uses_collector_time_when_source_time_missing() -> None:
    collector_time = datetime(2026, 6, 9, 20, 10, 0, tzinfo=timezone.utc)

    message = {
        "id": "AMC",
        "price": "1.945",
        "dayVolume": "38428802",
    }

    event = quote_event_from_yfinance_message(
        message,
        collector_time=collector_time,
    )

    assert event.symbol == "AMC"
    assert event.event_time == collector_time
    assert event.ingest_time == collector_time
    assert event.quote.source_payload_ts_ms is None
    assert event.quote.price == Decimal("1.945")
    assert event.quote.day_volume == Decimal("38428802")


def test_yfinance_mapping_supports_symbol_fallback_key() -> None:
    collector_time = datetime(2026, 6, 9, 20, 10, 0, tzinfo=timezone.utc)

    message = {
        "symbol": "sofi",
        "regularMarketPrice": "7.12",
        "regularMarketVolume": "12345",
    }

    event = quote_event_from_yfinance_message(
        message,
        collector_time=collector_time,
    )

    assert event.symbol == "SOFI"
    assert event.quote.price == Decimal("7.12")
    assert event.quote.day_volume == Decimal("12345")


def test_yfinance_mapping_requires_symbol() -> None:
    collector_time = datetime(2026, 6, 9, 20, 10, 0, tzinfo=timezone.utc)

    message = {
        "price": 4.58,
        "dayVolume": 54329445,
    }

    with pytest.raises(ValueError, match="Unable to determine symbol"):
        quote_event_from_yfinance_message(
            message,
            collector_time=collector_time,
        )


def test_yfinance_mapping_event_id_is_deterministic_for_same_message() -> None:
    collector_time = datetime(2026, 6, 9, 20, 10, 0, tzinfo=timezone.utc)

    message = {
        "id": "SPCE",
        "price": 4.58,
        "dayVolume": 54329445,
        "time": 1781035759950,
    }

    first = quote_event_from_yfinance_message(
        message,
        collector_time=collector_time,
    )
    second = quote_event_from_yfinance_message(
        message,
        collector_time=collector_time,
    )

    assert first.event_id == second.event_id

    # tests deterministic/idempotency works for message with similar attributes but different time
    def test_yfinance_mapping_event_id_is_deterministic_for_different_message() -> None:
        collector_time1 = datetime(2026, 6, 9, 20, 10, 0, tzinfo=timezone.utc)
        collector_time2 = datetime(2026, 6, 9, 20, 10, 2, tzinfo=timezone.utc)


        message1 = {
            "id": "SPCE",
            "price": 4.58,
            "dayVolume": 54329445,
            "time": 1781035759950,
        }

        message2 = {
            "id": "SPCE",
            "price": 4.51,
            "dayVolume": 54329445,
            "time": 1781035759953,
        }

        first = quote_event_from_yfinance_message(
            message,
            collector_time=collector_time1,
        )
        second = quote_event_from_yfinance_message(
            message,
            collector_time=collector_time2,
        )

        assert first.event_id != second.event_id
        assert first.symbol == second.symbol
        assert first.event_time == second.event_time
        assert first.ingest_time == second.ingest_time
        assert first.quote.source_payload_ts_ms is None
        assert first.quote.price == second.quote.price
        assert first.quote.day_volume == second.quote.day_volume