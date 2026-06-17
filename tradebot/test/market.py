from events.market import quote_event_from_yfinance_message
from decimal import Decimal


def test_market_quote_event_from_yfinance_like_message():
    message = {
        "id": "spce",
        "price": 4.58,
        "dayVolume": 54329445,
        "time": 1781035759950,
        "exchange": "NYQ",
        "quoteType": "EQUITY",
        "marketHours": "REGULAR_MARKET",
    }

    event = quote_event_from_yfinance_message(message)

    assert event.event_type == "market.quote.v1"
    assert event.symbol == "SPCE"
    assert event.quote.symbol == "SPCE"
    assert event.quote.price == Decimal("4.58")
    assert event.quote.day_volume == Decimal("54329445")
    assert event.quote.source_payload_ts_ms == 1781035759950