from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


EVENT_TYPE_CANDIDATE_SNAPSHOT_V1 = "candidate.snapshot.v1"
SCHEMA_VERSION = "1.0.0"


class CandidateSnapshot(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    symbol: str = Field(min_length=1, max_length=32)
    update_time: datetime
    scanner_tags: list[str] = Field(min_length=1)

    price: Decimal | None = Field(default=None, ge=Decimal("0"))
    volume: Decimal | None = Field(default=None, ge=Decimal("0"))
    dollar_volume: Decimal | None = Field(default=None, ge=Decimal("0"))
    previous_close: Decimal | None = Field(default=None, ge=Decimal("0"))

    day_high: Decimal | None = Field(default=None, ge=Decimal("0"))
    day_low: Decimal | None = Field(default=None, ge=Decimal("0"))
    open: Decimal | None = Field(default=None, ge=Decimal("0"))

    bid: Decimal | None = Field(default=None, ge=Decimal("0"))
    ask: Decimal | None = Field(default=None, ge=Decimal("0"))

    spread: Decimal | None = Field(default=None, ge=Decimal("0"))
    spread_bps: Decimal | None = Field(default=None, ge=Decimal("0"))
    change_percent: Decimal | None = None

    short_name: str | None = None
    analyst_rating: str | None = None

    # Only use this if the upstream source actually provides it.
    source_payload_ts_ms: int | None = Field(default=None, ge=0)

    # Use this if the scanner generated the timestamp itself.
    scanner_ts_ms: int | None = Field(default=None, ge=0)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        return str(value).strip().upper()

    @field_validator("scanner_tags")
    @classmethod
    def normalize_scanner_tags(cls, value: list[str]) -> list[str]:
        tags = sorted({str(tag).strip() for tag in value if str(tag).strip()})
        if not tags:
            raise ValueError("scanner_tags must contain at least one non-empty tag")
        return tags

    @field_validator("update_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("update_time must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def populate_derived_fields(self) -> CandidateSnapshot:
        if self.dollar_volume is None and self.price is not None and self.volume is not None:
            self.dollar_volume = self.price * self.volume

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

        if (
            self.change_percent is None
            and self.price is not None
            and self.previous_close is not None
            and self.previous_close > 0
        ):
            self.change_percent = (
                (self.price - self.previous_close) / self.previous_close
            ) * Decimal("100")

        return self


class CandidateSnapshotEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: Literal["candidate.snapshot.v1"] = EVENT_TYPE_CANDIDATE_SNAPSHOT_V1
    schema_version: Literal["1.0.0"] = SCHEMA_VERSION
    event_id: str

    source: Literal["yfinance(YAHOO_FINANCE)_predefined_screens"] = "yfinance(YAHOO_FINANCE)_predefined_screens"
    update_time: datetime
    candidate: CandidateSnapshot

    @field_validator("update_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("update_time must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def event_time_must_match_candidate_time(self) -> CandidateSnapshotEvent:
        if self.update_time != self.candidate.update_time:
            raise ValueError("event.update_time must match candidate.update_time")
        return self

    @classmethod
    def from_candidate(
        cls,
        candidate: CandidateSnapshot | dict[str, Any],
        *,
        source: Literal["yfinance(YAHOO_FINANCE)_predefined_screens"] = "yfinance(YAHOO_FINANCE)_predefined_screens",
    ) -> CandidateSnapshotEvent:
        parsed_candidate = (
            candidate
            if isinstance(candidate, CandidateSnapshot)
            else CandidateSnapshot.model_validate(candidate)
        )

        event_id = build_candidate_event_id(
            source=source,
            symbol=parsed_candidate.symbol,
            update_time=parsed_candidate.update_time,
            scanner_tags=parsed_candidate.scanner_tags,
        )

        return cls(
            event_id=event_id,
            source=source,
            update_time=parsed_candidate.update_time,
            candidate=parsed_candidate,
        )


def build_candidate_event_id(
    *,
    source: str,
    symbol: str,
    update_time: datetime,
    scanner_tags: list[str],
) -> str:
    raw = "|".join(
        [
            EVENT_TYPE_CANDIDATE_SNAPSHOT_V1,
            source,
            symbol.upper(),
            update_time.astimezone(timezone.utc).isoformat(),
            ",".join(sorted(scanner_tags)),
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"evt_{digest}"