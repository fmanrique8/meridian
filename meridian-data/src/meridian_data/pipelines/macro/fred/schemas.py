"""Schemas for FRED client and pipeline contracts."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

DEFAULT_FRED_BASE_URL = "https://api.stlouisfed.org/fred"
DEFAULT_FRED_SERIES_IDS = ["FEDFUNDS", "CPIAUCSL", "UNRATE"]


class FredSeriesMetadata(BaseModel):
    """Metadata for a FRED series response record."""

    model_config = ConfigDict(extra="ignore")

    id: str
    title: str
    frequency: str | None = None
    units: str | None = None
    seasonal_adjustment: str | None = None
    notes: str | None = None
    last_updated: str | None = None


class FredSeriesResponse(BaseModel):
    """Top-level response model for `/fred/series`."""

    model_config = ConfigDict(extra="ignore")

    seriess: list[FredSeriesMetadata]


class FredObservation(BaseModel):
    """Observation record from `/fred/series/observations`."""

    model_config = ConfigDict(extra="ignore")

    date: str
    value: str


class FredObservationsResponse(BaseModel):
    """Top-level response model for `/fred/series/observations`."""

    model_config = ConfigDict(extra="ignore")

    observations: list[FredObservation]


class FredPipelineParameters(BaseModel):
    """Runtime parameters for the FRED pipeline."""

    model_config = ConfigDict(extra="ignore")

    api_key: str
    base_url: str = DEFAULT_FRED_BASE_URL
    timeout_seconds: float = 20.0
    max_retries: int = 3
    backoff_seconds: float = 0.5
    jitter_seconds: float = 0.2
    observation_start: str | None = None
    observation_end: str | None = None
    sort_order: Literal["asc", "desc"] = "asc"
    limit: int | None = None
    run_date: date | None = None
    series_ids: list[str] = Field(default_factory=lambda: list(DEFAULT_FRED_SERIES_IDS))

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: str) -> str:
        """Ensure API key is present."""
        key = value.strip()
        if not key:
            raise ValueError("FRED api_key is required.")
        return key

    @field_validator("series_ids")
    @classmethod
    def validate_series_ids(cls, value: list[str]) -> list[str]:
        """Ensure series list is not empty."""
        if not value:
            raise ValueError("At least one FRED series_id is required.")
        return value
