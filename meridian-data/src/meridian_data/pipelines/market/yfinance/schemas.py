"""Schemas for yfinance client and pipeline contracts."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class YfinanceDataQualityParameters(BaseModel):
    """Data-quality assertions for processed yfinance ETF observations."""

    model_config = ConfigDict(extra="ignore")

    max_null_ratio_per_symbol: float = 0.0
    enforce_unique_symbol_date: bool = True
    enforce_valid_dates: bool = True
    enforce_non_null_ohlcv: bool = True

    @field_validator("max_null_ratio_per_symbol")
    @classmethod
    def validate_max_null_ratio_per_symbol(cls, value: float) -> float:
        """Ensure null-ratio threshold is between 0 and 1."""
        if value < 0.0 or value > 1.0:
            raise ValueError("max_null_ratio_per_symbol must be between 0.0 and 1.0.")
        return value


class YfinancePipelineParameters(BaseModel):
    """Runtime parameters for the yfinance ETF ingestion pipeline."""

    model_config = ConfigDict(extra="ignore")

    timeout_seconds: float = 20.0
    max_retries: int = 3
    backoff_seconds: float = 0.5
    jitter_seconds: float = 0.2
    history_years: int = 10
    interval: Literal["1d"] = "1d"
    sync_mode: Literal["full", "incremental"] = "incremental"
    overlap_days: int = 7
    run_date: date | None = None
    source: str = "yfinance"
    symbols: list[str]
    data_quality: YfinanceDataQualityParameters = Field(
        default_factory=YfinanceDataQualityParameters
    )

    @field_validator("timeout_seconds")
    @classmethod
    def validate_timeout_seconds(cls, value: float) -> float:
        """Ensure timeout is positive."""
        if value <= 0:
            raise ValueError("timeout_seconds must be > 0.")
        return value

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, value: int) -> int:
        """Ensure retry attempts are bounded and positive."""
        if value <= 0:
            raise ValueError("max_retries must be >= 1.")
        return value

    @field_validator("backoff_seconds", "jitter_seconds")
    @classmethod
    def validate_backoff_and_jitter(cls, value: float) -> float:
        """Ensure backoff and jitter are non-negative."""
        if value < 0:
            raise ValueError("backoff_seconds and jitter_seconds must be >= 0.")
        return value

    @field_validator("history_years")
    @classmethod
    def validate_history_years(cls, value: int) -> int:
        """Ensure full history window is positive."""
        if value <= 0:
            raise ValueError("history_years must be >= 1.")
        return value

    @field_validator("overlap_days")
    @classmethod
    def validate_overlap_days(cls, value: int) -> int:
        """Ensure incremental overlap window is non-negative."""
        if value < 0:
            raise ValueError("overlap_days must be >= 0.")
        return value

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str) -> str:
        """Ensure source name is present."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("source must not be empty.")
        return cleaned

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[str]) -> list[str]:
        """Ensure symbol list is non-empty and normalized."""
        normalized = [symbol.strip().upper() for symbol in value if symbol.strip()]
        if not normalized:
            raise ValueError("At least one symbol is required.")
        unique_symbols = list(dict.fromkeys(normalized))
        return unique_symbols

