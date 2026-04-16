"""Schemas for the macro convergence (Layer-1) pipeline."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ConvergenceThresholds(BaseModel):
    """Threshold and rule knobs for v1 state/regime classification.

    Note:
        Defaults are initial calibrated v1 values chosen to reduce
        noise-driven state flips. They remain configurable for future tuning.
    """

    model_config = ConfigDict(extra="ignore")

    inflation_sticky_band: float = 0.3
    labor_unrate_tight_threshold: float = -0.1
    labor_claims_tight_threshold: float = 0.0
    labor_unrate_deteriorating_threshold: float = 0.2
    labor_claims_deteriorating_threshold: float = 15000.0
    growth_expansion_relative_threshold: float = 0.05
    growth_contraction_threshold: float = 0.0
    liquidity_curve_tightening_threshold: float = 0.0
    liquidity_rates_tightening_threshold: float = 0.0
    liquidity_curve_easing_threshold: float = 0.25
    liquidity_rates_easing_threshold: float = -0.25

    @field_validator("inflation_sticky_band")
    @classmethod
    def validate_inflation_sticky_band(cls, value: float) -> float:
        """Ensure sticky-band threshold is non-negative."""
        if value < 0:
            raise ValueError("inflation_sticky_band must be >= 0.")
        return value


class ConvergenceParameters(BaseModel):
    """Runtime parameters for weekly convergence signal computation."""

    model_config = ConfigDict(extra="ignore")

    run_date: date | None = None
    time_grain: Literal["weekly"] = "weekly"
    week_anchor: Literal["friday"] = "friday"
    source_set_version: str = "fred_v1"
    thresholds: ConvergenceThresholds = Field(default_factory=ConvergenceThresholds)

    @field_validator("source_set_version")
    @classmethod
    def validate_source_set_version(cls, value: str) -> str:
        """Ensure source-set version is present."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("source_set_version must not be empty.")
        return cleaned
