from __future__ import annotations

import pytest
from pydantic import ValidationError

from meridian_data.pipelines.macro.convergence.schemas import ConvergenceParameters

EXPECTED_THRESHOLDS = {
    "inflation_sticky_band": 0.3,
    "labor_unrate_tight_threshold": -0.1,
    "labor_claims_tight_threshold": 0.0,
    "labor_unrate_deteriorating_threshold": 0.2,
    "labor_claims_deteriorating_threshold": 15000.0,
    "growth_expansion_relative_threshold": 0.05,
    "growth_contraction_threshold": 0.0,
    "liquidity_curve_tightening_threshold": 0.0,
    "liquidity_rates_tightening_threshold": 0.0,
    "liquidity_curve_easing_threshold": 0.25,
    "liquidity_rates_easing_threshold": -0.25,
}


def test_convergence_parameters_default_values() -> None:
    parameters = ConvergenceParameters()

    assert parameters.time_grain == "weekly"
    assert parameters.week_anchor == "friday"
    assert parameters.source_set_version == "fred_v1"
    assert parameters.thresholds.model_dump() == EXPECTED_THRESHOLDS


def test_convergence_parameters_rejects_invalid_week_anchor() -> None:
    with pytest.raises(ValidationError, match="week_anchor"):
        ConvergenceParameters(week_anchor="monday")


def test_convergence_parameters_rejects_empty_source_set_version() -> None:
    with pytest.raises(ValidationError, match="source_set_version"):
        ConvergenceParameters(source_set_version="   ")
