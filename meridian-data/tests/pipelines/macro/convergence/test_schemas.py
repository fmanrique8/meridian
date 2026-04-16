from __future__ import annotations

import pytest
from pydantic import ValidationError

from meridian_data.pipelines.macro.convergence.schemas import ConvergenceParameters

EXPECTED_STICKY_BAND = 0.3


def test_convergence_parameters_default_values() -> None:
    parameters = ConvergenceParameters()

    assert parameters.time_grain == "weekly"
    assert parameters.week_anchor == "friday"
    assert parameters.source_set_version == "fred_v1"
    assert parameters.thresholds.inflation_sticky_band == EXPECTED_STICKY_BAND


def test_convergence_parameters_rejects_invalid_week_anchor() -> None:
    with pytest.raises(ValidationError, match="week_anchor"):
        ConvergenceParameters(week_anchor="monday")


def test_convergence_parameters_rejects_empty_source_set_version() -> None:
    with pytest.raises(ValidationError, match="source_set_version"):
        ConvergenceParameters(source_set_version="   ")
