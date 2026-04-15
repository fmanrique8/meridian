from __future__ import annotations

import pytest
from pydantic import ValidationError

from meridian_data.pipelines.macro.fred.schemas import FredPipelineParameters

EXPECTED_MAX_NULL_RATIO = 0.2


def test_fred_pipeline_parameters_requires_series_ids() -> None:
    with pytest.raises(ValidationError, match="series_ids"):
        FredPipelineParameters(
            api_key="fred-test-key",
            base_url="https://api.stlouisfed.org/fred",
            timeout_seconds=1.0,
            max_retries=3,
            backoff_seconds=0.0,
            jitter_seconds=0.0,
        )


def test_fred_pipeline_parameters_default_data_quality_values() -> None:
    parameters = FredPipelineParameters(
        api_key="fred-test-key",
        base_url="https://api.stlouisfed.org/fred",
        timeout_seconds=1.0,
        max_retries=3,
        backoff_seconds=0.0,
        jitter_seconds=0.0,
        series_ids=["UNRATE"],
    )

    assert parameters.data_quality.max_null_ratio_per_series == EXPECTED_MAX_NULL_RATIO
    assert parameters.data_quality.enforce_monotonic_dates is True
    assert parameters.data_quality.enforce_unique_series_date is True
    assert parameters.data_quality.enforce_numeric_parse is True
    assert parameters.data_quality.enforce_valid_dates is True
    assert parameters.sync_mode == "full"
