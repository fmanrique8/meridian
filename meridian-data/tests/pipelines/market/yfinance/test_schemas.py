from __future__ import annotations

import pytest
from pydantic import ValidationError

from meridian_data.pipelines.market.yfinance.schemas import YfinancePipelineParameters

EXPECTED_OVERLAP_DAYS = 7
EXPECTED_HISTORY_YEARS = 10


def test_yfinance_pipeline_parameters_requires_symbols() -> None:
    with pytest.raises(ValidationError, match="symbols"):
        YfinancePipelineParameters()


def test_yfinance_pipeline_parameters_defaults() -> None:
    parameters = YfinancePipelineParameters(symbols=[" spy ", "QQQ", "SPY"])

    assert parameters.interval == "1d"
    assert parameters.sync_mode == "incremental"
    assert parameters.overlap_days == EXPECTED_OVERLAP_DAYS
    assert parameters.history_years == EXPECTED_HISTORY_YEARS
    assert parameters.symbols == ["SPY", "QQQ"]


def test_yfinance_pipeline_parameters_rejects_negative_overlap() -> None:
    with pytest.raises(ValidationError, match="overlap_days"):
        YfinancePipelineParameters(symbols=["SPY"], overlap_days=-1)
