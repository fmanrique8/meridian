from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from meridian_data.pipelines.market.yfinance import client
from meridian_data.pipelines.market.yfinance.client import (
    YfinanceClient,
    YfinanceRequestError,
)
from meridian_data.pipelines.market.yfinance.schemas import YfinancePipelineParameters

EXPECTED_RETRY_ATTEMPTS = 3
EXPECTED_ROW_COUNT = 2
EXPECTED_OPEN_PRICE = 500.0
EXPECTED_VOLUME = 1_000_000


def _build_client(*, max_retries: int = 3) -> YfinanceClient:
    parameters = YfinancePipelineParameters(
        symbols=["SPY"],
        timeout_seconds=1.0,
        max_retries=max_retries,
        backoff_seconds=0.0,
        jitter_seconds=0.0,
    )
    return YfinanceClient.from_parameters(parameters)


def _sample_history_frame() -> pd.DataFrame:
    index = pd.to_datetime(["2026-04-14", "2026-04-15"])
    return pd.DataFrame(
        {
            "Open": [500.0, 501.0],
            "High": [502.0, 503.0],
            "Low": [499.5, 500.5],
            "Close": [501.5, 502.5],
            "Adj Close": [501.0, 502.0],
            "Volume": [1_000_000, 1_100_000],
        },
        index=index,
    )


def test_get_history_returns_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client, "_download_history", lambda **_: _sample_history_frame())

    with _build_client() as yfinance_client:
        history = yfinance_client.get_history(
            symbol="SPY",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 16),
            interval="1d",
        )

    assert history.symbol == "SPY"
    assert len(history.rows) == EXPECTED_ROW_COUNT
    assert history.rows[0]["date"] == "2026-04-14"
    assert history.rows[0]["open"] == EXPECTED_OPEN_PRICE
    assert history.rows[0]["volume"] == EXPECTED_VOLUME
    assert history.source_last_updated == "2026-04-15"


def test_get_history_retries_transient_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = {"count": 0}

    def failing_then_success(**_: object) -> pd.DataFrame:
        attempts["count"] += 1
        if attempts["count"] < EXPECTED_RETRY_ATTEMPTS:
            raise RuntimeError("temporary yfinance failure")
        return _sample_history_frame()

    monkeypatch.setattr(client, "_download_history", failing_then_success)

    with _build_client(max_retries=EXPECTED_RETRY_ATTEMPTS) as yfinance_client:
        history = yfinance_client.get_history(
            symbol="SPY",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 16),
            interval="1d",
        )

    assert attempts["count"] == EXPECTED_RETRY_ATTEMPTS
    assert len(history.rows) == EXPECTED_ROW_COUNT


def test_get_history_raises_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    def always_fail(**_: object) -> pd.DataFrame:
        raise RuntimeError("permanent failure")

    monkeypatch.setattr(client, "_download_history", always_fail)

    with _build_client(max_retries=2) as yfinance_client:
        with pytest.raises(YfinanceRequestError, match="failed after retries"):
            yfinance_client.get_history(
                symbol="SPY",
                start_date=date(2026, 4, 1),
                end_date=date(2026, 4, 16),
                interval="1d",
            )
