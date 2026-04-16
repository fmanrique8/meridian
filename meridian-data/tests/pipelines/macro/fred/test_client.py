from __future__ import annotations

import httpx
import pytest

from meridian_data.pipelines.macro.fred.client import FredClient, FredRequestError
from meridian_data.pipelines.macro.fred.schemas import FredPipelineParameters

EXPECTED_RETRY_ATTEMPTS = 3
EXPECTED_OBSERVATION_COUNT = 2


def _build_client(
    *,
    transport: httpx.BaseTransport,
    max_retries: int = 3,
) -> FredClient:
    parameters = FredPipelineParameters(
        api_key="fred-test-key",
        base_url="https://api.stlouisfed.org/fred",
        timeout_seconds=1.0,
        max_retries=max_retries,
        backoff_seconds=0.0,
        jitter_seconds=0.0,
        series_ids=["FEDFUNDS"],
    )
    return FredClient.from_parameters(parameters, transport=transport)


def test_get_series_returns_metadata() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/fred/series")
        return httpx.Response(
            status_code=200,
            json={
                "seriess": [
                    {
                        "id": "FEDFUNDS",
                        "title": "Effective Federal Funds Rate",
                        "frequency": "Monthly",
                        "units": "Percent",
                        "seasonal_adjustment": "Not Seasonally Adjusted",
                    }
                ]
            },
        )

    with _build_client(transport=httpx.MockTransport(handler)) as client:
        metadata = client.get_series("FEDFUNDS")

    assert metadata.id == "FEDFUNDS"
    assert metadata.title == "Effective Federal Funds Rate"


def test_get_observations_retries_transient_failures() -> None:
    attempts = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/fred/series/observations")
        attempts["count"] += 1
        if attempts["count"] < EXPECTED_RETRY_ATTEMPTS:
            return httpx.Response(status_code=503, text="service unavailable")
        return httpx.Response(
            status_code=200,
            json={
                "observations": [
                    {"date": "2026-01-01", "value": "4.33"},
                    {"date": "2026-02-01", "value": "4.33"},
                ]
            },
        )

    with _build_client(transport=httpx.MockTransport(handler)) as client:
        observations = client.get_observations(series_id="FEDFUNDS")

    assert attempts["count"] == EXPECTED_RETRY_ATTEMPTS
    assert len(observations) == EXPECTED_OBSERVATION_COUNT
    assert observations[0].date == "2026-01-01"


def test_get_series_raises_on_non_retryable_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=400,
            json={"error_message": "Bad Request"},
        )

    with _build_client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FredRequestError) as exc_info:
            client.get_series("FEDFUNDS")

    assert "status=400" in str(exc_info.value)
