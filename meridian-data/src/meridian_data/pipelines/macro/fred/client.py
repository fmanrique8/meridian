"""HTTP client for the St. Louis Fed (FRED) API."""

from __future__ import annotations

import random
import time
from typing import Any

import httpx

from .schemas import (
    FredObservation,
    FredObservationsResponse,
    FredPipelineParameters,
    FredSeriesMetadata,
    FredSeriesResponse,
)

TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
HTTP_BAD_REQUEST = 400


class FredClientError(RuntimeError):
    """Base exception for FRED client errors."""


class FredRequestError(FredClientError):
    """Raised for request/response errors from FRED."""

    def __init__(
        self,
        message: str,
        *,
        endpoint: str,
        status_code: int | None = None,
    ) -> None:
        detail = f"{message} [endpoint={endpoint}]"
        if status_code is not None:
            detail = f"{detail} [status={status_code}]"
        super().__init__(detail)
        self.endpoint = endpoint
        self.status_code = status_code


class FredClient:
    """Typed, retrying client for FRED series and observations endpoints."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        api_key: str,
        base_url: str,
        timeout_seconds: float,
        max_retries: int,
        backoff_seconds: float,
        jitter_seconds: float,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_seconds = backoff_seconds
        self._jitter_seconds = jitter_seconds
        self._client = httpx.Client(transport=transport)

    @classmethod
    def from_parameters(
        cls,
        parameters: FredPipelineParameters,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> FredClient:
        """Create a client from pipeline parameters."""
        return cls(
            api_key=parameters.api_key,
            base_url=parameters.base_url,
            timeout_seconds=parameters.timeout_seconds,
            max_retries=parameters.max_retries,
            backoff_seconds=parameters.backoff_seconds,
            jitter_seconds=parameters.jitter_seconds,
            transport=transport,
        )

    def __enter__(self) -> FredClient:
        """Return context-managed client."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Close the underlying HTTP client."""
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def get_series(self, series_id: str) -> FredSeriesMetadata:
        """Fetch metadata for a single FRED series."""
        payload = self._request("series", params={"series_id": series_id})
        response = FredSeriesResponse.model_validate(payload)
        if not response.seriess:
            raise FredRequestError(
                f"No metadata returned for series_id={series_id}",
                endpoint="series",
            )
        return response.seriess[0]

    def get_observations(
        self,
        *,
        series_id: str,
        observation_start: str | None = None,
        observation_end: str | None = None,
        sort_order: str = "asc",
        limit: int | None = None,
    ) -> list[FredObservation]:
        """Fetch observations for a FRED series."""
        params: dict[str, Any] = {
            "series_id": series_id,
            "sort_order": sort_order,
        }
        if observation_start is not None:
            params["observation_start"] = observation_start
        if observation_end is not None:
            params["observation_end"] = observation_end
        if limit is not None:
            params["limit"] = limit

        payload = self._request("series/observations", params=params)
        response = FredObservationsResponse.model_validate(payload)
        return response.observations

    def _request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """Issue a GET request with bounded retries for transient failures."""
        url = f"{self._base_url}/{endpoint}"
        query_params = {"api_key": self._api_key, "file_type": "json", **params}
        attempt = 1
        last_exception: Exception | None = None

        while attempt <= self._max_retries:
            try:
                response = self._client.get(
                    url,
                    params=query_params,
                    timeout=self._timeout_seconds,
                )
            except httpx.TimeoutException as exc:
                last_exception = exc
                if attempt == self._max_retries:
                    break
                self._sleep_before_retry(attempt)
                attempt += 1
                continue
            except httpx.TransportError as exc:
                last_exception = exc
                if attempt == self._max_retries:
                    break
                self._sleep_before_retry(attempt)
                attempt += 1
                continue

            if response.status_code in TRANSIENT_STATUS_CODES:
                if attempt == self._max_retries:
                    raise FredRequestError(
                        "FRED request failed after retries.",
                        endpoint=endpoint,
                        status_code=response.status_code,
                    )
                self._sleep_before_retry(attempt)
                attempt += 1
                continue

            if response.status_code >= HTTP_BAD_REQUEST:
                raise FredRequestError(
                    f"FRED request failed: {response.text}",
                    endpoint=endpoint,
                    status_code=response.status_code,
                )

            payload = response.json()
            if "error_message" in payload:
                raise FredRequestError(
                    str(payload["error_message"]),
                    endpoint=endpoint,
                    status_code=response.status_code,
                )
            if not isinstance(payload, dict):
                raise FredRequestError(
                    "FRED response payload must be a JSON object.",
                    endpoint=endpoint,
                    status_code=response.status_code,
                )
            return payload

        if last_exception is not None:
            raise FredRequestError(
                f"FRED request failed after retries: {last_exception}",
                endpoint=endpoint,
            ) from last_exception

        raise FredRequestError(
            "FRED request failed after retries.",
            endpoint=endpoint,
        )

    def _sleep_before_retry(self, attempt: int) -> None:
        """Sleep with exponential backoff and jitter."""
        base = self._backoff_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, self._jitter_seconds)
        time.sleep(base + jitter)
