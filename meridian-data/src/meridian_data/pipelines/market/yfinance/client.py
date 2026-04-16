"""HTTP client wrapper for yfinance ETF history downloads."""

from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

from .schemas import YfinancePipelineParameters


def _download_history(**kwargs: Any) -> Any:
    """Call yfinance download."""
    return yf.download(**kwargs)


@dataclass(frozen=True)
class YfinanceHistoryResponse:
    """Normalized yfinance history payload for one symbol."""

    symbol: str
    rows: list[dict[str, Any]]
    source_last_updated: str | None


class YfinanceClientError(RuntimeError):
    """Base exception for yfinance client errors."""


class YfinanceRequestError(YfinanceClientError):
    """Raised for request/response errors from yfinance."""

    def __init__(self, message: str, *, symbol: str) -> None:
        detail = f"{message} [symbol={symbol}]"
        super().__init__(detail)
        self.symbol = symbol


class YfinanceClient:
    """Retrying yfinance history client with typed output."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        max_retries: int,
        backoff_seconds: float,
        jitter_seconds: float,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_seconds = backoff_seconds
        self._jitter_seconds = jitter_seconds

    @classmethod
    def from_parameters(cls, parameters: YfinancePipelineParameters) -> YfinanceClient:
        """Create a client from pipeline parameters."""
        return cls(
            timeout_seconds=parameters.timeout_seconds,
            max_retries=parameters.max_retries,
            backoff_seconds=parameters.backoff_seconds,
            jitter_seconds=parameters.jitter_seconds,
        )

    def __enter__(self) -> YfinanceClient:
        """Return context-managed client."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """No-op context manager exit for API symmetry."""
        return None

    def get_history(
        self,
        *,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str,
    ) -> YfinanceHistoryResponse:
        """Fetch daily OHLCV history for one symbol."""
        frame = self._request(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            interval=interval,
        )
        rows = self._normalize_rows(symbol=symbol, frame=frame)
        source_last_updated = rows[-1]["date"] if rows else None
        return YfinanceHistoryResponse(
            symbol=symbol,
            rows=rows,
            source_last_updated=source_last_updated,
        )

    def _request(
        self,
        *,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str,
    ) -> Any:
        """Issue a yfinance request with bounded retries."""
        attempt = 1
        last_exception: Exception | None = None
        end_date_exclusive = end_date + timedelta(days=1)

        while attempt <= self._max_retries:
            try:
                return _download_history(
                    tickers=symbol,
                    start=start_date.isoformat(),
                    end=end_date_exclusive.isoformat(),
                    interval=interval,
                    auto_adjust=False,
                    actions=False,
                    progress=False,
                    threads=False,
                    timeout=self._timeout_seconds,
                )
            except Exception as exc:  # noqa: BLE001
                last_exception = exc
                if attempt == self._max_retries:
                    break
                self._sleep_before_retry(attempt)
                attempt += 1

        if last_exception is not None:
            raise YfinanceRequestError(
                f"yfinance request failed after retries: {last_exception}",
                symbol=symbol,
            ) from last_exception
        raise YfinanceRequestError(
            "yfinance request failed after retries.",
            symbol=symbol,
        )

    def _sleep_before_retry(self, attempt: int) -> None:
        """Sleep with exponential backoff and jitter."""
        base = self._backoff_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, self._jitter_seconds)
        time.sleep(base + jitter)

    def _normalize_rows(self, *, symbol: str, frame: Any) -> list[dict[str, Any]]:
        """Normalize a yfinance dataframe into canonical row dictionaries."""
        if not isinstance(frame, pd.DataFrame):
            raise YfinanceRequestError(
                f"Expected pandas DataFrame, got {type(frame)!r}.",
                symbol=symbol,
            )
        if frame.empty:
            return []

        normalized = frame.copy()
        if isinstance(normalized.columns, pd.MultiIndex):
            normalized.columns = normalized.columns.get_level_values(0)
        normalized = normalized.reset_index()

        date_column = "Date" if "Date" in normalized.columns else normalized.columns[0]

        rows: list[dict[str, Any]] = []
        for record in normalized.to_dict(orient="records"):
            parsed_date = pd.to_datetime(record.get(date_column), errors="coerce")
            if pd.isna(parsed_date):
                date_value = str(record.get(date_column))
            else:
                date_value = parsed_date.date().isoformat()

            rows.append(
                {
                    "date": date_value,
                    "open": _as_float(record.get("Open")),
                    "high": _as_float(record.get("High")),
                    "low": _as_float(record.get("Low")),
                    "close": _as_float(record.get("Close")),
                    "adj_close": _as_float(record.get("Adj Close")),
                    "volume": _as_int(record.get("Volume")),
                }
            )

        return rows


def _as_float(value: Any) -> float | None:
    """Parse numeric values to float with null safety."""
    if value is None:
        return None
    try:
        parsed = float(value)
        if math.isnan(parsed):
            return None
    except (TypeError, ValueError):
        return None
    return parsed


def _as_int(value: Any) -> int | None:
    """Parse numeric values to integer with null safety."""
    if value is None:
        return None
    try:
        parsed = float(value)
        if math.isnan(parsed):
            return None
    except (TypeError, ValueError):
        return None
    return int(parsed)
