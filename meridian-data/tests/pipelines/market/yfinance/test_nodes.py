from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl
import pytest

from meridian_data.pipelines.market.yfinance import nodes
from meridian_data.pipelines.market.yfinance.client import YfinanceHistoryResponse

EXPECTED_TOTAL_ROWS = 3
EXPECTED_ENTITY_COUNT = 2
EXPECTED_MERGED_SPY_ROWS = 3
EXPECTED_MERGED_SPY_CLOSE = 501.0


def _yfinance_parameters() -> dict[str, Any]:
    return {
        "timeout_seconds": 1.0,
        "max_retries": 3,
        "backoff_seconds": 0.0,
        "jitter_seconds": 0.0,
        "history_years": 10,
        "interval": "1d",
        "sync_mode": "incremental",
        "overlap_days": 7,
        "run_date": "2026-04-16",
        "source": "yfinance",
        "symbols": ["SPY", "QQQ"],
        "data_quality": {
            "max_null_ratio_per_symbol": 0.0,
            "enforce_unique_symbol_date": True,
            "enforce_valid_dates": True,
            "enforce_non_null_ohlcv": True,
        },
    }


class StubYfinanceClient:
    requested_windows: dict[str, date] = {}

    @classmethod
    def from_parameters(cls, parameters: Any) -> StubYfinanceClient:  # noqa: ARG003
        cls.requested_windows = {}
        return cls()

    def __enter__(self) -> StubYfinanceClient:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        return None

    def get_history(
        self,
        *,
        symbol: str,
        start_date: date,
        end_date: date,  # noqa: ARG002
        interval: str,  # noqa: ARG002
    ) -> YfinanceHistoryResponse:
        self.requested_windows[symbol] = start_date
        if symbol == "SPY":
            rows = [
                {
                    "date": "2026-04-09",
                    "open": 500.0,
                    "high": 502.0,
                    "low": 499.0,
                    "close": 501.0,
                    "adj_close": 500.5,
                    "volume": 1_000_000,
                },
                {
                    "date": "2026-04-10",
                    "open": 501.0,
                    "high": 503.0,
                    "low": 500.0,
                    "close": 502.0,
                    "adj_close": 501.5,
                    "volume": 1_010_000,
                },
            ]
        else:
            rows = [
                {
                    "date": "2026-04-15",
                    "open": 450.0,
                    "high": 452.0,
                    "low": 449.0,
                    "close": 451.0,
                    "adj_close": 450.5,
                    "volume": 900_000,
                }
            ]
        source_last_updated = rows[-1]["date"] if rows else None
        return YfinanceHistoryResponse(
            symbol=symbol,
            rows=rows,
            source_last_updated=source_last_updated,
        )


def _watermark_partitions() -> dict[str, pl.DataFrame]:
    return {
        "entity_id=SPY/yfinance_entity_watermark": pl.DataFrame(
            {
                "entity_id": ["SPY"],
                "watermark_observation_date": [date(2026, 4, 10)],
                "run_date": [date(2026, 4, 10)],
            }
        )
    }


def test_ingest_process_and_partition_yfinance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(nodes, "YfinanceClient", StubYfinanceClient)

    raw_df = nodes.ingest_yfinance_etf_prices(_yfinance_parameters(), _watermark_partitions())

    assert raw_df.height == EXPECTED_TOTAL_ROWS
    assert sorted(raw_df["symbol"].unique().to_list()) == ["QQQ", "SPY"]
    assert StubYfinanceClient.requested_windows["SPY"] == date(2026, 4, 3)
    assert StubYfinanceClient.requested_windows["QQQ"] == date(2016, 4, 16)

    processed_df = nodes.process_yfinance_etf_prices(raw_df, _yfinance_parameters())

    assert processed_df.height == EXPECTED_TOTAL_ROWS
    assert processed_df.schema["date"] == pl.Date
    assert processed_df.schema["run_date"] == pl.Date

    raw_partitions = nodes.partition_yfinance_etf_prices(raw_df)
    assert list(raw_partitions.keys()) == ["run_date=2026-04-16/yfinance_etf_prices"]
    assert (
        raw_partitions["run_date=2026-04-16/yfinance_etf_prices"].height
        == EXPECTED_TOTAL_ROWS
    )

    existing_latest = {
        "symbol=SPY/year=2026/yfinance_etf_prices_latest": pl.DataFrame(
            {
                "symbol": ["SPY", "SPY"],
                "date": [date(2026, 4, 8), date(2026, 4, 9)],
                "open": [499.0, 499.5],
                "high": [500.0, 501.0],
                "low": [498.0, 498.5],
                "close": [499.5, 500.0],
                "adj_close": [499.2, 499.8],
                "volume": [950_000, 960_000],
                "ingested_at_utc": ["2026-04-09T00:00:00+00:00"] * 2,
                "source": ["yfinance"] * 2,
                "source_last_updated": ["2026-04-09"] * 2,
                "sync_mode": ["full"] * 2,
                "fetch_start": ["2016-04-09"] * 2,
                "fetch_end": ["2026-04-09"] * 2,
                "run_date": [date(2026, 4, 9)] * 2,
            }
        )
    }
    latest_partitions = nodes.partition_yfinance_etf_prices_latest(
        processed_df,
        existing_latest,
    )
    merged_spy = latest_partitions["symbol=SPY/year=2026/yfinance_etf_prices_latest"]
    assert merged_spy.height == EXPECTED_MERGED_SPY_ROWS
    assert (
        merged_spy.filter(pl.col("date") == date(2026, 4, 9))["close"].item()
        == EXPECTED_MERGED_SPY_CLOSE
    )

    ingestion_metadata = nodes.build_yfinance_ingestion_metadata(
        processed_df,
        _yfinance_parameters(),
    )
    assert ingestion_metadata.height == EXPECTED_ENTITY_COUNT
    assert sorted(ingestion_metadata["entity_id"].to_list()) == ["QQQ", "SPY"]

    metadata_partitions = nodes.partition_yfinance_ingestion_metadata(ingestion_metadata)
    assert list(metadata_partitions.keys()) == [
        "run_date=2026-04-16/yfinance_ingestion_metadata"
    ]

    watermark_partitions = nodes.partition_yfinance_entity_watermarks(ingestion_metadata)
    assert sorted(watermark_partitions.keys()) == [
        "entity_id=QQQ/yfinance_entity_watermark",
        "entity_id=SPY/yfinance_entity_watermark",
    ]


def test_process_yfinance_handles_empty_input() -> None:
    empty_raw = pl.DataFrame(schema=nodes.RAW_SCHEMA)
    processed_df = nodes.process_yfinance_etf_prices(empty_raw, _yfinance_parameters())

    assert processed_df.is_empty()
    assert processed_df.schema == nodes.PROCESSED_SCHEMA
    assert nodes.partition_yfinance_etf_prices(empty_raw) == {}
    assert nodes.partition_yfinance_etf_prices_latest(processed_df, {}) == {}
    metadata_df = nodes.build_yfinance_ingestion_metadata(
        processed_df,
        _yfinance_parameters(),
    )
    assert metadata_df.is_empty()
    assert metadata_df.schema == nodes.INGESTION_METADATA_SCHEMA
    assert nodes.partition_yfinance_ingestion_metadata(metadata_df) == {}
    assert nodes.partition_yfinance_entity_watermarks(metadata_df) == {}


def test_process_yfinance_fails_on_duplicate_symbol_date_rows() -> None:
    raw_df = pl.DataFrame(
        {
            "symbol": ["SPY", "SPY"],
            "date": ["2026-04-10", "2026-04-10"],
            "open": [500.0, 500.0],
            "high": [502.0, 502.0],
            "low": [499.0, 499.0],
            "close": [501.0, 501.0],
            "adj_close": [500.5, 500.5],
            "volume": [1_000_000, 1_000_000],
            "ingested_at_utc": ["2026-04-16T00:00:00+00:00"] * 2,
            "source": ["yfinance"] * 2,
            "source_last_updated": ["2026-04-10"] * 2,
            "sync_mode": ["incremental"] * 2,
            "fetch_start": ["2026-04-03"] * 2,
            "fetch_end": ["2026-04-16"] * 2,
            "run_date": [date(2026, 4, 16)] * 2,
        },
        schema=nodes.RAW_SCHEMA,
    )

    with pytest.raises(ValueError, match="duplicate \\(symbol, date\\) rows"):
        nodes.process_yfinance_etf_prices(raw_df, _yfinance_parameters())


def test_process_yfinance_fails_on_null_critical_fields() -> None:
    raw_df = pl.DataFrame(
        {
            "symbol": ["SPY"],
            "date": ["2026-04-10"],
            "open": [None],
            "high": [502.0],
            "low": [499.0],
            "close": [501.0],
            "adj_close": [500.5],
            "volume": [1_000_000],
            "ingested_at_utc": ["2026-04-16T00:00:00+00:00"],
            "source": ["yfinance"],
            "source_last_updated": ["2026-04-10"],
            "sync_mode": ["incremental"],
            "fetch_start": ["2026-04-03"],
            "fetch_end": ["2026-04-16"],
            "run_date": [date(2026, 4, 16)],
        },
        schema=nodes.RAW_SCHEMA,
    )

    with pytest.raises(ValueError, match="null critical OHLCV fields"):
        nodes.process_yfinance_etf_prices(raw_df, _yfinance_parameters())
