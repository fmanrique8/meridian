from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl
import pytest

from meridian_data.pipelines.macro.fred import nodes
from meridian_data.pipelines.macro.fred.schemas import (
    FredObservation,
    FredSeriesMetadata,
)

EXPECTED_ROWS = 8
EXPECTED_ROWS_PER_SERIES_YEAR = 2
EXPECTED_GDPC1_ROWS = 2
EXPECTED_SERIES_COUNT = 4


class StubFredClient:
    @classmethod
    def from_parameters(cls, parameters: Any) -> StubFredClient:  # noqa: ARG003
        return cls()

    def __enter__(self) -> StubFredClient:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        return None

    def get_series(self, series_id: str) -> FredSeriesMetadata:
        frequency = "Quarterly" if series_id == "GDPC1" else "Monthly"
        return FredSeriesMetadata(
            id=series_id,
            title=f"{series_id} title",
            frequency=frequency,
            units="Percent",
            seasonal_adjustment="Seasonally Adjusted",
            last_updated="2026-04-15 12:00:00-05",
        )

    def get_observations(
        self,
        *,
        series_id: str,
        observation_start: str | None = None,  # noqa: ARG002
        observation_end: str | None = None,  # noqa: ARG002
        sort_order: str = "asc",  # noqa: ARG002
        limit: int | None = None,  # noqa: ARG002
    ) -> list[FredObservation]:
        if series_id == "GDPC1":
            return [
                FredObservation(date="2025-10-01", value="22857.30"),
                FredObservation(date="2026-01-01", value="22931.20"),
            ]
        if series_id == "CPIAUCSL":
            return [
                FredObservation(date="2026-02-01", value="."),
                FredObservation(date="2026-03-01", value="319.40"),
            ]
        return [
            FredObservation(date="2026-02-01", value="4.33"),
            FredObservation(date="2026-03-01", value="4.33"),
        ]


def _fred_parameters() -> dict[str, Any]:
    return {
        "api_key": "fred-test-key",
        "base_url": "https://api.stlouisfed.org/fred",
        "timeout_seconds": 1.0,
        "max_retries": 3,
        "backoff_seconds": 0.0,
        "jitter_seconds": 0.0,
        "observation_start": "2026-02-01",
        "sort_order": "asc",
        "data_quality": {
            "max_null_ratio_per_series": 1.0,
            "enforce_monotonic_dates": True,
            "enforce_unique_series_date": True,
            "enforce_numeric_parse": True,
            "enforce_valid_dates": True,
        },
        "series_ids": ["FEDFUNDS", "CPIAUCSL", "UNRATE", "GDPC1"],
        "run_date": "2026-04-15",
    }


def test_ingest_process_and_partition_fred_series(monkeypatch) -> None:
    monkeypatch.setattr(nodes, "FredClient", StubFredClient)

    raw_df = nodes.ingest_fred_series(_fred_parameters())

    assert raw_df.height == EXPECTED_ROWS
    assert "value_raw" in raw_df.columns

    processed_df = nodes.process_fred_series(raw_df, _fred_parameters())

    assert processed_df.height == EXPECTED_ROWS
    assert processed_df.schema["date"] == pl.Date
    assert processed_df.schema["run_date"] == pl.Date
    assert (
        processed_df.filter(pl.col("series_id") == "CPIAUCSL")["value"].null_count()
        == 1
    )
    gdpc1_df = processed_df.filter(pl.col("series_id") == "GDPC1")
    assert gdpc1_df.height == EXPECTED_GDPC1_ROWS
    assert gdpc1_df["frequency"].unique().to_list() == ["Quarterly"]
    assert gdpc1_df["date"].to_list() == [date(2025, 10, 1), date(2026, 1, 1)]
    assert processed_df["run_date"].unique().to_list() == [date(2026, 4, 15)]

    partitions = nodes.partition_fred_series(processed_df)

    assert list(partitions.keys()) == ["run_date=2026-04-15/fred_series"]
    assert partitions["run_date=2026-04-15/fred_series"].height == EXPECTED_ROWS

    latest_partitions = nodes.partition_fred_series_latest(processed_df)

    assert sorted(latest_partitions.keys()) == [
        "series_id=CPIAUCSL/year=2026/fred_series_latest",
        "series_id=FEDFUNDS/year=2026/fred_series_latest",
        "series_id=GDPC1/year=2025/fred_series_latest",
        "series_id=GDPC1/year=2026/fred_series_latest",
        "series_id=UNRATE/year=2026/fred_series_latest",
    ]
    assert (
        latest_partitions["series_id=FEDFUNDS/year=2026/fred_series_latest"].height
        == EXPECTED_ROWS_PER_SERIES_YEAR
    )
    assert latest_partitions["series_id=GDPC1/year=2025/fred_series_latest"].height == 1
    assert latest_partitions["series_id=GDPC1/year=2026/fred_series_latest"].height == 1

    ingestion_metadata = nodes.build_fred_ingestion_metadata(
        processed_df,
        _fred_parameters(),
    )
    assert ingestion_metadata.height == EXPECTED_SERIES_COUNT
    assert sorted(ingestion_metadata["entity_id"].to_list()) == sorted(
        ["FEDFUNDS", "CPIAUCSL", "UNRATE", "GDPC1"]
    )
    assert sorted(ingestion_metadata["series_id"].to_list()) == sorted(
        ["FEDFUNDS", "CPIAUCSL", "UNRATE", "GDPC1"]
    )
    assert "watermark_observation_date" in ingestion_metadata.columns
    gdpc1_metadata = ingestion_metadata.filter(pl.col("entity_id") == "GDPC1")
    assert gdpc1_metadata["watermark_observation_date"].to_list() == [date(2026, 1, 1)]

    metadata_partitions = nodes.partition_fred_ingestion_metadata(ingestion_metadata)
    assert list(metadata_partitions.keys()) == [
        "run_date=2026-04-15/fred_ingestion_metadata"
    ]
    assert metadata_partitions[
        "run_date=2026-04-15/fred_ingestion_metadata"
    ].height == (EXPECTED_SERIES_COUNT)

    watermark_partitions = nodes.partition_fred_entity_watermarks(ingestion_metadata)
    assert sorted(watermark_partitions.keys()) == [
        "entity_id=CPIAUCSL/fred_entity_watermark",
        "entity_id=FEDFUNDS/fred_entity_watermark",
        "entity_id=GDPC1/fred_entity_watermark",
        "entity_id=UNRATE/fred_entity_watermark",
    ]


def test_process_fred_series_handles_empty_input() -> None:
    empty_raw = pl.DataFrame(schema=nodes.RAW_SCHEMA)
    processed_df = nodes.process_fred_series(empty_raw, _fred_parameters())

    assert processed_df.is_empty()
    assert processed_df.schema == nodes.PROCESSED_SCHEMA
    assert nodes.partition_fred_series_latest(processed_df) == {}
    metadata_df = nodes.build_fred_ingestion_metadata(processed_df, _fred_parameters())
    assert metadata_df.is_empty()
    assert metadata_df.schema == nodes.INGESTION_METADATA_SCHEMA
    assert nodes.partition_fred_ingestion_metadata(metadata_df) == {}
    assert nodes.partition_fred_entity_watermarks(metadata_df) == {}


def test_process_fred_series_fails_on_duplicate_series_date_rows() -> None:
    raw_df = pl.DataFrame(
        {
            "series_id": ["UNRATE", "UNRATE"],
            "series_name": ["UNRATE title", "UNRATE title"],
            "frequency": ["Monthly", "Monthly"],
            "units": ["Percent", "Percent"],
            "seasonal_adjustment": ["Seasonally Adjusted", "Seasonally Adjusted"],
            "last_updated": ["2026-04-15 12:00:00-05", "2026-04-15 12:00:00-05"],
            "date": ["2026-03-01", "2026-03-01"],
            "value_raw": ["4.33", "4.33"],
        },
        schema=nodes.RAW_SCHEMA,
    )
    params = _fred_parameters()

    with pytest.raises(ValueError, match="duplicate \\(series_id, date\\) rows"):
        nodes.process_fred_series(raw_df, params)


def test_process_fred_series_fails_on_non_monotonic_dates() -> None:
    raw_df = pl.DataFrame(
        {
            "series_id": ["UNRATE", "UNRATE"],
            "series_name": ["UNRATE title", "UNRATE title"],
            "frequency": ["Monthly", "Monthly"],
            "units": ["Percent", "Percent"],
            "seasonal_adjustment": ["Seasonally Adjusted", "Seasonally Adjusted"],
            "last_updated": ["2026-04-15 12:00:00-05", "2026-04-15 12:00:00-05"],
            "date": ["2026-03-01", "2026-02-01"],
            "value_raw": ["4.33", "4.33"],
        },
        schema=nodes.RAW_SCHEMA,
    )
    params = _fred_parameters()

    with pytest.raises(ValueError, match="dates are not monotonic"):
        nodes.process_fred_series(raw_df, params)


def test_process_fred_series_fails_on_invalid_numeric_value() -> None:
    raw_df = pl.DataFrame(
        {
            "series_id": ["UNRATE"],
            "series_name": ["UNRATE title"],
            "frequency": ["Monthly"],
            "units": ["Percent"],
            "seasonal_adjustment": ["Seasonally Adjusted"],
            "last_updated": ["2026-04-15 12:00:00-05"],
            "date": ["2026-03-01"],
            "value_raw": ["not-a-number"],
        },
        schema=nodes.RAW_SCHEMA,
    )
    params = _fred_parameters()

    with pytest.raises(ValueError, match="non-numeric values"):
        nodes.process_fred_series(raw_df, params)


def test_process_fred_series_fails_on_null_ratio_threshold() -> None:
    raw_df = pl.DataFrame(
        {
            "series_id": ["UNRATE", "UNRATE"],
            "series_name": ["UNRATE title", "UNRATE title"],
            "frequency": ["Monthly", "Monthly"],
            "units": ["Percent", "Percent"],
            "seasonal_adjustment": ["Seasonally Adjusted", "Seasonally Adjusted"],
            "last_updated": ["2026-04-15 12:00:00-05", "2026-04-15 12:00:00-05"],
            "date": ["2026-02-01", "2026-03-01"],
            "value_raw": [".", "4.33"],
        },
        schema=nodes.RAW_SCHEMA,
    )
    params = _fred_parameters()
    params["data_quality"]["max_null_ratio_per_series"] = 0.4

    with pytest.raises(ValueError, match="null ratio exceeded threshold"):
        nodes.process_fred_series(raw_df, params)
