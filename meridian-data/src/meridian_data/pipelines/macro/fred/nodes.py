"""Nodes for ingesting and preparing FRED macroeconomic series."""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from .client import FredClient
from .schemas import (
    FredDataQualityParameters,
    FredPipelineParameters,
    FredSeriesMetadata,
)

RAW_SCHEMA: dict[str, pl.DataType] = {
    "series_id": pl.Utf8,
    "series_name": pl.Utf8,
    "frequency": pl.Utf8,
    "units": pl.Utf8,
    "seasonal_adjustment": pl.Utf8,
    "last_updated": pl.Utf8,
    "date": pl.Utf8,
    "value_raw": pl.Utf8,
}

PROCESSED_SCHEMA: dict[str, pl.DataType] = {
    "series_id": pl.Utf8,
    "series_name": pl.Utf8,
    "frequency": pl.Utf8,
    "units": pl.Utf8,
    "seasonal_adjustment": pl.Utf8,
    "last_updated": pl.Utf8,
    "date": pl.Date,
    "value": pl.Float64,
    "run_date": pl.Date,
}

QUALITY_SAMPLE_LIMIT = 5


def ingest_fred_series(fred_parameters: dict[str, Any]) -> pl.DataFrame:
    """
    Ingest configured FRED series observations into a raw long-format table.

    Args:
        fred_parameters: Runtime parameters under `params:fred`.

    Returns:
        Raw FRED observations with metadata and unparsed `value_raw`.
    """
    parameters = FredPipelineParameters.model_validate(fred_parameters)
    rows: list[dict[str, str | None]] = []

    with FredClient.from_parameters(parameters) as client:
        for series_id in parameters.series_ids:
            metadata = client.get_series(series_id=series_id)
            observations = client.get_observations(
                series_id=series_id,
                observation_start=parameters.observation_start,
                observation_end=parameters.observation_end,
                sort_order=parameters.sort_order,
                limit=parameters.limit,
            )
            rows.extend(
                _build_observation_rows(
                    metadata=metadata,
                    series_id=series_id,
                    observations=observations,
                )
            )

    if not rows:
        return pl.DataFrame(schema=RAW_SCHEMA)
    return pl.DataFrame(rows, schema=RAW_SCHEMA)


def process_fred_series(
    fred_raw_series: pl.DataFrame,
    fred_parameters: dict[str, Any],
) -> pl.DataFrame:
    """
    Parse and standardize FRED raw observations for downstream storage.

    Args:
        fred_raw_series: Raw FRED observations from ingest node.
        fred_parameters: Runtime parameters under `params:fred`.

    Returns:
        Processed long-format table with typed dates/numerics and `run_date`.
    """
    if fred_raw_series.is_empty():
        return pl.DataFrame(schema=PROCESSED_SCHEMA)

    parameters = FredPipelineParameters.model_validate(fred_parameters)
    run_date = parameters.run_date or date.today()
    processed_with_quality_columns = fred_raw_series.with_row_index(
        "series_row_order"
    ).with_columns(
        [
            pl.col("date").str.strptime(pl.Date, strict=False),
            pl.when(pl.col("value_raw") == ".")
            .then(None)
            .otherwise(pl.col("value_raw").cast(pl.Float64, strict=False))
            .alias("value"),
            pl.lit(run_date).cast(pl.Date).alias("run_date"),
        ]
    )
    _assert_data_quality(
        processed_with_quality_columns,
        sort_order=parameters.sort_order,
        data_quality=parameters.data_quality,
    )
    return processed_with_quality_columns.drop(["value_raw", "series_row_order"]).sort(
        ["series_id", "date"]
    )


def partition_fred_series(
    fred_processed_series: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build Kedro partition payload keyed by `run_date=YYYY-MM-DD`.

    Args:
        fred_processed_series: Processed FRED table.

    Returns:
        Mapping of partition keys to partition dataframes.
    """
    if fred_processed_series.is_empty():
        return {}

    with_run_date = fred_processed_series.with_columns(
        pl.col("run_date").dt.strftime("%Y-%m-%d").alias("run_date_partition")
    )
    partitions: dict[str, pl.DataFrame] = {}

    for run_date in with_run_date.get_column("run_date_partition").unique().to_list():
        key = f"run_date={run_date}/fred_series"
        partitions[key] = with_run_date.filter(
            pl.col("run_date_partition") == run_date
        ).drop("run_date_partition")

    return partitions


def partition_fred_series_latest(
    fred_processed_series: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build query-friendly partitions keyed by `series_id` and observation `year`.

    Args:
        fred_processed_series: Processed FRED table.

    Returns:
        Mapping of partition keys to partition dataframes.
    """
    if fred_processed_series.is_empty():
        return {}

    with_partition_keys = fred_processed_series.with_columns(
        [
            pl.col("series_id").alias("series_id_partition"),
            pl.col("date").dt.year().cast(pl.Int64).alias("year_partition"),
        ]
    )
    partitions: dict[str, pl.DataFrame] = {}

    partition_values = with_partition_keys.select(
        ["series_id_partition", "year_partition"]
    ).unique()
    series_ids = partition_values.get_column("series_id_partition").to_list()
    years = partition_values.get_column("year_partition").to_list()

    for series_id_value, year_value in zip(series_ids, years, strict=True):
        series_id = str(series_id_value)
        year = int(year_value)
        key = f"series_id={series_id}/year={year}/fred_series_latest"
        partitions[key] = with_partition_keys.filter(
            (pl.col("series_id_partition") == series_id)
            & (pl.col("year_partition") == year)
        ).drop(["series_id_partition", "year_partition"])

    return partitions


def _build_observation_rows(
    *,
    metadata: FredSeriesMetadata,
    series_id: str,
    observations: list[Any],
) -> list[dict[str, str | None]]:
    """Normalize FRED observation rows with metadata fields."""
    rows: list[dict[str, str | None]] = []
    for observation in observations:
        rows.append(
            {
                "series_id": series_id,
                "series_name": metadata.title,
                "frequency": metadata.frequency,
                "units": metadata.units,
                "seasonal_adjustment": metadata.seasonal_adjustment,
                "last_updated": metadata.last_updated,
                "date": observation.date,
                "value_raw": observation.value,
            }
        )
    return rows


def _assert_data_quality(
    fred_series_with_quality_columns: pl.DataFrame,
    *,
    sort_order: str,
    data_quality: FredDataQualityParameters,
) -> None:
    """Validate duplicates, ordering, parse quality, and null thresholds."""
    if data_quality.enforce_valid_dates:
        _assert_valid_dates(fred_series_with_quality_columns)
    if data_quality.enforce_numeric_parse:
        _assert_numeric_parse(fred_series_with_quality_columns)
    if data_quality.enforce_unique_series_date:
        _assert_unique_series_date(fred_series_with_quality_columns)
    if data_quality.enforce_monotonic_dates:
        _assert_monotonic_dates(fred_series_with_quality_columns, sort_order=sort_order)
    _assert_null_ratio_per_series(
        fred_series_with_quality_columns,
        max_null_ratio_per_series=data_quality.max_null_ratio_per_series,
    )


def _assert_valid_dates(fred_series_with_quality_columns: pl.DataFrame) -> None:
    """Fail if any observation date cannot be parsed."""
    invalid_dates = fred_series_with_quality_columns.filter(pl.col("date").is_null())
    if invalid_dates.is_empty():
        return

    sample = invalid_dates.select(["series_id", "value_raw"]).head(QUALITY_SAMPLE_LIMIT)
    raise ValueError(
        "FRED data quality check failed: found rows with invalid observation dates. "
        f"sample={sample.to_dicts()}"
    )


def _assert_numeric_parse(fred_series_with_quality_columns: pl.DataFrame) -> None:
    """Fail if non-placeholder numeric strings cannot be parsed."""
    invalid_numeric = fred_series_with_quality_columns.filter(
        (pl.col("value_raw") != ".") & pl.col("value").is_null()
    )
    if invalid_numeric.is_empty():
        return

    sample = invalid_numeric.select(["series_id", "date", "value_raw"]).head(
        QUALITY_SAMPLE_LIMIT
    )
    raise ValueError(
        "FRED data quality check failed: found non-numeric values outside '.' "
        f"placeholder handling. sample={sample.to_dicts()}"
    )


def _assert_unique_series_date(fred_series_with_quality_columns: pl.DataFrame) -> None:
    """Fail if duplicate `(series_id, date)` keys are present."""
    duplicates = (
        fred_series_with_quality_columns.group_by(["series_id", "date"])
        .len()
        .filter(pl.col("len") > 1)
    )
    if duplicates.is_empty():
        return

    sample = duplicates.head(QUALITY_SAMPLE_LIMIT)
    raise ValueError(
        "FRED data quality check failed: duplicate (series_id, date) rows detected. "
        f"sample={sample.to_dicts()}"
    )


def _assert_monotonic_dates(
    fred_series_with_quality_columns: pl.DataFrame,
    *,
    sort_order: str,
) -> None:
    """Fail if source observation ordering violates the requested sort order."""
    with_previous = fred_series_with_quality_columns.with_columns(
        pl.col("date").shift(1).over("series_id").alias("previous_date")
    )
    if sort_order == "asc":
        ordering_violations = with_previous.filter(
            pl.col("previous_date").is_not_null()
            & (pl.col("date") < pl.col("previous_date"))
        )
    else:
        ordering_violations = with_previous.filter(
            pl.col("previous_date").is_not_null()
            & (pl.col("date") > pl.col("previous_date"))
        )

    if ordering_violations.is_empty():
        return

    sample = ordering_violations.select(
        ["series_id", "previous_date", "date", "value_raw"]
    ).head(QUALITY_SAMPLE_LIMIT)
    raise ValueError(
        "FRED data quality check failed: observation dates are not monotonic for "
        f"sort_order={sort_order}. sample={sample.to_dicts()}"
    )


def _assert_null_ratio_per_series(
    fred_series_with_quality_columns: pl.DataFrame,
    *,
    max_null_ratio_per_series: float,
) -> None:
    """Fail if per-series null ratio exceeds the configured threshold."""
    null_ratio_by_series = fred_series_with_quality_columns.group_by("series_id").agg(
        [
            pl.len().alias("row_count"),
            pl.col("value").null_count().alias("null_count"),
            pl.col("value").is_null().mean().alias("null_ratio"),
        ]
    )
    threshold_violations = null_ratio_by_series.filter(
        pl.col("null_ratio") > max_null_ratio_per_series
    )
    if threshold_violations.is_empty():
        return

    sample = threshold_violations.head(QUALITY_SAMPLE_LIMIT)
    raise ValueError(
        "FRED data quality check failed: per-series null ratio exceeded threshold "
        f"{max_null_ratio_per_series}. sample={sample.to_dicts()}"
    )
