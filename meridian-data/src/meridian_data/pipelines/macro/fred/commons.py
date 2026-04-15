"""Shared helpers and contracts for the FRED pipeline."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from .schemas import (
    FredDataQualityParameters,
    FredPipelineParameters,
    FredSeriesMetadata,
)

LOGGER = logging.getLogger(__name__)

PIPELINE_NAME = "macro.fred"
SOURCE_NAME = "fred"
QUALITY_SAMPLE_LIMIT = 5

SchemaMap = dict[str, pl.DataType]

RAW_SCHEMA: SchemaMap = {
    "series_id": pl.String(),
    "series_name": pl.String(),
    "frequency": pl.String(),
    "units": pl.String(),
    "seasonal_adjustment": pl.String(),
    "last_updated": pl.String(),
    "date": pl.String(),
    "value_raw": pl.String(),
}

PROCESSED_SCHEMA: SchemaMap = {
    "series_id": pl.String(),
    "series_name": pl.String(),
    "frequency": pl.String(),
    "units": pl.String(),
    "seasonal_adjustment": pl.String(),
    "last_updated": pl.String(),
    "date": pl.Date(),
    "value": pl.Float64(),
    "run_date": pl.Date(),
}

INGESTION_METADATA_SCHEMA: SchemaMap = {
    "run_date": pl.Date(),
    "entity_id": pl.String(),
    "series_id": pl.String(),
    "row_count": pl.Int64(),
    "null_count": pl.Int64(),
    "null_ratio": pl.Float64(),
    "min_observation_date": pl.Date(),
    "max_observation_date": pl.Date(),
    "watermark_observation_date": pl.Date(),
    "frequency": pl.String(),
    "source_last_updated": pl.String(),
    "observation_start": pl.String(),
    "observation_end": pl.String(),
    "sort_order": pl.String(),
    "sync_mode": pl.String(),
    "generated_at_utc": pl.String(),
}


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(UTC).isoformat()


def log_kv_event(  # noqa: PLR0913
    *,
    level: int,
    event: str,
    node: str,
    run_date: date | None,
    status: str,
    row_count: int | None = None,
    entity_count: int | None = None,
    duration_ms: int | None = None,
    **extra_fields: Any,
) -> None:
    """Emit a standardized key-value observability event."""
    fields: dict[str, Any] = {
        "event": event,
        "pipeline": PIPELINE_NAME,
        "source": SOURCE_NAME,
        "node": node,
        "run_date": run_date.isoformat() if run_date is not None else None,
        "row_count": row_count,
        "entity_count": entity_count,
        "duration_ms": duration_ms,
        "status": status,
        **extra_fields,
    }
    payload = " ".join(
        f"{key}={_format_log_value(value)}" for key, value in fields.items()
    )
    LOGGER.log(level, payload)


def build_observation_rows(
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


def assert_data_quality(
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


def build_ingestion_metadata_frame(
    fred_processed_series: pl.DataFrame,
    *,
    parameters: FredPipelineParameters,
    run_date: date,
    generated_at_utc: str,
) -> pl.DataFrame:
    """Build per-entity ingestion metadata for incremental watermarking."""
    metadata_df = (
        fred_processed_series.group_by("series_id")
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("value").null_count().alias("null_count"),
                pl.col("value").is_null().mean().alias("null_ratio"),
                pl.col("date").min().alias("min_observation_date"),
                pl.col("date").max().alias("max_observation_date"),
                pl.col("frequency").drop_nulls().first().alias("frequency"),
                pl.col("last_updated")
                .drop_nulls()
                .first()
                .alias("source_last_updated"),
            ]
        )
        .with_columns(
            [
                pl.col("series_id").alias("entity_id"),
                pl.col("max_observation_date").alias("watermark_observation_date"),
                pl.lit(run_date).cast(pl.Date()).alias("run_date"),
                pl.lit(parameters.observation_start)
                .cast(pl.String())
                .alias("observation_start"),
                pl.lit(parameters.observation_end)
                .cast(pl.String())
                .alias("observation_end"),
                pl.lit(parameters.sort_order).cast(pl.String()).alias("sort_order"),
                pl.lit(parameters.sync_mode).cast(pl.String()).alias("sync_mode"),
                pl.lit(generated_at_utc).cast(pl.String()).alias("generated_at_utc"),
            ]
        )
        .select(list(INGESTION_METADATA_SCHEMA.keys()))
    )
    return metadata_df


def build_run_date_partitions(
    dataset: pl.DataFrame,
    *,
    filename_stem: str,
) -> dict[str, pl.DataFrame]:
    """Build run-date keyed partitions with a common `<stem>` filename."""
    with_run_date = dataset.with_columns(
        pl.col("run_date").dt.strftime("%Y-%m-%d").alias("run_date_partition")
    )
    partitions: dict[str, pl.DataFrame] = {}
    for run_date in with_run_date.get_column("run_date_partition").unique().to_list():
        key = f"run_date={run_date}/{filename_stem}"
        partitions[key] = with_run_date.filter(
            pl.col("run_date_partition") == run_date
        ).drop("run_date_partition")
    return partitions


def build_series_year_partitions(
    dataset: pl.DataFrame,
    *,
    filename_stem: str,
) -> dict[str, pl.DataFrame]:
    """Build query-friendly partitions keyed by `series_id` and observation year."""
    with_partition_keys = dataset.with_columns(
        [
            pl.col("series_id").alias("series_id_partition"),
            pl.col("date").dt.year().cast(pl.Int64()).alias("year_partition"),
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
        key = f"series_id={series_id}/year={year}/{filename_stem}"
        partitions[key] = with_partition_keys.filter(
            (pl.col("series_id_partition") == series_id)
            & (pl.col("year_partition") == year)
        ).drop(["series_id_partition", "year_partition"])
    return partitions


def build_entity_watermark_partitions(
    ingestion_metadata: pl.DataFrame,
    *,
    filename_stem: str,
) -> dict[str, pl.DataFrame]:
    """Build per-entity watermark partitions from ingestion metadata."""
    partitions: dict[str, pl.DataFrame] = {}
    entity_ids = ingestion_metadata.get_column("entity_id").unique().to_list()
    for entity_id in entity_ids:
        key = f"entity_id={entity_id}/{filename_stem}"
        partitions[key] = ingestion_metadata.filter(pl.col("entity_id") == entity_id)
    return partitions


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


def _format_log_value(value: Any) -> str:
    """Format key-value log field values consistently."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
