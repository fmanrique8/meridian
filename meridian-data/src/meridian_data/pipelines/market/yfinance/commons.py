"""Shared helpers and contracts for the yfinance pipeline."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import date, timedelta

import polars as pl

from meridian_data.commons.observability import (
    build_log_kv_emitter,
)
from meridian_data.commons.observability import (
    now_utc_iso as _now_utc_iso,
)

from .schemas import YfinanceDataQualityParameters, YfinancePipelineParameters

LOGGER = logging.getLogger(__name__)

PIPELINE_NAME = "market.yfinance"
SOURCE_NAME = "yfinance"
QUALITY_SAMPLE_LIMIT = 5

SchemaMap = dict[str, pl.DataType]
PartitionFrame = pl.DataFrame | Callable[[], pl.DataFrame]
PartitionInput = Mapping[str, PartitionFrame]

RAW_SCHEMA: SchemaMap = {
    "symbol": pl.String(),
    "date": pl.String(),
    "open": pl.Float64(),
    "high": pl.Float64(),
    "low": pl.Float64(),
    "close": pl.Float64(),
    "adj_close": pl.Float64(),
    "volume": pl.Int64(),
    "ingested_at_utc": pl.String(),
    "source": pl.String(),
    "source_last_updated": pl.String(),
    "sync_mode": pl.String(),
    "fetch_start": pl.String(),
    "fetch_end": pl.String(),
    "run_date": pl.Date(),
}

PROCESSED_SCHEMA: SchemaMap = {
    "symbol": pl.String(),
    "date": pl.Date(),
    "open": pl.Float64(),
    "high": pl.Float64(),
    "low": pl.Float64(),
    "close": pl.Float64(),
    "adj_close": pl.Float64(),
    "volume": pl.Int64(),
    "ingested_at_utc": pl.String(),
    "source": pl.String(),
    "source_last_updated": pl.String(),
    "sync_mode": pl.String(),
    "fetch_start": pl.String(),
    "fetch_end": pl.String(),
    "run_date": pl.Date(),
}

INGESTION_METADATA_SCHEMA: SchemaMap = {
    "run_date": pl.Date(),
    "entity_id": pl.String(),
    "symbol": pl.String(),
    "row_count": pl.Int64(),
    "null_count": pl.Int64(),
    "null_ratio": pl.Float64(),
    "min_observation_date": pl.Date(),
    "max_observation_date": pl.Date(),
    "watermark_observation_date": pl.Date(),
    "source_last_updated": pl.String(),
    "observation_start": pl.String(),
    "observation_end": pl.String(),
    "sync_mode": pl.String(),
    "generated_at_utc": pl.String(),
}

CRITICAL_NUMERIC_COLUMNS = ["open", "high", "low", "close", "volume"]

log_kv_event = build_log_kv_emitter(
    logger=LOGGER,
    pipeline=PIPELINE_NAME,
    source=SOURCE_NAME,
)
now_utc_iso = _now_utc_iso


def resolve_run_date(parameters: YfinancePipelineParameters) -> date:
    """Resolve run date from params or current execution date."""
    if parameters.run_date is not None:
        return parameters.run_date
    return date.today()


def subtract_years(run_date: date, years: int) -> date:
    """Subtract calendar years while preserving month/day where possible."""
    try:
        return run_date.replace(year=run_date.year - years)
    except ValueError:
        # Leap-day fallback when target year is non-leap.
        return run_date.replace(year=run_date.year - years, day=28)


def materialize_partitions(partitions: PartitionInput) -> pl.DataFrame:
    """Materialize a Kedro PartitionedDataset payload into one DataFrame."""
    if not partitions:
        return pl.DataFrame()
    frames: list[pl.DataFrame] = []
    for partition_key in sorted(partitions):
        partition_value = partitions[partition_key]
        frame = partition_value() if callable(partition_value) else partition_value
        if not isinstance(frame, pl.DataFrame):
            raise TypeError(
                "Expected partition to materialize to a Polars DataFrame, "
                f"got {type(frame)!r} for key={partition_key}."
            )
        frames.append(frame)
    return pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()


def normalize_processed_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize processed-frame columns to pipeline contract types."""
    if frame.is_empty():
        return pl.DataFrame(schema=PROCESSED_SCHEMA)

    normalized = frame
    for column_name, data_type in PROCESSED_SCHEMA.items():
        if column_name not in normalized.columns:
            normalized = normalized.with_columns(
                pl.lit(None).cast(data_type).alias(column_name)
            )

    normalized = normalized.select(list(PROCESSED_SCHEMA.keys())).with_columns(
        [
            pl.col("symbol").cast(pl.String()).str.to_uppercase().alias("symbol"),
            pl.col("date")
            .cast(pl.String())
            .str.strptime(pl.Date, strict=False)
            .alias("date"),
            pl.col("open").cast(pl.Float64, strict=False).alias("open"),
            pl.col("high").cast(pl.Float64, strict=False).alias("high"),
            pl.col("low").cast(pl.Float64, strict=False).alias("low"),
            pl.col("close").cast(pl.Float64, strict=False).alias("close"),
            pl.col("adj_close").cast(pl.Float64, strict=False).alias("adj_close"),
            pl.col("volume").cast(pl.Int64, strict=False).alias("volume"),
            pl.col("ingested_at_utc").cast(pl.String()).alias("ingested_at_utc"),
            pl.col("source").cast(pl.String()).alias("source"),
            pl.col("source_last_updated").cast(pl.String()).alias("source_last_updated"),
            pl.col("sync_mode").cast(pl.String()).alias("sync_mode"),
            pl.col("fetch_start").cast(pl.String()).alias("fetch_start"),
            pl.col("fetch_end").cast(pl.String()).alias("fetch_end"),
            pl.col("run_date")
            .cast(pl.String())
            .str.strptime(pl.Date, strict=False)
            .alias("run_date"),
        ]
    )
    return normalized


def merge_serving_history(
    new_rows: pl.DataFrame,
    existing_rows: pl.DataFrame,
) -> pl.DataFrame:
    """Merge incremental rows with existing serving rows and de-duplicate keys."""
    normalized_new = normalize_processed_frame(new_rows)
    if existing_rows.is_empty():
        return normalized_new

    normalized_existing = normalize_processed_frame(existing_rows)
    combined = pl.concat([normalized_existing, normalized_new], how="vertical_relaxed")
    combined_sorted = combined.sort(["symbol", "date", "run_date", "ingested_at_utc"])

    value_columns = [
        column_name
        for column_name in PROCESSED_SCHEMA
        if column_name not in {"symbol", "date"}
    ]
    deduplicated = combined_sorted.group_by(["symbol", "date"], maintain_order=True).agg(
        [pl.col(column).drop_nulls().last().alias(column) for column in value_columns]
    )
    return deduplicated.sort(["symbol", "date"])


def extract_symbol_watermarks(
    watermark_partitions: pl.DataFrame,
) -> dict[str, date]:
    """Extract per-symbol watermark dates from partitioned watermark inputs."""
    if watermark_partitions.is_empty():
        return {}
    required_columns = {"entity_id", "watermark_observation_date"}
    if not required_columns.issubset(set(watermark_partitions.columns)):
        return {}

    normalized = watermark_partitions.with_columns(
        [
            pl.col("entity_id").cast(pl.String()).str.to_uppercase().alias("entity_id"),
            pl.col("watermark_observation_date")
            .cast(pl.String())
            .str.strptime(pl.Date, strict=False)
            .alias("watermark_observation_date"),
        ]
    ).filter(pl.col("entity_id").is_not_null() & pl.col("watermark_observation_date").is_not_null())
    if normalized.is_empty():
        return {}

    latest = normalized.group_by("entity_id").agg(
        pl.col("watermark_observation_date").max().alias("watermark_observation_date")
    )
    return {
        row["entity_id"]: row["watermark_observation_date"]
        for row in latest.iter_rows(named=True)
    }


def infer_fetch_start_date(
    *,
    symbol: str,
    run_date: date,
    parameters: YfinancePipelineParameters,
    watermarks: dict[str, date],
) -> date:
    """Infer per-symbol fetch start date for full vs incremental sync mode."""
    full_start = subtract_years(run_date, parameters.history_years)
    if parameters.sync_mode != "incremental":
        return full_start

    symbol_watermark = watermarks.get(symbol.upper())
    if symbol_watermark is None:
        return full_start

    overlap_start = symbol_watermark - timedelta(days=parameters.overlap_days)
    return max(full_start, overlap_start)


def assert_data_quality(
    processed_series_with_quality_columns: pl.DataFrame,
    *,
    data_quality: YfinanceDataQualityParameters,
) -> None:
    """Validate duplicates, date parsing, and critical-field null policies."""
    if data_quality.enforce_valid_dates:
        _assert_valid_dates(processed_series_with_quality_columns)
    if data_quality.enforce_unique_symbol_date:
        _assert_unique_symbol_date(processed_series_with_quality_columns)
    if data_quality.enforce_non_null_ohlcv:
        _assert_non_null_ohlcv(processed_series_with_quality_columns)
    _assert_null_ratio_per_symbol(
        processed_series_with_quality_columns,
        max_null_ratio_per_symbol=data_quality.max_null_ratio_per_symbol,
    )


def build_ingestion_metadata_frame(
    processed_series: pl.DataFrame,
    *,
    parameters: YfinancePipelineParameters,
    run_date: date,
    generated_at_utc: str,
) -> pl.DataFrame:
    """Build per-symbol ingestion metadata for incremental watermarking."""
    critical_null_expr = pl.any_horizontal(
        [pl.col(column).is_null() for column in CRITICAL_NUMERIC_COLUMNS]
    )
    metadata_df = (
        processed_series.group_by("symbol")
        .agg(
            [
                pl.len().alias("row_count"),
                critical_null_expr.sum().cast(pl.Int64()).alias("null_count"),
                critical_null_expr.mean().alias("null_ratio"),
                pl.col("date").min().alias("min_observation_date"),
                pl.col("date").max().alias("max_observation_date"),
                pl.col("source_last_updated")
                .drop_nulls()
                .last()
                .alias("source_last_updated"),
                pl.col("fetch_start").drop_nulls().first().alias("observation_start"),
                pl.col("fetch_end").drop_nulls().last().alias("observation_end"),
            ]
        )
        .with_columns(
            [
                pl.col("symbol").alias("entity_id"),
                pl.col("max_observation_date").alias("watermark_observation_date"),
                pl.lit(run_date).cast(pl.Date()).alias("run_date"),
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
    """Build run-date keyed partitions with a common filename stem."""
    with_run_date = dataset.with_columns(
        pl.col("run_date").dt.strftime("%Y-%m-%d").alias("run_date_partition")
    )
    partitions: dict[str, pl.DataFrame] = {}
    run_dates = with_run_date.get_column("run_date_partition").unique().to_list()
    for run_date in run_dates:
        key = f"run_date={run_date}/{filename_stem}"
        partitions[key] = with_run_date.filter(
            pl.col("run_date_partition") == run_date
        ).drop("run_date_partition")
    return partitions


def build_symbol_year_partitions(
    dataset: pl.DataFrame,
    *,
    filename_stem: str,
) -> dict[str, pl.DataFrame]:
    """Build query-friendly partitions keyed by symbol and observation year."""
    with_partition_keys = dataset.with_columns(
        [
            pl.col("symbol").alias("symbol_partition"),
            pl.col("date").dt.year().cast(pl.Int64()).alias("year_partition"),
        ]
    )
    partition_values = with_partition_keys.select(
        ["symbol_partition", "year_partition"]
    ).unique()

    partitions: dict[str, pl.DataFrame] = {}
    for row in partition_values.iter_rows(named=True):
        symbol = str(row["symbol_partition"])
        year = int(row["year_partition"])
        key = f"symbol={symbol}/year={year}/{filename_stem}"
        partitions[key] = with_partition_keys.filter(
            (pl.col("symbol_partition") == symbol)
            & (pl.col("year_partition") == year)
        ).drop(["symbol_partition", "year_partition"])
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


def _assert_valid_dates(processed_series_with_quality_columns: pl.DataFrame) -> None:
    """Fail if any observation date cannot be parsed."""
    invalid_dates = processed_series_with_quality_columns.filter(pl.col("date").is_null())
    if invalid_dates.is_empty():
        return
    sample = invalid_dates.select(["symbol", "fetch_start", "fetch_end"]).head(
        QUALITY_SAMPLE_LIMIT
    )
    raise ValueError(
        "yfinance data quality check failed: found rows with invalid observation "
        f"dates. sample={sample.to_dicts()}"
    )


def _assert_unique_symbol_date(processed_series_with_quality_columns: pl.DataFrame) -> None:
    """Fail if duplicate `(symbol, date)` keys are present."""
    duplicates = (
        processed_series_with_quality_columns.group_by(["symbol", "date"])
        .len()
        .filter(pl.col("len") > 1)
    )
    if duplicates.is_empty():
        return
    sample = duplicates.head(QUALITY_SAMPLE_LIMIT)
    raise ValueError(
        "yfinance data quality check failed: duplicate (symbol, date) rows "
        f"detected. sample={sample.to_dicts()}"
    )


def _assert_non_null_ohlcv(processed_series_with_quality_columns: pl.DataFrame) -> None:
    """Fail if critical OHLCV fields are null."""
    critical_nulls = processed_series_with_quality_columns.filter(
        pl.any_horizontal([pl.col(column).is_null() for column in CRITICAL_NUMERIC_COLUMNS])
    )
    if critical_nulls.is_empty():
        return
    sample = critical_nulls.select(["symbol", "date", *CRITICAL_NUMERIC_COLUMNS]).head(
        QUALITY_SAMPLE_LIMIT
    )
    raise ValueError(
        "yfinance data quality check failed: null critical OHLCV fields detected. "
        f"sample={sample.to_dicts()}"
    )


def _assert_null_ratio_per_symbol(
    processed_series_with_quality_columns: pl.DataFrame,
    *,
    max_null_ratio_per_symbol: float,
) -> None:
    """Fail if per-symbol critical-null ratio exceeds configured threshold."""
    critical_null_expr = pl.any_horizontal(
        [pl.col(column).is_null() for column in CRITICAL_NUMERIC_COLUMNS]
    )
    null_ratio_by_symbol = processed_series_with_quality_columns.group_by("symbol").agg(
        [
            pl.len().alias("row_count"),
            critical_null_expr.sum().cast(pl.Int64()).alias("null_count"),
            critical_null_expr.mean().alias("null_ratio"),
        ]
    )
    threshold_violations = null_ratio_by_symbol.filter(
        pl.col("null_ratio") > max_null_ratio_per_symbol
    )
    if threshold_violations.is_empty():
        return
    sample = threshold_violations.head(QUALITY_SAMPLE_LIMIT)
    raise ValueError(
        "yfinance data quality check failed: per-symbol null ratio exceeded "
        f"threshold {max_null_ratio_per_symbol}. sample={sample.to_dicts()}"
    )
