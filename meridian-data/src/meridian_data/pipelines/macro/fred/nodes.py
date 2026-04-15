"""Nodes for ingesting and preparing FRED macroeconomic series."""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from .client import FredClient
from .schemas import FredPipelineParameters, FredSeriesMetadata

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

    return (
        fred_raw_series.with_columns(
            [
                pl.col("date").str.strptime(pl.Date, strict=False),
                pl.when(pl.col("value_raw") == ".")
                .then(None)
                .otherwise(pl.col("value_raw").cast(pl.Float64, strict=False))
                .alias("value"),
                pl.lit(run_date).cast(pl.Date).alias("run_date"),
            ]
        )
        .drop("value_raw")
        .sort(["series_id", "date"])
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
