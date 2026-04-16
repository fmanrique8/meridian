"""Nodes for ingesting and preparing FRED macroeconomic series."""

from __future__ import annotations

import logging
from datetime import date
from time import perf_counter
from typing import Any

import polars as pl

from .client import FredClient
from .commons import (
    INGESTION_METADATA_SCHEMA,
    PROCESSED_SCHEMA,
    RAW_SCHEMA,
    assert_data_quality,
    build_entity_watermark_partitions,
    build_ingestion_metadata_frame,
    build_observation_rows,
    build_run_date_partitions,
    build_series_year_partitions,
    log_kv_event,
    now_utc_iso,
)
from .schemas import FredPipelineParameters

LOGGER = logging.getLogger(__name__)
INGEST_NODE = "ingest_fred_series_node"
PROCESS_NODE = "process_fred_series_node"
PARTITION_RAW_NODE = "partition_fred_series_node"
PARTITION_LATEST_NODE = "partition_fred_series_latest_node"
BUILD_METADATA_NODE = "build_fred_ingestion_metadata_node"
PARTITION_METADATA_NODE = "partition_fred_ingestion_metadata_node"
PARTITION_WATERMARK_NODE = "partition_fred_entity_watermarks_node"


def ingest_fred_series(fred_parameters: dict[str, Any]) -> pl.DataFrame:
    """
    Ingest configured FRED series observations into a raw long-format table.

    Args:
        fred_parameters: Runtime parameters under `params:fred`.

    Returns:
        Raw FRED observations with metadata and unparsed `value_raw`.
    """
    parameters = FredPipelineParameters.model_validate(fred_parameters)
    run_date = parameters.run_date or date.today()
    started_at = perf_counter()
    log_kv_event(
        level=logging.INFO,
        event="ingest_start",
        node=INGEST_NODE,
        run_date=run_date,
        status="started",
        entity_count=len(parameters.series_ids),
        sort_order=parameters.sort_order,
        observation_start=parameters.observation_start,
        observation_end=parameters.observation_end,
    )

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
                build_observation_rows(
                    metadata=metadata,
                    series_id=series_id,
                    observations=observations,
                )
            )
            log_kv_event(
                level=logging.INFO,
                event="series_fetch_complete",
                node=INGEST_NODE,
                run_date=run_date,
                status="ok",
                row_count=len(observations),
                entity_count=1,
                entity_id=series_id,
            )

    if not rows:
        duration_ms = int((perf_counter() - started_at) * 1000)
        log_kv_event(
            level=logging.WARNING,
            event="ingest_complete",
            node=INGEST_NODE,
            run_date=run_date,
            status="empty",
            row_count=0,
            entity_count=len(parameters.series_ids),
            duration_ms=duration_ms,
        )
        return pl.DataFrame(schema=RAW_SCHEMA)

    raw_df = pl.DataFrame(rows, schema=RAW_SCHEMA)
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="ingest_complete",
        node=INGEST_NODE,
        run_date=run_date,
        status="ok",
        row_count=raw_df.height,
        entity_count=raw_df.get_column("series_id").n_unique(),
        duration_ms=duration_ms,
    )
    return raw_df


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
    parameters = FredPipelineParameters.model_validate(fred_parameters)
    run_date = parameters.run_date or date.today()
    started_at = perf_counter()

    if fred_raw_series.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="process_complete",
            node=PROCESS_NODE,
            run_date=run_date,
            status="empty",
            row_count=0,
            entity_count=0,
            duration_ms=0,
        )
        return pl.DataFrame(schema=PROCESSED_SCHEMA)

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
    try:
        assert_data_quality(
            processed_with_quality_columns,
            sort_order=parameters.sort_order,
            data_quality=parameters.data_quality,
        )
    except Exception as exc:
        duration_ms = int((perf_counter() - started_at) * 1000)
        log_kv_event(
            level=logging.ERROR,
            event="dq_check_fail",
            node=PROCESS_NODE,
            run_date=run_date,
            status="failed",
            row_count=processed_with_quality_columns.height,
            entity_count=processed_with_quality_columns.get_column(
                "series_id"
            ).n_unique(),
            duration_ms=duration_ms,
            error=str(exc),
        )
        raise

    log_kv_event(
        level=logging.INFO,
        event="dq_check_pass",
        node=PROCESS_NODE,
        run_date=run_date,
        status="ok",
        row_count=processed_with_quality_columns.height,
        entity_count=processed_with_quality_columns.get_column("series_id").n_unique(),
    )

    processed_df = processed_with_quality_columns.drop(
        ["value_raw", "series_row_order"]
    ).sort(["series_id", "date"])
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="process_complete",
        node=PROCESS_NODE,
        run_date=run_date,
        status="ok",
        row_count=processed_df.height,
        entity_count=processed_df.get_column("series_id").n_unique(),
        duration_ms=duration_ms,
        min_observation_date=processed_df.get_column("date").min(),
        max_observation_date=processed_df.get_column("date").max(),
    )
    return processed_df


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
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_RAW_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="macro__fred__raw__series",
        )
        return {}

    started_at = perf_counter()
    run_date = fred_processed_series.get_column("run_date").min()
    partitions = build_run_date_partitions(
        fred_processed_series,
        filename_stem="fred_series",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_RAW_NODE,
        run_date=run_date,
        status="ok",
        row_count=fred_processed_series.height,
        entity_count=fred_processed_series.get_column("series_id").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="macro__fred__raw__series",
    )
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
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_LATEST_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="macro__fred__primary__series_latest",
        )
        return {}

    started_at = perf_counter()
    run_date = fred_processed_series.get_column("run_date").min()
    partitions = build_series_year_partitions(
        fred_processed_series,
        filename_stem="fred_series_latest",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_LATEST_NODE,
        run_date=run_date,
        status="ok",
        row_count=fred_processed_series.height,
        entity_count=fred_processed_series.get_column("series_id").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="macro__fred__primary__series_latest",
    )
    return partitions


def build_fred_ingestion_metadata(
    fred_processed_series: pl.DataFrame,
    fred_parameters: dict[str, Any],
) -> pl.DataFrame:
    """
    Build per-entity ingestion metadata for incremental watermarking.

    Args:
        fred_processed_series: Processed FRED observations.
        fred_parameters: Runtime parameters under `params:fred`.

    Returns:
        Per-entity metadata table with run-level and watermark fields.
    """
    parameters = FredPipelineParameters.model_validate(fred_parameters)
    run_date = parameters.run_date or date.today()
    started_at = perf_counter()

    if fred_processed_series.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="metadata_build_complete",
            node=BUILD_METADATA_NODE,
            run_date=run_date,
            status="empty",
            row_count=0,
            entity_count=0,
            duration_ms=0,
            dataset="macro__fred__raw__ingestion_metadata",
        )
        return pl.DataFrame(schema=INGESTION_METADATA_SCHEMA)

    metadata_df = build_ingestion_metadata_frame(
        fred_processed_series,
        parameters=parameters,
        run_date=run_date,
        generated_at_utc=now_utc_iso(),
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="metadata_build_complete",
        node=BUILD_METADATA_NODE,
        run_date=run_date,
        status="ok",
        row_count=metadata_df.height,
        entity_count=metadata_df.get_column("entity_id").n_unique(),
        duration_ms=duration_ms,
        dataset="macro__fred__raw__ingestion_metadata",
        sync_mode=parameters.sync_mode,
    )
    return metadata_df


def partition_fred_ingestion_metadata(
    fred_ingestion_metadata: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build run-date partition payload for ingestion metadata snapshots.

    Args:
        fred_ingestion_metadata: Per-entity ingestion metadata.

    Returns:
        Mapping of run-date partition keys to metadata dataframes.
    """
    if fred_ingestion_metadata.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_METADATA_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="macro__fred__raw__ingestion_metadata",
        )
        return {}

    started_at = perf_counter()
    run_date = fred_ingestion_metadata.get_column("run_date").min()
    partitions = build_run_date_partitions(
        fred_ingestion_metadata,
        filename_stem="fred_ingestion_metadata",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_METADATA_NODE,
        run_date=run_date,
        status="ok",
        row_count=fred_ingestion_metadata.height,
        entity_count=fred_ingestion_metadata.get_column("entity_id").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="macro__fred__raw__ingestion_metadata",
    )
    return partitions


def partition_fred_entity_watermarks(
    fred_ingestion_metadata: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build query-friendly per-entity watermark partitions for incremental reads.

    Args:
        fred_ingestion_metadata: Per-entity ingestion metadata.

    Returns:
        Mapping of per-entity partition keys to watermark metadata dataframes.
    """
    if fred_ingestion_metadata.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_WATERMARK_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="macro__fred__primary__entity_watermarks",
        )
        return {}

    started_at = perf_counter()
    run_date = fred_ingestion_metadata.get_column("run_date").min()
    partitions = build_entity_watermark_partitions(
        fred_ingestion_metadata,
        filename_stem="fred_entity_watermark",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_WATERMARK_NODE,
        run_date=run_date,
        status="ok",
        row_count=fred_ingestion_metadata.height,
        entity_count=fred_ingestion_metadata.get_column("entity_id").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="macro__fred__primary__entity_watermarks",
    )
    return partitions
