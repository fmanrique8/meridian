"""Nodes for ingesting and preparing yfinance ETF series."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any

import polars as pl

from .client import YfinanceClient
from .commons import (
    INGESTION_METADATA_SCHEMA,
    PROCESSED_SCHEMA,
    RAW_SCHEMA,
    assert_data_quality,
    build_entity_watermark_partitions,
    build_ingestion_metadata_frame,
    build_run_date_partitions,
    build_symbol_year_partitions,
    extract_symbol_watermarks,
    infer_fetch_start_date,
    log_kv_event,
    materialize_partitions,
    merge_serving_history,
    normalize_processed_frame,
    now_utc_iso,
    resolve_run_date,
)
from .schemas import YfinancePipelineParameters

LOGGER = logging.getLogger(__name__)
INGEST_NODE = "ingest_yfinance_etf_prices_node"
PROCESS_NODE = "process_yfinance_etf_prices_node"
PARTITION_RAW_NODE = "partition_yfinance_etf_prices_node"
PARTITION_LATEST_NODE = "partition_yfinance_etf_prices_latest_node"
BUILD_METADATA_NODE = "build_yfinance_ingestion_metadata_node"
PARTITION_METADATA_NODE = "partition_yfinance_ingestion_metadata_node"
PARTITION_WATERMARK_NODE = "partition_yfinance_entity_watermarks_node"


def ingest_yfinance_etf_prices(
    yfinance_parameters: dict[str, Any],
    existing_entity_watermarks: dict[str, Any],
) -> pl.DataFrame:
    """
    Ingest configured ETF OHLCV observations from yfinance.

    Args:
        yfinance_parameters: Runtime parameters under `params:yfinance`.
        existing_entity_watermarks: Existing symbol watermark partitions.

    Returns:
        Raw yfinance observations with source metadata.
    """
    parameters = YfinancePipelineParameters.model_validate(yfinance_parameters)
    run_date = resolve_run_date(parameters)
    started_at = perf_counter()

    watermark_df = materialize_partitions(existing_entity_watermarks)
    symbol_watermarks = extract_symbol_watermarks(watermark_df)
    log_kv_event(
        level=logging.INFO,
        event="ingest_start",
        node=INGEST_NODE,
        run_date=run_date,
        status="started",
        row_count=0,
        entity_count=len(parameters.symbols),
        sync_mode=parameters.sync_mode,
        overlap_days=parameters.overlap_days,
        history_years=parameters.history_years,
    )

    ingested_at_utc = now_utc_iso()
    rows: list[dict[str, Any]] = []
    with YfinanceClient.from_parameters(parameters) as client:
        for symbol in parameters.symbols:
            fetch_start = infer_fetch_start_date(
                symbol=symbol,
                run_date=run_date,
                parameters=parameters,
                watermarks=symbol_watermarks,
            )
            history = client.get_history(
                symbol=symbol,
                start_date=fetch_start,
                end_date=run_date,
                interval=parameters.interval,
            )
            for history_row in history.rows:
                rows.append(
                    {
                        "symbol": symbol,
                        "date": history_row["date"],
                        "open": history_row["open"],
                        "high": history_row["high"],
                        "low": history_row["low"],
                        "close": history_row["close"],
                        "adj_close": history_row["adj_close"],
                        "volume": history_row["volume"],
                        "ingested_at_utc": ingested_at_utc,
                        "source": parameters.source,
                        "source_last_updated": history.source_last_updated,
                        "sync_mode": parameters.sync_mode,
                        "fetch_start": fetch_start.isoformat(),
                        "fetch_end": run_date.isoformat(),
                        "run_date": run_date,
                    }
                )

            log_kv_event(
                level=logging.INFO,
                event="series_fetch_complete",
                node=INGEST_NODE,
                run_date=run_date,
                status="ok",
                row_count=len(history.rows),
                entity_count=1,
                entity_id=symbol,
                fetch_start=fetch_start,
                fetch_end=run_date,
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
            entity_count=len(parameters.symbols),
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
        entity_count=raw_df.get_column("symbol").n_unique(),
        duration_ms=duration_ms,
    )
    return raw_df


def process_yfinance_etf_prices(
    yfinance_raw_etf_prices: pl.DataFrame,
    yfinance_parameters: dict[str, Any],
) -> pl.DataFrame:
    """
    Parse and standardize yfinance raw observations for downstream storage.

    Args:
        yfinance_raw_etf_prices: Raw yfinance observations from ingest node.
        yfinance_parameters: Runtime parameters under `params:yfinance`.

    Returns:
        Processed table with typed dates/numerics and run-level metadata.
    """
    parameters = YfinancePipelineParameters.model_validate(yfinance_parameters)
    run_date = resolve_run_date(parameters)
    started_at = perf_counter()

    if yfinance_raw_etf_prices.is_empty():
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

    processed_with_quality_columns = normalize_processed_frame(
        yfinance_raw_etf_prices.with_row_index("symbol_row_order")
    )
    try:
        assert_data_quality(
            processed_with_quality_columns,
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
            entity_count=processed_with_quality_columns.get_column("symbol").n_unique(),
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
        entity_count=processed_with_quality_columns.get_column("symbol").n_unique(),
    )

    processed_df = processed_with_quality_columns.sort(["symbol", "date"]).select(
        list(PROCESSED_SCHEMA.keys())
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="process_complete",
        node=PROCESS_NODE,
        run_date=run_date,
        status="ok",
        row_count=processed_df.height,
        entity_count=processed_df.get_column("symbol").n_unique(),
        duration_ms=duration_ms,
        min_observation_date=processed_df.get_column("date").min(),
        max_observation_date=processed_df.get_column("date").max(),
    )
    return processed_df


def partition_yfinance_etf_prices(
    yfinance_raw_etf_prices: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build Kedro partition payload keyed by `run_date=YYYY-MM-DD`.

    Args:
        yfinance_raw_etf_prices: Raw yfinance table.

    Returns:
        Mapping of partition keys to partition dataframes.
    """
    if yfinance_raw_etf_prices.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_RAW_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="market__yfinance__raw__etf_prices",
        )
        return {}

    started_at = perf_counter()
    normalized_raw = yfinance_raw_etf_prices.with_columns(
        pl.col("run_date")
        .cast(pl.String())
        .str.strptime(pl.Date, strict=False)
        .alias("run_date")
    )
    run_date = normalized_raw.get_column("run_date").min()
    partitions = build_run_date_partitions(
        normalized_raw,
        filename_stem="yfinance_etf_prices",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_RAW_NODE,
        run_date=run_date,
        status="ok",
        row_count=normalized_raw.height,
        entity_count=normalized_raw.get_column("symbol").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="market__yfinance__raw__etf_prices",
    )
    return partitions


def partition_yfinance_etf_prices_latest(
    yfinance_processed_etf_prices: pl.DataFrame,
    existing_serving_partitions: dict[str, Any],
) -> dict[str, pl.DataFrame]:
    """
    Build serving partitions keyed by `symbol` and observation `year`.

    Args:
        yfinance_processed_etf_prices: Processed yfinance table.
        existing_serving_partitions: Existing serving partitions for merge-on-write.

    Returns:
        Mapping of partition keys to partition dataframes.
    """
    if yfinance_processed_etf_prices.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_LATEST_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="market__yfinance__primary__etf_prices_latest",
        )
        return {}

    started_at = perf_counter()
    existing_df = materialize_partitions(existing_serving_partitions)
    merged_df = merge_serving_history(yfinance_processed_etf_prices, existing_df)
    run_date = yfinance_processed_etf_prices.get_column("run_date").min()
    partitions = build_symbol_year_partitions(
        merged_df,
        filename_stem="yfinance_etf_prices_latest",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_LATEST_NODE,
        run_date=run_date,
        status="ok",
        row_count=merged_df.height,
        entity_count=merged_df.get_column("symbol").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        incoming_row_count=yfinance_processed_etf_prices.height,
        dataset="market__yfinance__primary__etf_prices_latest",
    )
    return partitions


def build_yfinance_ingestion_metadata(
    yfinance_processed_etf_prices: pl.DataFrame,
    yfinance_parameters: dict[str, Any],
) -> pl.DataFrame:
    """
    Build per-symbol ingestion metadata for incremental watermarking.

    Args:
        yfinance_processed_etf_prices: Processed yfinance observations.
        yfinance_parameters: Runtime parameters under `params:yfinance`.

    Returns:
        Per-symbol metadata table with run-level and watermark fields.
    """
    parameters = YfinancePipelineParameters.model_validate(yfinance_parameters)
    run_date = resolve_run_date(parameters)
    started_at = perf_counter()

    if yfinance_processed_etf_prices.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="metadata_build_complete",
            node=BUILD_METADATA_NODE,
            run_date=run_date,
            status="empty",
            row_count=0,
            entity_count=0,
            duration_ms=0,
            dataset="market__yfinance__raw__ingestion_metadata",
        )
        return pl.DataFrame(schema=INGESTION_METADATA_SCHEMA)

    metadata_df = build_ingestion_metadata_frame(
        yfinance_processed_etf_prices,
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
        dataset="market__yfinance__raw__ingestion_metadata",
        sync_mode=parameters.sync_mode,
    )
    return metadata_df


def partition_yfinance_ingestion_metadata(
    yfinance_ingestion_metadata: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build run-date partition payload for ingestion metadata snapshots.

    Args:
        yfinance_ingestion_metadata: Per-symbol ingestion metadata.

    Returns:
        Mapping of run-date partition keys to metadata dataframes.
    """
    if yfinance_ingestion_metadata.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_METADATA_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="market__yfinance__raw__ingestion_metadata",
        )
        return {}

    started_at = perf_counter()
    run_date = yfinance_ingestion_metadata.get_column("run_date").min()
    partitions = build_run_date_partitions(
        yfinance_ingestion_metadata,
        filename_stem="yfinance_ingestion_metadata",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_METADATA_NODE,
        run_date=run_date,
        status="ok",
        row_count=yfinance_ingestion_metadata.height,
        entity_count=yfinance_ingestion_metadata.get_column("entity_id").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="market__yfinance__raw__ingestion_metadata",
    )
    return partitions


def partition_yfinance_entity_watermarks(
    yfinance_ingestion_metadata: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build query-friendly per-symbol watermark partitions for incremental reads.

    Args:
        yfinance_ingestion_metadata: Per-symbol ingestion metadata.

    Returns:
        Mapping of per-symbol partition keys to watermark metadata dataframes.
    """
    if yfinance_ingestion_metadata.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_WATERMARK_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="market__yfinance__primary__entity_watermarks",
        )
        return {}

    started_at = perf_counter()
    run_date = yfinance_ingestion_metadata.get_column("run_date").min()
    partitions = build_entity_watermark_partitions(
        yfinance_ingestion_metadata,
        filename_stem="yfinance_entity_watermark",
    )
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_WATERMARK_NODE,
        run_date=run_date,
        status="ok",
        row_count=yfinance_ingestion_metadata.height,
        entity_count=yfinance_ingestion_metadata.get_column("entity_id").n_unique(),
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="market__yfinance__primary__entity_watermarks",
    )
    return partitions

