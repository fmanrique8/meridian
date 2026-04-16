"""Nodes for macro convergence (Layer-1) weekly state generation."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any

import polars as pl

from .commons import (
    FRED_REQUIRED_SERIES,
    STATE_REQUIRED_DIAGNOSTICS,
    STATE_SCHEMA,
    apply_state_rules,
    asof_join_metrics,
    build_energy_metrics,
    build_growth_metrics,
    build_inflation_metrics,
    build_labor_metrics,
    build_liquidity_metrics,
    build_state_history_partitions,
    build_state_latest_partition,
    build_weekly_timeline,
    log_kv_event,
    materialize_partitions,
    normalize_fred_series_frame,
    now_utc_iso,
    resolve_run_date,
)
from .schemas import ConvergenceParameters

LOGGER = logging.getLogger(__name__)
BUILD_HISTORY_NODE = "build_convergence_state_history_node"
BUILD_LATEST_NODE = "build_convergence_state_latest_node"
PARTITION_HISTORY_NODE = "partition_convergence_state_history_node"
PARTITION_LATEST_NODE = "partition_convergence_state_latest_node"


def build_convergence_state_history(
    fred_series_latest: dict[str, Any],
    fred_entity_watermarks: dict[str, Any],
    convergence_parameters: dict[str, Any],
) -> pl.DataFrame:
    """
    Build weekly convergence state history from FRED serving data.

    Args:
        fred_series_latest: Partitioned FRED serving data (`series_id/year`).
        fred_entity_watermarks: Partitioned FRED ingestion metadata watermarks.
        convergence_parameters: Runtime parameters under `params:convergence`.

    Returns:
        Weekly state history at Friday `as_of_date` grain.
    """
    parameters = ConvergenceParameters.model_validate(convergence_parameters)
    started_at = perf_counter()

    series_latest_df = normalize_fred_series_frame(
        materialize_partitions(fred_series_latest)
    )
    run_date = resolve_run_date(parameters)
    watermarks_df = materialize_partitions(fred_entity_watermarks)

    if series_latest_df.is_empty():
        log_kv_event(
            level=logging.WARNING,
            event="process_complete",
            node=BUILD_HISTORY_NODE,
            run_date=run_date,
            status="empty",
            row_count=0,
            entity_count=0,
            duration_ms=0,
            dataset="macro__convergence__feature__state_history",
        )
        return pl.DataFrame(schema=STATE_SCHEMA)

    available_series = set(series_latest_df.get_column("series_id").unique().to_list())
    missing_series = sorted(FRED_REQUIRED_SERIES - available_series)
    if missing_series:
        raise ValueError(
            "Convergence requires FRED series missing from serving input: "
            f"{missing_series}"
        )

    bounded_source = series_latest_df.filter(pl.col("date") <= pl.lit(run_date))
    if bounded_source.is_empty():
        return pl.DataFrame(schema=STATE_SCHEMA)

    inflation_metrics = build_inflation_metrics(bounded_source)
    unrate_metrics, claims_metrics = build_labor_metrics(bounded_source)
    growth_metrics = build_growth_metrics(bounded_source)
    liquidity_metrics = build_liquidity_metrics(bounded_source)
    energy_metrics = build_energy_metrics(bounded_source)

    start_date = bounded_source.get_column("date").min()
    weekly_timeline = build_weekly_timeline(start_date=start_date, end_date=run_date)

    weekly_metrics = weekly_timeline
    weekly_metrics = asof_join_metrics(weekly_metrics, inflation_metrics)
    weekly_metrics = asof_join_metrics(weekly_metrics, unrate_metrics)
    weekly_metrics = asof_join_metrics(weekly_metrics, claims_metrics)
    weekly_metrics = asof_join_metrics(weekly_metrics, growth_metrics)
    weekly_metrics = asof_join_metrics(weekly_metrics, liquidity_metrics)
    weekly_metrics = asof_join_metrics(weekly_metrics, energy_metrics)
    weekly_metrics = weekly_metrics.filter(
        pl.all_horizontal(
            [pl.col(column).is_not_null() for column in STATE_REQUIRED_DIAGNOSTICS]
        )
    )

    if weekly_metrics.is_empty():
        log_kv_event(
            level=logging.WARNING,
            event="process_complete",
            node=BUILD_HISTORY_NODE,
            run_date=run_date,
            status="empty",
            row_count=0,
            entity_count=0,
            duration_ms=int((perf_counter() - started_at) * 1000),
            dataset="macro__convergence__feature__state_history",
            reason="insufficient_lookback_for_required_metrics",
        )
        return pl.DataFrame(schema=STATE_SCHEMA)

    state_history = (
        apply_state_rules(
            weekly_metrics.sort("as_of_date"),
            parameters=parameters,
            run_date=run_date,
        )
        .sort("as_of_date")
        .select(list(STATE_SCHEMA.keys()))
    )

    duration_ms = int((perf_counter() - started_at) * 1000)
    watermark_max_date = (
        watermarks_df.get_column("watermark_observation_date").max()
        if not watermarks_df.is_empty()
        and "watermark_observation_date" in watermarks_df.columns
        else None
    )
    log_kv_event(
        level=logging.INFO,
        event="process_complete",
        node=BUILD_HISTORY_NODE,
        run_date=run_date,
        status="ok",
        row_count=state_history.height,
        entity_count=1,
        duration_ms=duration_ms,
        dataset="macro__convergence__feature__state_history",
        source_series_count=len(available_series),
        source_max_observation_date=bounded_source.get_column("date").max(),
        watermark_max_observation_date=watermark_max_date,
        generated_at_utc=now_utc_iso(),
    )
    return state_history


def build_convergence_state_latest(
    convergence_state_history: pl.DataFrame,
    convergence_parameters: dict[str, Any],
) -> pl.DataFrame:
    """
    Build the latest weekly convergence state snapshot.

    Args:
        convergence_state_history: Weekly state history output.
        convergence_parameters: Runtime parameters under `params:convergence`.

    Returns:
        Latest single-row convergence state snapshot.
    """
    parameters = ConvergenceParameters.model_validate(convergence_parameters)
    run_date = parameters.run_date

    if convergence_state_history.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="process_complete",
            node=BUILD_LATEST_NODE,
            run_date=run_date,
            status="empty",
            row_count=0,
            entity_count=0,
            dataset="macro__convergence__primary__state_latest",
        )
        return pl.DataFrame(schema=STATE_SCHEMA)

    latest = (
        convergence_state_history.sort("as_of_date", descending=True)
        .head(1)
        .sort("as_of_date")
    )
    resolved_run_date = latest.get_column("run_date").max()
    log_kv_event(
        level=logging.INFO,
        event="process_complete",
        node=BUILD_LATEST_NODE,
        run_date=resolved_run_date,
        status="ok",
        row_count=latest.height,
        entity_count=1,
        dataset="macro__convergence__primary__state_latest",
        as_of_date=latest.get_column("as_of_date").max(),
    )
    return latest


def partition_convergence_state_history(
    convergence_state_history: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build run-date partitions for convergence state history output.

    Args:
        convergence_state_history: Weekly convergence state history.

    Returns:
        Mapping of partition keys to partition dataframes.
    """
    if convergence_state_history.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_HISTORY_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="macro__convergence__feature__state_history",
        )
        return {}

    started_at = perf_counter()
    run_date = convergence_state_history.get_column("run_date").max()
    partitions = build_state_history_partitions(convergence_state_history)
    duration_ms = int((perf_counter() - started_at) * 1000)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_HISTORY_NODE,
        run_date=run_date,
        status="ok",
        row_count=convergence_state_history.height,
        entity_count=1,
        duration_ms=duration_ms,
        partition_count=len(partitions),
        dataset="macro__convergence__feature__state_history",
    )
    return partitions


def partition_convergence_state_latest(
    convergence_state_latest: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """
    Build latest overwrite partition for convergence state snapshot output.

    Args:
        convergence_state_latest: Latest convergence state snapshot.

    Returns:
        Mapping of partition keys to partition dataframes.
    """
    if convergence_state_latest.is_empty():
        log_kv_event(
            level=logging.INFO,
            event="partition_write_complete",
            node=PARTITION_LATEST_NODE,
            run_date=None,
            status="empty",
            row_count=0,
            entity_count=0,
            partition_count=0,
            dataset="macro__convergence__primary__state_latest",
        )
        return {}

    run_date = convergence_state_latest.get_column("run_date").max()
    partitions = build_state_latest_partition(convergence_state_latest)
    log_kv_event(
        level=logging.INFO,
        event="partition_write_complete",
        node=PARTITION_LATEST_NODE,
        run_date=run_date,
        status="ok",
        row_count=convergence_state_latest.height,
        entity_count=1,
        partition_count=len(partitions),
        dataset="macro__convergence__primary__state_latest",
    )
    return partitions
