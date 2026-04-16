"""Shared helpers and contracts for the macro convergence pipeline."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

import polars as pl

from .schemas import ConvergenceParameters

LOGGER = logging.getLogger(__name__)

PIPELINE_NAME = "macro.convergence"
SOURCE_NAME = "fred"

SchemaMap = dict[str, pl.DataType]
PartitionFrame = pl.DataFrame | Callable[[], pl.DataFrame]
PartitionInput = Mapping[str, PartitionFrame]

STATE_SCHEMA: SchemaMap = {
    "as_of_date": pl.Date(),
    "inflation_state": pl.String(),
    "labor_state": pl.String(),
    "growth_state": pl.String(),
    "liquidity_state": pl.String(),
    "macro_regime": pl.String(),
    "cpi_yoy": pl.Float64(),
    "cpi_3m_annualized": pl.Float64(),
    "unrate_3m_change": pl.Float64(),
    "claims_4w_avg": pl.Float64(),
    "claims_trend": pl.Float64(),
    "payrolls_3m": pl.Float64(),
    "payrolls_6m": pl.Float64(),
    "yield_curve": pl.Float64(),
    "curve_trend": pl.Float64(),
    "rates_trend": pl.Float64(),
    "source_set_version": pl.String(),
    "run_date": pl.Date(),
}

FRED_REQUIRED_SERIES = {
    "CPIAUCSL",
    "UNRATE",
    "PAYEMS",
    "ICSA",
    "DGS10",
    "DGS2",
}

STATE_REQUIRED_DIAGNOSTICS = [
    "cpi_yoy",
    "cpi_3m_annualized",
    "unrate_3m_change",
    "claims_4w_avg",
    "claims_trend",
    "payrolls_3m",
    "payrolls_6m",
    "yield_curve",
    "curve_trend",
    "rates_trend",
]


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


def normalize_fred_series_frame(fred_series_latest: pl.DataFrame) -> pl.DataFrame:
    """Normalize the FRED serving frame for convergence metric generation."""
    if fred_series_latest.is_empty():
        return pl.DataFrame(
            schema={
                "series_id": pl.String(),
                "date": pl.Date(),
                "value": pl.Float64(),
                "run_date": pl.Date(),
            }
        )

    normalized = fred_series_latest.with_columns(
        [
            _cast_to_date("date"),
            _cast_to_float("value"),
            _cast_to_date("run_date"),
            pl.col("series_id").cast(pl.String()),
        ]
    ).filter(pl.col("date").is_not_null() & pl.col("value").is_not_null())

    deduplicated = (
        normalized.sort(["series_id", "date", "run_date"])
        .group_by(["series_id", "date"], maintain_order=True)
        .agg(
            [
                pl.col("value").drop_nulls().last().alias("value"),
                pl.col("run_date").drop_nulls().last().alias("run_date"),
            ]
        )
        .sort(["series_id", "date"])
    )
    return deduplicated


def resolve_run_date(
    parameters: ConvergenceParameters, source_data: pl.DataFrame
) -> date:
    """Resolve convergence run date from params or current execution date."""
    _ = source_data
    if parameters.run_date is not None:
        return parameters.run_date
    return date.today()


def build_weekly_timeline(*, start_date: date, end_date: date) -> pl.DataFrame:
    """Build Friday-aligned weekly as-of dates between start and end."""
    if start_date > end_date:
        return pl.DataFrame(schema={"as_of_date": pl.Date()})
    days_to_friday = (4 - start_date.weekday()) % 7
    current = start_date + timedelta(days=days_to_friday)
    as_of_dates: list[date] = []
    while current <= end_date:
        as_of_dates.append(current)
        current += timedelta(days=7)
    return pl.DataFrame({"as_of_date": as_of_dates}, schema={"as_of_date": pl.Date()})


def asof_join_metrics(
    timeline: pl.DataFrame,
    metric_frame: pl.DataFrame,
    *,
    metric_date_column: str = "date",
) -> pl.DataFrame:
    """Backward as-of join metric values to weekly `as_of_date` timeline."""
    if metric_frame.is_empty():
        return timeline
    joined = timeline.sort("as_of_date").join_asof(
        metric_frame.sort(metric_date_column),
        left_on="as_of_date",
        right_on=metric_date_column,
        strategy="backward",
    )
    return joined.drop(metric_date_column)


def build_inflation_metrics(fred_series_latest: pl.DataFrame) -> pl.DataFrame:
    """Compute CPI year-over-year and 3-month annualized inflation metrics."""
    cpi = _series_frame(fred_series_latest, "CPIAUCSL")
    if cpi.is_empty():
        return pl.DataFrame(schema={"date": pl.Date(), "cpi_yoy": pl.Float64()})
    return cpi.with_columns(
        [
            ((pl.col("value") / pl.col("value").shift(12)) - 1.0)
            .mul(100.0)
            .alias("cpi_yoy"),
            ((pl.col("value") / pl.col("value").shift(3)) - 1.0)
            .mul(400.0)
            .alias("cpi_3m_annualized"),
        ]
    ).select(["date", "cpi_yoy", "cpi_3m_annualized"])


def build_labor_metrics(
    fred_series_latest: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Compute labor metrics on source-native monthly and weekly cadence."""
    unrate = _series_frame(fred_series_latest, "UNRATE")
    claims = _series_frame(fred_series_latest, "ICSA")

    if unrate.is_empty():
        unrate_metrics = pl.DataFrame(
            schema={"date": pl.Date(), "unrate_3m_change": pl.Float64()}
        )
    else:
        unrate_metrics = unrate.with_columns(
            (pl.col("value") - pl.col("value").shift(3)).alias("unrate_3m_change")
        ).select(["date", "unrate_3m_change"])

    if claims.is_empty():
        claims_metrics = pl.DataFrame(
            schema={
                "date": pl.Date(),
                "claims_4w_avg": pl.Float64(),
                "claims_trend": pl.Float64(),
            }
        )
    else:
        claims_metrics = (
            claims.with_columns(
                pl.col("value")
                .rolling_mean(window_size=4, min_samples=4)
                .alias("claims_4w_avg")
            )
            .with_columns(
                (pl.col("claims_4w_avg") - pl.col("claims_4w_avg").shift(4)).alias(
                    "claims_trend"
                )
            )
            .select(["date", "claims_4w_avg", "claims_trend"])
        )

    return unrate_metrics, claims_metrics


def build_growth_metrics(fred_series_latest: pl.DataFrame) -> pl.DataFrame:
    """Compute payroll-based growth momentum metrics."""
    payrolls = _series_frame(fred_series_latest, "PAYEMS")
    if payrolls.is_empty():
        return pl.DataFrame(
            schema={
                "date": pl.Date(),
                "payrolls_3m": pl.Float64(),
                "payrolls_6m": pl.Float64(),
            }
        )
    return payrolls.with_columns(
        [
            ((pl.col("value") / pl.col("value").shift(3)) - 1.0)
            .mul(100.0)
            .alias("payrolls_3m"),
            ((pl.col("value") / pl.col("value").shift(6)) - 1.0)
            .mul(100.0)
            .alias("payrolls_6m"),
        ]
    ).select(["date", "payrolls_3m", "payrolls_6m"])


def build_liquidity_metrics(fred_series_latest: pl.DataFrame) -> pl.DataFrame:
    """Compute yield-curve and rates-trend liquidity metrics."""
    dgs10 = _series_frame(fred_series_latest, "DGS10").rename({"value": "dgs10"})
    dgs2 = _series_frame(fred_series_latest, "DGS2").rename({"value": "dgs2"})
    if dgs10.is_empty() or dgs2.is_empty():
        return pl.DataFrame(
            schema={
                "date": pl.Date(),
                "yield_curve": pl.Float64(),
                "curve_trend": pl.Float64(),
                "rates_trend": pl.Float64(),
            }
        )

    curve = (
        dgs10.join(dgs2, on="date", how="inner")
        .with_columns((pl.col("dgs10") - pl.col("dgs2")).alias("yield_curve"))
        .sort("date")
    )
    lag_reference = curve.select(
        [
            pl.col("date").alias("trend_lookup_date"),
            pl.col("yield_curve").alias("yield_curve_lag"),
            pl.col("dgs10").alias("dgs10_lag"),
        ]
    ).sort("trend_lookup_date")

    return (
        curve.with_columns(
            pl.col("date").dt.offset_by("-3mo").alias("trend_lookup_date")
        )
        .join_asof(
            lag_reference,
            on="trend_lookup_date",
            strategy="backward",
        )
        .with_columns(
            [
                (pl.col("yield_curve") - pl.col("yield_curve_lag")).alias(
                    "curve_trend"
                ),
                (pl.col("dgs10") - pl.col("dgs10_lag")).alias("rates_trend"),
            ]
        )
        .select(["date", "yield_curve", "curve_trend", "rates_trend"])
    )


def apply_state_rules(
    weekly_metrics: pl.DataFrame,
    *,
    parameters: ConvergenceParameters,
    run_date: date,
) -> pl.DataFrame:
    """Apply v1 signal and regime rules with parameterized thresholds."""
    thresholds = parameters.thresholds
    with_states = weekly_metrics.with_columns(
        [
            pl.when(pl.col("cpi_3m_annualized") < pl.col("cpi_yoy"))
            .then(pl.lit("cooling"))
            .when(
                (pl.col("cpi_3m_annualized") - pl.col("cpi_yoy"))
                .abs()
                .lt(thresholds.inflation_sticky_band)
            )
            .then(pl.lit("sticky"))
            .otherwise(pl.lit("reaccelerating"))
            .alias("inflation_state"),
            pl.when(
                (pl.col("unrate_3m_change") < thresholds.labor_unrate_tight_threshold)
                & (pl.col("claims_trend") <= thresholds.labor_claims_tight_threshold)
            )
            .then(pl.lit("tight"))
            .when(
                (
                    pl.col("unrate_3m_change")
                    > thresholds.labor_unrate_deteriorating_threshold
                )
                & (
                    pl.col("claims_trend")
                    > thresholds.labor_claims_deteriorating_threshold
                )
            )
            .then(pl.lit("deteriorating"))
            .otherwise(pl.lit("softening"))
            .alias("labor_state"),
            pl.when(
                pl.col("payrolls_3m")
                > (
                    pl.col("payrolls_6m")
                    + thresholds.growth_expansion_relative_threshold
                )
            )
            .then(pl.lit("expansion"))
            .when(pl.col("payrolls_3m") < thresholds.growth_contraction_threshold)
            .then(pl.lit("contraction"))
            .otherwise(pl.lit("slowdown"))
            .alias("growth_state"),
            pl.when(
                (
                    pl.col("yield_curve")
                    < thresholds.liquidity_curve_tightening_threshold
                )
                & (
                    pl.col("rates_trend")
                    > thresholds.liquidity_rates_tightening_threshold
                )
            )
            .then(pl.lit("tightening"))
            .when(
                (pl.col("yield_curve") > thresholds.liquidity_curve_easing_threshold)
                & (pl.col("rates_trend") < thresholds.liquidity_rates_easing_threshold)
            )
            .then(pl.lit("easing"))
            .otherwise(pl.lit("neutral"))
            .alias("liquidity_state"),
        ]
    )
    with_regime = with_states.with_columns(
        pl.when(
            (pl.col("inflation_state") == "cooling")
            & (pl.col("growth_state") == "slowdown")
        )
        .then(pl.lit("soft_landing"))
        .when(
            pl.col("inflation_state").is_in(["sticky", "reaccelerating"])
            & (pl.col("growth_state") == "slowdown")
        )
        .then(pl.lit("stagflation_risk"))
        .when(
            (pl.col("growth_state") == "contraction")
            & (pl.col("labor_state") == "deteriorating")
        )
        .then(pl.lit("recession"))
        .when(
            (pl.col("growth_state") == "expansion")
            & (pl.col("inflation_state") == "reaccelerating")
        )
        .then(pl.lit("reacceleration"))
        .otherwise(pl.lit("mixed"))
        .alias("macro_regime")
    )
    return with_regime.with_columns(
        [
            pl.lit(parameters.source_set_version)
            .cast(pl.String())
            .alias("source_set_version"),
            pl.lit(run_date).cast(pl.Date()).alias("run_date"),
        ]
    ).select(list(STATE_SCHEMA.keys()))


def build_state_history_partitions(
    convergence_state_history: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Build run-date partitions for convergence state history output."""
    with_run_date = convergence_state_history.with_columns(
        pl.col("run_date").dt.strftime("%Y-%m-%d").alias("run_date_partition")
    )
    partitions: dict[str, pl.DataFrame] = {}
    for run_date_value in (
        with_run_date.get_column("run_date_partition").unique().to_list()
    ):
        key = f"run_date={run_date_value}/convergence_state_history"
        partitions[key] = with_run_date.filter(
            pl.col("run_date_partition") == run_date_value
        ).drop("run_date_partition")
    return partitions


def build_state_latest_partition(
    convergence_state_latest: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Build a single overwrite partition for current latest state snapshot."""
    return {"state_latest/convergence_state_latest": convergence_state_latest}


def _series_frame(dataset: pl.DataFrame, series_id: str) -> pl.DataFrame:
    """Return sorted [date, value] frame for one FRED series."""
    return (
        dataset.filter(pl.col("series_id") == series_id)
        .select(["date", "value"])
        .sort("date")
    )


def _cast_to_date(column: str) -> pl.Expr:
    """Cast string/date-like column to Date with tolerant parsing."""
    return (
        pl.col(column).cast(pl.String).str.strptime(pl.Date, strict=False).alias(column)
    )


def _cast_to_float(column: str) -> pl.Expr:
    """Cast numeric/string-like column to Float64 with tolerant parsing."""
    return (
        pl.when(pl.col(column).is_null())
        .then(None)
        .otherwise(pl.col(column).cast(pl.Float64, strict=False))
        .alias(column)
    )


def _format_log_value(value: Any) -> str:
    """Format key-value log field values consistently."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
