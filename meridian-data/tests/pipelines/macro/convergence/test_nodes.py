from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import polars as pl
import pytest

from meridian_data.pipelines.macro.convergence.commons import (
    STATE_SCHEMA,
    apply_state_rules,
)
from meridian_data.pipelines.macro.convergence.nodes import (
    build_convergence_state_history,
    build_convergence_state_latest,
    partition_convergence_regime_history_json,
    partition_convergence_regime_latest_json,
    partition_convergence_state_history,
    partition_convergence_state_latest,
)
from meridian_data.pipelines.macro.convergence.schemas import ConvergenceParameters

MONTHS_IN_YEAR = 12
FRIDAY_WEEKDAY = 4
REGIME_HISTORY_WINDOW_WEEKS = 104
REGIME_JSON_KEYS = [
    "date",
    "inflation_state",
    "labor_state",
    "growth_state",
    "liquidity_state",
    "macro_regime",
    "bias",
    "generated_at_utc",
]
ALLOWED_BIAS = {"bullish", "defensive", "neutral"}
SHORT_HISTORY_ROWS = 8


def _monthly_dates(start_year: int, start_month: int, count: int) -> list[date]:
    dates: list[date] = []
    year = start_year
    month = start_month
    for _ in range(count):
        dates.append(date(year, month, 1))
        month += 1
        if month > MONTHS_IN_YEAR:
            month = 1
            year += 1
    return dates


def _weekly_dates(start_date: date, count: int) -> list[date]:
    return [start_date + timedelta(days=7 * idx) for idx in range(count)]


def _convergence_parameters(run_date: date) -> dict[str, Any]:
    return {
        "run_date": run_date.isoformat(),
        "time_grain": "weekly",
        "week_anchor": "friday",
        "source_set_version": "fred_v1",
        "thresholds": {
            "inflation_sticky_band": 0.3,
            "labor_unrate_tight_threshold": 0.0,
            "labor_claims_tight_threshold": 0.0,
            "labor_unrate_deteriorating_threshold": 0.0,
            "labor_claims_deteriorating_threshold": 0.0,
            "growth_expansion_relative_threshold": 0.0,
            "growth_contraction_threshold": 0.0,
            "liquidity_curve_tightening_threshold": 0.0,
            "liquidity_rates_tightening_threshold": 0.0,
            "liquidity_curve_easing_threshold": 0.0,
            "liquidity_rates_easing_threshold": 0.0,
        },
    }


def _build_partitioned_fred_series(run_date: date) -> dict[str, pl.DataFrame]:
    monthly = _monthly_dates(2024, 1, 30)
    weekly = _weekly_dates(date(2025, 1, 3), 65)

    cpi_values = [280.0 + 0.5 * idx for idx in range(len(monthly))]
    cpi_values[-4] = cpi_values[-5] + 0.10
    cpi_values[-3] = cpi_values[-4] + 0.10
    cpi_values[-2] = cpi_values[-3] + 0.10
    cpi_values[-1] = cpi_values[-2] + 0.10

    unrate_values = [4.0 + 0.01 * idx for idx in range(len(monthly))]

    payroll_values = [150_000.0 + 120.0 * idx for idx in range(24)] + [
        152_700.0,
        152_500.0,
        152_100.0,
        151_800.0,
        151_400.0,
        151_000.0,
    ]

    claims_values = [200_000.0 + 650.0 * idx for idx in range(len(weekly))]
    dgs10_values = [3.00 + 0.02 * idx for idx in range(len(weekly))]
    dgs2_values = [4.00 + 0.01 * idx for idx in range(len(weekly))]
    wti_values = [70.0 + 0.60 * idx for idx in range(len(monthly))]
    gasoline_values = [3.10 + 0.01 * idx for idx in range(len(monthly))]
    natgas_values = [2.20 + 0.02 * idx for idx in range(len(monthly))]

    rows: list[dict[str, Any]] = []
    rows.extend(_rows_for_series("CPIAUCSL", monthly, cpi_values, run_date, "Monthly"))
    rows.extend(_rows_for_series("UNRATE", monthly, unrate_values, run_date, "Monthly"))
    rows.extend(
        _rows_for_series("PAYEMS", monthly, payroll_values, run_date, "Monthly")
    )
    rows.extend(_rows_for_series("ICSA", weekly, claims_values, run_date, "Weekly"))
    rows.extend(_rows_for_series("DGS10", weekly, dgs10_values, run_date, "Daily"))
    rows.extend(_rows_for_series("DGS2", weekly, dgs2_values, run_date, "Daily"))
    rows.extend(_rows_for_series("DCOILWTICO", monthly, wti_values, run_date, "Monthly"))
    rows.extend(
        _rows_for_series("GASREGW", monthly, gasoline_values, run_date, "Monthly")
    )
    rows.extend(_rows_for_series("DHHNGSP", monthly, natgas_values, run_date, "Monthly"))

    fred_series = pl.DataFrame(
        rows,
        schema={
            "series_id": pl.String(),
            "series_name": pl.String(),
            "frequency": pl.String(),
            "units": pl.String(),
            "seasonal_adjustment": pl.String(),
            "last_updated": pl.String(),
            "date": pl.Date(),
            "value": pl.Float64(),
            "run_date": pl.Date(),
        },
    )
    return {"series_id=synthetic/year=2026/fred_series_latest": fred_series}


def _build_partitioned_watermarks(run_date: date) -> dict[str, pl.DataFrame]:
    watermark_rows = pl.DataFrame(
        {
            "entity_id": [
                "CPIAUCSL",
                "UNRATE",
                "PAYEMS",
                "ICSA",
                "DGS10",
                "DGS2",
                "DCOILWTICO",
                "GASREGW",
                "DHHNGSP",
            ],
            "series_id": [
                "CPIAUCSL",
                "UNRATE",
                "PAYEMS",
                "ICSA",
                "DGS10",
                "DGS2",
                "DCOILWTICO",
                "GASREGW",
                "DHHNGSP",
            ],
            "watermark_observation_date": [run_date] * 9,
            "run_date": [run_date] * 9,
        }
    )
    return {"entity_id=synthetic/fred_entity_watermark": watermark_rows}


def _rows_for_series(
    series_id: str,
    dates: list[date],
    values: list[float],
    run_date: date,
    frequency: str,
) -> list[dict[str, Any]]:
    return [
        {
            "series_id": series_id,
            "series_name": f"{series_id} title",
            "frequency": frequency,
            "units": "Index",
            "seasonal_adjustment": "Seasonally Adjusted",
            "last_updated": "2026-04-16 00:00:00-05",
            "date": observation_date,
            "value": value,
            "run_date": run_date,
        }
        for observation_date, value in zip(dates, values, strict=True)
    ]


def test_build_state_history_and_latest_from_fred_partitions() -> None:
    run_date = date(2026, 3, 27)
    history = build_convergence_state_history(
        _build_partitioned_fred_series(run_date),
        _build_partitioned_watermarks(run_date),
        _convergence_parameters(run_date),
    )

    assert history.height > 0
    assert history.schema == STATE_SCHEMA
    assert all(
        observation_date.weekday() == FRIDAY_WEEKDAY
        for observation_date in history["as_of_date"]
    )
    assert history["run_date"].unique().to_list() == [run_date]
    assert history["energy_trend"].null_count() == 0

    latest = build_convergence_state_latest(history, _convergence_parameters(run_date))

    assert latest.height == 1
    assert latest["inflation_state"].item() == "cooling"
    assert latest["labor_state"].item() == "deteriorating"
    assert latest["growth_state"].item() == "contraction"
    assert latest["liquidity_state"].item() == "tightening"
    assert latest["macro_regime"].item() == "recession"
    assert latest["as_of_date"].item() <= run_date
    assert latest["energy_trend"].item() is not None

    history_partitions = partition_convergence_state_history(history)
    assert list(history_partitions.keys()) == [
        "run_date=2026-03-27/convergence_state_history"
    ]
    latest_partitions = partition_convergence_state_latest(latest)
    assert list(latest_partitions.keys()) == ["state_latest/convergence_state_latest"]


def test_apply_state_rules_uses_notion_condition_order_for_inflation() -> None:
    weekly_metrics = pl.DataFrame(
        {
            "as_of_date": [date(2026, 4, 3)],
            "cpi_yoy": [2.00],
            "cpi_3m_annualized": [1.80],
            "unrate_3m_change": [0.05],
            "claims_4w_avg": [220000.0],
            "claims_trend": [1000.0],
            "payrolls_3m": [0.10],
            "payrolls_6m": [0.20],
            "yield_curve": [0.25],
            "curve_trend": [0.10],
            "rates_trend": [-0.15],
            "energy_trend": [5.00],
        }
    )
    parameters = ConvergenceParameters()

    state_df = apply_state_rules(
        weekly_metrics,
        parameters=parameters,
        run_date=date(2026, 4, 3),
    )

    # Difference is inside sticky band but rule order applies "cooling" first.
    assert state_df["inflation_state"].item() == "cooling"
    assert state_df["macro_regime"].item() == "soft_landing"


def test_build_state_history_fails_when_required_series_missing() -> None:
    run_date = date(2026, 3, 27)
    fred_partitions = _build_partitioned_fred_series(run_date)
    fred_df = next(iter(fred_partitions.values())).filter(
        pl.col("series_id") != "DHHNGSP"
    )
    missing_series_partitions = {
        "series_id=synthetic/year=2026/fred_series_latest": fred_df
    }

    with pytest.raises(ValueError, match="DHHNGSP"):
        build_convergence_state_history(
            missing_series_partitions,
            _build_partitioned_watermarks(run_date),
            _convergence_parameters(run_date),
        )


def test_build_state_history_returns_empty_when_wti_history_too_short() -> None:
    run_date = date(2026, 3, 27)
    fred_partitions = _build_partitioned_fred_series(run_date)
    fred_df = next(iter(fred_partitions.values()))
    short_wti = fred_df.filter(pl.col("series_id") == "DCOILWTICO").sort("date").head(2)
    short_history_df = pl.concat(
        [fred_df.filter(pl.col("series_id") != "DCOILWTICO"), short_wti],
        how="vertical_relaxed",
    )
    short_history_partitions = {
        "series_id=synthetic/year=2026/fred_series_latest": short_history_df
    }

    history = build_convergence_state_history(
        short_history_partitions,
        _build_partitioned_watermarks(run_date),
        _convergence_parameters(run_date),
    )

    assert history.is_empty()
    assert history.schema == STATE_SCHEMA


def test_build_state_history_handles_null_wti_values() -> None:
    run_date = date(2026, 3, 27)
    fred_partitions = _build_partitioned_fred_series(run_date)
    fred_df = next(iter(fred_partitions.values()))
    with_null_wti = fred_df.with_columns(
        pl.when(
            (pl.col("series_id") == "DCOILWTICO")
            & (pl.col("date").dt.month().is_in([2, 6, 10]))
        )
        .then(None)
        .otherwise(pl.col("value"))
        .alias("value")
    )
    null_wti_partitions = {
        "series_id=synthetic/year=2026/fred_series_latest": with_null_wti
    }

    history = build_convergence_state_history(
        null_wti_partitions,
        _build_partitioned_watermarks(run_date),
        _convergence_parameters(run_date),
    )

    assert history.height > 0
    assert history["energy_trend"].null_count() == 0


def test_partition_regime_latest_json_emits_strict_136_schema() -> None:
    run_date = date(2026, 3, 27)
    history = build_convergence_state_history(
        _build_partitioned_fred_series(run_date),
        _build_partitioned_watermarks(run_date),
        _convergence_parameters(run_date),
    )
    latest = build_convergence_state_latest(history, _convergence_parameters(run_date))

    partitions = partition_convergence_regime_latest_json(latest)

    assert list(partitions.keys()) == ["regime_latest/regime_latest"]
    payload = partitions["regime_latest/regime_latest"]
    assert list(payload.keys()) == REGIME_JSON_KEYS
    assert payload["date"] == latest["as_of_date"].item().isoformat()
    assert payload["bias"] in ALLOWED_BIAS
    assert payload["generated_at_utc"]


def test_partition_regime_history_json_caps_to_104_weeks() -> None:
    as_of_dates = _weekly_dates(date(2024, 1, 5), 120)
    synthetic_history = pl.DataFrame(
        {
            "as_of_date": as_of_dates,
            "inflation_state": ["cooling"] * 120,
            "labor_state": ["softening"] * 120,
            "growth_state": ["slowdown"] * 120,
            "liquidity_state": ["neutral"] * 120,
            "macro_regime": ["soft_landing"] * 120,
            "run_date": [date(2026, 4, 11)] * 120,
        }
    )

    partitions = partition_convergence_regime_history_json(synthetic_history)

    assert list(partitions.keys()) == ["regime_history/regime_history"]
    payload = partitions["regime_history/regime_history"]
    assert len(payload) == REGIME_HISTORY_WINDOW_WEEKS
    assert list(payload[0].keys()) == REGIME_JSON_KEYS
    assert payload[0]["date"] == as_of_dates[-REGIME_HISTORY_WINDOW_WEEKS].isoformat()
    assert payload[-1]["date"] == as_of_dates[-1].isoformat()
    assert {record["bias"] for record in payload} == {"bullish"}


def test_partition_regime_history_json_emits_full_history_when_under_limit() -> None:
    short_history = pl.DataFrame(
        {
            "as_of_date": _weekly_dates(date(2026, 1, 2), SHORT_HISTORY_ROWS),
            "inflation_state": ["reaccelerating"] * SHORT_HISTORY_ROWS,
            "labor_state": ["deteriorating"] * SHORT_HISTORY_ROWS,
            "growth_state": ["contraction"] * SHORT_HISTORY_ROWS,
            "liquidity_state": ["tightening"] * SHORT_HISTORY_ROWS,
            "macro_regime": ["recession"] * SHORT_HISTORY_ROWS,
            "run_date": [date(2026, 4, 11)] * SHORT_HISTORY_ROWS,
        }
    )

    partitions = partition_convergence_regime_history_json(short_history)

    payload = partitions["regime_history/regime_history"]
    assert len(payload) == SHORT_HISTORY_ROWS
    assert all(record["bias"] == "defensive" for record in payload)
    assert all(record["generated_at_utc"] for record in payload)
