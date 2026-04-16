# Convergence Pipeline (`macro/convergence`)

This pipeline computes weekly Layer-1 macro states from FRED serving outputs.
It is intentionally derived-only (no API ingestion) and consumes:

- `macro__fred__primary__series_latest` for signal math
- `macro__fred__primary__entity_watermarks` for coverage monitoring

## Scope (v1)

- Source scope: FRED-only
- Time grain: weekly
- Week anchor: Friday
- Output families:
  - `macro__convergence__feature__state_history`
  - `macro__convergence__primary__state_latest`

## Rule Implementation

Rules are implemented as simple, interpretable formulas:

- Inflation:
  - `cpi_yoy = 12m % change (CPIAUCSL)`
  - `cpi_3m_annualized = 3m % change * 4`
  - states: `cooling`, `sticky`, `reaccelerating`
- Labor:
  - `unrate_3m_change = UNRATE(t) - UNRATE(t-3m)`
  - `claims_4w_avg = MA4(ICSA)`
  - `claims_trend = claims_4w_avg(t) - claims_4w_avg(t-4w)`
  - states: `tight`, `softening`, `deteriorating`
- Growth:
  - `payrolls_3m = 3m % change (PAYEMS)`
  - `payrolls_6m = 6m % change (PAYEMS)`
  - states: `expansion`, `slowdown`, `contraction`
- Liquidity:
  - `yield_curve = DGS10 - DGS2`
  - `curve_trend = yield_curve(t) - yield_curve(t-3mo)`
  - `rates_trend = DGS10(t) - DGS10(t-3mo)`
  - states: `tightening`, `neutral`, `easing`
- Macro regime precedence:
  1. `soft_landing`
  2. `stagflation_risk`
  3. `recession`
  4. `reacceleration`
  5. `mixed`

## Parameters

Defined under `params:convergence` in `conf/base/parameters.yml`:

- `run_date` (optional)
- `time_grain` (`weekly`)
- `week_anchor` (`friday`)
- `source_set_version` (traceability)
- `thresholds.*` for all v1 rule knobs

Threshold defaults are initial calibrated v1 values and fully tunable from
config.

Current defaults:

- Inflation:
  - `inflation_sticky_band: 0.3`
- Labor:
  - `labor_unrate_tight_threshold: -0.1`
  - `labor_claims_tight_threshold: 0.0`
  - `labor_unrate_deteriorating_threshold: 0.2`
  - `labor_claims_deteriorating_threshold: 15000.0`
- Growth:
  - `growth_expansion_relative_threshold: 0.05`
  - `growth_contraction_threshold: 0.0`
- Liquidity:
  - `liquidity_curve_tightening_threshold: 0.0`
  - `liquidity_rates_tightening_threshold: 0.0`
  - `liquidity_curve_easing_threshold: 0.25`
  - `liquidity_rates_easing_threshold: -0.25`

These values are intended as defensible starting points and should be refined
with historical backtesting.

## Output Contract

Both history and latest rows include:

- `as_of_date`
- states: `inflation_state`, `labor_state`, `growth_state`, `liquidity_state`, `macro_regime`
- diagnostics:
  - `cpi_yoy`, `cpi_3m_annualized`
  - `unrate_3m_change`, `claims_4w_avg`, `claims_trend`
  - `payrolls_3m`, `payrolls_6m`
  - `yield_curve`, `curve_trend`, `rates_trend`
- traceability: `source_set_version`, `run_date`

## Logging and Standards

Convergence nodes emit structured key-value lifecycle logs. Kedro logger
configuration remains centralized in `conf/logging.yml`.

## Run

- `kedro run` (default runs `fred + convergence`)
- `kedro run --pipelines=fred,convergence`
- `kedro run --env=prod --pipelines=fred,convergence`
