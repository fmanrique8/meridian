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

## Rule Implementation (Notion v1)

Rules are implemented from the Convergence Signal Framework section 13:

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

Threshold defaults are Notion-compatible and fully tunable from config.

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

Convergence nodes emit structured key-value lifecycle logs aligned with:

- `D:\projects\meridian\internal_docs\AI_AGENT_LOGGING_OBSERVABILITY_STANDARD.md`
- `D:\projects\meridian\internal_docs\AI_AGENT_INCREMENTAL_SYNC_METADATA_STANDARD.md`

Kedro logger configuration remains centralized in `conf/logging.yml`.

## Run

- `kedro run --pipelines=fred,convergence`
- `kedro run --env=prod --pipelines=fred,convergence`
