# FRED Pipeline (`macro/fred`)

This pipeline ingests macroeconomic series from the St. Louis Fed FRED API,
normalizes them into a long-format table, and writes partitioned outputs for
lineage, serving, and incremental metadata use cases.

## What We Have So Far

- `client.py`
  - `FredClient` wraps FRED HTTP calls with bounded retries for transient errors (`429`, `500`, `502`, `503`, `504`) and explicit error messages.
  - Endpoints used:
    - `series`
    - `series/observations`
- `schemas.py`
  - Pydantic models for API responses and runtime parameters (`FredPipelineParameters`).
- `commons.py`
  - Shared schema constants, data-quality helpers, metadata builders, partition helpers, and standardized key-value logging helpers.
- `nodes.py`
  - `ingest_fred_series`: fetches metadata + observations and returns a raw long table.
  - `process_fred_series`: parses dates, converts `value_raw` to numeric `value`, handles `"."` as null, and stamps `run_date`.
  - `partition_fred_series`: snapshot partitions by `run_date`.
  - `partition_fred_series_latest`: query-friendly partitions by `series_id/year`.
  - `build_fred_ingestion_metadata`: builds per-entity watermark metadata for incremental state.
  - `partition_fred_ingestion_metadata`: snapshot partitions for metadata by `run_date`.
  - `partition_fred_entity_watermarks`: serving partitions for metadata by `entity_id`.
- `pipeline.py`
  - Node order:
    1. ingest
    2. process
    3. partition snapshots
    4. partition latest-serving
    5. build ingestion metadata
    6. partition metadata snapshots
    7. partition metadata watermarks

## Parameters and Defaults

Configured under `conf/base/parameters.yml` as `fred`:

- API and runtime:
  - `api_key` from `${oc.env:FRED_API_KEY}`
  - `base_url`, `timeout_seconds`, `max_retries`, `backoff_seconds`, `jitter_seconds`
- Pull window:
  - `observation_start` default: `1990-01-01`
  - `observation_end` optional
  - `sort_order` default: `asc`
  - `sync_mode` default: `full` (`full` or `incremental`)
  - `limit` optional
  - `run_date` optional (defaults to current date if unset)
- Data quality assertions:
  - `data_quality.max_null_ratio_per_series` (default `0.2`)
  - `data_quality.enforce_monotonic_dates`
  - `data_quality.enforce_unique_series_date`
  - `data_quality.enforce_numeric_parse`
  - `data_quality.enforce_valid_dates`
- Series list (v1 core FRED set complete):
  - Inflation:
    - `CPIAUCSL` (headline CPI)
    - `CPILFESL` (core CPI)
    - `PCEPI` (headline PCE)
    - `PCEPILFE` (core PCE)
  - Labor:
    - `UNRATE` (unemployment rate)
    - `PAYEMS` (nonfarm payrolls)
    - `CIVPART` (labor force participation)
    - `ICSA` (initial jobless claims)
  - Growth:
    - `GDPC1` (real GDP, quarterly)
  - Rates / liquidity:
    - `FEDFUNDS` (monthly policy-rate proxy)
    - `DFF` (daily effective fed funds rate)
    - `DGS10` (10-year treasury yield)
    - `DGS2` (2-year treasury yield)
    - `T10Y2Y` (10Y-2Y spread)

Mixed frequencies (daily, weekly, monthly, quarterly) are stored as-ingested in
Layer 0/1 ingestion for this wave; no resampling is applied.

`series_ids` now has a single source of truth in `conf/base/parameters.yml`.
Schema defaults no longer duplicate the list.

## Outputs and Partition Strategy

Defined in `conf/base/catalog.yml`:

- `macro__fred__raw__series` (snapshot layer)
  - Path: `data/01_raw/macro/fred/series`
  - Partition key shape: `run_date=YYYY-MM-DD/fred_series.parquet`
  - Purpose: reproducibility, backtesting, revision/audit trail

- `macro__fred__primary__series_latest` (serving layer)
  - Path: `data/03_primary/macro/fred/series_latest`
  - Partition key shape: `series_id=<ID>/year=<YYYY>/fred_series_latest.parquet`
  - Purpose: efficient downstream reads by indicator and time

- `macro__fred__raw__ingestion_metadata` (metadata snapshot layer)
  - Path: `data/01_raw/macro/fred/ingestion_metadata`
  - Partition key shape: `run_date=YYYY-MM-DD/fred_ingestion_metadata.parquet`
  - Purpose: immutable run metadata for observability and lineage

- `macro__fred__primary__entity_watermarks` (metadata serving layer)
  - Path: `data/03_primary/macro/fred/entity_watermarks`
  - Partition key shape: `entity_id=<ID>/fred_entity_watermark.parquet`
  - Purpose: incremental watermark state per entity (`watermark_observation_date`)

Partition strategy rationale:

- `run_date` partitions preserve immutable snapshot lineage for audits and
  backtests.
- `series_id/year` partitions support cheap query pruning for downstream
  readers.

Both outputs are Parquet via `polars.EagerPolarsDataset`.

When running with `--env=prod`, `conf/prod/catalog.yml` overrides these paths to S3:

- `macro__fred__raw__series`
  - `s3://${S3_BUCKET_NAME}/meridian/01_raw/macro/fred/series`
- `macro__fred__primary__series_latest`
  - `s3://${S3_BUCKET_NAME}/meridian/03_primary/macro/fred/series_latest`
- `macro__fred__raw__ingestion_metadata`
  - `s3://${S3_BUCKET_NAME}/meridian/01_raw/macro/fred/ingestion_metadata`
- `macro__fred__primary__entity_watermarks`
  - `s3://${S3_BUCKET_NAME}/meridian/03_primary/macro/fred/entity_watermarks`

## Run and Test

- Default project run:
  - `kedro run` (executes `fred + convergence`)
- Run local filesystem output:
  - `kedro run --pipelines=fred`
- Run S3 output using prod config:
  - `kedro run --env=prod --pipelines=fred`
  - Note: S3 runs require `s3fs` (included in project dependencies).
- Targeted tests:
  - `pytest tests/pipelines/macro/fred -q`

## Layer-1 Signal Status

Layer-1 signal computation is intentionally deferred in this wave.
No signal nodes are added yet for:

- `inflation_state`
- `labor_state`
- `growth_state`
- `liquidity_state`
- `macro_regime`

Signal logic starts after ingestion architecture gates are completed for the
remaining non-FRED source families.

## Consumed by Convergence Pipeline

`macro/fred` remains ingestion-focused and now serves as the source contract
for `macro/convergence` (Layer-1 derived signals).

Convergence reads:

- `macro__fred__primary__series_latest` (formula inputs)
- `macro__fred__primary__entity_watermarks` (coverage/monitoring)

This keeps source ingestion concerns decoupled from signal derivation concerns.

## Logging and Observability

FRED nodes now emit structured INFO logs for:

- ingest start and completion summaries
- per-series fetch row counts
- process summary (rows, series count, observation window, null ratio)
- partition build counts for raw, serving, and metadata outputs

Event payloads use standardized key-value fields including:
`event`, `pipeline`, `source`, `node`, `run_date`, `row_count`,
`entity_count`, `duration_ms`, and `status`.

Kedro logging uses rich console output plus `info.log` file rotation
(`conf/logging.yml`).

## Remaining Sources Backlog (Non-FRED)

- BLS labor detail:
  - sector-level employment
  - wage metrics / average hourly earnings
  - JOLTS-style labor tightness signals
- EIA energy:
  - WTI crude
  - gasoline-related series
  - natural gas (optional early add)
- Market feed (Twelve Data target):
  - index ETFs (`SPY`, `QQQ`, `IWM`)
  - sector ETFs (`XLE`, `XLK`, `XLF`, `XLI`, `XLP`, `XLV`, `XLRE`)
  - overlays (`TLT`, `GLD`, `VIX` or proxy)
- Discretionary big-cap overlay:
  - `AAPL`, `AMZN`, `GOOGL`, `META`, `NVDA`, `ORCL`

## How We Extend It

- Add series:
  - Update `fred.series_ids` in `parameters.yml`.
- Adjust date scope:
  - Change `observation_start`/`observation_end`.
- Add derived metrics:
  - Add processing node(s) after `process_fred_series`.
- Change partitioning:
  - Keep `run_date` snapshots for lineage.
  - Tune serving partitions in `partition_fred_series_latest` for query patterns.
