# FRED Pipeline (`macro/fred`)

This pipeline ingests selected macroeconomic series from the St. Louis Fed FRED API, normalizes them into a long-format table, and writes two partitioned outputs for different use cases.

## What We Have So Far

- `client.py`
  - `FredClient` wraps FRED HTTP calls with bounded retries for transient errors (`429`, `500`, `502`, `503`, `504`) and explicit error messages.
  - Endpoints used:
    - `series`
    - `series/observations`
- `schemas.py`
  - Pydantic models for API responses and runtime parameters (`FredPipelineParameters`).
- `nodes.py`
  - `ingest_fred_series`: fetches metadata + observations and returns a raw long table.
  - `process_fred_series`: parses dates, converts `value_raw` to numeric `value`, handles `"."` as null, and stamps `run_date`.
  - `partition_fred_series`: snapshot partitions by `run_date`.
  - `partition_fred_series_latest`: query-friendly partitions by `series_id/year`.
- `pipeline.py`
  - Node order:
    1. ingest
    2. process
    3. partition snapshots
    4. partition latest-serving

## Parameters and Defaults

Configured under `conf/base/parameters.yml` as `fred`:

- API and runtime:
  - `api_key` from `${oc.env:FRED_API_KEY}`
  - `base_url`, `timeout_seconds`, `max_retries`, `backoff_seconds`, `jitter_seconds`
- Pull window:
  - `observation_start` default: `1990-01-01`
  - `observation_end` optional
  - `sort_order` default: `asc`
  - `limit` optional
  - `run_date` optional (defaults to current date if unset)
- Series list (v1):
  - `FEDFUNDS`
  - `CPIAUCSL`
  - `UNRATE`

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

Both outputs are Parquet via `polars.EagerPolarsDataset`.

## Run and Test

- Run only this pipeline:
  - `kedro run --pipelines=fred`
- Targeted tests:
  - `pytest tests/pipelines/macro/fred -q`

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

