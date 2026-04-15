# Macro Ingestion Architecture

This document defines the ingestion-first architecture for macro sources in
Meridian before Layer-1 signal computation begins.

## Objective

Build stable source contracts first, then compute converged macro states.

Signals are deferred until the source onboarding gates below are complete.

## Source Boundaries and Onboarding Order

1. FRED (`macro/fred`) [complete for v1 core]
2. BLS (`macro/bls`) [next]
3. EIA (`macro/eia`) [after BLS]
4. Market feed (`macro/markets/twelve_data`) [after EIA]

Boundary rules:

- Each source owns its API client, schemas, nodes, pipeline, and tests.
- No cross-source transform logic inside source ingestion nodes.
- Cross-source joins happen only in later interpretation layers.

## Dataset Contract Pattern

Each source should publish two dataset families in catalog naming form:

- Snapshot lineage dataset:
  - Name pattern: `macro__<source>__raw__<entity>`
  - Partition pattern: `run_date=YYYY-MM-DD/...`
  - Purpose: reproducibility, audit trail, backtesting by ingest run.

- Serving dataset:
  - Name pattern: `macro__<source>__primary__<entity>_latest`
  - Partition pattern: source-optimized keys (for FRED: `series_id/year`).
  - Purpose: efficient downstream access for analysis and feature generation.

Environment policy:

- `conf/base/catalog.yml` defines default local paths.
- `conf/prod/catalog.yml` overrides only path targets to S3.

## Cadence Expectations

- FRED:
  - Daily/weekly/monthly/quarterly mixed frequency as-ingested.
  - Run cadence: at least weekly, plus monthly after major macro prints.

- BLS:
  - Monthly core labor updates, with weekly refresh only if selected series
    require it.

- EIA:
  - Weekly commodity/energy refresh baseline.

- Market (Twelve Data):
  - Daily close baseline for regime context mapping.

## Dependency Gates Before Layer-1 Signals

Signal pipeline implementation starts only after these gates are met:

1. Source ingestion gates:
  - each source has `client.py`, `schemas.py`, `nodes.py`, `pipeline.py`
  - source test suite exists and passes
  - raw + serving datasets are cataloged in base and prod

2. Contract gates:
  - date field typed consistently
  - value field numeric consistently
  - partition keys documented in source README

3. Ops gates:
  - local and prod runs succeed for each source pipeline
  - credentials wiring is env-var driven only

When these gates are complete for FRED + BLS + EIA + market baseline,
Layer-1 (`inflation_state`, `labor_state`, `growth_state`, `liquidity_state`,
`macro_regime`) can be implemented on stable ingestion outputs.
