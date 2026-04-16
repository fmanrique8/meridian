# yfinance Pipeline (`market/yfinance`)

This pipeline ingests ETF history from yfinance, normalizes daily OHLCV data,
and writes partitioned outputs for raw reproducibility, serving access, and
incremental watermark state.

## Scope

- Pipeline package: `src/meridian_data/pipelines/market/yfinance/`
- Pipeline ID: `market_yfinance`
- Ingest cadence: daily bars (`interval=1d`)
- Universe default:
  - Core market: `SPY`, `QQQ`, `IWM`
  - Sectors: `XLE`, `XLP`, `XLK`, `XLF`, `XLI`, `XLV`, `XLRE`
  - Overlays: `TLT`, `GLD`

## Sync Behavior

- First run (no watermark): fetch full configured history window (`history_years=10`).
- Re-runs with `sync_mode=incremental`:
  - per-symbol fetch start = `max(full_window_start, watermark - overlap_days)`
  - default overlap is `7` days for revision safety.

## Output Datasets

- `market__yfinance__raw__etf_prices`
  - path: `data/01_raw/market/yfinance/etf_prices`
  - partition key: `run_date=YYYY-MM-DD/yfinance_etf_prices.parquet`
- `market__yfinance__primary__etf_prices_latest`
  - path: `data/03_primary/market/yfinance/etf_prices_latest`
  - partition key: `symbol=<SYM>/year=<YYYY>/yfinance_etf_prices_latest.parquet`
- `market__yfinance__raw__ingestion_metadata`
  - path: `data/01_raw/market/yfinance/ingestion_metadata`
  - partition key: `run_date=YYYY-MM-DD/yfinance_ingestion_metadata.parquet`
- `market__yfinance__primary__entity_watermarks`
  - path: `data/03_primary/market/yfinance/entity_watermarks`
  - partition key: `entity_id=<SYM>/yfinance_entity_watermark.parquet`

### Incremental Read Aliases

- `market__yfinance__primary__etf_prices_latest_existing`
- `market__yfinance__primary__entity_watermarks_existing`

These read from the same physical serving paths and are used to merge incremental
updates before writing the new partitions.

## Canonical Serving Schema

- `symbol`
- `date`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `volume`
- `ingested_at_utc`
- `source`
- `source_last_updated`
- `sync_mode`
- `fetch_start`
- `fetch_end`
- `run_date`

`source_last_updated` uses yfinance-derived max observation date per symbol as a
deterministic fallback when no explicit upstream update timestamp is available.

## Data Quality

- Duplicate key check: `(symbol, date)` must be unique.
- Date parse check: parsed `date` must not be null.
- Critical null policy: `open`, `high`, `low`, `close`, `volume` cannot be null.
- Per-symbol null threshold: configurable via
  `data_quality.max_null_ratio_per_symbol`.

## Logging

Nodes emit required key-value lifecycle events:

- `ingest_start`
- `ingest_complete`
- `process_complete`
- `dq_check_pass` or `dq_check_fail`
- `partition_write_complete`

## Run

```powershell
python -m kedro run --pipelines=market_yfinance
python -m kedro run --env=prod --pipelines=market_yfinance
```

