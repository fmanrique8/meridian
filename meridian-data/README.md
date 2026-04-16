# meridian-data

Kedro project for Meridian macro ingestion and convergence signal generation.

## Pipelines

- `fred`: FRED ingestion, processing, partitioning, and ingestion metadata
- `convergence`: FRED-first weekly Layer-1 macro states
- `market_yfinance`: ETF daily OHLCV ingestion with incremental symbol watermarks
- `credentials_context`: isolated credentials loading context

## Default Run Behavior

`kedro run` executes `fred + convergence`.

Use explicit selection when needed:

```powershell
python -m kedro run --pipelines=fred
python -m kedro run --pipelines=convergence
python -m kedro run --pipelines=market_yfinance
python -m kedro run --pipelines=credentials_context
python -m kedro run --env=prod --pipelines=fred,convergence
python -m kedro run --env=prod --pipelines=market_yfinance
```

## Dependency Source of Truth

Runtime dependencies are defined in this file:

- `meridian-data/pyproject.toml`

Workspace root metadata intentionally does not duplicate these dependencies.

## Development

```powershell
python -m ruff check src tests
python -m black --check src tests
python -m pytest tests/pipelines/macro/fred -q
python -m pytest tests/pipelines/macro/convergence -q
python -m pytest tests/pipelines/credentials_context/test_credentials_context.py -q
python -m pytest tests/test_run.py -q
```
