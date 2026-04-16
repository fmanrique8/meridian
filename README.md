# Meridian Workspace

This repository is a workspace containing the `meridian-data` Kedro project
plus private/internal assets.

## Workspace Layout

- `meridian-data/`: primary Kedro codebase and runtime project
- `internal_docs/`: private internal standards and architecture notes
- root `pyproject.toml`: workspace metadata only (not runtime dependency source)

## Dependency Authority

Runtime dependencies are managed in:

- `meridian-data/pyproject.toml` (authoritative source)

The root `pyproject.toml` intentionally avoids duplicating app/runtime
dependencies to prevent drift.

## Day-to-Day Commands

Run from `meridian-data/`:

```powershell
python -m kedro run
```

Default pipeline execution runs `fred + convergence`.

For explicit runs:

```powershell
python -m kedro run --pipelines=fred
python -m kedro run --pipelines=convergence
python -m kedro run --pipelines=credentials_context
python -m kedro run --env=prod --pipelines=fred,convergence
```
