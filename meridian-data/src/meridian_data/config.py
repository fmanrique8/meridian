"""Shared configuration utilities for runtime environment and credentials."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from dotenv import load_dotenv
from kedro.config import OmegaConfigLoader


def _project_root() -> Path:
    """Return the Kedro project root (`meridian-data`)."""
    return Path(__file__).resolve().parents[2]


def bootstrap_env(project_root: Path | None = None) -> list[Path]:
    """Load .env files without overriding already-exported OS variables.

    Load order is most-specific first:
    1. `<project_root>/.env`
    2. `<project_root>/../.env` (workspace-level fallback)
    """
    root = project_root or _project_root()
    env_files = [root / ".env", root.parent / ".env"]
    loaded_files: list[Path] = []

    for env_file in env_files:
        if env_file.exists():
            load_dotenv(dotenv_path=env_file, override=False)
            loaded_files.append(env_file)

    return loaded_files


def load_kedro_credentials(env: str = "local") -> dict[str, Any]:
    """Load and return the project's `s3` Kedro credentials for an environment."""
    conf_path = _project_root() / "conf"
    loader = OmegaConfigLoader(conf_source=str(conf_path), env=env)
    credentials = loader["credentials"].get("s3")

    if credentials is None:
        raise KeyError("Missing `s3` key in Kedro credentials configuration.")
    if not isinstance(credentials, dict):
        raise TypeError("`s3` credentials must be a mapping in credentials config.")

    return cast(dict[str, Any], credentials)


__all__ = ["bootstrap_env", "load_kedro_credentials"]
