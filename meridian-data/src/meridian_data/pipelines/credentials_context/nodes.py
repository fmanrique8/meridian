"""Nodes for loading reusable credentials context."""

from __future__ import annotations

from typing import Any

from meridian_data.config import load_kedro_credentials as _load_kedro_credentials


def load_kedro_credentials(env: str = "local") -> dict[str, Any]:
    """Load S3 credentials from Kedro configuration for the selected environment."""
    return _load_kedro_credentials(env=env)
