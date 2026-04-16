"""Project pipelines."""

from __future__ import annotations

from kedro.pipeline import Pipeline

from meridian_data.pipelines.credentials_context import (
    create_pipeline as create_credentials_context_pipeline,
)
from meridian_data.pipelines.macro.convergence import (
    create_pipeline as create_convergence_pipeline,
)
from meridian_data.pipelines.macro.fred import create_pipeline as create_fred_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = {
        "credentials_context": create_credentials_context_pipeline(),
        "fred": create_fred_pipeline(),
        "convergence": create_convergence_pipeline(),
    }
    # Keep default pipeline side-effect free for local test runs.
    pipelines["__default__"] = pipelines["credentials_context"]
    return pipelines
