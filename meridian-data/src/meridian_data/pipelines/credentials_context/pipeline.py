"""Credentials context pipeline definition."""

from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import load_kedro_credentials


def create_pipeline(**kwargs) -> Pipeline:
    """Create a reusable pipeline that exposes S3 credentials as node output."""
    return pipeline(
        [
            node(
                func=load_kedro_credentials,
                inputs=None,
                outputs="s3_credentials",
                name="load_kedro_credentials_node",
            )
        ],
        **kwargs,
    )
