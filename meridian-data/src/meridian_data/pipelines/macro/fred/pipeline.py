"""Pipeline definition for FRED macroeconomic ingestion."""

from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_fred_series,
    partition_fred_series,
    partition_fred_series_latest,
    process_fred_series,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the FRED ingestion pipeline.

    Args:
        **kwargs: Optional keyword arguments passed to `kedro.pipeline.pipeline`.

    Returns:
        Kedro Pipeline for FRED ingest/process/partition workflow.
    """
    return pipeline(
        [
            node(
                func=ingest_fred_series,
                inputs="params:fred",
                outputs="macro__fred__staging__series_raw",
                name="ingest_fred_series_node",
            ),
            node(
                func=process_fred_series,
                inputs=["macro__fred__staging__series_raw", "params:fred"],
                outputs="macro__fred__staging__series_processed",
                name="process_fred_series_node",
            ),
            node(
                func=partition_fred_series,
                inputs="macro__fred__staging__series_processed",
                outputs="macro__fred__raw__series",
                name="partition_fred_series_node",
            ),
            node(
                func=partition_fred_series_latest,
                inputs="macro__fred__staging__series_processed",
                outputs="macro__fred__primary__series_latest",
                name="partition_fred_series_latest_node",
            ),
        ],
        **kwargs,
    )
