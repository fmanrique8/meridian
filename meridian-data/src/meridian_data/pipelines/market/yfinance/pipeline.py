"""Pipeline definition for yfinance ETF ingestion."""

from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    build_yfinance_ingestion_metadata,
    ingest_yfinance_etf_prices,
    partition_yfinance_entity_watermarks,
    partition_yfinance_etf_prices,
    partition_yfinance_etf_prices_latest,
    partition_yfinance_ingestion_metadata,
    process_yfinance_etf_prices,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the yfinance ETF ingestion pipeline.

    Args:
        **kwargs: Optional keyword arguments passed to `kedro.pipeline.pipeline`.

    Returns:
        Kedro Pipeline for yfinance ingest/process/partition workflow.
    """
    return pipeline(
        [
            node(
                func=ingest_yfinance_etf_prices,
                inputs=[
                    "params:yfinance",
                    "market__yfinance__primary__entity_watermarks_existing",
                ],
                outputs="market__yfinance__staging__etf_prices_raw",
                name="ingest_yfinance_etf_prices_node",
            ),
            node(
                func=process_yfinance_etf_prices,
                inputs=[
                    "market__yfinance__staging__etf_prices_raw",
                    "params:yfinance",
                ],
                outputs="market__yfinance__staging__etf_prices_processed",
                name="process_yfinance_etf_prices_node",
            ),
            node(
                func=partition_yfinance_etf_prices,
                inputs="market__yfinance__staging__etf_prices_raw",
                outputs="market__yfinance__raw__etf_prices",
                name="partition_yfinance_etf_prices_node",
            ),
            node(
                func=partition_yfinance_etf_prices_latest,
                inputs=[
                    "market__yfinance__staging__etf_prices_processed",
                    "market__yfinance__primary__etf_prices_latest_existing",
                ],
                outputs="market__yfinance__primary__etf_prices_latest",
                name="partition_yfinance_etf_prices_latest_node",
            ),
            node(
                func=build_yfinance_ingestion_metadata,
                inputs=[
                    "market__yfinance__staging__etf_prices_processed",
                    "params:yfinance",
                ],
                outputs="market__yfinance__staging__ingestion_metadata",
                name="build_yfinance_ingestion_metadata_node",
            ),
            node(
                func=partition_yfinance_ingestion_metadata,
                inputs="market__yfinance__staging__ingestion_metadata",
                outputs="market__yfinance__raw__ingestion_metadata",
                name="partition_yfinance_ingestion_metadata_node",
            ),
            node(
                func=partition_yfinance_entity_watermarks,
                inputs="market__yfinance__staging__ingestion_metadata",
                outputs="market__yfinance__primary__entity_watermarks",
                name="partition_yfinance_entity_watermarks_node",
            ),
        ],
        **kwargs,
    )

