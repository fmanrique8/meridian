from __future__ import annotations

from meridian_data.pipeline_registry import register_pipelines
from meridian_data.pipelines.market.yfinance.pipeline import create_pipeline


def test_create_pipeline_has_expected_nodes() -> None:
    pipeline_yfinance = create_pipeline()
    node_names = [node.name for node in pipeline_yfinance.nodes]

    assert sorted(node_names) == sorted(
        [
            "ingest_yfinance_etf_prices_node",
            "process_yfinance_etf_prices_node",
            "partition_yfinance_etf_prices_node",
            "partition_yfinance_etf_prices_latest_node",
            "build_yfinance_ingestion_metadata_node",
            "partition_yfinance_ingestion_metadata_node",
            "partition_yfinance_entity_watermarks_node",
        ]
    )


def test_register_pipelines_includes_market_yfinance_and_default_unchanged() -> None:
    pipelines = register_pipelines()
    default_node_names = [node.name for node in pipelines["__default__"].nodes]

    assert "market_yfinance" in pipelines
    assert "ingest_yfinance_etf_prices_node" not in default_node_names
    assert "ingest_fred_series_node" in default_node_names

