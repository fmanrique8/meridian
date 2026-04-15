from __future__ import annotations

from meridian_data.pipeline_registry import register_pipelines
from meridian_data.pipelines.macro.fred.pipeline import create_pipeline


def test_create_pipeline_has_expected_nodes() -> None:
    pipeline_fred = create_pipeline()
    node_names = [node.name for node in pipeline_fred.nodes]

    assert sorted(node_names) == sorted(
        [
            "build_fred_ingestion_metadata_node",
            "ingest_fred_series_node",
            "partition_fred_ingestion_metadata_node",
            "process_fred_series_node",
            "partition_fred_series_node",
            "partition_fred_series_latest_node",
            "partition_fred_entity_watermarks_node",
        ]
    )


def test_register_pipelines_includes_fred_and_default_is_side_effect_free() -> None:
    pipelines = register_pipelines()
    default_node_names = [node.name for node in pipelines["__default__"].nodes]

    assert "fred" in pipelines
    assert "credentials_context" in pipelines
    assert "ingest_fred_series_node" not in default_node_names
