from __future__ import annotations

from meridian_data.pipeline_registry import register_pipelines
from meridian_data.pipelines.macro.convergence.pipeline import create_pipeline


def test_create_pipeline_has_expected_nodes() -> None:
    convergence_pipeline = create_pipeline()
    node_names = [node.name for node in convergence_pipeline.nodes]

    assert sorted(node_names) == sorted(
        [
            "build_convergence_state_history_node",
            "build_convergence_state_latest_node",
            "partition_convergence_regime_latest_json_node",
            "partition_convergence_regime_history_json_node",
            "partition_convergence_state_history_node",
            "partition_convergence_state_latest_node",
        ]
    )


def test_register_pipelines_includes_convergence_in_default() -> None:
    pipelines = register_pipelines()
    default_node_names = [node.name for node in pipelines["__default__"].nodes]

    assert "convergence" in pipelines
    assert "build_convergence_state_history_node" in default_node_names
    assert "ingest_fred_series_node" in default_node_names
