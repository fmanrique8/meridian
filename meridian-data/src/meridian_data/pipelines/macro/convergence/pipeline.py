"""Pipeline definition for macro convergence (Layer-1) signals."""

from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    build_convergence_state_history,
    build_convergence_state_latest,
    partition_convergence_regime_history_json,
    partition_convergence_regime_latest_json,
    partition_convergence_state_history,
    partition_convergence_state_latest,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the macro convergence signal pipeline.

    Args:
        **kwargs: Optional keyword arguments passed to `kedro.pipeline.pipeline`.

    Returns:
        Kedro Pipeline for weekly convergence state generation.
    """
    return pipeline(
        [
            node(
                func=build_convergence_state_history,
                inputs=[
                    "macro__fred__primary__series_latest",
                    "macro__fred__primary__entity_watermarks",
                    "params:convergence",
                ],
                outputs="macro__convergence__staging__state_history",
                name="build_convergence_state_history_node",
            ),
            node(
                func=build_convergence_state_latest,
                inputs=[
                    "macro__convergence__staging__state_history",
                    "params:convergence",
                ],
                outputs="macro__convergence__staging__state_latest",
                name="build_convergence_state_latest_node",
            ),
            node(
                func=partition_convergence_regime_latest_json,
                inputs="macro__convergence__staging__state_latest",
                outputs="macro__convergence__primary__regime_latest",
                name="partition_convergence_regime_latest_json_node",
            ),
            node(
                func=partition_convergence_regime_history_json,
                inputs="macro__convergence__staging__state_history",
                outputs="macro__convergence__primary__regime_history",
                name="partition_convergence_regime_history_json_node",
            ),
            node(
                func=partition_convergence_state_history,
                inputs="macro__convergence__staging__state_history",
                outputs="macro__convergence__feature__state_history",
                name="partition_convergence_state_history_node",
            ),
            node(
                func=partition_convergence_state_latest,
                inputs="macro__convergence__staging__state_latest",
                outputs="macro__convergence__primary__state_latest",
                name="partition_convergence_state_latest_node",
            ),
        ],
        **kwargs,
    )
