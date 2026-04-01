"""Perturbation fan-out assets and aggregate."""

from datetime import timedelta
from typing import Any

from dagster import AssetExecutionContext, DataVersion, FreshnessPolicy, MaterializeResult, MetadataValue, asset

from src.dagster.config import PerturbationPipelineConfig
from src.dagster.lineage.fingerprints import combine_version_token
from src.dagster.assets.helpers.io_helpers import mock_model_version_info
from src.dagster.assets.helpers.metadata_helpers import (
    compact,
    model_meta,
)
from src.dagster.assets.helpers.perturbation_helpers import run_perturbation_type


def _active_runs_by_type(runs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Keep only materialized typed runs and index by perturbation type."""
    active_runs = [run for run in runs if not bool(run.get("skipped")) and run.get("run_prefix_logical")]
    return {str(run["perturbation_type"]): run for run in active_runs}


def _aggregate_data_version(model_name: str, perturbation_types: list[str]) -> DataVersion:
    return DataVersion(
        combine_version_token("perturbation_run_aggregate", model_name, *sorted(perturbation_types))
    )


def _typed_perturbation_asset(name: str, perturbation_type: str):
    @asset(name=name, group_name="mock_pipeline")
    def _typed_asset(
        context: AssetExecutionContext, config: PerturbationPipelineConfig, synthetic_adata: str
    ) -> MaterializeResult:
        return run_perturbation_type(context, config, synthetic_adata, perturbation_type)

    return _typed_asset


perturbation_run_gene_knockout = _typed_perturbation_asset(
    "perturbation_run_gene_knockout", "gene_knockout"
)
perturbation_run_gene_overexpression = _typed_perturbation_asset(
    "perturbation_run_gene_overexpression", "gene_overexpression"
)
perturbation_run_gene_activation = _typed_perturbation_asset(
    "perturbation_run_gene_activation", "gene_activation"
)


@asset(
    group_name="mock_pipeline",
    freshness_policy=FreshnessPolicy.time_window(fail_window=timedelta(hours=6)),
    metadata={"lineage_role": MetadataValue.text("Aggregated typed perturbation runs.")},
)
def perturbation_run(
    context: AssetExecutionContext,
    config: PerturbationPipelineConfig,
    perturbation_run_gene_knockout: dict[str, Any],
    perturbation_run_gene_overexpression: dict[str, Any],
    perturbation_run_gene_activation: dict[str, Any],
) -> MaterializeResult:
    typed_runs = [
        perturbation_run_gene_knockout,
        perturbation_run_gene_overexpression,
        perturbation_run_gene_activation,
    ]
    runs_by_type = _active_runs_by_type(typed_runs)
    model_info = mock_model_version_info(config.model_name)
    perturbation_types = sorted(runs_by_type.keys())
    return MaterializeResult(
        value={"runs_by_type": runs_by_type, "embedding_model_name": config.model_name},
        metadata={
            "active_perturbation_types": MetadataValue.json(perturbation_types),
            "n_active_runs": MetadataValue.int(len(runs_by_type)),
            "run_prefixes_by_type": MetadataValue.json(
                {ptype: str(run["run_prefix_logical"]) for ptype, run in runs_by_type.items()}
            ),
            **model_meta(config.model_name),
        },
        data_version=_aggregate_data_version(config.model_name, perturbation_types),
        tags={
            "lineage/asset_role": "embeddings_silver_aggregate",
            "model_version": str(model_info["model_version"]),
            "model_name": str(model_info["model_name"]),
            "perturbation_types": compact(perturbation_types),
        },
    )
