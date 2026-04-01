"""Job that materializes all mock pipeline assets in dependency order."""

from dagster import AssetSelection, define_asset_job

from src.dagster.assets import (
    comparison_results,
    perturbation_run,
    perturbation_run_gene_activation,
    perturbation_run_gene_knockout,
    perturbation_run_gene_overexpression,
    preview_data,
    synthetic_adata,
)
from src.dagster.run_config_defaults import build_mock_pipeline_run_config

MOCK_PIPELINE_SELECTION = AssetSelection.assets(
    synthetic_adata,
    preview_data,
    perturbation_run_gene_knockout,
    perturbation_run_gene_overexpression,
    perturbation_run_gene_activation,
    perturbation_run,
    comparison_results,
)

_MOCK_PIPELINE_DEFAULT_CONFIG = build_mock_pipeline_run_config()

mock_pipeline_job = define_asset_job(
    name="mock_pipeline_job",
    description="End-to-end mock perturbation pipeline with lineage metadata on each asset.",
    selection=MOCK_PIPELINE_SELECTION,
    config=_MOCK_PIPELINE_DEFAULT_CONFIG,
    tags={
        "pipeline": "mock_perturbation",
        "lineage_instrumented": "MaterializeResult+data_version+asset_tags",
    },
    run_tags={
        "pipeline": "mock_perturbation",
    },
)
