"""Dagster run / asset configuration shared by pipeline assets/jobs."""

from dagster import Config
from typing import Literal


class MockPipelineConfig(Config):
    """Shared defaults aligned with the standalone CLI scripts."""

    # Synthetic data generation parameters.
    n_cells: int = 1_000
    n_genes: int = 500
    n_donors: int = 8
    n_batches: int = 4
    seed: int = 7

    # Core logical paths for pipeline artifacts.
    synthetic_adata_path: str = "outputs/synthetic_adata.h5ad"
    perturbation_config_path: str = "configs/perturbation_config.json"
    perturbation_runs_root: str = "outputs/silver/perturbation_runs"
    comparison_results_root: str = "outputs/silver/comparison_results"
    gold_root: str = "outputs/gold"
    baseline_embeddings_path: str | None = None
    preview_max_categories: int = 20
    pipeline_config_path: str = "configs/pipeline_config.json"
    #: ``generate`` — run ``build_synthetic_adata`` and write. ``external`` — assume AnnData
    #: already exists at ``synthetic_adata_path`` (e.g. written by CLI or file sensor).
    synthetic_adata_materialization_mode: str = "generate"
    # User-supplied dataset version label. This is also used as the Dagster partition key
    # for the dataset and all downstream lineage assets.
    dataset_version: str = "default"
    # If true, typed perturbation and comparison assets reuse existing artifacts when
    # the memoization token (input fingerprints + config + model version) is unchanged.
    enable_memoization: bool = True
    # Number of perturbation experiments grouped into each dynamic experiment batch.
    perturbation_batch_size: int = 1
    # Optional dynamic-batch filter for perturbation type in UI run config.
    selected_perturbation_type: Literal[
        "all",
        "gene_knockout",
        "gene_overexpression",
        "gene_activation",
    ] = "all"
