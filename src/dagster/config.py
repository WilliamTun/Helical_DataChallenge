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
    # If true, typed perturbation and comparison assets reuse existing artifacts when
    # the memoization token (input fingerprints + config + model version) is unchanged.
    enable_memoization: bool = True
    # Number of perturbation experiments grouped into each dynamic experiment batch.
    perturbation_batch_size: int = 1


class PerturbationPipelineConfig(MockPipelineConfig):
    """Config for assets that run the mock embedding model on perturbations.

    ``model_name`` applies only to these steps—not to synthetic data, preview, or comparison.
    Downstream comparison assets record the model as reference metadata from upstream outputs.
    """

    model_name: Literal[
        "mock_foundation_model_1",
        "mock_foundation_model_2",
        "mock_foundation_model_3",
    ] = "mock_foundation_model_1"
