"""Shared default run-config builders for jobs and sensor-launched runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _base_pipeline_config() -> dict[str, Any]:
    return {
        "n_cells": 1_000,
        "n_genes": 500,
        "n_donors": 8,
        "n_batches": 4,
        "seed": 7,
        "synthetic_adata_path": "outputs/synthetic_adata.h5ad",
        "perturbation_config_path": "configs/perturbation_config.json",
        "perturbation_runs_root": "outputs/silver/perturbation_runs",
        "comparison_results_root": "outputs/silver/comparison_results",
        "gold_root": "outputs/gold",
        "baseline_embeddings_path": None,
        "preview_max_categories": 20,
        "pipeline_config_path": "configs/pipeline_config.json",
        "synthetic_adata_materialization_mode": "generate",
        "enable_memoization": True,
        "perturbation_batch_size": 1,
    }


def _perturbation_pipeline_config() -> dict[str, Any]:
    return {**_base_pipeline_config(), "model_name": "mock_foundation_model_1"}


def build_mock_pipeline_run_config(
    *,
    synthetic_adata_materialization_mode: str = "generate",
    synthetic_adata_path: str | None = None,
) -> dict[str, Any]:
    base = _base_pipeline_config()
    base["synthetic_adata_materialization_mode"] = synthetic_adata_materialization_mode
    if synthetic_adata_path is not None:
        base["synthetic_adata_path"] = synthetic_adata_path
    pert = _perturbation_pipeline_config()
    pert["synthetic_adata_materialization_mode"] = synthetic_adata_materialization_mode
    if synthetic_adata_path is not None:
        pert["synthetic_adata_path"] = synthetic_adata_path
    return {
        "ops": {
            "synthetic_adata": {"config": dict(base)},
            "preview_data": {"config": dict(base)},
            "perturbation_run_gene_knockout": {"config": dict(pert)},
            "perturbation_run_gene_overexpression": {"config": dict(pert)},
            "perturbation_run_gene_activation": {"config": dict(pert)},
            "perturbation_run": {"config": dict(pert)},
            "comparison_results": {"config": dict(base)},
        }
    }


def build_dynamic_batch_run_config() -> dict[str, Any]:
    return {
        "ops": {
            "perturbation_run_batch": {"config": dict(_perturbation_pipeline_config())},
            "comparison_results_batch": {"config": dict(_base_pipeline_config())},
        }
    }


def to_repo_relative_logical(path: Path, root: Path) -> str:
    """Convert a filesystem path to repo-relative logical path when possible."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve()).replace("\\", "/")
