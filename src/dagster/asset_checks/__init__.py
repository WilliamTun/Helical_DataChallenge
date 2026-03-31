"""Dagster asset checks (lineage + data quality gates)."""

from src.dagster.asset_checks.comparison_checks import (
    comparison_results_has_expected_manifest,
)
from src.dagster.asset_checks.perturbation_checks import (
    perturbation_run_embeddings_have_expected_shape,
)
from src.dagster.asset_checks.synthetic_checks import (
    synthetic_adata_has_expected_schema,
)

__all__ = [
    "synthetic_adata_has_expected_schema",
    "perturbation_run_embeddings_have_expected_shape",
    "comparison_results_has_expected_manifest",
]

