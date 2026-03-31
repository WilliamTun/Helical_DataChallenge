"""Asset checks for synthetic dataset quality."""

from typing import Any

import anndata as ad
from dagster import AssetCheckResult, asset_check

from src.dagster.asset_checks.common import build_check_result, make_artifact_reader
from src.dagster.assets import synthetic_adata
from src.dagster.config import MockPipelineConfig
from src.dagster.assets.helpers.path_helpers import normalize_logical_path

from src.pipeline.helpers.artifact_io import read_h5ad  # noqa: E402

REQUIRED_OBS_COLUMNS = ["cell_type", "donor", "batch"]
REQUIRED_VAR_COLUMNS = ["gene_symbol", "chromosome", "gene_id", "is_target_gene"]


def _check_obs_columns(adata: ad.AnnData, required: list[str]) -> tuple[bool, dict[str, Any]]:
    missing = [c for c in required if c not in adata.obs.columns]
    return (len(missing) == 0, {"missing_obs_columns": missing, "n_obs": int(adata.n_obs)})


def _check_var_columns(adata: ad.AnnData, required: list[str]) -> tuple[bool, dict[str, Any]]:
    missing = [c for c in required if c not in adata.var.columns]
    return (len(missing) == 0, {"missing_var_columns": missing, "n_vars": int(adata.n_vars)})


@asset_check(
    asset=synthetic_adata,
    name="synthetic_adata_has_expected_schema",
    description="Ensure synthetic AnnData has required obs/var schema for downstream perturbations.",
)
def synthetic_adata_has_expected_schema(config: MockPipelineConfig) -> AssetCheckResult:
    reader = make_artifact_reader(config)
    synthetic_adata_logical = normalize_logical_path(config.synthetic_adata_path)
    if not reader.exists(synthetic_adata_logical):
        raise FileNotFoundError(
            "Synthetic AnnData not found at "
            f"{synthetic_adata_logical!r}. Materialize 'synthetic_adata' for the selected "
            "dataset_version first (or point synthetic_adata_path to an existing file)."
        )
    adata = read_h5ad(reader, synthetic_adata_logical)

    ok_obs, obs_meta = _check_obs_columns(
        adata,
        required=REQUIRED_OBS_COLUMNS,
    )
    ok_var, var_meta = _check_var_columns(
        adata,
        required=REQUIRED_VAR_COLUMNS,
    )
    ok = ok_obs and ok_var

    return build_check_result(
        passed=ok,
        metadata={
            "ok_obs": ok_obs,
            "ok_var": ok_var,
            **obs_meta,
            **var_meta,
        },
    )
