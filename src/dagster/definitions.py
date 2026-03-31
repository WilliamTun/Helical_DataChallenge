"""Dagster code location entrypoint.

This module registers all assets, jobs, sensors, and asset checks exposed by this
repository. It is the canonical target for:

    uv run dagster dev -f src/dagster/definitions.py
"""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dagster import Definitions  # noqa: E402

from src.dagster.assets import (  # noqa: E402
    batch_manifest,
    comparison_results,
    comparison_results_batch,
    perturbation_run_batch,
    perturbation_run,
    perturbation_run_gene_activation,
    perturbation_run_gene_knockout,
    perturbation_run_gene_overexpression,
    preview_data,
    synthetic_adata,
)
from src.dagster.jobs import (  # noqa: E402
    asset_check_alert_job,
    mock_pipeline_dynamic_batch_job,
    mock_pipeline_job,
)
from src.dagster.sensors import (  # noqa: E402
    ann_data_sensor,
    asset_check_failure_alert_sensor,
    experiment_batch_partitions_sensor,
)
from src.dagster.asset_checks import (  # noqa: E402
    comparison_results_has_expected_manifest,
    perturbation_run_embeddings_have_expected_shape,
    synthetic_adata_has_expected_schema,
)

PIPELINE_ASSETS = [
    synthetic_adata,
    preview_data,
    perturbation_run_gene_knockout,
    perturbation_run_gene_overexpression,
    perturbation_run_gene_activation,
    perturbation_run,
    comparison_results,
    batch_manifest,
    perturbation_run_batch,
    comparison_results_batch,
]
PIPELINE_JOBS = [mock_pipeline_job, mock_pipeline_dynamic_batch_job, asset_check_alert_job]
PIPELINE_SENSORS = [
    ann_data_sensor,
    asset_check_failure_alert_sensor,
    experiment_batch_partitions_sensor,
]
PIPELINE_ASSET_CHECKS = [
    synthetic_adata_has_expected_schema,
    perturbation_run_embeddings_have_expected_shape,
    comparison_results_has_expected_manifest,
]

defs = Definitions(
    assets=PIPELINE_ASSETS,
    jobs=PIPELINE_JOBS,
    sensors=PIPELINE_SENSORS,
    asset_checks=PIPELINE_ASSET_CHECKS,
)
