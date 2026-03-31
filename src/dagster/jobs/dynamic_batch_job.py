"""Job for dynamic experiment-batch fan-out materialization."""

from dagster import AssetSelection, define_asset_job

from src.dagster.assets import comparison_results_batch, perturbation_run_batch
from src.dagster.partitions import dataset_batch_partitions
from src.dagster.run_config_defaults import build_dynamic_batch_run_config

DYNAMIC_BATCH_SELECTION = AssetSelection.assets(
    perturbation_run_batch,
    comparison_results_batch,
)

_DYNAMIC_DEFAULT_CONFIG = build_dynamic_batch_run_config(dataset_version="default")

mock_pipeline_dynamic_batch_job = define_asset_job(
    name="mock_pipeline_dynamic_batch_job",
    description=(
        "Dynamic fan-out job over experiment_batch partitions for perturbation and comparison steps."
    ),
    selection=DYNAMIC_BATCH_SELECTION,
    partitions_def=dataset_batch_partitions,
    config=_DYNAMIC_DEFAULT_CONFIG,
    tags={"pipeline": "mock_perturbation_dynamic_batch", "fanout_mode": "dynamic_partitions"},
    run_tags={"pipeline": "mock_perturbation_dynamic_batch"},
)
