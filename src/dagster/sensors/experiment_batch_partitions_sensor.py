"""Sensor that keeps experiment_batch dynamic partitions in sync with config."""

from dagster import DefaultSensorStatus, SensorEvaluationContext, SensorResult, SkipReason, sensor

from src.dagster.assets.helpers.perturbation_helpers import load_perturbation_batches
from src.dagster.config import MockPipelineConfig
from src.dagster.partitions import experiment_batch_partitions


@sensor(
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def experiment_batch_partitions_sensor(context: SensorEvaluationContext):
    """Ensure dynamic experiment_batch keys exist before users materialize dynamic assets."""
    config = MockPipelineConfig()
    batches = load_perturbation_batches(config)
    batch_keys = [str(batch["batch_key"]) for batch in batches]
    if not batch_keys:
        yield SkipReason("No experiment batches discovered from perturbation config.")
        return

    yield SensorResult(
        dynamic_partitions_requests=[experiment_batch_partitions.build_add_request(batch_keys)]
    )
