"""Dagster partitions for production lineage tracking.

We treat `dataset_version` as a first-class partition dimension so that multiple dataset
snapshots (and their downstream perturbation/comparison artifacts) can be queried side-by-side
in the Dagster UI.
"""

from dagster import DynamicPartitionsDefinition, MultiPartitionsDefinition

# Dynamic so users / sensors can add new dataset_version keys at runtime.
dataset_version_partitions = DynamicPartitionsDefinition(name="dataset_version")

# Dynamic experiment batch keys (e.g. batch_0000, batch_0001) for fan-out workloads.
experiment_batch_partitions = DynamicPartitionsDefinition(name="experiment_batch")

# Two-dimensional partitioning so dynamic fan-out is scoped by dataset + batch.
dataset_batch_partitions = MultiPartitionsDefinition(
    {
        "dataset_version": dataset_version_partitions,
        "experiment_batch": experiment_batch_partitions,
    }
)

__all__ = [
    "dataset_version_partitions",
    "experiment_batch_partitions",
    "dataset_batch_partitions",
]

