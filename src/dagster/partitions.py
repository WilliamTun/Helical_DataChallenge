"""Dagster partitions for dynamic fan-out lineage tracking."""

from dagster import DynamicPartitionsDefinition, MultiPartitionsDefinition, StaticPartitionsDefinition

# Dynamic experiment batch keys (e.g. batch_0000, batch_0001) for fan-out workloads.
experiment_batch_partitions = DynamicPartitionsDefinition(name="experiment_batch")

# Static perturbation types; users can only select supported values in UI.
perturbation_type_partitions = StaticPartitionsDefinition(
    ["gene_knockout", "gene_overexpression", "gene_activation"]
)

# Two-dimensional partitioning for dynamic fan-out by perturbation type + batch.
dataset_batch_partitions = MultiPartitionsDefinition(
    {
        "perturbation_type": perturbation_type_partitions,
        "experiment_batch": experiment_batch_partitions,
    }
)

__all__ = [
    "perturbation_type_partitions",
    "experiment_batch_partitions",
    "dataset_batch_partitions",
]

