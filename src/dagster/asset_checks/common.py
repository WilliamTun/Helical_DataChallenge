"""Shared helpers for Dagster asset checks."""

from collections.abc import Mapping
from typing import Any

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    AssetRecordsFilter,
)

from src.dagster.config import MockPipelineConfig
from src.dagster.project import repo_root

from src.pipeline.helpers.artifact_io import load_settings_optional, make_reader  # noqa: E402


def make_artifact_reader(config: MockPipelineConfig) -> Any:
    """Build a repo-aware artifact reader using pipeline backend settings."""
    root = repo_root()
    out_settings = load_settings_optional(root, root / config.pipeline_config_path)
    return make_reader(root, out_settings)


def check_severity(passed: bool) -> AssetCheckSeverity:
    """Use ERROR for failing checks, WARN for passing checks."""
    return AssetCheckSeverity.WARN if passed else AssetCheckSeverity.ERROR


def build_check_result(passed: bool, metadata: Mapping[str, Any]) -> AssetCheckResult:
    """Build a standardized check result with shared severity policy."""
    return AssetCheckResult(
        passed=passed,
        severity=check_severity(passed),
        metadata=dict(metadata),
    )


def passing_check(metadata: Mapping[str, Any]) -> AssetCheckResult:
    """Build a concise successful check result for no-op/empty runs."""
    return AssetCheckResult(passed=True, metadata=dict(metadata))


def latest_materialization_metadata(
    context: AssetCheckExecutionContext, asset_name: str
) -> dict[str, Any]:
    """Fetch latest materialization metadata for an asset in current partition scope."""
    partition_keys = [context.partition_key] if context.has_partition_key else None
    result = context.instance.fetch_materializations(
        records_filter=AssetRecordsFilter(
            asset_key=AssetKey(asset_name),
            asset_partitions=partition_keys,
        ),
        limit=1,
        ascending=False,
    )
    if not result.records:
        partition_hint = (
            f" for partition {context.partition_key!r}" if context.has_partition_key else ""
        )
        raise FileNotFoundError(
            f"No materialization found for asset {asset_name!r}{partition_hint}. "
            "Materialize upstream asset first, then rerun checks."
        )
    materialization = result.records[0].event_log_entry.asset_materialization
    if materialization is None:
        return {}
    return dict(materialization.metadata)


def metadata_value_as_python(value: Any) -> Any:
    """Unwrap Dagster metadata values into plain Python objects when possible."""
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "data"):
        return value.data
    return value
