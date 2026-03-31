"""Sensor: when the on-disk AnnData file changes, run the mock pipeline job.

Use this when ``generate_data.py`` (or another process) writes ``outputs/synthetic_adata.h5ad``
outside Dagster. The job runs with ``synthetic_adata`` in ``external`` mode so data is not
regenerated inside Dagster.

Enable the sensor in Dagster UI (default is stopped to avoid surprise runs in development).
"""

import os
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from src.dagster.jobs.mock_pipeline_job import mock_pipeline_job
from src.dagster.project import repo_root
from src.dagster.partitions import dataset_version_partitions
from src.dagster.run_config_defaults import (
    build_mock_pipeline_run_config,
    to_repo_relative_logical,
)

ENV_DATASET_VERSION = "DAGSTER_DATASET_VERSION"
ENV_SENSOR_REL_PATH = "DAGSTER_ANN_DATA_SENSOR_REL_PATH"
ENV_SENSOR_INTERVAL_SEC = "DAGSTER_ANN_DATA_SENSOR_INTERVAL_SEC"


def _watched_ann_data_path(*, dataset_version: str) -> Path:
    rel_template = os.environ.get(
        ENV_SENSOR_REL_PATH,
        "outputs/synthetic_adata.h5ad",
    )
    rel = rel_template.replace("{dataset_version}", dataset_version)
    return repo_root() / rel


def _resolve_dataset_version_and_path() -> tuple[str, Path] | None:
    """Return (dataset_version, watched_path) or None if fallback path not present yet."""
    base_dataset_version = os.environ.get(ENV_DATASET_VERSION)
    path = _watched_ann_data_path(dataset_version=base_dataset_version or "unknown")
    if base_dataset_version is None:
        if not path.is_file():
            return None
        st = path.stat()
        dataset_version = os.environ.get(ENV_DATASET_VERSION, f"mtime_{st.st_mtime_ns}")
        return dataset_version, path

    dataset_version = base_dataset_version
    return dataset_version, _watched_ann_data_path(dataset_version=dataset_version)


def _build_run_config(dataset_version: str, watched_path: Path) -> dict:
    return build_mock_pipeline_run_config(
        dataset_version=dataset_version,
        synthetic_adata_materialization_mode="external",
        synthetic_adata_path=to_repo_relative_logical(watched_path, repo_root()),
    )


@sensor(
    job=mock_pipeline_job,
    minimum_interval_seconds=int(os.environ.get(ENV_SENSOR_INTERVAL_SEC, "30")),
    default_status=DefaultSensorStatus.STOPPED,
)
def ann_data_sensor(context: SensorEvaluationContext):
    """Fire a pipeline run when the watched ``.h5ad`` file appears or its mtime/size changes."""
    resolved = _resolve_dataset_version_and_path()
    if resolved is None:
        path = _watched_ann_data_path(dataset_version="unknown")
        yield SkipReason(f"AnnData file not present yet: {path}")
        return
    dataset_version, path = resolved

    if not path.is_file():
        yield SkipReason(f"AnnData file not present yet: {path}")
        return

    st = path.stat()
    cursor_key = f"{st.st_mtime_ns}:{st.st_size}"
    if context.cursor == cursor_key:
        return
    add_partition_request = dataset_version_partitions.build_add_request([dataset_version])
    yield SensorResult(
        cursor=cursor_key,
        dynamic_partitions_requests=[add_partition_request],
        run_requests=[
            RunRequest(
                run_key=f"ann_data_{cursor_key}",
                partition_key=dataset_version,
                run_config=_build_run_config(dataset_version, path),
            )
        ],
    )
