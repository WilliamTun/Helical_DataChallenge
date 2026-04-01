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
from src.dagster.run_config_defaults import (
    build_mock_pipeline_run_config,
    to_repo_relative_logical,
)

ENV_SENSOR_REL_PATH = "DAGSTER_ANN_DATA_SENSOR_REL_PATH"
ENV_SENSOR_INTERVAL_SEC = "DAGSTER_ANN_DATA_SENSOR_INTERVAL_SEC"


def _watched_ann_data_path() -> Path:
    rel = os.environ.get(ENV_SENSOR_REL_PATH, "outputs/synthetic_adata.h5ad")
    return repo_root() / rel


def _resolve_watched_path() -> Path | None:
    """Return watched path or None if file not present yet."""
    path = _watched_ann_data_path()
    if not path.is_file():
        return None
    return path


def _build_run_config(watched_path: Path) -> dict:
    return build_mock_pipeline_run_config(
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
    path = _resolve_watched_path()
    if path is None:
        path = _watched_ann_data_path()
        yield SkipReason(f"AnnData file not present yet: {path}")
        return

    if not path.is_file():
        yield SkipReason(f"AnnData file not present yet: {path}")
        return

    st = path.stat()
    cursor_key = f"{st.st_mtime_ns}:{st.st_size}"
    if context.cursor == cursor_key:
        return
    yield SensorResult(
        cursor=cursor_key,
        run_requests=[
            RunRequest(
                run_key=f"ann_data_{cursor_key}",
                run_config=_build_run_config(path),
            )
        ],
    )
