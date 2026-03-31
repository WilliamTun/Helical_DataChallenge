"""Load ``configs/pipeline_config.json`` output / storage settings."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

BackendName = Literal["local_folder", "duckdb"]

STEP_KEYS = ("generate_data", "preview_data", "run_perturbations", "compute_comparisons")


@dataclass(frozen=True)
class DuckDBOutputConfig:
    database_path: str
    artifacts_table: str = "pipeline_artifacts"


@dataclass(frozen=True)
class PipelineOutputSettings:
    default_backend: BackendName
    duckdb: DuckDBOutputConfig | None
    step_backends: dict[str, BackendName]

    def backend_for_step(self, step: str) -> BackendName:
        if step not in STEP_KEYS:
            raise ValueError(f"Unknown pipeline step: {step}. Expected one of {STEP_KEYS}.")
        return self.step_backends.get(step, self.default_backend)

    def duckdb_required(self) -> DuckDBOutputConfig:
        if self.duckdb is None:
            raise ValueError(
                "pipeline_config references duckdb backend but pipeline_output.duckdb is missing."
            )
        return self.duckdb


def _parse_backend(raw: Any) -> BackendName:
    if raw not in ("local_folder", "duckdb"):
        raise ValueError(f"Invalid output backend: {raw!r}. Use 'local_folder' or 'duckdb'.")
    return raw  # type: ignore[return-value]


def load_pipeline_output_settings(config_path: Path) -> PipelineOutputSettings:
    with config_path.open("r", encoding="utf-8") as f:
        root = json.load(f)
    po = root.get("pipeline_output")
    if not isinstance(po, dict):
        raise ValueError(f"{config_path}: missing object 'pipeline_output'.")
    default_backend = _parse_backend(po.get("default_backend", "local_folder"))
    duckdb_block = po.get("duckdb")
    duckdb_cfg: DuckDBOutputConfig | None = None
    if duckdb_block is not None:
        if not isinstance(duckdb_block, dict):
            raise ValueError("pipeline_output.duckdb must be an object.")
        db_path = duckdb_block.get("database_path")
        if not db_path or not isinstance(db_path, str):
            raise ValueError("pipeline_output.duckdb.database_path (string) is required.")
        table = duckdb_block.get("artifacts_table", "pipeline_artifacts")
        if not isinstance(table, str):
            raise ValueError("pipeline_output.duckdb.artifacts_table must be a string.")
        duckdb_cfg = DuckDBOutputConfig(database_path=db_path, artifacts_table=table)
    steps_block = po.get("steps") or {}
    if not isinstance(steps_block, dict):
        raise ValueError("pipeline_output.steps must be an object.")
    step_backends: dict[str, BackendName] = {}
    for key in STEP_KEYS:
        spec = steps_block.get(key)
        if spec is None:
            continue
        if not isinstance(spec, dict):
            raise ValueError(f"pipeline_output.steps.{key} must be an object.")
        b = spec.get("backend")
        if b is None:
            continue
        step_backends[key] = _parse_backend(b)
    settings = PipelineOutputSettings(
        default_backend=default_backend,
        duckdb=duckdb_cfg,
        step_backends=step_backends,
    )
    for step, backend in settings.step_backends.items():
        if backend == "duckdb" and settings.duckdb is None:
            raise ValueError(
                f"Step {step!r} uses duckdb but pipeline_output.duckdb is not configured."
            )
    if default_backend == "duckdb" and settings.duckdb is None:
        raise ValueError("default_backend is duckdb but pipeline_output.duckdb is not configured.")
    return settings


def default_local_settings() -> PipelineOutputSettings:
    return PipelineOutputSettings(default_backend="local_folder", duckdb=None, step_backends={})
