"""Artifact read/write for local folder and duckdb backends."""

from __future__ import annotations

import io
import json
import tempfile
from pathlib import Path
from typing import Any, Protocol

import anndata as ad
import duckdb
import numpy as np

from src.pipeline.helpers.io_config import (
    DuckDBOutputConfig,
    PipelineOutputSettings,
    default_local_settings,
    load_pipeline_output_settings,
)


def default_repo_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent.parent


def to_logical_posix(path: Path, repo_root: Path) -> str:
    try:
        rel = path.resolve().relative_to(repo_root.resolve())
    except ValueError:
        return str(path).replace("\\", "/")
    return rel.as_posix()


def _is_h5ad_path(logical_path: str) -> bool:
    return logical_path.replace("\\", "/").rstrip("/").lower().endswith(".h5ad")


class GlobalReader:
    def __init__(self, repo_root: Path, duckdb_cfg: DuckDBOutputConfig | None) -> None:
        self.repo_root = repo_root.resolve()
        self._duckdb_cfg = duckdb_cfg

    def read_bytes(self, logical_path: str) -> bytes:
        key = logical_path.replace("\\", "/").lstrip("/")
        local = self.repo_root / key
        if local.is_file():
            return local.read_bytes()
        if _is_h5ad_path(logical_path):
            raise FileNotFoundError(key)
        if self._duckdb_cfg is not None:
            return _duckdb_get_blob(self._duckdb_cfg, self.repo_root, key)
        raise FileNotFoundError(key)

    def exists(self, logical_path: str) -> bool:
        key = logical_path.replace("\\", "/").lstrip("/")
        if (self.repo_root / key).is_file():
            return True
        if _is_h5ad_path(logical_path):
            return False
        if self._duckdb_cfg is None:
            return False
        return _duckdb_exists(self._duckdb_cfg, self.repo_root, key)

    def read_json(self, logical_path: str) -> Any:
        return json.loads(self.read_bytes(logical_path).decode("utf-8"))


class ArtifactWriter(Protocol):
    def write_bytes(self, logical_path: str, data: bytes) -> None: ...


class LocalFolderWriter:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root.resolve()

    def write_bytes(self, logical_path: str, data: bytes) -> None:
        key = logical_path.replace("\\", "/").lstrip("/")
        out = self.repo_root / key
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)


class DuckDBWriter:
    def __init__(self, repo_root: Path, cfg: DuckDBOutputConfig) -> None:
        self.repo_root = repo_root.resolve()
        self._cfg = cfg

    def write_bytes(self, logical_path: str, data: bytes) -> None:
        key = logical_path.replace("\\", "/").lstrip("/")
        _duckdb_put_blob(self._cfg, self.repo_root, key, data)


class DuckDBHybridWriter:
    def __init__(self, repo_root: Path, cfg: DuckDBOutputConfig) -> None:
        self._local = LocalFolderWriter(repo_root)
        self._duck = DuckDBWriter(repo_root, cfg)

    def write_bytes(self, logical_path: str, data: bytes) -> None:
        if _is_h5ad_path(logical_path):
            self._local.write_bytes(logical_path, data)
        else:
            self._duck.write_bytes(logical_path, data)


def _ensure_table(con: duckdb.DuckDBPyConnection, table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            logical_path VARCHAR PRIMARY KEY,
            content BLOB NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _db_path(cfg: DuckDBOutputConfig, repo_root: Path) -> Path:
    p = Path(cfg.database_path)
    if not p.is_absolute():
        p = repo_root / p
    return p


def _duckdb_put_blob(cfg: DuckDBOutputConfig, repo_root: Path, logical_path: str, data: bytes) -> None:
    if _is_h5ad_path(logical_path):
        raise ValueError("AnnData (.h5ad) must not be stored in DuckDB; use local filesystem.")
    db_file = _db_path(cfg, repo_root)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_file))
    try:
        _ensure_table(con, cfg.artifacts_table)
        con.execute(
            f"INSERT OR REPLACE INTO {cfg.artifacts_table} (logical_path, content) VALUES (?, ?)",
            [logical_path, data],
        )
    finally:
        con.close()


def _duckdb_get_blob(cfg: DuckDBOutputConfig, repo_root: Path, logical_path: str) -> bytes:
    if _is_h5ad_path(logical_path):
        raise FileNotFoundError(logical_path)
    db_file = _db_path(cfg, repo_root)
    if not db_file.is_file():
        raise FileNotFoundError(logical_path)
    con = duckdb.connect(str(db_file), read_only=True)
    try:
        row = con.execute(
            f"SELECT content FROM {cfg.artifacts_table} WHERE logical_path = ?",
            [logical_path],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        raise FileNotFoundError(logical_path)
    return row[0]


def _duckdb_exists(cfg: DuckDBOutputConfig, repo_root: Path, logical_path: str) -> bool:
    if _is_h5ad_path(logical_path):
        return False
    db_file = _db_path(cfg, repo_root)
    if not db_file.is_file():
        return False
    con = duckdb.connect(str(db_file), read_only=True)
    try:
        row = con.execute(
            f"SELECT 1 FROM {cfg.artifacts_table} WHERE logical_path = ? LIMIT 1",
            [logical_path],
        ).fetchone()
    finally:
        con.close()
    return row is not None


def make_reader(repo_root: Path, settings: PipelineOutputSettings) -> GlobalReader:
    return GlobalReader(repo_root, settings.duckdb)


def make_writer(repo_root: Path, settings: PipelineOutputSettings, step: str) -> ArtifactWriter:
    backend = settings.backend_for_step(step)
    if backend == "local_folder":
        return LocalFolderWriter(repo_root)
    cfg = settings.duckdb_required()
    return DuckDBHybridWriter(repo_root, cfg)


def load_settings_optional(repo_root: Path, pipeline_config_path: Path | None) -> PipelineOutputSettings:
    if pipeline_config_path is None or not pipeline_config_path.is_file():
        return default_local_settings()
    return load_pipeline_output_settings(pipeline_config_path.resolve())


def resolve_io_for_cli(
    pipeline_config_path: Path | None,
) -> tuple[Path, PipelineOutputSettings, GlobalReader]:
    repo = default_repo_root()
    out_settings = load_settings_optional(repo, pipeline_config_path)
    return repo, out_settings, make_reader(repo, out_settings)


def to_logical_from_user_path(path: Path, repo_root: Path) -> str:
    resolved = path if path.is_absolute() else (repo_root / path)
    return to_logical_posix(resolved.resolve(), repo_root)


def to_optional_logical_from_user_path(path: Path | None, repo_root: Path) -> str | None:
    if path is None:
        return None
    return to_logical_from_user_path(path, repo_root)


def ensure_logical_exists(reader: GlobalReader, logical_path: str, label: str) -> None:
    """Raise a consistent error when a required logical artifact is missing."""
    if not reader.exists(logical_path):
        raise FileNotFoundError(f"{label} not found at logical path: {logical_path}")


def run_id_from_logical_prefix(logical_prefix: str) -> str:
    return logical_prefix.rstrip("/").split("/")[-1]


def write_json(writer: ArtifactWriter, logical_path: str, obj: Any) -> None:
    writer.write_bytes(logical_path, json.dumps(obj, indent=2).encode("utf-8"))


def write_npy(writer: ArtifactWriter, logical_path: str, arr: np.ndarray) -> None:
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    writer.write_bytes(logical_path, buf.getvalue())


def write_npz(writer: ArtifactWriter, logical_path: str, arrays: dict[str, np.ndarray]) -> None:
    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    writer.write_bytes(logical_path, buf.getvalue())


def read_npy(reader: GlobalReader, logical_path: str) -> np.ndarray:
    buf = io.BytesIO(reader.read_bytes(logical_path))
    return np.load(buf, allow_pickle=False)


def read_npz(reader: GlobalReader, logical_path: str) -> Any:
    buf = io.BytesIO(reader.read_bytes(logical_path))
    return np.load(buf, allow_pickle=False)


def read_h5ad(reader: GlobalReader, logical_path: str) -> ad.AnnData:
    key = logical_path.replace("\\", "/").lstrip("/")
    local = reader.repo_root / key
    if not local.is_file():
        raise FileNotFoundError(
            f"AnnData (.h5ad) must exist on local disk (not in DuckDB): {logical_path}"
        )
    return ad.read_h5ad(local)


def write_h5ad_adata(writer: ArtifactWriter, logical_path: str, adata: ad.AnnData) -> None:
    with tempfile.NamedTemporaryFile(suffix=".h5ad", delete=False) as tmp:
        path = Path(tmp.name)
    try:
        adata.write_h5ad(path)
        writer.write_bytes(logical_path, path.read_bytes())
    finally:
        path.unlink(missing_ok=True)
