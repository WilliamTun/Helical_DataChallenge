"""Stable short fingerprints for datasets and bundles (lineage, not crypto)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Protocol


def fingerprint_local_file(repo_root: Path, logical_posix_path: str) -> str:
    """Fingerprint a file under ``repo_root`` using path + mtime + size (fast, good for lineage)."""
    key = logical_posix_path.replace("\\", "/").lstrip("/")
    path = repo_root / key
    if not path.is_file():
        return "missing"
    st = path.stat()
    raw = f"{key}|{st.st_mtime_ns}|{st.st_size}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def fingerprint_bytes(data: bytes, label: str = "") -> str:
    """Content hash prefix for small blobs (manifests, json)."""
    h = hashlib.sha256((label + "|").encode() + data).hexdigest()
    return h[:20]


def combine_version_token(*parts: str) -> str:
    """Single token for :class:`dagster.DataVersion` from multiple lineage components."""
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode()).hexdigest()[:24]


class _BytesReader(Protocol):
    def read_bytes(self, logical_path: str) -> bytes: ...


def fingerprint_stored_artifact(repo_root: Path, reader: _BytesReader, logical_path: str) -> str:
    """Prefer local file fingerprint; otherwise hash stored bytes (e.g. DuckDB-backed artifacts)."""
    key = logical_path.replace("\\", "/").lstrip("/")
    local = repo_root / key
    if local.is_file():
        return fingerprint_local_file(repo_root, logical_path)
    return fingerprint_bytes(reader.read_bytes(logical_path), key)
