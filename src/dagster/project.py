"""Repository path helpers for Dagster modules."""

from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    """Project root (parent of ``src``)."""
    return Path(__file__).resolve().parent.parent.parent
