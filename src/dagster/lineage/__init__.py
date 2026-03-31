"""Lineage helpers: fingerprints and Dagster ``MaterializeResult`` metadata.

Uses Dagster-native hooks:

- **MaterializeResult** — ``metadata`` (UI), ``data_version`` (logical versioning), ``tags``
- **@asset(metadata=...)** — static catalog metadata on each asset
- **Job** — ``tags`` / ``run_tags`` for run-level filtering in the UI

See ``fingerprints.py`` for content-adjacent fingerprints; wire-up lives in ``dagster.assets modules``.
"""

from src.dagster.lineage.fingerprints import (
    combine_version_token,
    fingerprint_bytes,
    fingerprint_local_file,
    fingerprint_stored_artifact,
)

__all__ = [
    "combine_version_token",
    "fingerprint_bytes",
    "fingerprint_local_file",
    "fingerprint_stored_artifact",
]
