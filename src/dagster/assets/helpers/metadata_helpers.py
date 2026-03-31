"""Metadata utilities shared by Dagster assets."""

from time import perf_counter

from dagster import MetadataValue

from src.dagster.assets.helpers.io_helpers import mock_model_version_info


def model_meta() -> dict[str, MetadataValue]:
    info = mock_model_version_info()
    return {
        "model_name": MetadataValue.text(str(info["model_name"])),
        "model_version": MetadataValue.text(str(info["model_version"])),
        "embedding_dim": MetadataValue.int(int(info["embedding_dim"])),
        "embedding_dtype": MetadataValue.text(str(info["embedding_dtype"])),
    }


def runtime_metadata(started: float) -> dict[str, MetadataValue]:
    return {"runtime_ms": MetadataValue.int(int((perf_counter() - started) * 1000))}


def compact(values: list[str]) -> str:
    """Build a Dagster-safe compact tag value (strict chars, <=63 chars)."""
    normalized = sorted({slug(v) for v in values if str(v).strip()})
    if not normalized:
        return "none"
    compact_value = "_".join(normalized[:3]) + ("_more" if len(normalized) > 3 else "")
    if len(compact_value) <= 63:
        return compact_value
    return compact_value[:63].rstrip("_-.") or "none"


def slug(value: str) -> str:
    out = []
    for ch in value:
        if ch.isalnum() or ch in {"-", "_"}:
            out.append(ch)
        else:
            out.append("_")
    return "".join(out).strip("_") or "default"


__all__ = ["compact", "model_meta", "runtime_metadata", "slug"]
