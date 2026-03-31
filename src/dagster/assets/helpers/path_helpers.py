"""Path and partition helpers for Dagster assets."""

from dagster import AssetExecutionContext

from src.dagster.config import MockPipelineConfig
from src.dagster.assets.helpers.io_helpers import load_settings_optional, make_reader, repo_root


def normalize_logical_path(path: str) -> str:
    return path.replace("\\", "/").lstrip("/")


def normalize_logical_root(path: str) -> str:
    return path.replace("\\", "/").rstrip("/")


def resolve_dataset_version(context: AssetExecutionContext, config: MockPipelineConfig) -> str:
    return context.partition_key or config.dataset_version


def resolve_io(config: MockPipelineConfig):
    root = repo_root()
    out_settings = load_settings_optional(root, root / config.pipeline_config_path)
    reader = make_reader(root, out_settings)
    return root, out_settings, reader


def resolve_optional_baseline_path(config: MockPipelineConfig) -> str | None:
    if not config.baseline_embeddings_path:
        return None
    return normalize_logical_path(config.baseline_embeddings_path)


__all__ = [
    "normalize_logical_path",
    "normalize_logical_root",
    "resolve_optional_baseline_path",
    "resolve_dataset_version",
    "resolve_io",
]
