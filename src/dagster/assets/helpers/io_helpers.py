"""IO and pipeline dependency helpers for Dagster assets."""

from src.dagster.project import repo_root

from src.pipeline.helpers.artifact_io import (  # noqa: E402
    load_settings_optional,
    make_reader,
    make_writer,
    read_h5ad,
    write_h5ad_adata,
)
from src.pipeline.compute_comparisons import (  # noqa: E402
    compute_comparisons,
    export_gold_from_silver as export_comparison_gold,
)
from src.pipeline.generate_data import build_synthetic_adata  # noqa: E402
from src.pipeline.helpers.mock_model import mock_model_version_info  # noqa: E402
from src.pipeline.preview_data import summarize_adata  # noqa: E402
from src.pipeline.run_perturbations import (  # noqa: E402
    _read_config,
    export_gold_from_silver as export_perturbation_gold,
    run_perturbations,
)

__all__ = [
    "_read_config",
    "build_synthetic_adata",
    "compute_comparisons",
    "export_comparison_gold",
    "export_perturbation_gold",
    "load_settings_optional",
    "make_reader",
    "make_writer",
    "mock_model_version_info",
    "read_h5ad",
    "repo_root",
    "run_perturbations",
    "summarize_adata",
    "write_h5ad_adata",
]
