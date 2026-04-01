"""Preview/QA asset for synthetic AnnData."""

from datetime import timedelta
from time import perf_counter

from dagster import (
    AssetExecutionContext,
    DataVersion,
    FreshnessPolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.dagster.config import MockPipelineConfig
from src.dagster.lineage.fingerprints import combine_version_token, fingerprint_stored_artifact
from src.dagster.assets.helpers.io_helpers import (
    load_settings_optional,
    make_reader,
    mock_model_version_info,
    read_h5ad,
    repo_root,
    summarize_adata,
)
from src.dagster.assets.helpers.metadata_helpers import model_technical_meta, runtime_metadata
@asset(
    group_name="mock_pipeline",
    freshness_policy=FreshnessPolicy.time_window(fail_window=timedelta(hours=12)),
    metadata={
        "lineage_role": MetadataValue.text("QA summary of the AnnData (prints to compute logs)."),
        "lineage_layer": MetadataValue.text("inspection"),
    },
)
def preview_data(context: AssetExecutionContext, config: MockPipelineConfig, synthetic_adata: str) -> MaterializeResult:
    started = perf_counter()
    root = repo_root()
    out_settings = load_settings_optional(root, root / config.pipeline_config_path)
    reader = make_reader(root, out_settings)
    adata = read_h5ad(reader, synthetic_adata)
    summarize_adata(adata, max_categories=config.preview_max_categories)
    dfp = fingerprint_stored_artifact(root, reader, synthetic_adata)
    model_info = mock_model_version_info()
    return MaterializeResult(
        value=synthetic_adata,
        metadata={
            **runtime_metadata(started),
            **model_technical_meta(),
        },
        data_version=DataVersion(combine_version_token("preview_data", synthetic_adata, dfp)),
        tags={
            "lineage/asset_role": "preview",
            "model_version": str(model_info["model_version"]),
        },
    )
