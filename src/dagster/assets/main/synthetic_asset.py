"""Synthetic AnnData source asset."""

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
from src.dagster.lineage.fingerprints import combine_version_token, fingerprint_local_file, fingerprint_stored_artifact
from src.dagster.partitions import dataset_version_partitions
from src.dagster.assets.helpers.io_helpers import (
    build_synthetic_adata,
    load_settings_optional,
    make_reader,
    make_writer,
    mock_model_version_info,
    repo_root,
    write_h5ad_adata,
)
from src.dagster.assets.helpers.metadata_helpers import model_meta, runtime_metadata
from src.dagster.assets.helpers.path_helpers import (
    normalize_logical_path,
    resolve_dataset_version,
)

@asset(
    group_name="mock_pipeline",
    partitions_def=dataset_version_partitions,
    freshness_policy=FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
    metadata={
        "lineage_role": MetadataValue.text("Canonical AnnData dataset for this pipeline run."),
        "lineage_layer": MetadataValue.text("source"),
    },
)
def synthetic_adata(context: AssetExecutionContext, config: MockPipelineConfig) -> MaterializeResult:
    started = perf_counter()
    root = repo_root()
    out_settings = load_settings_optional(root, root / config.pipeline_config_path)
    logical = normalize_logical_path(config.synthetic_adata_path)
    mode = config.synthetic_adata_materialization_mode
    dataset_version = resolve_dataset_version(context, config)
    model_info = mock_model_version_info()
    if mode == "external":
        reader = make_reader(root, out_settings)
        if not reader.exists(logical):
            raise FileNotFoundError(
                f"external mode: expected AnnData at logical path {logical!r} (run generate_data first)."
            )
        dfp = fingerprint_stored_artifact(root, reader, logical)
    else:
        writer = make_writer(root, out_settings, "generate_data")
        adata = build_synthetic_adata(
            n_cells=config.n_cells,
            n_genes=config.n_genes,
            n_donors=config.n_donors,
            n_batches=config.n_batches,
            seed=config.seed,
        )
        write_h5ad_adata(writer, logical, adata)
        dfp = fingerprint_local_file(root, logical)
    return MaterializeResult(
        value=logical,
        metadata={
            "dataset_logical_path": MetadataValue.text(logical),
            "dataset_version": MetadataValue.text(dataset_version),
            "dataset_fingerprint": MetadataValue.text(dfp),
            "dataset_materialization_mode": MetadataValue.text(mode),
            **runtime_metadata(started),
            **model_meta(),
        },
        data_version=DataVersion(
            combine_version_token("synthetic_adata", dataset_version, dfp, mode, str(model_info["model_version"]))
        ),
        tags={
            "lineage/asset_role": "dataset",
            "dataset_version": dataset_version,
            "model_version": str(model_info["model_version"]),
        },
    )
