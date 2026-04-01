"""Dynamic batch fan-out assets for perturbations and comparisons."""

from datetime import timedelta
from time import perf_counter
from typing import Any

from dagster import (
    AllPartitionMapping,
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    FreshnessPolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.dagster.assets.helpers.io_helpers import (
    compute_comparisons,
    export_comparison_gold,
    export_perturbation_gold,
    mock_model_version_info,
    read_h5ad,
    run_perturbations,
)
from src.dagster.assets.helpers.metadata_helpers import compact, model_meta, runtime_metadata
from src.dagster.assets.helpers.path_helpers import (
    normalize_logical_path,
    normalize_logical_root,
    resolve_io,
    resolve_optional_baseline_path,
)
from src.dagster.assets.helpers.perturbation_helpers import get_batch_by_key, load_perturbation_batches
from src.dagster.config import MockPipelineConfig, PerturbationPipelineConfig
from src.dagster.lineage.fingerprints import (
    combine_version_token,
    fingerprint_local_file,
    fingerprint_stored_artifact,
)
from src.dagster.partitions import dataset_batch_partitions

PERTURBATION_BATCH_ROLE = "embeddings_silver_batch"
COMPARISON_BATCH_ROLE = "comparison_silver_batch"


def _partition_keys(context: AssetExecutionContext) -> tuple[str, str]:
    keys = context.partition_key.keys_by_dimension if context.partition_key else {}
    perturbation_type = keys.get("perturbation_type", "gene_knockout")
    experiment_batch = keys.get("experiment_batch", "batch_0000")
    return perturbation_type, experiment_batch


def _comparison_paths(config: MockPipelineConfig, run_id: str) -> tuple[str, str, str]:
    comparison_results_root = normalize_logical_root(config.comparison_results_root)
    gold_root = normalize_logical_root(config.gold_root)
    results_prefix = f"{comparison_results_root}/{run_id}"
    return comparison_results_root, gold_root, results_prefix


def _skipped_comparison_batch_result(perturbation_type: str, batch_key: str, started: float) -> MaterializeResult:
    return MaterializeResult(
        value={"results_prefix_logical": None, "skipped": True},
        metadata={
            "perturbation_type": MetadataValue.text(perturbation_type),
            "experiment_batch": MetadataValue.text(batch_key),
            "status": MetadataValue.text("skipped_upstream_batch"),
            **runtime_metadata(started),
        },
        data_version=DataVersion(
            combine_version_token("comparison_results_batch", perturbation_type, batch_key, "skipped")
        ),
    )


@asset(
    group_name="mock_pipeline_dynamic",
    partitions_def=dataset_batch_partitions,
    freshness_policy=FreshnessPolicy.time_window(fail_window=timedelta(hours=6)),
    deps=[AssetDep(AssetKey("synthetic_adata"), partition_mapping=AllPartitionMapping())],
    metadata={
        "lineage_role": MetadataValue.text("Dynamic batch fan-out perturbation execution."),
        "lineage_layer": MetadataValue.text("silver"),
    },
)
def perturbation_run_batch(
    context: AssetExecutionContext, config: PerturbationPipelineConfig
) -> MaterializeResult:
    started = perf_counter()
    root, out_settings, reader = resolve_io(config)
    perturbation_type, batch_key = _partition_keys(context)
    batch = get_batch_by_key(config, batch_key)
    model_info = mock_model_version_info(config.model_name)
    synthetic_adata = normalize_logical_path(config.synthetic_adata_path)

    if not reader.exists(synthetic_adata):
        raise FileNotFoundError(
            "Dynamic batch run requires source AnnData to exist at "
            f"{synthetic_adata!r}. Materialize 'synthetic_adata' first, then retry this "
            "batch partition."
        )

    if batch is None:
        return MaterializeResult(
            value={"batch_key": batch_key, "skipped": True, "embedding_model_name": config.model_name},
            metadata={
                "perturbation_type": MetadataValue.text(perturbation_type),
                "experiment_batch": MetadataValue.text(batch_key),
                "status": MetadataValue.text("skipped_missing_batch"),
                **runtime_metadata(started),
                **model_meta(config.model_name),
            },
            data_version=DataVersion(
                combine_version_token("perturbation_run_batch", perturbation_type, batch_key, "missing")
            ),
            tags={"perturbation_type": perturbation_type, "experiment_batch": batch_key, "status": "skipped"},
        )

    selected_specs = [
        spec
        for spec in list(batch.get("perturbations", []))
        if str(spec.get("perturbation_type", "")).strip().lower() == perturbation_type
    ]
    if not selected_specs:
        return MaterializeResult(
            value={
                "run_prefix_logical": None,
                "synthetic_adata_logical": synthetic_adata,
                "experiment_batch": batch_key,
                "skipped": True,
                "embedding_model_name": config.model_name,
            },
            metadata={
                "perturbation_type": MetadataValue.text(perturbation_type),
                "experiment_batch": MetadataValue.text(batch_key),
                "status": MetadataValue.text("skipped_no_matching_partition_perturbation_type"),
                **runtime_metadata(started),
                **model_meta(config.model_name),
            },
            data_version=DataVersion(
                combine_version_token(
                    "perturbation_run_batch", perturbation_type, batch_key, "empty"
                )
            ),
            tags={
                "perturbation_type": perturbation_type,
                "experiment_batch": batch_key,
                "status": "skipped",
            },
        )

    adata = read_h5ad(reader, synthetic_adata)
    perturbation_runs_root = normalize_logical_root(config.perturbation_runs_root)
    gold_root = normalize_logical_root(config.gold_root)

    dataset_fp = fingerprint_stored_artifact(root, reader, synthetic_adata)
    cfg_fp = fingerprint_local_file(root, f"{config.perturbation_config_path}")
    memo_token = combine_version_token(
        "perturbation_batch",
        perturbation_type,
        batch_key,
        str(model_info["model_name"]),
        dataset_fp,
        cfg_fp,
        str(model_info["model_version"]),
    )
    run_id = f"{perturbation_type}__{batch_key}__{memo_token[:12]}"
    run_prefix = f"{perturbation_runs_root}/{run_id}"
    manifest_path = f"{run_prefix}/manifest.json"
    cache_hit = bool(config.enable_memoization and reader.exists(manifest_path))

    if not cache_hit:
        run_perturbations(
            adata=adata,
            config={
                "perturbation_types": [perturbation_type],
                "perturbations": selected_specs,
            },
            run_id=run_id,
            repo_root=root,
            output_settings=out_settings,
            perturbation_runs_root_logical=perturbation_runs_root,
        )

    export_perturbation_gold(
        repo_root=root,
        output_settings=out_settings,
        reader=reader,
        run_prefix_logical=run_prefix,
        gold_root_logical=gold_root,
    )

    return MaterializeResult(
        value={
            "run_prefix_logical": run_prefix,
            "synthetic_adata_logical": synthetic_adata,
            "experiment_batch": batch_key,
            "skipped": False,
            "embedding_model_name": config.model_name,
        },
        metadata={
            "perturbation_type": MetadataValue.text(perturbation_type),
            "experiment_batch": MetadataValue.text(batch_key),
            "n_experiments": MetadataValue.int(len(selected_specs)),
            "memoization_token": MetadataValue.text(memo_token),
            "memoization_cache_hit": MetadataValue.bool(cache_hit),
            "run_prefix_logical": MetadataValue.text(run_prefix),
            **runtime_metadata(started),
            **model_meta(config.model_name),
        },
        data_version=DataVersion(
            combine_version_token("perturbation_run_batch", perturbation_type, batch_key, memo_token)
        ),
        tags={
            "lineage/asset_role": PERTURBATION_BATCH_ROLE,
            "perturbation_type": perturbation_type,
            "experiment_batch": batch_key,
            "model_version": str(model_info["model_version"]),
            "model_name": str(model_info["model_name"]),
            "cache_hit": "true" if cache_hit else "false",
            "perturbation_types": compact([perturbation_type]),
        },
    )


@asset(
    group_name="mock_pipeline_dynamic",
    partitions_def=dataset_batch_partitions,
    freshness_policy=FreshnessPolicy.time_window(fail_window=timedelta(hours=6)),
    metadata={
        "lineage_role": MetadataValue.text("Dynamic batch fan-out comparison execution."),
        "lineage_layer": MetadataValue.text("silver"),
    },
)
def comparison_results_batch(
    context: AssetExecutionContext,
    config: MockPipelineConfig,
    perturbation_run_batch: dict[str, Any],
) -> MaterializeResult:
    started = perf_counter()
    if bool(perturbation_run_batch.get("skipped")):
        perturbation_type, batch_key = _partition_keys(context)
        return _skipped_comparison_batch_result(perturbation_type, batch_key, started)

    root, out_settings, reader = resolve_io(config)
    perturbation_type, batch_key = _partition_keys(context)
    run_prefix = str(perturbation_run_batch["run_prefix_logical"])
    run_id = run_prefix.rstrip("/").split("/")[-1]
    comparison_results_root, gold_root, results_prefix = _comparison_paths(config, run_id)
    manifest_path = f"{results_prefix}/comparison_manifest.json"
    cache_hit = bool(config.enable_memoization and reader.exists(manifest_path))

    baseline_logical = resolve_optional_baseline_path(config)

    if not cache_hit:
        results_prefix = compute_comparisons(
            repo_root=root,
            output_settings=out_settings,
            run_prefix_logical=run_prefix,
            input_adata_logical=str(perturbation_run_batch["synthetic_adata_logical"]),
            results_root_logical=comparison_results_root,
            baseline_logical=baseline_logical,
        )

    export_comparison_gold(
        repo_root=root,
        output_settings=out_settings,
        reader=reader,
        results_prefix_logical=results_prefix,
        gold_root_logical=gold_root,
    )
    manifest_fp = fingerprint_stored_artifact(root, reader, f"{results_prefix}/comparison_manifest.json")
    ref_name = perturbation_run_batch.get("embedding_model_name")
    ref_name_str = str(ref_name) if ref_name is not None else None
    model_info = mock_model_version_info(ref_name_str)
    return MaterializeResult(
        value={"results_prefix_logical": results_prefix, "experiment_batch": batch_key, "skipped": False},
        metadata={
            "perturbation_type": MetadataValue.text(perturbation_type),
            "experiment_batch": MetadataValue.text(batch_key),
            "results_prefix_logical": MetadataValue.text(results_prefix),
            "memoization_cache_hit": MetadataValue.bool(cache_hit),
            **runtime_metadata(started),
            **model_meta(ref_name_str),
        },
        data_version=DataVersion(
            combine_version_token(
                "comparison_results_batch",
                perturbation_type,
                batch_key,
                manifest_fp,
                str(model_info["model_version"]),
                str(model_info["model_name"]),
            )
        ),
        tags={
            "lineage/asset_role": COMPARISON_BATCH_ROLE,
            "perturbation_type": perturbation_type,
            "experiment_batch": batch_key,
            "model_version": str(model_info["model_version"]),
            "model_name": str(model_info["model_name"]),
            "cache_hit": "true" if cache_hit else "false",
        },
    )


@asset(group_name="mock_pipeline_dynamic")
def batch_manifest(config: MockPipelineConfig) -> MaterializeResult:
    batches = load_perturbation_batches(config)
    keys = [str(b["batch_key"]) for b in batches]
    return MaterializeResult(
        value={"experiment_batches": keys},
        metadata={
            "n_batches": MetadataValue.int(len(keys)),
            "experiment_batches": MetadataValue.json(keys),
            "batch_size": MetadataValue.int(max(1, int(config.perturbation_batch_size))),
        },
    )
