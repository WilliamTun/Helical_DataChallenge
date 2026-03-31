"""Comparison asset over typed perturbation runs."""

from datetime import timedelta
from typing import Any
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
from src.dagster.partitions import dataset_version_partitions
from src.dagster.assets.helpers.io_helpers import (
    compute_comparisons,
    export_comparison_gold,
    mock_model_version_info,
)
from src.dagster.assets.helpers.metadata_helpers import (
    compact,
    model_meta,
    runtime_metadata,
)
from src.dagster.assets.helpers.path_helpers import (
    normalize_logical_root,
    resolve_optional_baseline_path,
    resolve_dataset_version,
    resolve_io,
)
from src.dagster.assets.helpers.perturbation_helpers import perturbation_lineage


def _results_prefix_for_run(config: MockPipelineConfig, run_prefix_logical: str) -> str:
    run_id = run_prefix_logical.rstrip("/").split("/")[-1]
    return f"{normalize_logical_root(config.comparison_results_root)}/{run_id}"


def _collect_lineage_dimensions(
    cmp_manifest: dict[str, Any],
) -> tuple[list[str], list[str]]:
    _, target_cell_types, target_donors = perturbation_lineage(
        {"experiments": cmp_manifest.get("experiments", [])}
    )
    return target_cell_types, target_donors


@asset(
    group_name="mock_pipeline",
    partitions_def=dataset_version_partitions,
    freshness_policy=FreshnessPolicy.time_window(fail_window=timedelta(hours=6)),
    metadata={
        "lineage_role": MetadataValue.text("Silver comparison metrics vs baseline embeddings."),
        "lineage_layer": MetadataValue.text("silver"),
    },
)
def comparison_results(
    context: AssetExecutionContext,
    config: MockPipelineConfig,
    perturbation_run: dict[str, Any],
) -> MaterializeResult:
    started = perf_counter()
    root, out_settings, reader = resolve_io(config)
    dataset_version = resolve_dataset_version(context, config)
    baseline_logical = resolve_optional_baseline_path(config)
    comparison_results_root = normalize_logical_root(config.comparison_results_root)
    gold_root = normalize_logical_root(config.gold_root)

    runs_by_type: dict[str, dict[str, Any]] = dict(perturbation_run.get("runs_by_type", {}))
    results_by_type: dict[str, str] = {}
    target_cell_types_all: list[str] = []
    target_donors_all: list[str] = []
    fps: list[str] = []
    cache_hit_count = 0
    cache_miss_count = 0
    for ptype, run_info in runs_by_type.items():
        run_prefix = str(run_info["run_prefix_logical"])
        results_prefix = _results_prefix_for_run(config, run_prefix)
        cmp_manifest_path = f"{results_prefix}/comparison_manifest.json"
        cache_hit = bool(config.enable_memoization and reader.exists(cmp_manifest_path))
        if not cache_hit:
            results_prefix = compute_comparisons(
                repo_root=root,
                output_settings=out_settings,
                run_prefix_logical=run_prefix,
                input_adata_logical=str(run_info["synthetic_adata_logical"]),
                results_root_logical=comparison_results_root,
                baseline_logical=baseline_logical,
            )
            cache_miss_count += 1
        else:
            cache_hit_count += 1

        export_comparison_gold(
            repo_root=root,
            output_settings=out_settings,
            reader=reader,
            results_prefix_logical=results_prefix,
            gold_root_logical=gold_root,
        )
        results_by_type[ptype] = results_prefix
        cmp_manifest = reader.read_json(f"{results_prefix}/comparison_manifest.json")
        cts, dns = _collect_lineage_dimensions(cmp_manifest)
        target_cell_types_all.extend(cts)
        target_donors_all.extend(dns)
        fps.append(fingerprint_stored_artifact(root, reader, f"{results_prefix}/comparison_manifest.json"))

    model_info = mock_model_version_info()
    perturbation_types = sorted(results_by_type.keys())
    target_cell_types = sorted(set(target_cell_types_all))
    target_donors = sorted(set(target_donors_all))
    return MaterializeResult(
        value={"results_by_type": results_by_type},
        metadata={
            "dataset_version": MetadataValue.text(dataset_version),
            "n_result_sets": MetadataValue.int(len(results_by_type)),
            "memoization_cache_hits": MetadataValue.int(cache_hit_count),
            "memoization_cache_misses": MetadataValue.int(cache_miss_count),
            "results_prefixes_by_type": MetadataValue.json(results_by_type),
            "perturbation_types": MetadataValue.json(perturbation_types),
            "target_cell_types": MetadataValue.json(target_cell_types),
            "target_donors": MetadataValue.json(target_donors),
            **runtime_metadata(started),
            **model_meta(),
        },
        data_version=DataVersion(
            combine_version_token(
                "comparison_results",
                dataset_version,
                *sorted(fps),
                str(model_info["model_version"]),
            )
        ),
        tags={
            "lineage/asset_role": "comparison_silver",
            "dataset_version": dataset_version,
            "model_version": str(model_info["model_version"]),
            "cache_hits": str(cache_hit_count),
            "cache_misses": str(cache_miss_count),
            "perturbation_types": compact(perturbation_types),
            "target_cell_types": compact(target_cell_types),
            "target_donors": compact(target_donors),
        },
    )
