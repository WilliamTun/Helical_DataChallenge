"""Perturbation-specific helpers for Dagster assets."""

from typing import Any

from dagster import AssetExecutionContext, DataVersion, MaterializeResult, MetadataValue

from src.dagster.config import MockPipelineConfig
from src.dagster.lineage.fingerprints import (
    combine_version_token,
    fingerprint_local_file,
    fingerprint_stored_artifact,
)
from src.dagster.assets.helpers.io_helpers import (
    _read_config,
    export_perturbation_gold,
    mock_model_version_info,
    read_h5ad,
    run_perturbations,
)
from src.dagster.assets.helpers.metadata_helpers import compact, model_meta, runtime_metadata, slug
from src.dagster.assets.helpers.path_helpers import normalize_logical_root, resolve_dataset_version, resolve_io


def perturbation_lineage(manifest: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    exps = list(manifest.get("experiments", []))
    ptypes = sorted(
        {str(exp.get("perturbation_type")) for exp in exps if exp.get("perturbation_type") is not None}
    )
    cell_types = sorted({str(ct) for exp in exps for ct in (exp.get("target_cell_types") or [])})
    donors = sorted({str(d) for exp in exps for d in (exp.get("target_donors") or [])})
    return ptypes, cell_types, donors


def load_perturbation_batches(config: MockPipelineConfig) -> list[dict[str, Any]]:
    root, _, _ = resolve_io(config)
    cfg = _read_config(root / config.perturbation_config_path)
    specs = list(cfg.get("perturbations", []))
    batch_size = max(1, int(config.perturbation_batch_size))
    batches: list[dict[str, Any]] = []
    for i in range(0, len(specs), batch_size):
        chunk = specs[i : i + batch_size]
        batch_idx = i // batch_size
        types = sorted(
            {
                str(spec.get("perturbation_type", "")).strip().lower()
                for spec in chunk
                if spec.get("perturbation_type") is not None
            }
        )
        batches.append(
            {
                "batch_key": f"batch_{batch_idx:04d}",
                "batch_index": batch_idx,
                "perturbations": chunk,
                "perturbation_types": types,
            }
        )
    return batches


def get_batch_by_key(config: MockPipelineConfig, batch_key: str) -> dict[str, Any] | None:
    for batch in load_perturbation_batches(config):
        if str(batch["batch_key"]) == batch_key:
            return batch
    return None


def run_perturbation_type(
    context: AssetExecutionContext,
    config: MockPipelineConfig,
    synthetic_adata: str,
    perturbation_type: str,
) -> MaterializeResult:
    from time import perf_counter

    started = perf_counter()
    root, out_settings, reader = resolve_io(config)
    dataset_version = resolve_dataset_version(context, config)
    adata = read_h5ad(reader, synthetic_adata)
    cfg = _read_config(root / config.perturbation_config_path)
    filtered = [
        spec
        for spec in cfg.get("perturbations", [])
        if str(spec.get("perturbation_type", "")).strip().lower() == perturbation_type
    ]
    model_info = mock_model_version_info()
    if not filtered:
        out = {
            "run_prefix_logical": None,
            "synthetic_adata_logical": synthetic_adata,
            "perturbation_type": perturbation_type,
            "skipped": True,
        }
        return MaterializeResult(
            value=out,
            metadata={
                "dataset_version": MetadataValue.text(dataset_version),
                "perturbation_type": MetadataValue.text(perturbation_type),
                "n_experiments": MetadataValue.int(0),
                "status": MetadataValue.text("skipped_no_matching_specs"),
                **runtime_metadata(started),
                **model_meta(),
            },
            data_version=DataVersion(
                combine_version_token("perturbation_run", dataset_version, perturbation_type, "empty")
            ),
            tags={
                "lineage/asset_role": "embeddings_silver",
                "dataset_version": dataset_version,
                "perturbation_type": perturbation_type,
                "model_version": str(model_info["model_version"]),
                "status": "skipped",
            },
        )

    sub_cfg = {"perturbation_types": [perturbation_type], "perturbations": filtered}
    perturb_root = normalize_logical_root(config.perturbation_runs_root)
    dataset_fp = fingerprint_stored_artifact(root, reader, synthetic_adata)
    cfg_fp = fingerprint_local_file(root, f"{config.perturbation_config_path}")
    memo_token = combine_version_token(
        "perturbation_typed",
        dataset_version,
        perturbation_type,
        dataset_fp,
        cfg_fp,
        str(model_info["model_version"]),
    )
    run_id = f"{slug(dataset_version)}__{perturbation_type}__{memo_token[:12]}"
    run_prefix = f"{perturb_root}/{run_id}"
    manifest_path = f"{run_prefix}/manifest.json"
    cache_hit = bool(config.enable_memoization and reader.exists(manifest_path))

    if not cache_hit:
        run_perturbations(
            adata=adata,
            config=sub_cfg,
            run_id=run_id,
            repo_root=root,
            output_settings=out_settings,
            perturbation_runs_root_logical=perturb_root,
        )
    else:
        context.log.info("Cache hit for [%s], reusing run: %s", perturbation_type, run_prefix)

    export_perturbation_gold(
        repo_root=root,
        output_settings=out_settings,
        reader=reader,
        run_prefix_logical=run_prefix,
        gold_root_logical=normalize_logical_root(config.gold_root),
    )

    manifest = reader.read_json(manifest_path)
    npz_path = f"{run_prefix}/all_embeddings.npz"
    emb_fp = fingerprint_stored_artifact(root, reader, npz_path)
    ptypes, target_cell_types, target_donors = perturbation_lineage(manifest)
    n_exp = len(manifest.get("experiments", []))
    out = {
        "run_prefix_logical": run_prefix,
        "synthetic_adata_logical": synthetic_adata,
        "perturbation_type": perturbation_type,
        "skipped": False,
    }
    return MaterializeResult(
        value=out,
        metadata={
            "run_id": MetadataValue.text(run_id),
            "run_prefix_logical": MetadataValue.text(run_prefix),
            "dataset_version": MetadataValue.text(dataset_version),
            "memoization_token": MetadataValue.text(memo_token),
            "memoization_cache_hit": MetadataValue.bool(cache_hit),
            "input_dataset_fingerprint": MetadataValue.text(dataset_fp),
            "embedding_bundle_fingerprint": MetadataValue.text(emb_fp),
            "perturbation_config_fingerprint": MetadataValue.text(cfg_fp),
            "n_experiments": MetadataValue.int(n_exp),
            "perturbation_type": MetadataValue.text(perturbation_type),
            "perturbation_types": MetadataValue.json(ptypes),
            "target_cell_types": MetadataValue.json(target_cell_types),
            "target_donors": MetadataValue.json(target_donors),
            **runtime_metadata(started),
            **model_meta(),
        },
        data_version=DataVersion(
            combine_version_token(
                "perturbation_run",
                dataset_version,
                perturbation_type,
                run_id,
                dataset_fp,
                emb_fp,
                str(model_info["model_version"]),
            )
        ),
        tags={
            "lineage/asset_role": "embeddings_silver",
            "dataset_version": dataset_version,
            "model_version": str(model_info["model_version"]),
            "run_id": run_id,
            "perturbation_type": perturbation_type,
            "cache_hit": "true" if cache_hit else "false",
            "target_cell_types": compact(target_cell_types),
            "target_donors": compact(target_donors),
        },
    )


__all__ = [
    "get_batch_by_key",
    "load_perturbation_batches",
    "perturbation_lineage",
    "run_perturbation_type",
]
