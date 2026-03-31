"""Compute mock embedding comparisons for perturbation experiments.

This script compares each perturbation embedding against a healthy baseline
embedding using cosine distance (cell-wise + aggregate summaries).

Example:
    uv run python src/pipeline/compute_comparisons.py \
        --run-dir outputs/perturbation_runs/20260323T175115Z \
        --input outputs/synthetic_adata.h5ad
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np

from src.pipeline.helpers.artifact_io import (
    ensure_logical_exists,
    GlobalReader,
    make_reader,
    make_writer,
    read_h5ad,
    read_npy,
    resolve_io_for_cli,
    run_id_from_logical_prefix,
    to_logical_from_user_path,
    to_optional_logical_from_user_path,
    write_json,
    write_npz,
    write_npy,
)
from src.pipeline.helpers.gold_export_helpers import comparison_ui_card, run_scoped_payload
from src.pipeline.helpers.io_config import PipelineOutputSettings
from src.pipeline.helpers.mock_model import run_model


def _load_manifest(reader: GlobalReader, run_prefix_logical: str) -> dict[str, Any]:
    manifest_path = f"{run_prefix_logical}/manifest.json"
    ensure_logical_exists(reader, manifest_path, "manifest.json")
    return reader.read_json(manifest_path)


def _cosine_distance_per_row(a: np.ndarray, b: np.ndarray, eps: float = 1e-8) -> np.ndarray:
    if a.shape != b.shape:
        raise ValueError(f"Shape mismatch for cosine distance: {a.shape} vs {b.shape}")
    a_norm = np.linalg.norm(a, axis=1)
    b_norm = np.linalg.norm(b, axis=1)
    denom = np.maximum(a_norm * b_norm, eps)
    cosine_sim = np.sum(a * b, axis=1) / denom
    cosine_sim = np.clip(cosine_sim, -1.0, 1.0)
    return 1.0 - cosine_sim


def _build_or_load_baseline(
    reader: GlobalReader,
    input_adata_logical: str,
    baseline_logical: str | None,
) -> np.ndarray:
    if baseline_logical is not None and reader.exists(baseline_logical):
        baseline = read_npy(reader, baseline_logical)
        if baseline.ndim != 2 or baseline.shape[1] != 512:
            raise ValueError("Baseline embeddings must have shape (n_cells, 512).")
        return baseline.astype(np.float32, copy=False)

    adata = read_h5ad(reader, input_adata_logical)
    baseline = run_model(adata)
    return baseline.astype(np.float32, copy=False)


def _resolve_target_mask_from_manifest_exp(
    adata: ad.AnnData, exp: dict[str, Any]
) -> np.ndarray:
    mask = np.ones(adata.n_obs, dtype=bool)
    target_donors = exp.get("target_donors", [])
    target_cell_types = exp.get("target_cell_types", [])

    if isinstance(target_donors, list) and len(target_donors) > 0:
        if "donor" not in adata.obs.columns:
            return np.zeros(adata.n_obs, dtype=bool)
        mask &= adata.obs["donor"].astype(str).isin([str(x) for x in target_donors]).to_numpy()

    if isinstance(target_cell_types, list) and len(target_cell_types) > 0:
        if "cell_type" not in adata.obs.columns:
            return np.zeros(adata.n_obs, dtype=bool)
        mask &= adata.obs["cell_type"].astype(str).isin([str(x) for x in target_cell_types]).to_numpy()

    return mask


def compute_comparisons(
    repo_root: Path,
    output_settings: PipelineOutputSettings,
    run_prefix_logical: str,
    input_adata_logical: str,
    results_root_logical: str,
    baseline_logical: str | None = None,
) -> str:
    reader = make_reader(repo_root, output_settings)
    writer = make_writer(repo_root, output_settings, "compute_comparisons")

    manifest = _load_manifest(reader, run_prefix_logical)
    run_id = run_id_from_logical_prefix(run_prefix_logical)
    results_prefix = f"{results_root_logical.rstrip('/')}/{run_id}"
    per_experiment_prefix = f"{results_prefix}/per_experiment"

    adata = read_h5ad(reader, input_adata_logical)
    baseline_embeddings = _build_or_load_baseline(reader, input_adata_logical, baseline_logical)
    write_npy(writer, f"{results_prefix}/healthy_baseline_embeddings.npy", baseline_embeddings)

    experiment_summaries: list[dict[str, Any]] = []
    for exp in manifest.get("experiments", []):
        exp_id = str(exp["experiment_id"])
        embedding_rel = exp["output_embeddings"]
        perturb_emb_logical = f"{run_prefix_logical}/{embedding_rel}"
        if not reader.exists(perturb_emb_logical):
            raise FileNotFoundError(f"Missing perturbation embedding: {perturb_emb_logical}")

        perturb_embeddings = read_npy(reader, perturb_emb_logical).astype(np.float32, copy=False)
        if perturb_embeddings.shape != baseline_embeddings.shape:
            raise ValueError(
                f"Embedding shape mismatch for {exp_id}: "
                f"{perturb_embeddings.shape} vs baseline {baseline_embeddings.shape}"
            )

        distances = _cosine_distance_per_row(perturb_embeddings, baseline_embeddings)
        write_npy(writer, f"{per_experiment_prefix}/{exp_id}_cosine_distances.npy", distances)
        target_mask = _resolve_target_mask_from_manifest_exp(adata, exp)
        target_distances = distances[target_mask]

        summary = {
            "experiment_id": exp_id,
            "perturbation_type": exp.get("perturbation_type"),
            "gene": exp.get("gene"),
            "genes": exp.get("genes"),
            "involved_donors": exp.get("involved_donors", []),
            "involved_cell_types": exp.get("involved_cell_types", []),
            "target_donors": exp.get("target_donors", []),
            "target_cell_types": exp.get("target_cell_types", []),
            "n_cells": int(distances.shape[0]),
            "n_target_cells": int(target_distances.shape[0]),
            "metric": "cosine_distance",
            "mean_distance": float(np.mean(distances)),
            "median_distance": float(np.median(distances)),
            "std_distance": float(np.std(distances)),
            "min_distance": float(np.min(distances)),
            "max_distance": float(np.max(distances)),
            "mean_distance_target_subset": (
                float(np.mean(target_distances)) if target_distances.size > 0 else None
            ),
            "median_distance_target_subset": (
                float(np.median(target_distances)) if target_distances.size > 0 else None
            ),
            "distance_file": f"per_experiment/{exp_id}_cosine_distances.npy",
        }
        experiment_summaries.append(summary)

        write_json(writer, f"{per_experiment_prefix}/{exp_id}_summary.json", summary)

    run_summary = {
        "computed_at_utc": datetime.now(UTC).isoformat(),
        "source_run_prefix_logical": run_prefix_logical,
        "baseline_source": baseline_logical if baseline_logical else "mocked_from_input_adata",
        "input_adata_logical": input_adata_logical,
        "metric": "cosine_distance",
        "n_experiments": len(experiment_summaries),
        "perturbation_types_defined": manifest.get("run_metadata", {}).get(
            "perturbation_types_defined", []
        ),
        "experiments": experiment_summaries,
    }

    write_json(writer, f"{results_prefix}/comparison_manifest.json", run_summary)

    return results_prefix


def export_gold_from_silver(
    repo_root: Path,
    output_settings: PipelineOutputSettings,
    reader: GlobalReader,
    results_prefix_logical: str,
    gold_root_logical: str,
) -> None:
    """Create UI- and compute-oriented gold artifacts from silver comparison outputs."""
    manifest_path = f"{results_prefix_logical}/comparison_manifest.json"
    ensure_logical_exists(reader, manifest_path, "Comparison manifest")
    comparison_manifest = reader.read_json(manifest_path)

    run_id = run_id_from_logical_prefix(results_prefix_logical)
    writer = make_writer(repo_root, output_settings, "compute_comparisons")
    gold_base = gold_root_logical.rstrip("/")

    ui_cards = [comparison_ui_card(exp) for exp in comparison_manifest.get("experiments", [])]
    write_json(
        writer,
        f"{gold_base}/ui_input_data/{run_id}/comparison_ui_cards.json",
        run_scoped_payload(run_id=run_id, experiments=ui_cards),
    )

    write_json(
        writer,
        f"{gold_base}/compute_input_data/{run_id}/comparison_index.json",
        comparison_manifest,
    )

    packed: dict[str, np.ndarray] = {}
    for exp in comparison_manifest.get("experiments", []):
        exp_id = str(exp["experiment_id"])
        dist_rel = exp.get("distance_file")
        if not dist_rel:
            continue
        dist_logical = f"{results_prefix_logical}/{dist_rel}"
        if reader.exists(dist_logical):
            packed[exp_id] = read_npy(reader, dist_logical).astype(np.float32, copy=False)
    if packed:
        write_npz(writer, f"{gold_base}/compute_input_data/{run_id}/cosine_distances_all.npz", packed)

    baseline_logical = f"{results_prefix_logical}/healthy_baseline_embeddings.npy"
    if reader.exists(baseline_logical):
        writer.write_bytes(
            f"{gold_base}/compute_input_data/{run_id}/healthy_baseline_embeddings.npy",
            reader.read_bytes(baseline_logical),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute mock cosine-distance comparisons between perturbation embeddings "
            "and healthy baseline embeddings."
        )
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="Perturbation run directory (under repo) containing manifest.json.",
    )
    parser.add_argument(
        "--pipeline-config",
        type=Path,
        default=Path("configs/pipeline_config.json"),
        help="Pipeline output backends (local_folder vs duckdb). Ignored if file is missing.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("outputs/synthetic_adata.h5ad"),
        help="Healthy AnnData path (under repo) for baseline embedding if needed.",
    )
    parser.add_argument(
        "--baseline-embeddings",
        type=Path,
        default=None,
        help="Optional baseline embedding .npy path (under repo).",
    )
    parser.add_argument(
        "--results-root",
        type=Path,
        default=Path("outputs/silver/comparison_results"),
        help="Root (under repo) where silver comparison outputs are written.",
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=Path("outputs/gold"),
        help="Root (under repo) where gold UI/compute input artifacts are written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo, out_settings, reader = resolve_io_for_cli(args.pipeline_config)

    run_prefix_logical = to_logical_from_user_path(args.run_dir, repo)
    ensure_logical_exists(reader, f"{run_prefix_logical}/manifest.json", "Run manifest")

    input_logical = to_logical_from_user_path(args.input, repo)
    ensure_logical_exists(reader, input_logical, "Input AnnData")

    baseline_logical = to_optional_logical_from_user_path(args.baseline_embeddings, repo)

    results_root_logical = to_logical_from_user_path(args.results_root, repo)
    gold_root_logical = to_logical_from_user_path(args.gold_root, repo)

    results_prefix = compute_comparisons(
        repo_root=repo,
        output_settings=out_settings,
        run_prefix_logical=run_prefix_logical,
        input_adata_logical=input_logical,
        results_root_logical=results_root_logical,
        baseline_logical=baseline_logical,
    )
    export_gold_from_silver(
        repo_root=repo,
        output_settings=out_settings,
        reader=reader,
        results_prefix_logical=results_prefix,
        gold_root_logical=gold_root_logical,
    )
    print(f"Silver comparison outputs logical prefix: {results_prefix}")
    print(f"Gold artifacts under logical root: {gold_root_logical}")


if __name__ == "__main__":
    main()