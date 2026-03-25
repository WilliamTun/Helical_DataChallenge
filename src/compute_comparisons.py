"""Compute mock embedding comparisons for perturbation experiments.

This script compares each perturbation embedding against a healthy baseline
embedding using cosine distance (cell-wise + aggregate summaries).

Example:
    uv run python src/compute_comparisons.py \
        --run-dir outputs/perturbation_runs/20260323T175115Z \
        --input outputs/synthetic_adata.h5ad
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np

from mock_model import run_model


def _load_manifest(run_dir: Path) -> dict[str, Any]:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in run dir: {run_dir}")
    with manifest_path.open("r", encoding="utf-8") as f:
        return json.load(f)


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
    input_adata_path: Path, baseline_path: Path | None
) -> np.ndarray:
    if baseline_path is not None and baseline_path.exists():
        baseline = np.load(baseline_path)
        if baseline.ndim != 2 or baseline.shape[1] != 512:
            raise ValueError("Baseline embeddings must have shape (n_cells, 512).")
        return baseline.astype(np.float32, copy=False)

    adata = ad.read_h5ad(input_adata_path)
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
    run_dir: Path,
    input_adata_path: Path,
    results_root: Path,
    baseline_path: Path | None = None,
) -> Path:
    manifest = _load_manifest(run_dir)
    run_id = run_dir.name
    results_dir = results_root / run_id
    per_experiment_dir = results_dir / "per_experiment"
    per_experiment_dir.mkdir(parents=True, exist_ok=True)

    adata = ad.read_h5ad(input_adata_path)
    baseline_embeddings = _build_or_load_baseline(input_adata_path, baseline_path)
    np.save(results_dir / "healthy_baseline_embeddings.npy", baseline_embeddings)

    experiment_summaries: list[dict[str, Any]] = []
    for exp in manifest.get("experiments", []):
        exp_id = str(exp["experiment_id"])
        embedding_rel = exp["output_embeddings"]
        perturb_emb_path = run_dir / embedding_rel
        if not perturb_emb_path.exists():
            raise FileNotFoundError(f"Missing perturbation embedding file: {perturb_emb_path}")

        perturb_embeddings = np.load(perturb_emb_path).astype(np.float32, copy=False)
        if perturb_embeddings.shape != baseline_embeddings.shape:
            raise ValueError(
                f"Embedding shape mismatch for {exp_id}: "
                f"{perturb_embeddings.shape} vs baseline {baseline_embeddings.shape}"
            )

        distances = _cosine_distance_per_row(perturb_embeddings, baseline_embeddings)
        np.save(per_experiment_dir / f"{exp_id}_cosine_distances.npy", distances)
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
            "distance_file": str(
                (per_experiment_dir / f"{exp_id}_cosine_distances.npy").relative_to(results_dir)
            ),
        }
        experiment_summaries.append(summary)

        with (per_experiment_dir / f"{exp_id}_summary.json").open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    run_summary = {
        "computed_at_utc": datetime.now(UTC).isoformat(),
        "source_run_dir": str(run_dir),
        "baseline_source": str(baseline_path) if baseline_path else "mocked_from_input_adata",
        "input_adata": str(input_adata_path),
        "metric": "cosine_distance",
        "n_experiments": len(experiment_summaries),
        "perturbation_types_defined": manifest.get("run_metadata", {}).get(
            "perturbation_types_defined", []
        ),
        "experiments": experiment_summaries,
    }

    with (results_dir / "comparison_manifest.json").open("w", encoding="utf-8") as f:
        json.dump(run_summary, f, indent=2)

    return results_dir


def export_gold_from_silver(results_dir: Path, gold_root: Path) -> None:
    """Create UI- and compute-oriented gold artifacts from silver comparison outputs."""
    manifest_path = results_dir / "comparison_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Expected comparison manifest in results dir: {results_dir}")
    with manifest_path.open("r", encoding="utf-8") as f:
        comparison_manifest = json.load(f)

    run_id = results_dir.name
    ui_dir = gold_root / "ui_input_data" / run_id
    compute_dir = gold_root / "compute_input_data" / run_id
    ui_dir.mkdir(parents=True, exist_ok=True)
    compute_dir.mkdir(parents=True, exist_ok=True)

    # UI-facing compact cards for browsing.
    ui_cards = []
    for exp in comparison_manifest.get("experiments", []):
        ui_cards.append(
            {
                "experiment_id": exp.get("experiment_id"),
                "perturbation_type": exp.get("perturbation_type"),
                "gene": exp.get("gene"),
                "genes": exp.get("genes"),
                "involved_donors": exp.get("involved_donors", []),
                "involved_cell_types": exp.get("involved_cell_types", []),
                "target_donors": exp.get("target_donors", []),
                "target_cell_types": exp.get("target_cell_types", []),
                "mean_distance": exp.get("mean_distance"),
                "mean_distance_target_subset": exp.get("mean_distance_target_subset"),
                "n_target_cells": exp.get("n_target_cells"),
            }
        )
    with (ui_dir / "comparison_ui_cards.json").open("w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "experiments": ui_cards}, f, indent=2)

    # Compute-facing full summaries + packed distances.
    with (compute_dir / "comparison_index.json").open("w", encoding="utf-8") as f:
        json.dump(comparison_manifest, f, indent=2)

    per_experiment_src = results_dir / "per_experiment"
    if per_experiment_src.exists():
        npy_files = sorted(per_experiment_src.glob("*_cosine_distances.npy"))
        packed: dict[str, np.ndarray] = {}
        for path in npy_files:
            exp_id = path.name.replace("_cosine_distances.npy", "")
            packed[exp_id] = np.load(path).astype(np.float32, copy=False)
        if packed:
            np.savez_compressed(compute_dir / "cosine_distances_all.npz", **packed)

    baseline_src = results_dir / "healthy_baseline_embeddings.npy"
    if baseline_src.exists():
        shutil.copy2(baseline_src, compute_dir / "healthy_baseline_embeddings.npy")


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
        help="Path to a perturbation run directory containing manifest.json.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("outputs/synthetic_adata.h5ad"),
        help="Path to healthy AnnData used to mock baseline embedding if needed.",
    )
    parser.add_argument(
        "--baseline-embeddings",
        type=Path,
        default=None,
        help="Optional path to precomputed baseline embedding (.npy).",
    )
    parser.add_argument(
        "--results-root",
        type=Path,
        default=Path("outputs/silver/comparison_results"),
        help="Root folder where silver comparison outputs are written.",
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=Path("outputs/gold"),
        help="Root folder where gold UI/compute input artifacts are written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {args.run_dir}")
    if not args.input.exists():
        raise FileNotFoundError(f"Input AnnData file not found: {args.input}")

    results_dir = compute_comparisons(
        run_dir=args.run_dir,
        input_adata_path=args.input,
        results_root=args.results_root,
        baseline_path=args.baseline_embeddings,
    )
    export_gold_from_silver(results_dir=results_dir, gold_root=args.gold_root)
    print(f"Silver comparison outputs written to: {results_dir}")
    print(f"Gold artifacts written under: {args.gold_root}")


if __name__ == "__main__":
    main()




'''

What it does
- Loads a perturbation run from --run-dir (reads manifest.json)
- Gets a healthy baseline embedding by:
    - loading --baseline-embeddings if provided, or
    - mocking baseline from --input AnnData via run_model(...)
- Computes cell-wise cosine distance for each perturbation experiment
- Writes per-experiment distance arrays + summaries
- Writes a run-level comparison manifest with metadata and aggregate stats

Output location
Under outputs/comparison_results/<run_id>/:
- healthy_baseline_embeddings.npy
- comparison_manifest.json
- per_experiment/exp_0000_cosine_distances.npy
- per_experiment/exp_0000_summary.json
- etc. for each experiment

Script usage
- Default/mock baseline from AnnData:
    - uv run python src/compute_comparisons.py --run-dir outputs/perturbation_runs/20260323T175115Z --input outputs/synthetic_adata.h5ad
- Optional precomputed baseline:
    - uv run python src/compute_comparisons.py --run-dir <run_dir> --input outputs/synthetic_adata.h5ad --baseline-embeddings <path/to/baseline.npy>



'''