"""Run perturbation experiments with a mock model on AnnData.

This script:
1) Loads synthetic AnnData.
2) Reads perturbation definitions from a JSON config.
3) Applies perturbations experiment-by-experiment.
4) Calls the mock model to produce embeddings.
5) Writes per-experiment outputs and run-level metadata artifacts.

Example:
    uv run python src/run_perturbations.py \
        --input outputs/synthetic_adata.h5ad \
        --config configs/perturbation_config.json
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
from anndata import AnnData
from scipy import sparse

from mock_model import run_model


ALLOWED_PERTURBATION_TYPES = {"gene_knockout", "gene_overexpression", "gene_activation"}


def _read_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)
    if "perturbation_types" not in config or not isinstance(config["perturbation_types"], list):
        raise ValueError("Config must include a list field: 'perturbation_types'.")
    if "perturbations" not in config or not isinstance(config["perturbations"], list):
        raise ValueError("Config must include a list field: 'perturbations'.")

    configured_types = {str(x).strip().lower() for x in config["perturbation_types"]}
    unknown = sorted(configured_types - ALLOWED_PERTURBATION_TYPES)
    if unknown:
        raise ValueError(
            "Config.perturbation_types includes unsupported values. "
            f"Allowed: {sorted(ALLOWED_PERTURBATION_TYPES)}. Got: {unknown}"
        )
    return config


def _ensure_dense_x(adata: AnnData) -> np.ndarray:
    if sparse.issparse(adata.X):
        return adata.X.toarray().astype(np.float32, copy=False)
    return np.asarray(adata.X, dtype=np.float32)


def _resolve_gene_indices(adata: AnnData, spec: dict[str, Any]) -> list[int]:
    if "genes" in spec and isinstance(spec["genes"], list):
        names = [str(g) for g in spec["genes"]]
    elif "gene" in spec and spec["gene"] is not None:
        names = [str(spec["gene"])]
    else:
        return []
    return [adata.var_names.get_loc(g) for g in names if g in adata.var_names]


def _resolve_target_row_indices(adata: AnnData, spec: dict[str, Any]) -> np.ndarray:
    mask = np.ones(adata.n_obs, dtype=bool)
    target_donors = spec.get("target_donors")
    target_cell_types = spec.get("target_cell_types")

    if isinstance(target_donors, list):
        if "donor" not in adata.obs.columns:
            raise ValueError("target_donors provided but 'donor' is missing in adata.obs.")
        donor_mask = adata.obs["donor"].astype(str).isin([str(x) for x in target_donors]).to_numpy()
        mask &= donor_mask

    if isinstance(target_cell_types, list):
        if "cell_type" not in adata.obs.columns:
            raise ValueError("target_cell_types provided but 'cell_type' is missing in adata.obs.")
        cell_type_mask = (
            adata.obs["cell_type"].astype(str).isin([str(x) for x in target_cell_types]).to_numpy()
        )
        mask &= cell_type_mask

    return np.where(mask)[0]


def _apply_expression_perturbation(adata: AnnData, spec: dict[str, Any]) -> AnnData:
    ptype = str(spec["perturbation_type"]).strip().lower()
    x = _ensure_dense_x(adata).copy()
    gene_indices = _resolve_gene_indices(adata, spec)
    row_idx = _resolve_target_row_indices(adata, spec)

    if ptype in {"gene_knockout", "gene_overexpression", "gene_activation"} and not gene_indices:
        raise ValueError(f"No valid gene(s) found in AnnData for perturbation spec: {spec}")

    if ptype == "gene_knockout":
        x[np.ix_(row_idx, gene_indices)] = 0.0
    elif ptype == "gene_overexpression":
        fold_change = float(spec.get("fold_change", 2.0))
        x[np.ix_(row_idx, gene_indices)] *= max(1.0, fold_change)
    elif ptype == "gene_activation":
        # Mock activation: allow either an additive delta or multiplicative fold_change.
        if "delta" in spec and spec["delta"] is not None:
            delta = float(spec.get("delta", 0.25))
            x[np.ix_(row_idx, gene_indices)] += delta
            x = np.clip(x, 0.0, None)
        else:
            fold_change = float(spec.get("fold_change", 1.5))
            x[np.ix_(row_idx, gene_indices)] *= max(1.0, fold_change)
    else:
        raise ValueError(f"Unknown perturbation_type: {spec['perturbation_type']}")

    out = ad.AnnData(X=x, obs=adata.obs.copy(), var=adata.var.copy(), uns=dict(adata.uns))
    out.uns["applied_perturbation"] = spec
    return out


def _involved_values(adata: AnnData, column: str) -> list[str]:
    if column not in adata.obs.columns:
        return []
    values = adata.obs[column].astype(str).dropna().unique().tolist()
    return sorted(str(v) for v in values)


def run_perturbations(
    adata: AnnData, config: dict[str, Any], run_dir: Path
) -> dict[str, np.ndarray]:
    run_dir.mkdir(parents=True, exist_ok=True)
    experiments_dir = run_dir / "experiments"
    experiments_dir.mkdir(parents=True, exist_ok=True)

    run_metadata = {
        "run_created_at_utc": datetime.now(UTC).isoformat(),
        "input_shape": {"n_cells": adata.n_obs, "n_genes": adata.n_vars},
        "perturbation_types_defined": config["perturbation_types"],
        "n_experiments": len(config["perturbations"]),
    }

    all_embeddings: dict[str, np.ndarray] = {}
    records: list[dict[str, Any]] = []

    for idx, spec in enumerate(config["perturbations"]):
        ptype = str(spec.get("perturbation_type", "")).strip().lower()
        if ptype not in {str(x).strip().lower() for x in config["perturbation_types"]}:
            raise ValueError(
                f"Perturbation '{ptype}' is not listed in config.perturbation_types."
            )

        exp_id = f"exp_{idx:04d}"
        exp_dir = experiments_dir / exp_id
        exp_dir.mkdir(parents=True, exist_ok=True)

        perturbed = _apply_expression_perturbation(adata, spec)
        embeddings = run_model(perturbed)
        involved_donors = _involved_values(perturbed, "donor")
        involved_cell_types = _involved_values(perturbed, "cell_type")
        target_donors = [str(x) for x in spec.get("target_donors", [])]
        target_cell_types = [str(x) for x in spec.get("target_cell_types", [])]
        target_row_idx = _resolve_target_row_indices(perturbed, spec)
        n_target_cells = int(target_row_idx.size)

        np.save(exp_dir / "embeddings.npy", embeddings)
        with (exp_dir / "metadata.json").open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "experiment_id": exp_id,
                    "perturbation_spec": spec,
                    "embedding_shape": list(embeddings.shape),
                    "involved_donors": involved_donors,
                    "involved_cell_types": involved_cell_types,
                    "target_donors": target_donors,
                    "target_cell_types": target_cell_types,
                    "n_target_cells": n_target_cells,
                },
                f,
                indent=2,
            )

        all_embeddings[exp_id] = embeddings
        records.append(
            {
                "experiment_id": exp_id,
                "perturbation_type": spec.get("perturbation_type"),
                "gene": spec.get("gene"),
                "genes": spec.get("genes"),
                "involved_donors": involved_donors,
                "involved_cell_types": involved_cell_types,
                "target_donors": target_donors,
                "target_cell_types": target_cell_types,
                "n_target_cells": n_target_cells,
                "output_embeddings": str((exp_dir / "embeddings.npy").relative_to(run_dir)),
            }
        )

    np.savez_compressed(run_dir / "all_embeddings.npz", **all_embeddings)
    with (run_dir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump({"run_metadata": run_metadata, "experiments": records}, f, indent=2)

    return all_embeddings


def export_gold_from_silver(run_dir: Path, gold_root: Path) -> None:
    """Create UI- and compute-oriented gold artifacts from silver outputs."""
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Expected manifest in run dir: {run_dir}")
    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    run_id = run_dir.name
    ui_dir = gold_root / "ui_input_data" / run_id
    compute_dir = gold_root / "compute_input_data" / run_id
    ui_dir.mkdir(parents=True, exist_ok=True)
    compute_dir.mkdir(parents=True, exist_ok=True)

    # UI-friendly lightweight overview records.
    ui_records = []
    for exp in manifest.get("experiments", []):
        ui_records.append(
            {
                "experiment_id": exp.get("experiment_id"),
                "perturbation_type": exp.get("perturbation_type"),
                "gene": exp.get("gene"),
                "genes": exp.get("genes"),
                "involved_donors": exp.get("involved_donors", []),
                "involved_cell_types": exp.get("involved_cell_types", []),
                "target_donors": exp.get("target_donors", []),
                "target_cell_types": exp.get("target_cell_types", []),
                "n_target_cells": exp.get("n_target_cells"),
            }
        )

    with (ui_dir / "perturbation_ui_records.json").open("w", encoding="utf-8") as f:
        json.dump(
            {"run_id": run_id, "run_metadata": manifest.get("run_metadata", {}), "experiments": ui_records},
            f,
            indent=2,
        )

    # Compute-friendly index + packed embeddings.
    all_embeddings_path = run_dir / "all_embeddings.npz"
    if all_embeddings_path.exists():
        shutil.copy2(all_embeddings_path, compute_dir / "embeddings_all.npz")

    with (compute_dir / "experiment_index.json").open("w", encoding="utf-8") as f:
        json.dump(
            {"run_id": run_id, "run_metadata": manifest.get("run_metadata", {}), "experiments": manifest.get("experiments", [])},
            f,
            indent=2,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply perturbations to AnnData and run a mock model."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("outputs/synthetic_adata.h5ad"),
        help="Path to input AnnData (.h5ad).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/perturbation_config.json"),
        help="Path to perturbation config (.json).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("outputs/silver/perturbation_runs"),
        help="Root folder where silver perturbation outputs are written.",
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
    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")
    if not args.config.exists():
        raise FileNotFoundError(f"Config file not found: {args.config}")

    adata = ad.read_h5ad(args.input)
    config = _read_config(args.config)

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / run_id

    embeddings = run_perturbations(adata=adata, config=config, run_dir=run_dir)
    export_gold_from_silver(run_dir=run_dir, gold_root=args.gold_root)
    print(f"Completed {len(embeddings)} perturbation experiments.")
    print(f"Silver run outputs written to: {run_dir}")
    print(f"Gold artifacts written under: {args.gold_root}")


if __name__ == "__main__":
    main()




'''

Implemented a full perturbation runner with config-driven perturbation types and per-experiment outputs.

Added files
- src/run_perturbations.py
-configs/perturbation_config.json

What the script does
- Loads synthetic AnnData from --input
- Reads perturbation settings/specs from --config
- Loops through perturbation specs (gene/genes + perturbation_type)
- Applies perturbation to expression (mocked behavior)
- Calls run_model(adata) from src/mock_model.py
- Saves outputs per perturbation experiment
- Writes run-level metadata including all configured perturbation types used

Output structure
For each run, a timestamped folder is created under outputs/silver/perturbation_runs/<run_id>/:

- manifest.json (run metadata + experiment index)
- all_embeddings.npz (all experiment embeddings keyed by experiment id)
- experiments/exp_0000/embeddings.npy
- experiments/exp_0000/metadata.json


manifest.json includes:
- perturbation_types_defined
-input shape
- experiment count
-mapping of each experiment to its embedding file

Config format
configs/perturbation_config.json defines:
- perturbation_types: allowed types
- perturbations: list of experiment specs
Included all requested perturbation categories (mapped to config keys):

- gene_knockout
- gene_overexpression
- gene_activation

Verified
Ran successfully:

`uv run python src/run_perturbations.py --input outputs/synthetic_adata.h5ad --config configs/perturbation_config.json`

Produced run outputs at:
`outputs/silver/perturbation_runs/20260323T175115Z`

'''