"""Run perturbation experiments with a mock model on AnnData.

This script:
1) Loads synthetic AnnData.
2) Reads perturbation definitions from a JSON config.
3) Applies perturbations experiment-by-experiment.
4) Calls the mock model to produce embeddings.
5) Writes per-experiment outputs and run-level metadata artifacts.

Example:
    uv run python src/pipeline/run_perturbations.py \
        --input outputs/synthetic_adata.h5ad \
        --config configs/perturbation_config.json
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
from anndata import AnnData
from scipy import sparse

from src.pipeline.helpers.artifact_io import (
    ensure_logical_exists,
    GlobalReader,
    make_writer,
    read_h5ad,
    resolve_io_for_cli,
    run_id_from_logical_prefix,
    to_logical_from_user_path,
    write_json,
    write_npz,
    write_npy,
)
from src.pipeline.helpers.gold_export_helpers import perturbation_ui_record, run_scoped_payload
from src.pipeline.helpers.io_config import PipelineOutputSettings
from src.pipeline.helpers.mock_model import run_model


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
    adata: AnnData,
    config: dict[str, Any],
    run_id: str,
    repo_root: Path,
    output_settings: PipelineOutputSettings,
    perturbation_runs_root_logical: str = "outputs/silver/perturbation_runs",
) -> dict[str, np.ndarray]:
    writer = make_writer(repo_root, output_settings, "run_perturbations")
    run_prefix = f"{perturbation_runs_root_logical.rstrip('/')}/{run_id}"

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
        exp_prefix = f"{run_prefix}/experiments/{exp_id}"

        perturbed = _apply_expression_perturbation(adata, spec)
        embeddings = run_model(perturbed)
        involved_donors = _involved_values(perturbed, "donor")
        involved_cell_types = _involved_values(perturbed, "cell_type")
        target_donors = [str(x) for x in spec.get("target_donors", [])]
        target_cell_types = [str(x) for x in spec.get("target_cell_types", [])]
        target_row_idx = _resolve_target_row_indices(perturbed, spec)
        n_target_cells = int(target_row_idx.size)

        write_npy(writer, f"{exp_prefix}/embeddings.npy", embeddings)
        write_json(
            writer,
            f"{exp_prefix}/metadata.json",
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
                "output_embeddings": f"experiments/{exp_id}/embeddings.npy",
            }
        )

    write_npz(writer, f"{run_prefix}/all_embeddings.npz", all_embeddings)
    write_json(writer, f"{run_prefix}/manifest.json", {"run_metadata": run_metadata, "experiments": records})

    return all_embeddings


def export_gold_from_silver(
    repo_root: Path,
    output_settings: PipelineOutputSettings,
    reader: GlobalReader,
    run_prefix_logical: str,
    gold_root_logical: str,
) -> None:
    """Create UI- and compute-oriented gold artifacts from silver outputs."""
    manifest_path = f"{run_prefix_logical}/manifest.json"
    ensure_logical_exists(reader, manifest_path, "Manifest")
    manifest = reader.read_json(manifest_path)

    run_id = run_id_from_logical_prefix(run_prefix_logical)
    writer = make_writer(repo_root, output_settings, "run_perturbations")

    ui_records = [perturbation_ui_record(exp) for exp in manifest.get("experiments", [])]

    gold_base = gold_root_logical.rstrip("/")
    write_json(
        writer,
        f"{gold_base}/ui_input_data/{run_id}/perturbation_ui_records.json",
        run_scoped_payload(
            run_id=run_id,
            run_metadata=manifest.get("run_metadata", {}),
            experiments=ui_records,
        ),
    )

    npz_path = f"{run_prefix_logical}/all_embeddings.npz"
    if reader.exists(npz_path):
        writer.write_bytes(
            f"{gold_base}/compute_input_data/{run_id}/embeddings_all.npz",
            reader.read_bytes(npz_path),
        )

    write_json(
        writer,
        f"{gold_base}/compute_input_data/{run_id}/experiment_index.json",
        run_scoped_payload(
            run_id=run_id,
            run_metadata=manifest.get("run_metadata", {}),
            experiments=list(manifest.get("experiments", [])),
        ),
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
        "--pipeline-config",
        type=Path,
        default=Path("configs/pipeline_config.json"),
        help="Pipeline output backends (local_folder vs duckdb). Ignored if file is missing.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("outputs/silver/perturbation_runs"),
        help="Root (under repo) where silver perturbation outputs are written.",
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

    input_logical = to_logical_from_user_path(args.input, repo)
    ensure_logical_exists(reader, input_logical, "Input AnnData")
    if not args.config.exists():
        raise FileNotFoundError(f"Config file not found: {args.config}")

    adata = read_h5ad(reader, input_logical)
    config = _read_config(args.config)

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    perturb_root_logical = to_logical_from_user_path(args.output_root, repo)
    gold_root_logical = to_logical_from_user_path(args.gold_root, repo)

    embeddings = run_perturbations(
        adata=adata,
        config=config,
        run_id=run_id,
        repo_root=repo,
        output_settings=out_settings,
        perturbation_runs_root_logical=perturb_root_logical,
    )
    run_prefix = f"{perturb_root_logical}/{run_id}"
    export_gold_from_silver(
        repo_root=repo,
        output_settings=out_settings,
        reader=reader,
        run_prefix_logical=run_prefix,
        gold_root_logical=gold_root_logical,
    )
    print(f"Completed {len(embeddings)} perturbation experiments.")
    print(f"Silver run logical prefix: {run_prefix}")
    print(f"Gold artifacts under logical root: {gold_root_logical}")


if __name__ == "__main__":
    main()