"""Generate synthetic AnnData for perturbation pipeline prototyping.

The output AnnData mimics single-cell gene expression with:
- realistic dimensions (scaled-down defaults)
- per-cell metadata in `.obs` (cell type, donor, batch)
- per-gene metadata in `.var` (gene symbol, chromosome)

Example:
    python src/generate_data.py --n-cells 1000 --n-genes 500 --output data/synthetic.h5ad
"""

from __future__ import annotations

import argparse
from pathlib import Path

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse


CELL_TYPES = [
    "T_cell",
    "B_cell",
    "Monocyte",
    "NK_cell",
    "Dendritic",
    "Epithelial",
]
CHROMOSOMES = [str(i) for i in range(1, 23)] + ["X", "Y"]


def _sample_categorical(
    rng: np.random.Generator, values: list[str], n: int, alpha: float = 1.0
) -> np.ndarray:
    """Sample `n` labels from `values` with random but non-uniform frequencies."""
    probs = rng.dirichlet(np.full(len(values), alpha))
    return rng.choice(values, size=n, p=probs)


def build_synthetic_adata(
    n_cells: int = 1_000,
    n_genes: int = 500,
    n_donors: int = 8,
    n_batches: int = 4,
    seed: int = 7,
) -> ad.AnnData:
    """Build a synthetic AnnData object for pipeline design and testing.

    Notes on simulation:
    - Uses a Poisson-like count model with overdispersion via a gamma-Poisson trick.
    - Introduces donor, batch, and cell-type effects.
    - Adds sparse marker structure so each cell type has elevated expression
      for a subset of genes.
    """

    if n_cells <= 0 or n_genes <= 0:
        raise ValueError("n_cells and n_genes must be positive integers.")
    if n_donors <= 0 or n_batches <= 0:
        raise ValueError("n_donors and n_batches must be positive integers.")

    rng = np.random.default_rng(seed)

    # ----- Cell metadata (.obs) -----
    donor_ids = [f"donor_{i:02d}" for i in range(1, n_donors + 1)]
    batch_ids = [f"batch_{i:02d}" for i in range(1, n_batches + 1)]

    cell_type = _sample_categorical(rng, CELL_TYPES, n_cells, alpha=0.8)
    donor = _sample_categorical(rng, donor_ids, n_cells, alpha=1.2)
    batch = _sample_categorical(rng, batch_ids, n_cells, alpha=1.5)

    obs = pd.DataFrame(
        {
            "cell_id": [f"cell_{i:06d}" for i in range(n_cells)],
            "cell_type": pd.Categorical(cell_type, categories=CELL_TYPES),
            "donor": pd.Categorical(donor, categories=donor_ids),
            "batch": pd.Categorical(batch, categories=batch_ids),
        }
    ).set_index("cell_id")

    # ----- Gene metadata (.var) -----
    gene_symbols = [f"GENE{i:04d}" for i in range(1, n_genes + 1)]
    chromosomes = rng.choice(CHROMOSOMES, size=n_genes, replace=True)
    gene_ids = [f"ENSGSIM{i:011d}" for i in range(1, n_genes + 1)]

    var = pd.DataFrame(
        {
            "gene_symbol": gene_symbols,
            "chromosome": pd.Categorical(chromosomes, categories=CHROMOSOMES),
            "gene_id": gene_ids,
            "is_target_gene": True,  # convenient flag for perturbation loops later
        },
        index=gene_symbols,
    )

    # ----- Expression counts (.X) -----
    # Global baseline per gene (log-scale), then per-cell modulation.
    gene_log_base = rng.normal(loc=1.0, scale=0.7, size=n_genes)
    gene_base = np.exp(gene_log_base)  # expected base counts per gene

    # Donor and batch effects (multiplicative in count scale).
    donor_effect_map = {
        d: rng.lognormal(mean=0.0, sigma=0.20) for d in donor_ids
    }
    batch_effect_map = {
        b: rng.lognormal(mean=0.0, sigma=0.15) for b in batch_ids
    }

    # Cell-type marker programs: each type has boosted expression in ~10% genes.
    marker_boost = np.ones((len(CELL_TYPES), n_genes), dtype=np.float32)
    marker_genes_per_type = max(5, int(0.1 * n_genes))
    for ct_idx in range(len(CELL_TYPES)):
        marker_idx = rng.choice(
            n_genes, size=marker_genes_per_type, replace=False
        )
        marker_boost[ct_idx, marker_idx] = rng.uniform(2.0, 5.0, size=marker_genes_per_type)
    ct_to_idx = {ct: i for i, ct in enumerate(CELL_TYPES)}

    # Library sizes (total molecule counts per cell), log-normal for heterogeneity.
    lib_size = rng.lognormal(mean=9.0, sigma=0.35, size=n_cells)
    lib_size = lib_size / np.median(lib_size)

    # Build expected counts matrix lambda (n_cells x n_genes) in chunks.
    chunk_size = 256
    x_counts = np.empty((n_cells, n_genes), dtype=np.int32)
    for start in range(0, n_cells, chunk_size):
        end = min(start + chunk_size, n_cells)
        rows = end - start

        ct_idx = np.array([ct_to_idx[c] for c in cell_type[start:end]])
        donor_mult = np.array([donor_effect_map[d] for d in donor[start:end]])
        batch_mult = np.array([batch_effect_map[b] for b in batch[start:end]])
        cell_mult = lib_size[start:end] * donor_mult * batch_mult

        lam = (
            gene_base[np.newaxis, :]
            * marker_boost[ct_idx, :]
            * cell_mult[:, np.newaxis]
        )

        # Gamma-Poisson (negative-binomial-like) overdispersion.
        # Smaller theta => more dispersion.
        theta = 10.0
        gamma_noise = rng.gamma(shape=theta, scale=1.0 / theta, size=lam.shape)
        lam_noisy = lam * gamma_noise
        x_counts[start:end, :] = rng.poisson(lam_noisy).astype(np.int32)

    x_sparse = sparse.csr_matrix(x_counts)

    adata = ad.AnnData(X=x_sparse, obs=obs, var=var)

    # Useful unstructured metadata for downstream pipeline bookkeeping.
    adata.uns["dataset_name"] = "synthetic_healthy_baseline"
    adata.uns["description"] = (
        "Synthetic single-cell baseline for perturbation pipeline prototyping. "
        "Scaled-down from a target production regime."
    )
    adata.uns["target_production_scale"] = {
        "n_cells": 50_000,
        "n_target_genes": 5_000,
        "perturbation_types": ["knockout", "overexpression", "activation"],
    }
    adata.uns["simulation_params"] = {
        "n_cells": int(n_cells),
        "n_genes": int(n_genes),
        "n_donors": int(n_donors),
        "n_batches": int(n_batches),
        "seed": int(seed),
    }

    return adata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic AnnData for perturbation simulations."
    )
    parser.add_argument("--n-cells", type=int, default=1_000, help="Number of cells.")
    parser.add_argument("--n-genes", type=int, default=500, help="Number of genes.")
    parser.add_argument("--n-donors", type=int, default=8, help="Number of donors.")
    parser.add_argument("--n-batches", type=int, default=4, help="Number of batches.")
    parser.add_argument("--seed", type=int, default=7, help="Random seed.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/synthetic_adata.h5ad"),
        help="Output .h5ad path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    adata = build_synthetic_adata(
        n_cells=args.n_cells,
        n_genes=args.n_genes,
        n_donors=args.n_donors,
        n_batches=args.n_batches,
        seed=args.seed,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    adata.write_h5ad(args.output)

    print(f"Saved AnnData to: {args.output}")
    print(f"Shape: {adata.n_obs} cells x {adata.n_vars} genes")
    print("obs columns:", ", ".join(adata.obs.columns))
    print("var columns:", ", ".join(adata.var.columns))


if __name__ == "__main__":
    main()





'''
Implemented src/generate_data.py as a complete synthetic AnnData generator tailored to your pipeline-design use case.

What it now does
- Builds a scaled-down but realistic baseline dataset (default 1000 cells x 500 genes)
-Creates meaningful .obs metadata:
   - cell_type
   - donor
   - batch
- Creates meaningful .var metadata:
    - gene_symbol
    -chromosome
    - gene_id
    - is_target_gene
- Simulates count-like expression with:
    - gene-level baseline variation
    - donor and batch effects
    - cell-type marker programs
    - overdispersed count noise (gamma-poisson / NB-like)
- Stores helpful context in .uns for future perturbation workflows:
    - target production scale (50k cells, 5k genes, 3 perturbation types)
    - simulation parameters
    - dataset description
- Saves to .h5ad from CLI
- Run it
    - python src/generate_data.py
- Optional:
    - python src/generate_data.py --n-cells 2000 --n-genes 1000 --output data/synthetic_adata.h5ad
'''
