"""Mock foundation model runner for AnnData pipeline testing.

This module intentionally does not load a real model. It only validates that
an AnnData object can flow through the expected interface and produces an
embedding matrix with the target shape.

Example:
    uv run python src/mock_model.py --input outputs/synthetic_adata.h5ad
"""

from __future__ import annotations

import argparse
from pathlib import Path

import anndata as ad
import numpy as np
from anndata import AnnData


def run_model(adata: AnnData) -> np.ndarray:
    """Return random mock embeddings with shape (n_cells, 512)."""
    if adata.n_obs < 0:
        raise ValueError("AnnData has invalid number of observations.")

    rng = np.random.default_rng()
    return rng.normal(loc=0.0, scale=1.0, size=(adata.n_obs, 512)).astype(np.float32)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a mock foundation model and return random embeddings."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("outputs/synthetic_adata.h5ad"),
        help="Path to input AnnData (.h5ad).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    adata = ad.read_h5ad(args.input)
    embeddings = run_model(adata)

    print(f"Loaded AnnData from: {args.input}")
    print(f"Input shape: {adata.n_obs} cells x {adata.n_vars} genes")
    print(f"Embedding shape: {embeddings.shape}")


if __name__ == "__main__":
    main()
