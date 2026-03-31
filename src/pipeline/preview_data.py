"""Preview and summarize AnnData files for quick inspection.

Example:
    uv run python src/pipeline/preview_data.py --input outputs/synthetic_adata.h5ad
"""

from __future__ import annotations

import argparse
from pathlib import Path

import anndata as ad

from src.pipeline.helpers.artifact_io import read_h5ad, resolve_io_for_cli, to_logical_from_user_path
import numpy as np
import pandas as pd
from scipy import sparse


def _series_stats(values: np.ndarray, name: str) -> pd.Series:
    """Return common descriptive statistics for numeric arrays."""
    s = pd.Series(values, name=name)
    return s.describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])


def _print_header(title: str) -> None:
    print(f"\n{'=' * 12} {title} {'=' * 12}")


def _print_dataframe(df: pd.DataFrame, max_rows: int = 10) -> None:
    if df.empty:
        print("(empty)")
        return
    with pd.option_context("display.max_rows", max_rows, "display.max_columns", None):
        print(df.head(max_rows).to_string())


def _is_categorical_or_object(series: pd.Series) -> bool:
    return isinstance(series.dtype, pd.CategoricalDtype) or series.dtype == object


def summarize_adata(adata: ad.AnnData, max_categories: int = 20) -> None:
    """Print a human-readable summary of an AnnData object."""
    _print_header("Basic Shape")
    print(f"Cells (n_obs):  {adata.n_obs:,}")
    print(f"Genes (n_vars): {adata.n_vars:,}")
    print(f"X dtype:        {adata.X.dtype}")
    print(f"X sparse:       {sparse.issparse(adata.X)}")

    # Matrix-level summary
    if sparse.issparse(adata.X):
        nnz = int(adata.X.nnz)
        total = adata.n_obs * adata.n_vars
        density = nnz / total if total > 0 else 0.0
        x_min = float(adata.X.data.min()) if nnz > 0 else 0.0
        x_max = float(adata.X.data.max()) if nnz > 0 else 0.0
    else:
        arr = np.asarray(adata.X)
        nnz = int(np.count_nonzero(arr))
        total = arr.size
        density = nnz / total if total > 0 else 0.0
        x_min = float(arr.min()) if total > 0 else 0.0
        x_max = float(arr.max()) if total > 0 else 0.0

    _print_header("Expression Matrix Overview")
    print(f"Non-zero entries: {nnz:,}")
    print(f"Density:          {density:.4f}")
    print(f"Sparsity:         {1.0 - density:.4f}")
    print(f"Value range:      [{x_min:.3f}, {x_max:.3f}]")

    # Per-cell and per-gene totals
    if sparse.issparse(adata.X):
        counts_per_cell = np.asarray(adata.X.sum(axis=1)).ravel()
        counts_per_gene = np.asarray(adata.X.sum(axis=0)).ravel()
        detected_genes_per_cell = np.asarray((adata.X > 0).sum(axis=1)).ravel()
        detected_cells_per_gene = np.asarray((adata.X > 0).sum(axis=0)).ravel()
    else:
        arr = np.asarray(adata.X)
        counts_per_cell = arr.sum(axis=1)
        counts_per_gene = arr.sum(axis=0)
        detected_genes_per_cell = (arr > 0).sum(axis=1)
        detected_cells_per_gene = (arr > 0).sum(axis=0)

    _print_header("Cell-Level Statistics")
    print(_series_stats(counts_per_cell, "total_counts_per_cell").to_string())
    print("\nDetected genes per cell:")
    print(_series_stats(detected_genes_per_cell, "detected_genes_per_cell").to_string())

    _print_header("Gene-Level Statistics")
    print(_series_stats(counts_per_gene, "total_counts_per_gene").to_string())
    print("\nDetected cells per gene:")
    print(_series_stats(detected_cells_per_gene, "detected_cells_per_gene").to_string())

    # Metadata overview
    _print_header("obs Metadata")
    print(f"obs columns ({len(adata.obs.columns)}): {list(adata.obs.columns)}")
    for col in adata.obs.columns:
        series = adata.obs[col]
        if _is_categorical_or_object(series):
            vc = series.value_counts(dropna=False).head(max_categories)
            print(f"\n- {col} (top categories):")
            print(vc.to_string())
        elif pd.api.types.is_bool_dtype(series):
            print(f"\n- {col} (boolean counts):")
            print(series.value_counts(dropna=False).to_string())
        elif pd.api.types.is_numeric_dtype(series):
            print(f"\n- {col} (numeric describe):")
            print(series.describe().to_string())
        else:
            print(f"\n- {col} (dtype={series.dtype})")

    _print_header("var Metadata")
    print(f"var columns ({len(adata.var.columns)}): {list(adata.var.columns)}")
    for col in adata.var.columns:
        series = adata.var[col]
        if _is_categorical_or_object(series):
            vc = series.value_counts(dropna=False).head(max_categories)
            print(f"\n- {col} (top categories):")
            print(vc.to_string())
        elif pd.api.types.is_bool_dtype(series):
            print(f"\n- {col} (boolean counts):")
            print(series.value_counts(dropna=False).to_string())
        elif pd.api.types.is_numeric_dtype(series):
            print(f"\n- {col} (numeric describe):")
            print(series.describe().to_string())
        else:
            print(f"\n- {col} (dtype={series.dtype})")

    _print_header("uns Keys")
    uns_keys = list(adata.uns.keys())
    print(f"uns keys ({len(uns_keys)}): {uns_keys}")
    for key in uns_keys:
        value = adata.uns[key]
        if isinstance(value, dict):
            print(f"- {key}: dict with {len(value)} keys")
        else:
            print(f"- {key}: {type(value).__name__}")

    _print_header("Preview Rows")
    print("obs head:")
    _print_dataframe(adata.obs, max_rows=5)
    print("\nvar head:")
    _print_dataframe(adata.var, max_rows=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview AnnData dimensions and descriptive statistics."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("outputs/synthetic_adata.h5ad"),
        help="Path to .h5ad file.",
    )
    parser.add_argument(
        "--max-categories",
        type=int,
        default=20,
        help="Maximum category counts shown per obs/var categorical column.",
    )
    parser.add_argument(
        "--pipeline-config",
        type=Path,
        default=Path("configs/pipeline_config.json"),
        help="Pipeline output backends (for reading AnnData from local or DuckDB).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo, _, reader = resolve_io_for_cli(args.pipeline_config)

    logical = to_logical_from_user_path(args.input, repo)
    if not reader.exists(logical):
        raise FileNotFoundError(f"Input not found at logical path: {logical}")

    adata = read_h5ad(reader, logical)
    print(f"Loaded AnnData from: {logical}")
    summarize_adata(adata, max_categories=args.max_categories)


if __name__ == "__main__":
    main()