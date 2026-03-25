# Project Workflow Design

This project is a mock end-to-end pipeline for testing data structures and orchestration logic around perturbation experiments on single-cell `AnnData`.

All model outputs and comparisons are intentionally mocked so we can validate pipeline behavior before integrating a real foundation model.

## 1) `generate_data.py`

**Goal:** Create synthetic baseline single-cell data.

- Builds an `AnnData` object with:
  - expression matrix (`.X`)
  - cell metadata (`.obs`, e.g. `cell_type`, `donor`, `batch`)
  - gene metadata (`.var`, e.g. `gene_symbol`, `gene_id`)
  - run context metadata (`.uns`)
- Saves synthetic dataset to `.h5ad`.

**Typical output:**
- `outputs/synthetic_adata.h5ad`

**Example:**
- `uv run python src/generate_data.py --output outputs/synthetic_adata.h5ad`

---

## 2) `preview_data.py`

**Goal:** Inspect and sanity-check the generated `AnnData`.

- Loads `.h5ad`
- Prints:
  - shape and matrix density/sparsity
  - per-cell and per-gene summary stats
  - metadata summaries from `.obs`, `.var`, `.uns`

This is a QA/debug step before running perturbation simulations.

**Example:**
- `uv run python src/preview_data.py --input outputs/synthetic_adata.h5ad`

---

## 3) `run_perturbations.py`

**Goal:** Simulate perturbation experiments and produce mock embeddings.

- Reads perturbation config from:
  - `configs/perturbation_config.json`
- For each perturbation spec (gene(s) + perturbation type):
  1. applies a mocked expression perturbation
  2. calls `run_model(adata)` from `mock_model.py`
  3. stores embeddings and experiment metadata
- Supports optional targeted in-silico cohorts per perturbation spec:
  - `target_donors` (e.g. `["donor_03"]`)
  - `target_cell_types` (e.g. `["T_cell"]`)
  - If provided, perturbation is applied only to matching cells.

**Supported perturbation categories (config-defined):**
- gene knockout (`gene_knockout`)
- gene overexpression (`gene_overexpression`)
- gene activation (`gene_activation`)

**Run outputs (timestamped):**
- Silver:
  - `outputs/silver/perturbation_runs/<run_id>/manifest.json`
  - `outputs/silver/perturbation_runs/<run_id>/all_embeddings.npz`
  - `outputs/silver/perturbation_runs/<run_id>/experiments/exp_XXXX/embeddings.npy`
  - `outputs/silver/perturbation_runs/<run_id>/experiments/exp_XXXX/metadata.json`
- Gold derivatives:
  - `outputs/gold/ui_input_data/<run_id>/perturbation_ui_records.json`
  - `outputs/gold/compute_input_data/<run_id>/experiment_index.json`
  - `outputs/gold/compute_input_data/<run_id>/embeddings_all.npz`

**Per-experiment metadata includes:**
- `gene` / `genes`
- `perturbation_type`
- `involved_donors`, `involved_cell_types`
- `target_donors`, `target_cell_types`
- `n_target_cells`

**Example:**
- `uv run python src/run_perturbations.py --input outputs/synthetic_adata.h5ad --config configs/perturbation_config.json`

---

## 4) `compute_comparisons.py`

**Goal:** Compare each perturbation embedding against healthy baseline embeddings.

- Loads perturbation run manifest from `run_perturbations.py`
- Loads baseline embeddings from `--baseline-embeddings` if provided
- Otherwise mocks baseline by running `run_model` on healthy input `AnnData`
- Computes mock cosine distance per cell for each perturbation condition
- Writes per-experiment metrics and run-level summary
- Propagates donor/cell-type metadata and computes target-subset shift metrics when targeting is used

**Comparison outputs:**
- Silver:
  - `outputs/silver/comparison_results/<run_id>/healthy_baseline_embeddings.npy`
  - `outputs/silver/comparison_results/<run_id>/comparison_manifest.json`
  - `outputs/silver/comparison_results/<run_id>/per_experiment/exp_XXXX_cosine_distances.npy`
  - `outputs/silver/comparison_results/<run_id>/per_experiment/exp_XXXX_summary.json`
- Gold derivatives:
  - `outputs/gold/ui_input_data/<run_id>/comparison_ui_cards.json`
  - `outputs/gold/compute_input_data/<run_id>/comparison_index.json`
  - `outputs/gold/compute_input_data/<run_id>/cosine_distances_all.npz`
  - `outputs/gold/compute_input_data/<run_id>/healthy_baseline_embeddings.npy`

**Per-experiment comparison summary includes:**
- `gene` / `genes`
- `perturbation_type`
- `involved_donors`, `involved_cell_types`
- `target_donors`, `target_cell_types`, `n_target_cells`
- global cosine-distance statistics (`mean`, `median`, `std`, `min`, `max`)
- target-subset cosine-distance statistics:
  - `mean_distance_target_subset`
  - `median_distance_target_subset`

**Example:**
- `uv run python src/compute_comparisons.py --run-dir outputs/perturbation_runs/<run_id> --input outputs/synthetic_adata.h5ad`

---

## End-to-End Dataflow

1. `generate_data.py` -> creates healthy synthetic `AnnData`
2. `preview_data.py` -> validates data quality/shape
3. `run_perturbations.py` -> writes silver perturbation outputs + gold UI/compute derivatives
4. `compute_comparisons.py` -> writes silver comparison outputs + gold UI/compute derivatives

This provides a complete mock workflow for validating structure, metadata tracking, and pipeline execution before real-model integration.
