# Helical Data Challenge (Mock Pipeline)

This repository contains a mock pipeline to validate perturbation workflow logic on synthetic `AnnData`.
All model and comparison outputs are intentionally mocked for pipeline prototyping.

## Quickstart

```bash
# Activate virtual environment
source .venv/bin/activate

# Generate synthetic healthy dataset
uv run python src/generate_data.py --output outputs/synthetic_adata.h5ad

# Optional preview/QA
uv run python src/preview_data.py --input outputs/synthetic_adata.h5ad

# Run perturbation experiments from config
uv run python src/run_perturbations.py \
  --input outputs/synthetic_adata.h5ad \
  --config configs/perturbation_config.json

# Compare perturbation embeddings vs healthy baseline embeddings
uv run python src/compute_comparisons.py \
  --run-dir outputs/silver/perturbation_runs/<run_id> \
  --input outputs/synthetic_adata.h5ad
```

## Targeted In-Silico Cohorts

`run_perturbations.py` supports optional subset targeting in each perturbation spec:
- `target_donors`: list of donor IDs (for example, `["donor_03"]`)
- `target_cell_types`: list of cell types (for example, `["T_cell"]`)

When provided, perturbations are applied only to matching cells.

Example spec:

```json
{
  "gene": "GENE0001",
  "perturbation_type": "gene_knockout",
  "target_donors": ["donor_03"],
  "target_cell_types": ["T_cell"]
}
```

## Output Metadata Highlights

- Perturbation run outputs include:
  - `gene`, `genes`, `perturbation_type`
  - `involved_donors`, `involved_cell_types`
  - `target_donors`, `target_cell_types`, `n_target_cells`
- Comparison outputs include:
  - overall cosine-distance metrics
  - target-subset metrics:
    - `mean_distance_target_subset`
    - `median_distance_target_subset`

## Output Layers

- Silver pipeline outputs:
  - `outputs/silver/perturbation_runs/<run_id>/...`
  - `outputs/silver/comparison_results/<run_id>/...`
- Gold consumer-ready outputs:
  - `outputs/gold/ui_input_data/<run_id>/...`
  - `outputs/gold/compute_input_data/<run_id>/...`

See `outputs/README.md` for the full silver-vs-gold and UI-vs-compute rationale.