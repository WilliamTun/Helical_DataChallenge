# Configuration files

Pipeline scripts and Dagster read paths from **`MockPipelineConfig`** (`src/dagster/config.py`). Defaults:

| Setting | Default path |
|---------|----------------|
| **`pipeline_config_path`** | **`pipeline_config.json`** (this folder) |
| **`perturbation_config_path`** | **`perturbation_config.json`** (this folder) |

CLI tools accept **`--pipeline-config`** / **`--config`** to override these.

---

## `pipeline_config.json` — artifact storage backends

Loaded by **`src/pipeline/helpers/io_config.py`**. Controls where **non-AnnData** artifacts go: **local files** under the repo or **DuckDB** tables.

**Rules:**

- **`pipeline_output.default_backend`:** `local_folder` or `duckdb`.
- **`pipeline_output.steps.<step>.backend`:** per-step override, or `null` to inherit `default_backend`.  
  Steps: **`generate_data`**, **`preview_data`**, **`run_perturbations`**, **`compute_comparisons`**.
- If **`default_backend`** or any step uses **`duckdb`**, you must define **`pipeline_output.duckdb`** with **`database_path`** and optional **`artifacts_table`** (default `pipeline_artifacts`).
- **AnnData (`.h5ad`) is always written to local disk** — never stored in DuckDB.

**Shipped files:**

| File | Purpose |
|------|---------|
| **`pipeline_config.json`** | Default: all steps inherit **`local_folder`**. |
| **`pipeline_config.example.duckdb.json`** | Example: DuckDB as default backend with **`.data/pipeline.duckdb`**. Copy or point **`pipeline_config_path`** at it to try hybrid storage. |

---

## `perturbation_config.json` — in-silico experiments

Loaded by **`src/pipeline/run_perturbations.py`** and by Dagster perturbation assets. Required top-level keys:

- **`perturbation_types`** — list of strings; each must be one of:  
  **`gene_knockout`**, **`gene_overexpression`**, **`gene_activation`**.
- **`perturbations`** — list of experiment objects. Each entry must include **`perturbation_type`** and gene(s) via **`gene`** or **`genes`**.

Optional per experiment:

- **`target_donors`**, **`target_cell_types`** — restrict which cells are perturbed (requires matching **`donor`** / **`cell_type`** columns in `.obs`).
- **`fold_change`** — overexpression (default `2.0`) or activation when **`delta`** is not used.
- **`delta`** — additive activation shift (alternative to **`fold_change`** for **`gene_activation`**).

**Shipped files:**

| File | Purpose |
|------|---------|
| **`perturbation_config.json`** | Default three experiment types and sample genes (used by README / Dagster defaults). |
| **`perturbation_config.example.targeted_cohort.json`** | Example: single **`gene_knockout`** with **`target_donors`** and **`target_cell_types`**. |

Point Dagster or CLI at an alternate file by setting **`perturbation_config_path`** (or **`--config`** for `run_perturbations.py`).

---

## Dagster-only settings

**`model_name`**, **`n_cells`**, partition behavior, memoization, and materialization mode are **not** in these JSON files—they live in **`MockPipelineConfig`** / **`PerturbationPipelineConfig`** and **`src/dagster/run_config_defaults.py`**. See the root **`README.md`**.
