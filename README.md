# Helical Data Challenge (mock pipeline)

This repo is a **small, end-to-end reference pipeline** for single-cell–style data: synthetic data generation, configurable perturbations, mock embeddings, and comparisons. It is meant to show how **CLI scripts**, **JSON configs**, **silver/gold outputs**, and **Dagster** fit together—similar to patterns you would use around real models and warehouses, but with **fake** “foundation model” steps.

**What it does (in order):**

1. Build a synthetic **`AnnData`** (cells × genes; perturbation type in metadata), saved as **`.h5ad`** (the usual [AnnData](https://anndata.readthedocs.io/) on-disk format).
2. Run **in-silico perturbations** from **`configs/perturbation_config.json`** (knockout, overexpression, activation).
3. Produce **placeholder embeddings** and **comparison metrics** via a mock model so you can exercise **paths, configs, and lineage** without GPUs.

**Prerequisites:** [uv](https://github.com/astral-sh/uv) (Python **≥ 3.13** and deps are pinned in **`pyproject.toml`**). [Dagster](https://dagster.io/) is included for the optional UI and asset jobs (**[Option B — Dagster](#option-b--dagster-ui--jobs)**). Run commands from the **repository root**.

---

## Contents

- [Quick start (CLI)](#quick-start-cli)
- [Silver and gold outputs](#silver-and-gold-outputs-medallion-style)
- [Pipeline flow](#pipeline-flow-logic)
- [Repository layout](#repository-layout)
- [Option A — Command line only](#option-a--command-line-only)
- [Key Dagster concepts](#key-dagster-concepts-and-how-this-repo-uses-them)
- [Option B — Dagster](#option-b--dagster-ui--jobs)
- [Configuration reference](#configuration-reference)
- [How this maps to production-style pipelines](#how-this-maps-to-production-style-pipelines)

---

## Quick start (CLI)

1. Run the four steps **in order**:

   ```bash
   uv run python src/pipeline/generate_data.py --output outputs/synthetic_adata.h5ad
   uv run python src/pipeline/preview_data.py --input outputs/synthetic_adata.h5ad
   uv run python src/pipeline/run_perturbations.py \
     --input outputs/synthetic_adata.h5ad \
     --config configs/perturbation_config.json
   uv run python src/pipeline/compute_comparisons.py \
     --run-dir outputs/silver/perturbation_runs/<run_id> \
     --input outputs/synthetic_adata.h5ad
   ```

2. After step 3, take the **new directory name** under **`outputs/silver/perturbation_runs/`** and use it as **`<run_id>`** in step 4.

3. For how **silver** and **gold** relate, see **[Silver and gold outputs](#silver-and-gold-outputs-medallion-style)** below.

---

## Silver and gold outputs (medallion-style)

Outputs follow a **medallion-style** layout (familiar from lakehouse docs): treat the synthetic **`.h5ad`** as an upstream **bronze** layer; **silver** holds **embedding model outputs**; **gold** holds **downstream-friendly** data shapes (UI vs analytics).

### Silver tier

**Silver** is what the (mock) embedding and comparison steps **actually wrote**: manifests, embedding tensors (e.g. **`.npz`** / **`.npy`**), per-experiment outputs, comparison manifests and metrics. Use silver for **debugging**, **reproducibility**, and **lineage**.

**Typical paths:** **`outputs/silver/perturbation_runs/<run_id>/`**, **`outputs/silver/comparison_results/<run_id>/`** (exact roots come from config).

### Gold tier

Gold **repackages** silver for consumers (not a second model pass).

| Subfolder | Consumer style |
|-----------|------------------|
| **`outputs/gold/ui_input_data/<run_id>/`** | **UI / presentation:** JSON and card-like records for dashboards—easy to bind to front ends without parsing numpy layouts. |
| **`outputs/gold/compute_input_data/<run_id>/`** | **Compute / training:** batched, array-heavy bundles (e.g. **`.npz`**, stacked tensors) for analytics or training. |

**Summary:** **Silver** = pipeline-faithful outputs. **Gold** = same information **reshaped** into **UI-oriented** vs **compute-oriented** formats.

---

## Pipeline flow (logic)

The CLI and Dagster graph follow the same order. **`configs/perturbation_config.json`** defines genes and perturbation types; optional **`target_donors`** / **`target_cell_types`** limit which cells are perturbed.

**Dependency shape** (CLI order and Dagster assets):

```text
                         ┌──► preview_data
                         │
synthetic_adata ─────────┼──► perturbation_run_gene_knockout ────┐
                         │                                       │
                         ├──► perturbation_run_gene_overexpression ──┼──► perturbation_run ──► comparison_results
                         │                                       │
                         └──► perturbation_run_gene_activation ────┘
```

In Dagster, the three perturbation branches can run **in parallel** when the executor allows; they **merge** into **`perturbation_run`**, then **`comparison_results`** runs on the combined output.

**CLI note:** **`compute_comparisons`** accepts optional **`--baseline-embeddings`** **`.npy`**; otherwise the mock baseline is derived from the unperturbed AnnData.

---

## Repository layout

| What | Where |
|------|--------|
| CLI entrypoints | `src/pipeline/` |
| IO, mock model, gold export helpers | `src/pipeline/helpers/` |
| Dagster definitions | **`src/dagster/definitions.py`** |
| Assets | `src/dagster/assets/main/` |
| Jobs / sensors | `src/dagster/jobs/`, `src/dagster/sensors/` |
| Fingerprints / version tokens | `src/dagster/lineage/` |

---

## Option A — Command line only

Use this when you want **no orchestrator**: plain scripts, easy stepping in one process.

| Step | Script | Output / notes |
|------|--------|----------------|
| 1 | `generate_data.py` | Writes `outputs/synthetic_adata.h5ad` (or path you pass) |
| 2 | `preview_data.py` | Logs summaries to stdout |
| 3 | `run_perturbations.py` | **`outputs/silver/perturbation_runs/<run_id>/`** — use this **`<run_id>`** in step 4 |
| 4 | `compute_comparisons.py` | Silver + gold comparison artifacts under configured roots |

---

## Key Dagster concepts (and how this repo uses them)

Skim if you already use Dagster; read if you need a map from **docs terminology** to **this repo’s files**. This does not replace [Dagster’s documentation](https://docs.dagster.io/).

### Asset graph

**Concept:** An **asset** is a durable output (often a file or table) with **upstream dependencies**. A **job** selects which assets to materialize; the UI shows a **DAG**.

**Here:** After `dagster dev`, open **Assets**. You should see **`synthetic_adata`**, **`preview_data`**, **`perturbation_run_gene_*`**, **`perturbation_run`**, **`comparison_results`**, and for the batch job **`perturbation_run_batch`**, **`comparison_results_batch`**. The batch job wires **`synthetic_adata`** as a dependency of **`perturbation_run_batch`** so the graph stays valid when AnnData is produced outside Dagster.

### Partitions

**Concept:** A **partition** is a named slice of an asset (e.g. one batch). **Multi-partitions** combine dimensions. You materialize one or many keys for **targeted backfills** and **parallel** runs.

**Here:** **`mock_pipeline_dynamic_batch_job`** uses **`MultiPartitionsDefinition`** with **`perturbation_type`** (`gene_knockout`, `gene_overexpression`, `gene_activation`) and **`experiment_batch`** (dynamic keys like `batch_0000` from **`perturbation_batch_size`** and the perturbation list). **`mock_pipeline_job`** does **not** partition those core assets—lineage there uses **fingerprints** and **config**, not partition keys. **Donor / cell-type** filtering lives in **`perturbation_config.json`**, not as Dagster partitions (you could add that later).

### Fingerprints

**Concept:** A **fingerprint** is a stable string summarizing **content or identity** (hash, file stats, etc.) so you can detect upstream changes.

**Here:** **`src/dagster/lineage/fingerprints.py`** (e.g. **`fingerprint_stored_artifact`**, **`fingerprint_local_file`**, **`combine_version_token`**). Fingerprints feed **`DataVersion`** and **memoization** for perturbation runs.

### Memoization (skip unchanged work)

**Concept:** If outputs already exist and **inputs + config** match what you care about, skip recomputation—useful when steps are expensive (e.g. real inference).

**Here:** **`enable_memoization`** on **`MockPipelineConfig`** (default **true**). Perturbation and comparison steps can reuse existing on-disk manifests when the **memoization token** (fingerprints, perturbation config, **`model_name`**, etc.) matches. Set **`enable_memoization: false`** to force a full recompute.

### Parallelism

**Concept:** Independent assets can run **concurrently** in one run, depending on the **executor** (in-process, multiprocess, remote).

**Here:** The three **`perturbation_run_gene_*`** assets are **siblings** and may run in parallel. Partitioned batch assets can parallelize across **keys** if the executor allows. This repo does **not** ship production **GPU** or **Kubernetes** executor configs—that is environment-specific.

### Traceability

**Concept:** Each materialization can attach **metadata**, **tags**, and **data versions** so runs are **auditable**.

**Here:** Assets return **`MaterializeResult`** with paths, fingerprints, timing, model info, **tags**, and often **`DataVersion`**. Comparison assets expose the embedding **model name** from upstream perturbations (`embedding_model_name`), not a separate **`model_name`** on comparison config.

---

## Option B — Dagster (UI + jobs)

Dagster runs the **same logical pipeline** as **assets**: dependency **graph**, **run history**, **partition pickers** (batch job), and **per-run metadata**.

### Start the UI

```bash
uv run dagster dev -f src/dagster/definitions.py
```

### Jobs

| Job | Role |
|-----|------|
| **`mock_pipeline_job`** | Full graph: synthetic AnnData → preview → three typed perturbation assets → **`perturbation_run`** → **`comparison_results`**. |
| **`mock_pipeline_dynamic_batch_job`** | **`perturbation_run_batch`** / **`comparison_results_batch`** partitioned by **`perturbation_type`** × **`experiment_batch`**. |

**Materialize all assets once (CLI):**

```bash
uv run dagster asset materialize -f src/dagster/definitions.py --select "*"
```

### Launchpad and config shape

1. **Jobs** → pick a job → **Launchpad**.
2. Defaults live in **`src/dagster/run_config_defaults.py`**.
3. Pipeline fields must sit under **`ops.<op_name>.config`**. Putting fields at the wrong level triggers errors like **`unexpected config entry "n_batches" at the root`**.

### YAML reference

**`mock_pipeline_job`** — **`model_name`** appears only on **perturbation** ops (`perturbation_run_gene_*`, `perturbation_run`). **`synthetic_adata`**, **`preview_data`**, **`comparison_results`** use **`MockPipelineConfig`** without **`model_name`**.

```yaml
ops:
  synthetic_adata:
    config:
      synthetic_adata_materialization_mode: "generate"
      n_cells: 1000
      n_genes: 500
      n_donors: 8
      n_batches: 4
      seed: 7
      synthetic_adata_path: "outputs/synthetic_adata.h5ad"
      perturbation_config_path: "configs/perturbation_config.json"
      perturbation_batch_size: 1
  preview_data:
    config: {}
  perturbation_run_gene_knockout:
    config:
      model_name: "mock_foundation_model_1"
  perturbation_run_gene_overexpression:
    config:
      model_name: "mock_foundation_model_1"
  perturbation_run_gene_activation:
    config:
      model_name: "mock_foundation_model_1"
  perturbation_run:
    config:
      model_name: "mock_foundation_model_1"
  comparison_results:
    config: {}
```

```bash
uv run dagster job launch -f src/dagster/definitions.py -j mock_pipeline_job -c dagster_run.yaml
```

**`mock_pipeline_dynamic_batch_job`** — **`model_name`** only on **`perturbation_run_batch`**.

```yaml
ops:
  perturbation_run_batch:
    config:
      perturbation_batch_size: 1
      model_name: "mock_foundation_model_1"
  comparison_results_batch:
    config:
      perturbation_batch_size: 1
```

```bash
uv run dagster job launch -f src/dagster/definitions.py -j mock_pipeline_dynamic_batch_job -c dagster_batch.yaml
```

### Sensors and persistence (optional)

- **`ann_data_sensor`** — Fires when the watched **`.h5ad`** changes; set **`DAGSTER_ANN_DATA_SENSOR_REL_PATH`** (default `outputs/synthetic_adata.h5ad`). Expects **`synthetic_adata_materialization_mode: external`**.
- **`experiment_batch_partitions_sensor`** — Registers **`experiment_batch`** partition keys from config.
- **`DAGSTER_HOME`** — Persist local Dagster state across restarts:

  ```bash
  export DAGSTER_HOME="$PWD/.dagster_home"
  uv run dagster dev -f src/dagster/definitions.py
  ```

---

## Configuration reference

### Dagster config classes (`src/dagster/config.py`)

| Class | Use |
|-------|-----|
| **`MockPipelineConfig`** | Synthetic data, preview, comparison, batch manifest, **`comparison_results_batch`**. No **`model_name`**. |
| **`PerturbationPipelineConfig`** | Adds **`model_name`** (`mock_foundation_model_1` / `_2` / `_3`) for **perturbation** assets only. |

**Representative fields:** `n_cells`, `n_genes`, `n_donors`, `n_batches`, `seed`, `synthetic_adata_path`, **`synthetic_adata_materialization_mode`** (`generate` \| `external`), **`perturbation_config_path`**, silver/gold roots, **`baseline_embeddings_path`**, **`perturbation_batch_size`**, **`enable_memoization`**, **`preview_max_categories`**, **`pipeline_config_path`**.

### JSON configs

Full index, schema notes, and example filenames: **`configs/README.md`**.

| File | Purpose |
|------|---------|
| **`configs/perturbation_config.json`** | **`perturbation_types`** and per-experiment **`perturbations`** (genes, optional **`target_donors`** / **`target_cell_types`**, type-specific fields). |
| **`configs/pipeline_config.json`** | Per-step **local_folder** vs **DuckDB** for JSON/numpy artifacts. **`.h5ad` is always local**—never stored in DuckDB. Loader: **`src/pipeline/helpers/io_config.py`**. |
| **`configs/pipeline_config.example.duckdb.json`** | Example with **DuckDB** as default backend (copy or set **`pipeline_config_path`**). |
| **`configs/perturbation_config.example.targeted_cohort.json`** | Example with **`target_donors`** and **`target_cell_types`**. |

---

## How this maps to production-style pipelines

The implementation is **mock**, but the **shape** matches common foundation-model / perturbation patterns: **fan-out** by experiment type, **optional batch partitioning**, **fingerprints** for change detection, **memoization** for expensive steps, and **rich metadata** for audits.

| Theme | In this repository |
|-------|---------------------|
| **Partitioned reruns** | Partioned reruns shown in **`mock_pipeline_dynamic_batch_job`** (`perturbation_type` × `experiment_batch`). To show contrast with weaker partioning, see **`mock_pipeline_job`** (no multi-partition keys on core assets). |
| **Cost / iteration** | **Memoization** plus **single-asset** or **single-partition** rematerialization cuts repeated work in dev. |
| **Evolving experiments** | New **batch** keys from config; **model** identity via **`PerturbationPipelineConfig.model_name`**; comparisons **read** upstream model metadata. |
| **Scale-out** | Parallel sibling perturbation assets; partition parallelism depends on your **executor** (no GPU cluster config here). |

For real workloads you would plug in **real models**, **GPU-capable executors**, and possibly **more partition dimensions** (e.g. cohorts as partitions). This repo focuses on **orchestration and lineage mechanics**, not production SLAs.
