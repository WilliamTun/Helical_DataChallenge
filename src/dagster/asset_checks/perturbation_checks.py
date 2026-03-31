"""Asset checks for perturbation embeddings."""

from dagster import AssetCheckExecutionContext, AssetCheckResult, asset_check

from src.dagster.asset_checks.common import (
    build_check_result,
    latest_materialization_metadata,
    make_artifact_reader,
    metadata_value_as_python,
    passing_check,
)
from src.dagster.assets import perturbation_run
from src.dagster.config import MockPipelineConfig

from src.pipeline.helpers.artifact_io import read_npy  # noqa: E402
from src.pipeline.helpers.mock_model import MOCK_EMBEDDING_DIM  # noqa: E402


@asset_check(
    asset=perturbation_run,
    name="perturbation_embeddings_have_expected_shape",
    description="Validate perturbation embedding dimension for all experiments in manifest.",
)
def perturbation_run_embeddings_have_expected_shape(
    context: AssetCheckExecutionContext, config: MockPipelineConfig
) -> AssetCheckResult:
    reader = make_artifact_reader(config)

    failures: list[str] = []
    total_experiments = 0
    metadata = latest_materialization_metadata(context, "perturbation_run")
    run_prefixes_by_type = metadata_value_as_python(metadata.get("run_prefixes_by_type")) or {}
    if not isinstance(run_prefixes_by_type, dict) or len(run_prefixes_by_type) == 0:
        return passing_check(
            metadata={"n_runs": 0, "n_experiments": 0, "note": "no active perturbation runs"},
        )

    for ptype, run_prefix in run_prefixes_by_type.items():
        run_prefix = str(run_prefix)
        manifest_path = f"{run_prefix}/manifest.json"
        if not reader.exists(manifest_path):
            failures.append(f"{ptype}: missing {manifest_path}")
            continue

        manifest = reader.read_json(manifest_path)
        exps = list(manifest.get("experiments", []))
        total_experiments += len(exps)
        for exp in exps:
            exp_id = exp.get("experiment_id")
            rel = exp.get("output_embeddings")
            if not rel:
                failures.append(f"{ptype}:{exp_id}: missing output_embeddings")
                continue
            emb_logical = f"{run_prefix}/{rel}"
            if not reader.exists(emb_logical):
                failures.append(f"{ptype}:{exp_id}: missing embeddings.npy at {emb_logical}")
                continue

            emb = read_npy(reader, emb_logical)
            if emb.ndim != 2 or int(emb.shape[1]) != int(MOCK_EMBEDDING_DIM):
                failures.append(
                    f"{ptype}:{exp_id}: expected shape (*,{MOCK_EMBEDDING_DIM}), got {tuple(emb.shape)}"
                )

    ok = len(failures) == 0
    return build_check_result(
        passed=ok,
        metadata={
            "n_runs": len(run_prefixes_by_type),
            "n_experiments": total_experiments,
            "failures_count": len(failures),
            "failures": failures[:10],
        },
    )
