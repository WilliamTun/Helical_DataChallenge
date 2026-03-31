"""Asset checks for comparison outputs."""

from dagster import AssetCheckExecutionContext, AssetCheckResult, asset_check

from src.dagster.asset_checks.common import (
    build_check_result,
    latest_materialization_metadata,
    make_artifact_reader,
    metadata_value_as_python,
    passing_check,
)
from src.dagster.assets import comparison_results
from src.dagster.config import MockPipelineConfig

REQUIRED_SUMMARY_FIELDS = ["experiment_id", "mean_distance", "median_distance", "n_target_cells"]
MAX_EXPERIMENTS_TO_SAMPLE = 20


@asset_check(
    asset=comparison_results,
    name="comparison_results_have_expected_manifest",
    description="Validate comparison manifest exists and contains expected experiment summaries.",
)
def comparison_results_has_expected_manifest(
    context: AssetCheckExecutionContext, config: MockPipelineConfig
) -> AssetCheckResult:
    reader = make_artifact_reader(config)

    metadata = latest_materialization_metadata(context, "comparison_results")
    results_by_type = metadata_value_as_python(metadata.get("results_prefixes_by_type")) or {}
    if not isinstance(results_by_type, dict) or len(results_by_type) == 0:
        return passing_check(
            metadata={"n_result_sets": 0, "n_experiments": 0, "note": "no comparison runs"},
        )

    missing_fields: list[str] = []
    total_experiments = 0
    for ptype, results_prefix in results_by_type.items():
        manifest_path = f"{results_prefix}/comparison_manifest.json"
        if not reader.exists(manifest_path):
            missing_fields.append(f"{ptype}: missing {manifest_path}")
            continue
        manifest = reader.read_json(manifest_path)
        exps = manifest.get("experiments", [])
        if not isinstance(exps, list):
            missing_fields.append(f"{ptype}: experiments is not a list")
            continue
        total_experiments += len(exps)
        for exp in exps[:MAX_EXPERIMENTS_TO_SAMPLE]:
            if not isinstance(exp, dict):
                missing_fields.append(f"{ptype}: non-dict experiment entry")
                continue
            for field_name in REQUIRED_SUMMARY_FIELDS:
                if field_name not in exp:
                    missing_fields.append(f"{ptype}:{field_name}")

    ok = len(missing_fields) == 0
    return build_check_result(
        passed=ok,
        metadata={
            "n_result_sets": len(results_by_type),
            "n_experiments": total_experiments,
            "missing_fields_sample": missing_fields[:MAX_EXPERIMENTS_TO_SAMPLE],
        },
    )
