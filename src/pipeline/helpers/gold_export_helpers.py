"""Shared record-shaping helpers for gold export artifacts."""

from __future__ import annotations

from typing import Any

PERTURBATION_CONTEXT_FIELDS = (
    "experiment_id",
    "perturbation_type",
    "gene",
    "genes",
    "involved_donors",
    "involved_cell_types",
    "target_donors",
    "target_cell_types",
)


def pick_fields(source: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: source.get(field) for field in fields}


def perturbation_ui_record(exp: dict[str, Any]) -> dict[str, Any]:
    row = pick_fields(exp, PERTURBATION_CONTEXT_FIELDS)
    row["n_target_cells"] = exp.get("n_target_cells")
    return row


def comparison_ui_card(exp: dict[str, Any]) -> dict[str, Any]:
    card = pick_fields(exp, PERTURBATION_CONTEXT_FIELDS)
    card["mean_distance"] = exp.get("mean_distance")
    card["mean_distance_target_subset"] = exp.get("mean_distance_target_subset")
    card["n_target_cells"] = exp.get("n_target_cells")
    return card


def run_scoped_payload(
    run_id: str,
    experiments: list[dict[str, Any]],
    run_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"run_id": run_id, "experiments": experiments}
    if run_metadata is not None:
        payload["run_metadata"] = run_metadata
    return payload
