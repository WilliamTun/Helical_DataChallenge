"""Sensor that emits alert runs for failed asset checks."""

import json
from datetime import UTC, datetime
from typing import Any

from dagster import (
    DefaultSensorStatus,
    DagsterEventType,
    EventRecordsFilter,
    RunShardedEventsCursor,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from src.dagster.jobs.asset_check_alert_job import asset_check_alert_job

EVENT_FETCH_LIMIT = 200
_EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)


def _parse_sensor_cursor(cursor: str | None) -> tuple[int, datetime]:
    """Support legacy int cursor and new run-sharded cursor payload."""
    if not cursor:
        return 0, _EPOCH_UTC

    if cursor.isdigit():
        return int(cursor), _EPOCH_UTC

    try:
        payload = json.loads(cursor)
    except json.JSONDecodeError:
        return 0, _EPOCH_UTC

    raw_id = payload.get("id", 0)
    raw_run_updated_after = payload.get("run_updated_after")
    try:
        cursor_id = int(raw_id)
    except (TypeError, ValueError):
        cursor_id = 0

    if isinstance(raw_run_updated_after, str):
        try:
            # Accept 'Z' timestamps and normalized ISO strings.
            run_updated_after = datetime.fromisoformat(raw_run_updated_after.replace("Z", "+00:00"))
        except ValueError:
            run_updated_after = _EPOCH_UTC
    else:
        run_updated_after = _EPOCH_UTC
    return cursor_id, run_updated_after


def _serialize_sensor_cursor(cursor_id: int, run_updated_after: datetime) -> str:
    return json.dumps(
        {"id": cursor_id, "run_updated_after": run_updated_after.astimezone(UTC).isoformat()}
    )


def _failed_check_run_request(record: Any) -> RunRequest | None:
    """Build a RunRequest only when the record contains a failed asset check."""
    dagster_event = record.event_log_entry.dagster_event
    if dagster_event is None:
        return None

    data = dagster_event.event_specific_data
    evaluation = getattr(data, "asset_check_evaluation", None)
    if evaluation is None or bool(getattr(evaluation, "passed", True)):
        return None

    asset_key = "/".join(getattr(evaluation.asset_key, "path", []))
    check_name = str(getattr(evaluation, "check_name", "unknown_check"))
    severity = str(getattr(evaluation, "severity", "ERROR"))
    description = str(getattr(evaluation, "description", "Asset check failed."))
    message = f"[{severity}] {description}"
    return RunRequest(
        run_key=f"asset_check_failure_{record.storage_id}",
        run_config={
            "ops": {
                "emit_asset_check_alert": {
                    "config": {
                        "asset_key": asset_key,
                        "check_name": check_name,
                        "message": message,
                    }
                }
            }
        },
        tags={
            "alert_type": "asset_check_failure",
            "asset_key": asset_key,
            "check_name": check_name,
            "severity": severity,
        },
    )


@sensor(
    job=asset_check_alert_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def asset_check_failure_alert_sensor(context: SensorEvaluationContext):
    last_cursor_id, run_updated_after = _parse_sensor_cursor(context.cursor)
    run_sharded_cursor = RunShardedEventsCursor(
        id=last_cursor_id, run_updated_after=run_updated_after
    )
    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_CHECK_EVALUATION,
            after_cursor=run_sharded_cursor,
        ),
        ascending=True,
        limit=EVENT_FETCH_LIMIT,
    )
    if not records:
        yield SkipReason("No new asset check evaluations.")
        return

    max_cursor = last_cursor_id
    emitted = 0
    for rec in records:
        max_cursor = max(max_cursor, int(rec.storage_id))
        run_request = _failed_check_run_request(rec)
        if run_request is None:
            continue
        emitted += 1
        yield run_request

    context.update_cursor(_serialize_sensor_cursor(max_cursor, datetime.now(UTC)))
    if emitted == 0:
        yield SkipReason("No failed asset checks in new events.")
