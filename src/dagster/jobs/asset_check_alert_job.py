"""Alert job triggered when asset checks fail."""

from dagster import job, op


@op(config_schema={"asset_key": str, "check_name": str, "message": str})
def emit_asset_check_alert(context) -> None:
    cfg = context.op_config
    context.log.error(
        "ASSET CHECK ALERT | asset=%s check=%s | %s",
        cfg["asset_key"],
        cfg["check_name"],
        cfg["message"],
    )


@job
def asset_check_alert_job():
    emit_asset_check_alert()
