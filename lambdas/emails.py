import os, json, time, boto3
from datetime import datetime, timezone, timedelta

dynamo = boto3.resource("dynamodb")
s3 = boto3.client("s3")
ses = boto3.client("ses", region_name="eu-west-2")
eventbridge_sched = boto3.client("scheduler", region_name="eu-west-2")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "metrosafety-bedrock-output-data-dev-bedrock-lambda")
HEARTBEAT_TABLE = os.environ.get("HEARTBEAT_TABLE", "ProofingHeartbeats")
SENDER = "luke.gasson@metrosafety.co.uk"
RECIPIENT = "metroit@metrosafety.co.uk"

def _iso(dt): return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

def process(event, context):
    workorder_id = event.get("workOrderId")
    workOrderNumber = event.get("workOrderNumber")
    workTypeRef = event.get("workTypeRef")
    buildingName = event.get("buildingName")

    if not workorder_id:
        return {"statusCode": 400, "body": "Missing workOrderId"}

    hb_tbl = dynamo.Table(HEARTBEAT_TABLE)
    resp = hb_tbl.get_item(Key={"workorder_id": workorder_id})
    item = resp.get("Item")
    if not item:
        # nothing to do
        return {"statusCode": 200, "body": "No heartbeat"}

    last_update = int(item.get("last_update", 0))
    csv_key = item.get("csv_key", f"changes/{workorder_id}_changes.csv")

    # ensure quiet period (e.g., 180s) since last update
    quiet_required = 180
    now = int(time.time())
    if (now - last_update) < quiet_required:
        # reschedule ourselves (small, one-off)
        name = f"finalize-{workorder_id}"
        run_at = datetime.now(timezone.utc) + timedelta(seconds=quiet_required)
        eventbridge_sched.update_schedule(
            Name=name,
            ScheduleExpression=f"at({run_at.strftime('%Y-%m-%dT%H:%M:%SZ')})",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED",
            Target={
                "Arn": context.invoked_function_arn,
                "RoleArn": os.environ["SCHEDULER_ROLE_ARN"],  # set as env
                "Input": json.dumps({"workOrderId": workorder_id}),
            }
        )
        return {"statusCode": 202, "body": "Rescheduled due to activity"}

    # check CSV exists & hasn't been modified very recently
    try:
        head = s3.head_object(Bucket=BUCKET_NAME, Key=csv_key)
    except s3.exceptions.NoSuchKey:
        return {"statusCode": 200, "body": f"No CSV yet at {csv_key}"}

    last_mod = head["LastModified"]  # datetime UTC
    if (datetime.now(timezone.utc) - last_mod) < timedelta(seconds=60):
        # very recent write → reschedule
        name = f"finalize-{workorder_id}"
        run_at = datetime.now(timezone.utc) + timedelta(seconds=120)
        eventbridge_sched.update_schedule(
            Name=name,
            ScheduleExpression=f"at({run_at.strftime('%Y-%m-%dT%H:%M:%SZ')})",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED",
            Target={
                "Arn": context.invoked_function_arn,
                "RoleArn": os.environ["SCHEDULER_ROLE_ARN"],
                "Input": json.dumps({"workOrderId": workorder_id}),
            }
        )
        return {"statusCode": 202, "body": "Rescheduled; CSV just updated"}

    # presign (link) or attach
    presigned = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET_NAME, "Key": csv_key},
        ExpiresIn=86400  # 24 hours
    )

    subject = (
        f"PASS || "
        f"{workOrderNumber}/"
        f"{workorder_id} || "
        f"{buildingName} || "
        f"{workTypeRef}"
    )
    body_text = (
        f"Please find below a link to the spelling and grammar changes made to the Building Description & Actions\n"
        f"{presigned}\n\n"
    )

    ses.send_email(
        Source=SENDER,
        Destination={"ToAddresses": [RECIPIENT]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body_text}}
        }
    )

    # (optional) clean up: delete the schedule so it can be recreated fresh next time
    try:
        eventbridge_sched.delete_schedule(Name=f"finalize-{workorder_id}")
    except Exception:
        pass

    return {"statusCode": 200, "body": "Email sent"}
