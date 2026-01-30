import os
import json
import time
import uuid
import boto3
from botocore.exceptions import ClientError

DDB_TABLE = os.environ.get("DEWRRA_JOBS_TABLE", "dewrra_jobs")
QUEUE_URL = os.environ.get("DEWRRA_JOBS_QUEUE_URL")  # required
RESULT_BUCKET_DEFAULT = os.environ.get("DEWRRA_RESULT_BUCKET")  # optional fallback

ddb = boto3.resource("dynamodb")
table = ddb.Table(DDB_TABLE)
sqs = boto3.client("sqs")
s3 = boto3.client("s3")


def _resp(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, separators=(",", ":"), ensure_ascii=False),
    }


def _now():
    return int(time.time())


def _get_json_body(event):
    body = event.get("body")
    if body is None:
        return {}
    if isinstance(body, str):
        return json.loads(body) if body.strip() else {}
    return body


def _route_key(event):
    # HTTP API v2 gives event["routeKey"] like "POST /dewrra/start"
    return event.get("routeKey") or ""


def start_job(event):
    payload = _get_json_body(event)
    work_order_id = payload.get("workOrderId") or payload.get("workorder_id") or payload.get("work_order_id")
    if not work_order_id:
        return _resp(400, {"ok": False, "error": "Missing workOrderId"})

    job_id = uuid.uuid4().hex
    ts = _now()

    item = {
        "jobId": job_id,
        "workOrderId": str(work_order_id),
        "status": "QUEUED",
        "createdAt": ts,
        "updatedAt": ts,
    }

    table.put_item(Item=item)

    if not QUEUE_URL:
        return _resp(500, {"ok": False, "error": "Missing env var DEWRRA_JOBS_QUEUE_URL"})

    # Put the job on the queue for the worker
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({"jobId": job_id}, separators=(",", ":")),
    )

    return _resp(202, {"ok": True, "jobId": job_id, "status": "QUEUED"})


def get_status(event):
    job_id = (event.get("pathParameters") or {}).get("jobId")
    if not job_id:
        return _resp(400, {"ok": False, "error": "Missing path param jobId"})

    res = table.get_item(Key={"jobId": job_id})
    item = res.get("Item")
    if not item:
        return _resp(404, {"ok": False, "error": "Job not found", "jobId": job_id})

    out = {
        "ok": True,
        "jobId": item["jobId"],
        "status": item.get("status"),
        "errorMessage": item.get("errorMessage"),
    }
    return _resp(200, out)


def get_results(event):
    job_id = (event.get("pathParameters") or {}).get("jobId")
    if not job_id:
        return _resp(400, {"ok": False, "error": "Missing path param jobId"})

    res = table.get_item(Key={"jobId": job_id})
    item = res.get("Item")
    if not item:
        return _resp(404, {"ok": False, "error": "Job not found", "jobId": job_id})

    status = item.get("status")
    if status in ("QUEUED", "RUNNING"):
        # Salesforce can treat this as "still processing"
        return _resp(409, {"ok": False, "jobId": job_id, "status": status, "error": "Not ready"})

    if status == "FAILED":
        return _resp(200, {
            "ok": False,
            "jobId": job_id,
            "status": "FAILED",
            "error": item.get("errorMessage", "Unknown error"),
        })

    # SUCCEEDED
    bucket = item.get("resultS3Bucket") or RESULT_BUCKET_DEFAULT
    key = item.get("resultS3Key")
    if not bucket or not key:
        return _resp(500, {"ok": False, "jobId": job_id, "error": "Missing result location"})

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read().decode("utf-8")
        data = json.loads(raw)
        return _resp(200, data)
    except ClientError as e:
        return _resp(500, {"ok": False, "jobId": job_id, "error": f"S3 read failed: {str(e)}"})


def process(event, context):
    rk = _route_key(event)

    if rk == "POST /dewrra/start":
        return start_job(event)

    if rk == "GET /dewrra/status/{jobId}":
        return get_status(event)

    if rk == "GET /dewrra/results/{jobId}":
        return get_results(event)

    return _resp(404, {"ok": False, "error": f"Unknown routeKey: {rk}"})
