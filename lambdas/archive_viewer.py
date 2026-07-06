import json
import os
import boto3

s3 = boto3.client("s3")

ARCHIVE_BUCKET = os.environ["ARCHIVE_BUCKET"]
ARCHIVE_PREFIX = os.environ.get("ARCHIVE_PREFIX", "salesforce/workorders")


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }


def process(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        work_order_id = path_params.get("workOrderId")

        if not work_order_id:
            return _response(400, {
                "error": "Missing workOrderId"
            })

        key = f"{ARCHIVE_PREFIX}/{work_order_id}/manifest.json"

        obj = s3.get_object(
            Bucket=ARCHIVE_BUCKET,
            Key=key
        )

        manifest = json.loads(obj["Body"].read().decode("utf-8"))

        return _response(200, manifest)

    except s3.exceptions.NoSuchKey:
        return _response(404, {
            "error": "Archive manifest not found"
        })

    except Exception as e:
        return _response(500, {
            "error": "Failed to retrieve archive manifest",
            "details": str(e)
        })