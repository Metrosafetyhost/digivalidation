import json
import os
import boto3
from botocore.exceptions import ClientError

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


def _get_json_from_s3(key):
    obj = s3.get_object(
        Bucket=ARCHIVE_BUCKET,
        Key=key
    )
    return json.loads(obj["Body"].read().decode("utf-8"))


def process(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        work_order_id = path_params.get("workOrderId")

        if not work_order_id:
            return _response(400, {
                "error": "Missing workOrderId"
            })

        raw_path = event.get("rawPath", "")

        if raw_path.endswith("/risk-assessment-questions"):
            filename = "risk_assessment_questions.json"
        elif raw_path.endswith("/answers"):
            filename = "answers.json"
        else:
            filename = "manifest.json"

        key = f"{ARCHIVE_PREFIX}/{work_order_id}/{filename}"

        data = _get_json_from_s3(key)

        return _response(200, data)

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        if error_code in ["NoSuchKey", "NoSuchBucket", "404"]:
            return _response(404, {
                "error": "Archive file not found"
            })

        if error_code == "AccessDenied":
            return _response(403, {
                "error": "Lambda does not have permission to read the archive file"
            })

        return _response(500, {
            "error": "S3 error while retrieving archive file",
            "details": str(e)
        })

    except Exception as e:
        return _response(500, {
            "error": "Failed to retrieve archive file",
            "details": str(e)
        })