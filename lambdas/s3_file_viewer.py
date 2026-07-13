import json
import os
from urllib.parse import unquote

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")

FILE_BUCKET = os.environ.get("FILE_BUCKET", "metrosafetyprodfiles")
WORK_ORDER_PREFIX = os.environ.get("WORK_ORDER_PREFIX", "WorkOrders")
PRESIGNED_URL_SECONDS = int(
    os.environ.get("PRESIGNED_URL_SECONDS", "300")
)

IGNORED_FILE_NAMES = {
    ".textract_ran",
    "textract_ran",
}


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }


def get_work_order_id(event: dict) -> str | None:
    path_parameters = event.get("pathParameters") or {}
    return path_parameters.get("workOrderId")


def list_work_order_files(work_order_id: str) -> list[dict]:
    prefix = f"{WORK_ORDER_PREFIX}/{work_order_id}/"
    paginator = s3.get_paginator("list_objects_v2")

    files = []

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=prefix
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            filename = key.rsplit("/", 1)[-1]

            if key.endswith("/"):
                continue

            if filename in IGNORED_FILE_NAMES:
                continue

            files.append({
                "key": key,
                "name": filename,
                "sizeBytes": item["Size"],
                "lastModified": item["LastModified"].isoformat()
            })

    files.sort(
        key=lambda item: item["lastModified"],
        reverse=True
    )

    return files


def create_presigned_url(key: str) -> str:
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": FILE_BUCKET,
            "Key": key
        },
        ExpiresIn=PRESIGNED_URL_SECONDS
    )


def process(event, context):
    try:
        work_order_id = get_work_order_id(event)

        if not work_order_id:
            return response(400, {
                "error": "Missing workOrderId"
            })

        raw_path = event.get("rawPath") or event.get("path") or ""

        if raw_path.endswith("/open"):
            query_parameters = event.get("queryStringParameters") or {}
            key = query_parameters.get("key")

            if not key:
                return response(400, {
                    "error": "Missing key"
                })

            key = unquote(key)
            expected_prefix = f"{WORK_ORDER_PREFIX}/{work_order_id}/"

            if not key.startswith(expected_prefix):
                return response(403, {
                    "error": "The requested object does not belong to this Work Order"
                })

            # Confirm the object exists before signing it.
            s3.head_object(
                Bucket=FILE_BUCKET,
                Key=key
            )

            return response(200, {
                "url": create_presigned_url(key),
                "expiresInSeconds": PRESIGNED_URL_SECONDS
            })

        files = list_work_order_files(work_order_id)

        return response(200, {
            "workOrderId": work_order_id,
            "recordCount": len(files),
            "files": files
        })

    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")

        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return response(404, {
                "error": "The requested S3 object was not found"
            })

        if error_code in {"AccessDenied", "403"}:
            return response(403, {
                "error": "The Lambda does not have permission to access this S3 object"
            })

        return response(500, {
            "error": "AWS failed to process the file request",
            "details": str(error)
        })

    except Exception as error:
        return response(500, {
            "error": "Failed to retrieve Work Order files",
            "details": str(error)
        })