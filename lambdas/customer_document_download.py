import base64
import json
import os
import re

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

FILE_BUCKET = os.environ["FILE_BUCKET"]
PRESIGNED_URL_SECONDS = int(os.environ.get("PRESIGNED_URL_SECONDS", "300"))

SALESFORCE_ID = re.compile(r"^[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?$")


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def process(event, context):
    try:
        method = (event.get("requestContext") or {}).get("http", {}).get("method")
        path = event.get("rawPath") or ""

        if method != "POST" or path not in {
            "/files/customer-documents/download",
            "/prod/files/customer-documents/download",
        }:
            return response(404, {"error": "Unsupported route"})

        body = event.get("body") or "{}"

        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")

        data = json.loads(body)

        if not isinstance(data, dict):
            return response(400, {"error": "Invalid request body"})

        work_order_id = data.get("workOrderId")
        object_key = data.get("objectKey")

        if not isinstance(work_order_id, str) or not SALESFORCE_ID.fullmatch(work_order_id):
            return response(400, {"error": "Invalid Work Order ID"})

        if not isinstance(object_key, str):
            return response(400, {"error": "Invalid S3 key"})

        expected_prefix = f"WorkOrders/{work_order_id}/"

        if (
            not object_key.startswith(expected_prefix)
            or len(object_key) <= len(expected_prefix)
            or object_key.endswith("/")
            or object_key != object_key.strip()
            or ".." in object_key
            or "\\" in object_key
            or any(ord(c) < 32 or ord(c) == 127 for c in object_key)
        ):
            return response(403, {"error": "Invalid document path"})

        tags = s3.get_object_tagging(
            Bucket=FILE_BUCKET,
            Key=object_key,
        ).get("TagSet", [])

        customer_visible = any(
            tag.get("Key") == "customer-visible"
            and tag.get("Value") == "true"
            for tag in tags
        )

        if not customer_visible:
            return response(404, {"error": "Document not available"})

        s3.head_object(
            Bucket=FILE_BUCKET,
            Key=object_key,
        )

        url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": FILE_BUCKET,
            "Key": object_key,
            "ResponseContentDisposition": "attachment",
        },
        ExpiresIn=PRESIGNED_URL_SECONDS,
    )

        return response(200, {
            "url": url,
            "expiresInSeconds": PRESIGNED_URL_SECONDS,
        })

    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
        return response(400, {"error": "Invalid request"})

    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")

        if code in {"404", "NoSuchKey", "NotFound", "AccessDenied", "403"}:
            return response(404, {"error": "Document not available"})

        return response(500, {"error": "Document download failed"})

    except Exception:
        return response(500, {"error": "Document download failed"})