import json
import os
from urllib.parse import unquote

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")

FILE_BUCKET = os.environ.get(
    "FILE_BUCKET",
    "metrosafetyprodfiles"
)

WORK_ORDER_PREFIX = os.environ.get(
    "WORK_ORDER_PREFIX",
    "WorkOrders"
)

BUILDING_PREFIX = os.environ.get(
    "BUILDING_PREFIX",
    "Buildings"
)

PRESIGNED_URL_SECONDS = int(
    os.environ.get(
        "PRESIGNED_URL_SECONDS",
        "300"
    )
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


def get_path_parameter(
    event: dict,
    parameter_name: str
) -> str | None:
    path_parameters = event.get("pathParameters") or {}
    return path_parameters.get(parameter_name)


def get_query_parameter(
    event: dict,
    parameter_name: str
) -> str | None:
    query_parameters = (
        event.get("queryStringParameters") or {}
    )

    value = query_parameters.get(parameter_name)

    if value is None:
        return None

    return unquote(value)


def list_files(prefix: str) -> list[dict]:
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
                "lastModified": (
                    item["LastModified"].isoformat()
                )
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


def normalise_building_prefix(
    building_prefix: str
) -> str:
    prefix = building_prefix.strip()

    if not prefix:
        raise ValueError(
            "The building prefix cannot be blank"
        )

    if not prefix.startswith(
        f"{BUILDING_PREFIX}/"
    ):
        raise ValueError(
            "The supplied prefix is not a building path"
        )

    if not prefix.endswith("/"):
        prefix += "/"

    return prefix


def process_work_order_request(
    event: dict,
    raw_path: str
) -> dict:
    work_order_id = get_path_parameter(
        event,
        "workOrderId"
    )

    if not work_order_id:
        return response(400, {
            "error": "Missing workOrderId"
        })

    expected_prefix = (
        f"{WORK_ORDER_PREFIX}/{work_order_id}/"
    )

    if raw_path.endswith("/open"):
        key = get_query_parameter(event, "key")

        if not key:
            return response(400, {
                "error": "Missing key"
            })

        if not key.startswith(expected_prefix):
            return response(403, {
                "error": (
                    "The requested object does not "
                    "belong to this Work Order"
                )
            })

        s3.head_object(
            Bucket=FILE_BUCKET,
            Key=key
        )

        return response(200, {
            "url": create_presigned_url(key),
            "expiresInSeconds":
                PRESIGNED_URL_SECONDS
        })

    files = list_files(expected_prefix)

    return response(200, {
        "workOrderId": work_order_id,
        "prefix": expected_prefix,
        "recordCount": len(files),
        "files": files
    })


def process_building_request(
    event: dict,
    raw_path: str
) -> dict:
    supplied_prefix = get_query_parameter(
        event,
        "buildingPrefix"
    )

    if not supplied_prefix:
        return response(400, {
            "error": "Missing buildingPrefix"
        })

    building_prefix = normalise_building_prefix(
        supplied_prefix
    )

    if raw_path.endswith("/open"):
        key = get_query_parameter(event, "key")

        if not key:
            return response(400, {
                "error": "Missing key"
            })

        if not key.startswith(building_prefix):
            return response(403, {
                "error": (
                    "The requested object does not "
                    "belong to this building folder"
                )
            })

        s3.head_object(
            Bucket=FILE_BUCKET,
            Key=key
        )

        return response(200, {
            "url": create_presigned_url(key),
            "expiresInSeconds":
                PRESIGNED_URL_SECONDS
        })

    files = list_files(building_prefix)

    return response(200, {
        "buildingPrefix": building_prefix,
        "recordCount": len(files),
        "files": files
    })


def process(event, context):
    try:
        raw_path = (
            event.get("rawPath")
            or event.get("path")
            or ""
        )

        print("RAW PATH:", repr(raw_path))
        print("EVENT VERSION:", event.get("version"))

        if raw_path.startswith(
            "/files/buildings"
        ):
            return process_building_request(
                event,
                raw_path
            )

        if raw_path.startswith(
            "/files/workorders/"
        ):
            return process_work_order_request(
                event,
                raw_path
            )

        return response(404, {
            "error": "Unsupported file viewer route",
            "rawPath": raw_path,
            "path": event.get("path"),
            "requestContext": (
                event.get("requestContext", {})
                .get("http", {})
            )
        })

    except ValueError as error:
        return response(400, {
            "error": str(error)
        })

    except ClientError as error:
        error_code = (
            error.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in {
            "NoSuchKey",
            "404",
            "NotFound"
        }:
            return response(404, {
                "error": (
                    "The requested S3 object "
                    "was not found"
                )
            })

        if error_code in {
            "AccessDenied",
            "403"
        }:
            return response(403, {
                "error": (
                    "The Lambda does not have "
                    "permission to access this "
                    "S3 object"
                )
            })

        return response(500, {
            "error": (
                "AWS failed to process the "
                "file request"
            ),
            "details": str(error)
        })

    except Exception as error:
        return response(500, {
            "error": "Failed to retrieve S3 files",
            "details": str(error)
        })