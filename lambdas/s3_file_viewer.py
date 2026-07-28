import base64
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

BUILDING_ASSESSMENT_PATH = (
    "/Compliance Documents/Fire/Assessment/"
)

COMPLIANCE_DOCUMENTS_MARKER = (
    "/Compliance Documents/"
)

ALLOWED_DISCIPLINES = {
    "ALL",
    "Asbestos",
    "Electricity",
    "Fire",
    "Gas",
    "Health & Safety",
    "Legionella",
    "Lifts",
    "Other",
    "Third Party",
}

ALLOWED_CATEGORIES = {
    "Assessment",
    "Maintenance, Training",
    "Plans, Procedures, Policies etc.",
    "Testing",
}

MAX_FILE_NAME_LENGTH = 255

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
    path_parameters = (
        event.get("pathParameters") or {}
    )

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


def get_json_body(event: dict) -> dict:
    body = event.get("body")

    if not body:
        return {}

    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(
                body
            ).decode("utf-8")
        except Exception as error:
            raise ValueError(
                "The encoded request body could "
                "not be read"
            ) from error

    try:
        parsed_body = json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(
            "The request body is not valid JSON"
        ) from error

    if not isinstance(parsed_body, dict):
        raise ValueError(
            "The request body must be a JSON object"
        )

    return parsed_body


def list_files(
    prefix: str,
    required_path: str | None = None
) -> list[dict]:
    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    files = []

    required_path_lower = (
        required_path.lower()
        if required_path
        else None
    )

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

            if (
                required_path_lower
                and required_path_lower
                not in key.lower()
            ):
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


def create_presigned_upload_url(
    key: str,
    content_type: str
) -> str:
    return s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": FILE_BUCKET,
            "Key": key,
            "ContentType": content_type
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

    expected_start = f"{BUILDING_PREFIX}//"

    if not prefix.startswith(expected_start):
        raise ValueError(
            "The supplied prefix is not a valid "
            "building path"
        )

    return prefix


def sanitise_file_name(
    file_name: str
) -> str:
    name = file_name.strip()

    name = name.replace("/", "_")
    name = name.replace("\\", "_")
    name = name.replace("\x00", "")

    name = "".join(
        character
        for character in name
        if ord(character) >= 32
    )

    if not name:
        raise ValueError(
            "The file name cannot be blank"
        )

    if name in {".", ".."}:
        raise ValueError(
            "The file name is invalid"
        )

    if len(name) > MAX_FILE_NAME_LENGTH:
        raise ValueError(
            "The file name is too long"
        )

    return name


def find_building_roots(
    building_prefix: str
) -> set[str]:
    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    building_roots = set()

    marker_lower = (
        COMPLIANCE_DOCUMENTS_MARKER.lower()
    )

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=building_prefix
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            key_lower = key.lower()

            marker_position = key_lower.find(
                marker_lower
            )

            if marker_position == -1:
                continue

            building_root = (
                key[:marker_position] + "/"
            )

            building_roots.add(
                building_root
            )

    return building_roots


def find_building_root(
    building_prefix: str
) -> str:
    building_roots = find_building_roots(
        building_prefix
    )

    if not building_roots:
        raise ValueError(
            "No existing Compliance Documents "
            "folder was found for this Building. "
            "No file was uploaded."
        )

    if len(building_roots) > 1:
        roots_text = ", ".join(
            sorted(building_roots)
        )

        raise ValueError(
            "Multiple S3 Building folders were "
            "found for this Building Number. "
            "No file was uploaded. Found: " +
            roots_text
        )

    return next(iter(building_roots))


def object_exists(key: str) -> bool:
    try:
        s3.head_object(
            Bucket=FILE_BUCKET,
            Key=key
        )

        return True

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
            return False

        raise


def is_building_assessment_key(
    key: str,
    building_prefix: str
) -> bool:
    return (
        key.startswith(building_prefix)
        and BUILDING_ASSESSMENT_PATH.lower()
        in key.lower()
    )


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
        key = get_query_parameter(
            event,
            "key"
        )

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
        key = get_query_parameter(
            event,
            "key"
        )

        if not key:
            return response(400, {
                "error": "Missing key"
            })

        if not is_building_assessment_key(
            key,
            building_prefix
        ):
            return response(403, {
                "error": (
                    "The requested object does not "
                    "belong to this Building's Fire "
                    "Assessment folder"
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

    files = list_files(
        prefix=building_prefix,
        required_path=BUILDING_ASSESSMENT_PATH
    )

    return response(200, {
        "buildingPrefix": building_prefix,
        "requiredPath":
            BUILDING_ASSESSMENT_PATH,
        "recordCount": len(files),
        "files": files
    })


def process_building_upload_request(
    event: dict
) -> dict:
    body = get_json_body(event)

    supplied_prefix = body.get(
        "buildingPrefix"
    )

    discipline = body.get(
        "discipline"
    )

    category = body.get(
        "category"
    )

    file_name = body.get(
        "fileName"
    )

    content_type = (
        body.get("contentType")
        or "application/octet-stream"
    )

    if not supplied_prefix:
        return response(400, {
            "error": "Missing buildingPrefix"
        })

    if discipline not in ALLOWED_DISCIPLINES:
        return response(400, {
            "error": (
                "Invalid discipline. Allowed values "
                "are: " +
                ", ".join(
                    sorted(ALLOWED_DISCIPLINES)
                )
            )
        })

    if category not in ALLOWED_CATEGORIES:
        return response(400, {
            "error": (
                "Invalid category. Allowed values "
                "are: " +
                ", ".join(
                    sorted(ALLOWED_CATEGORIES)
                )
            )
        })

    if not file_name:
        return response(400, {
            "error": "Missing fileName"
        })

    if not isinstance(content_type, str):
        return response(400, {
            "error": "Invalid contentType"
        })

    content_type = content_type.strip()

    if not content_type:
        content_type = (
            "application/octet-stream"
        )

    building_prefix = normalise_building_prefix(
        supplied_prefix
    )

    building_root = find_building_root(
        building_prefix
    )

    safe_file_name = sanitise_file_name(
        file_name
    )

    object_key = (
        f"{building_root}"
        f"Compliance Documents/"
        f"{discipline}/"
        f"{category}/"
        f"{safe_file_name}"
    )

    if object_exists(object_key):
        return response(409, {
            "error": (
                "A file with this name already "
                "exists in the selected folder. "
                "No file was uploaded."
            ),
            "objectKey": object_key
        })

    upload_url = create_presigned_upload_url(
        object_key,
        content_type
    )

    return response(200, {
        "uploadUrl": upload_url,
        "objectKey": object_key,
        "buildingRoot": building_root,
        "discipline": discipline,
        "category": category,
        "fileName": safe_file_name,
        "contentType": content_type,
        "expiresInSeconds":
            PRESIGNED_URL_SECONDS
    })


def process(event, context):
    try:
        raw_path = (
            event.get("rawPath")
            or event.get("path")
            or ""
        )

        print(
            "ORIGINAL RAW PATH:",
            repr(raw_path)
        )

        if raw_path.startswith("/prod/"):
            raw_path = raw_path[len("/prod"):]

        print(
            "ROUTING PATH:",
            repr(raw_path)
        )

        # This exact route must be checked before
        # the general /files/buildings route.
        if (
            raw_path
            == "/files/buildings/upload-url"
        ):
            return process_building_upload_request(
                event
            )

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
            "error": (
                "Failed to process the S3 "
                "file request"
            ),
            "details": str(error)
        })