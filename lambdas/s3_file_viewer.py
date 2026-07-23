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

# Blank means the Building viewer begins at the
# resolved Building folder instead of assuming that
# Compliance Documents exists.
DEFAULT_BUILDING_FOLDER = ""

IGNORED_FILE_NAMES = {
    ".textract_ran",
    "textract_ran",
}


def response(
    status_code: int,
    body: dict
) -> dict:
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


def list_files(
    prefix: str,
    required_path: str | None = None
) -> list[dict]:
    """
    Existing Work Order file-listing process.

    This intentionally continues to retrieve all files
    below the supplied Work Order prefix.
    """
    paginator = s3.get_paginator(
        "list_objects_v2"
    )

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

            if (
                required_path
                and required_path.lower()
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


def create_presigned_url(
    key: str
) -> str:
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

    expected_start = (
        f"{BUILDING_PREFIX}//"
    )

    if not prefix.startswith(expected_start):
        raise ValueError(
            "The supplied prefix is not a valid "
            "building path"
        )

    return prefix


def resolve_building_root(
    building_prefix: str
) -> str:
    """
    Resolves a partial Building prefix such as:

    Buildings//062654 |

    into the complete S3 folder such as:

    Buildings//062654 | 8A Boundary Row SE1 8HP/
    """
    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    matches = set()

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=building_prefix,
        Delimiter="/"
    ):
        for item in page.get(
            "CommonPrefixes",
            []
        ):
            matches.add(item["Prefix"])

    sorted_matches = sorted(matches)

    if not sorted_matches:
        raise ValueError(
            "No AWS folder was found for this Building"
        )

    if len(sorted_matches) > 1:
        raise ValueError(
            "More than one AWS folder matches this "
            "Building Number"
        )

    return sorted_matches[0]


def normalise_folder_path(
    folder_path: str | None
) -> str:
    path = (
        folder_path
        if folder_path is not None
        else DEFAULT_BUILDING_FOLDER
    )

    path = path.strip()
    path = path.replace("\\", "/")
    path = path.lstrip("/")

    parts = [
        part
        for part in path.split("/")
        if part
    ]

    if any(
        part in {".", ".."}
        for part in parts
    ):
        raise ValueError(
            "The folder path is invalid"
        )

    if not parts:
        return ""

    return "/".join(parts) + "/"


def list_folder(
    building_root: str,
    folder_path: str
) -> tuple[list[dict], list[dict]]:
    """
    Returns only the immediate folders and files inside
    the selected Building path.

    Delimiter="/" makes S3 behave like a folder browser
    instead of returning every nested file at once.
    """
    full_prefix = (
        building_root +
        folder_path
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    folders_by_path = {}
    files = []

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=full_prefix,
        Delimiter="/"
    ):
        for item in page.get(
            "CommonPrefixes",
            []
        ):
            folder_prefix = item["Prefix"]

            relative_path = folder_prefix[
                len(building_root):
            ]

            folder_name = (
                relative_path
                .rstrip("/")
                .rsplit("/", 1)[-1]
            )

            folders_by_path[relative_path] = {
                "name": folder_name,
                "path": relative_path
            }

        for item in page.get("Contents", []):
            key = item["Key"]
            filename = key.rsplit("/", 1)[-1]

            if (
                key == full_prefix
                or key.endswith("/")
            ):
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

    folders = sorted(
        folders_by_path.values(),
        key=lambda item: item["name"].lower()
    )

    files.sort(
        key=lambda item: item["lastModified"],
        reverse=True
    )

    return folders, files


def build_breadcrumbs(
    folder_path: str
) -> list[dict]:
    """
    Always includes Building Documents so the user can
    return to the Building root from any folder.
    """
    breadcrumbs = [{
        "key": "building-root",
        "name": "Building Documents",
        "path": ""
    }]

    path_parts = [
        part
        for part in folder_path.split("/")
        if part
    ]

    for index, name in enumerate(path_parts):
        path = (
            "/".join(
                path_parts[:index + 1]
            ) +
            "/"
        )

        breadcrumbs.append({
            "key": path,
            "name": name,
            "path": path
        })

    return breadcrumbs


def process_work_order_request(
    event: dict,
    raw_path: str
) -> dict:
    """
    Existing Work Order POC.

    The Work Order route and behaviour remain separate
    from the new Building folder-navigation process.
    """
    work_order_id = get_path_parameter(
        event,
        "workOrderId"
    )

    if not work_order_id:
        return response(400, {
            "error": "Missing workOrderId"
        })

    expected_prefix = (
        f"{WORK_ORDER_PREFIX}/"
        f"{work_order_id}/"
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

        if not key.startswith(
            expected_prefix
        ):
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

    building_prefix = (
        normalise_building_prefix(
            supplied_prefix
        )
    )

    building_root = resolve_building_root(
        building_prefix
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

        if not key.startswith(
            building_root
        ):
            return response(403, {
                "error": (
                    "The requested object does not "
                    "belong to this Building"
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

    folder_path = normalise_folder_path(
        get_query_parameter(
            event,
            "folderPath"
        )
    )

    folders, files = list_folder(
        building_root,
        folder_path
    )

    breadcrumbs = build_breadcrumbs(
        folder_path
    )

    current_folder_name = (
        breadcrumbs[-1]["name"]
        if breadcrumbs
        else "Building Documents"
    )

    return response(200, {
        "buildingPrefix": building_prefix,
        "buildingRoot": building_root,
        "currentPath": folder_path,
        "currentFolderName":
            current_folder_name,
        "breadcrumbs": breadcrumbs,
        "folders": folders,
        "folderCount": len(folders),
        "recordCount": len(files),
        "files": files
    })


def process(
    event,
    context
):
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
            "error":
                "Unsupported file viewer route",
            "rawPath": raw_path,
            "path": event.get("path"),
            "requestContext": (
                event.get(
                    "requestContext",
                    {}
                )
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
            "error":
                "Failed to retrieve S3 files",
            "details": str(error)
        })