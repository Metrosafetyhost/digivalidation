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

COMPLIANCE_DOCUMENTS_FOLDER = (
    "Compliance Documents/"
)

MAX_FILE_NAME_LENGTH = 255

IGNORED_FILE_NAMES = {
    ".textract_ran",
    "textract_ran",
}

STANDARD_CATEGORIES = (
    "Assessment",
    "Testing",
    "Maintenance, Training",
    "Plans, Procedures, Policies etc.",
)

CONFIGURED_FOLDER_STRUCTURE = {
    "Compliance Documents/": (
        "Fire",
        "Legionella",
        "Asbestos",
        "Gas",
        "Electricity",
        "Lifts",
        "Health & Safety",
        "Other",
        "Third Party",
        "ALL",
    ),
    "Compliance Documents/Fire/":
        STANDARD_CATEGORIES,
    "Compliance Documents/Legionella/":
        STANDARD_CATEGORIES,
    "Compliance Documents/Asbestos/":
        STANDARD_CATEGORIES,
    "Compliance Documents/Gas/":
        STANDARD_CATEGORIES,
    "Compliance Documents/Electricity/":
        STANDARD_CATEGORIES,
    "Compliance Documents/Lifts/":
        STANDARD_CATEGORIES,
    "Compliance Documents/Health & Safety/":
        STANDARD_CATEGORIES,
    "Compliance Documents/Other/":
        STANDARD_CATEGORIES,
    "Compliance Documents/ALL/": (
        "Summary Service Reports",
        "Plans, Procedures, Policies etc.",
    ),
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
        event.get("pathParameters")
        or {}
    )

    return path_parameters.get(
        parameter_name
    )


def get_query_parameter(
    event: dict,
    parameter_name: str
) -> str | None:
    query_parameters = (
        event.get("queryStringParameters")
        or {}
    )

    value = query_parameters.get(
        parameter_name
    )

    if value is None:
        return None

    return unquote(value)


def get_json_body(
    event: dict
) -> dict:
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
                "The encoded request body "
                "could not be read"
            ) from error

    try:
        parsed_body = json.loads(body)

    except json.JSONDecodeError as error:
        raise ValueError(
            "The request body is not valid JSON"
        ) from error

    if not isinstance(parsed_body, dict):
        raise ValueError(
            "The request body must be "
            "a JSON object"
        )

    return parsed_body


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
    prefix = str(
        building_prefix
    ).strip()

    if not prefix:
        raise ValueError(
            "The building prefix cannot be blank"
        )

    expected_start = (
        f"{BUILDING_PREFIX}//"
    )

    if not prefix.startswith(
        expected_start
    ):
        raise ValueError(
            "The supplied prefix is not "
            "a valid Building path"
        )

    return prefix


def get_building_lookup_token(
    building_prefix: str
) -> str:
    """
    Salesforce supplies a value such as:

        Buildings//018278 |

    The stable lookup token is:

        018278 |

    The name, address and postcode stored after
    this token are not required to match.
    """
    normalised_prefix = (
        normalise_building_prefix(
            building_prefix
        )
    )

    expected_start = (
        f"{BUILDING_PREFIX}//"
    )

    lookup_token = (
        normalised_prefix[
            len(expected_start):
        ]
    ).strip()

    if not lookup_token:
        raise ValueError(
            "The Building lookup value "
            "could not be determined"
        )

    return lookup_token


def get_building_parent_prefixes() -> tuple[str, ...]:
    """
    Check the current expected S3 structure first.

    Additional variants are included for older
    or inconsistent historical object keys.
    """
    return (
        f"{BUILDING_PREFIX}//",
        f"{BUILDING_PREFIX}/",
    )


def find_building_matches_under_parent(
    parent_prefix: str,
    lookup_token: str
) -> set[str]:
    """
    Search only the immediate folders beneath
    the supplied Buildings parent prefix.

    Delimiter="/" is correct here because this
    function is identifying Building folders,
    not counting nested documents.
    """
    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    matches = set()

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=parent_prefix,
        Delimiter="/"
    ):
        for common_prefix in page.get(
            "CommonPrefixes",
            []
        ):
            folder_prefix = (
                common_prefix.get(
                    "Prefix",
                    ""
                )
            )

            if not folder_prefix:
                continue

            folder_name = (
                folder_prefix[
                    len(parent_prefix):
                ]
            )

            if folder_name.startswith(
                lookup_token
            ):
                matches.add(
                    folder_prefix
                )

    return matches


def find_building_roots(
    building_prefix: str
) -> set[str]:
    """
    Resolve the complete S3 Building folder from
    the stable Building Number prefix.

    Example input:

        Buildings//018278 |

    Example resolved root:

        Buildings//018278 | In Store FC Travel
        Shop, Asda Store EH54 6NB/
    """
    lookup_token = (
        get_building_lookup_token(
            building_prefix
        )
    )

    all_matches = set()

    for index, parent_prefix in enumerate(
        get_building_parent_prefixes()
    ):
        matches = (
            find_building_matches_under_parent(
                parent_prefix,
                lookup_token
            )
        )

        if index == 0 and len(matches) == 1:
            return matches

        all_matches.update(
            matches
        )

    return all_matches


def find_building_root(
    building_prefix: str
) -> str:
    building_roots = (
        find_building_roots(
            building_prefix
        )
    )

    if not building_roots:
        raise ValueError(
            "No AWS documents or Building folder "
            "were found for this Building."
        )

    if len(building_roots) > 1:
        roots_text = ", ".join(
            sorted(building_roots)
        )

        raise ValueError(
            "Multiple S3 Building folders were "
            "found for this Building Number. "
            "The system could not safely choose "
            "one. Found: " +
            roots_text
        )

    return next(
        iter(building_roots)
    )


def normalise_folder_path(
    folder_path: str | None
) -> str:
    if not folder_path:
        return (
            COMPLIANCE_DOCUMENTS_FOLDER
        )

    path = unquote(
        str(folder_path)
    ).strip()

    path = path.replace(
        "\\",
        "/"
    )

    while path.startswith("/"):
        path = path[1:]

    while "//" in path:
        path = path.replace(
            "//",
            "/"
        )

    if not path:
        return (
            COMPLIANCE_DOCUMENTS_FOLDER
        )

    path_parts = [
        part.strip()
        for part in path.split("/")
        if part.strip()
    ]

    if any(
        part in {".", ".."}
        for part in path_parts
    ):
        raise ValueError(
            "The supplied folder path is invalid"
        )

    normalised_path = (
        "/".join(path_parts)
        + "/"
    )

    compliance_lower = (
        COMPLIANCE_DOCUMENTS_FOLDER.lower()
    )

    path_lower = (
        normalised_path.lower()
    )

    if (
        path_lower != compliance_lower
        and not path_lower.startswith(
            compliance_lower
        )
    ):
        raise ValueError(
            "The selected folder is outside "
            "Compliance Documents"
        )

    return normalised_path


def sanitise_file_name(
    file_name: str
) -> str:
    name = str(
        file_name
    ).strip()

    name = name.replace(
        "/",
        "_"
    )

    name = name.replace(
        "\\",
        "_"
    )

    name = name.replace(
        "\x00",
        ""
    )

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


def get_configured_child_names(
    folder_path: str
) -> tuple[str, ...]:
    return tuple(
        CONFIGURED_FOLDER_STRUCTURE.get(
            folder_path,
            ()
        )
    )


def is_configured_folder(
    folder_path: str
) -> bool:
    if (
        folder_path
        ==
        COMPLIANCE_DOCUMENTS_FOLDER
    ):
        return True

    if folder_path in (
        CONFIGURED_FOLDER_STRUCTURE
    ):
        return True

    for (
        parent_path,
        child_names
    ) in (
        CONFIGURED_FOLDER_STRUCTURE.items()
    ):
        for child_name in child_names:
            child_path = (
                parent_path
                + child_name
                + "/"
            )

            if child_path == folder_path:
                return True

    return False


def is_configured_upload_destination(
    folder_path: str
) -> bool:
    if not is_configured_folder(
        folder_path
    ):
        return False

    return (
        len(
            get_configured_child_names(
                folder_path
            )
        )
        == 0
    )


def object_exists(
    key: str
) -> bool:
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


def s3_prefix_has_contents(
    prefix: str
) -> bool:
    result = s3.list_objects_v2(
        Bucket=FILE_BUCKET,
        Prefix=prefix,
        MaxKeys=1
    )

    return (
        result.get(
            "KeyCount",
            0
        )
        > 0
    )


def building_folder_exists(
    building_root: str,
    folder_path: str
) -> bool:
    return s3_prefix_has_contents(
        building_root
        + folder_path
    )


def folder_has_child_folders(
    building_root: str,
    folder_path: str
) -> bool:
    full_prefix = (
        building_root
        + folder_path
    )

    result = s3.list_objects_v2(
        Bucket=FILE_BUCKET,
        Prefix=full_prefix,
        Delimiter="/",
        MaxKeys=1000
    )

    return bool(
        result.get(
            "CommonPrefixes"
        )
    )


def list_files(
    prefix: str
) -> list[dict]:
    """
    Existing Work Order listing behaviour.

    This intentionally lists all objects beneath
    the supplied Work Order prefix.
    """
    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    files = []

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=prefix
    ):
        for item in page.get(
            "Contents",
            []
        ):
            key = item["Key"]

            if key.endswith("/"):
                continue

            filename = (
                key.rsplit("/", 1)[-1]
            )

            if filename in (
                IGNORED_FILE_NAMES
            ):
                continue

            files.append({
                "key": key,
                "name": filename,
                "sizeBytes": item["Size"],
                "lastModified": (
                    item[
                        "LastModified"
                    ].isoformat()
                )
            })

    files.sort(
        key=lambda item:
            item["lastModified"],
        reverse=True
    )

    return files


def count_documents_under_folder(
    building_root: str,
    folder_path: str
) -> int:
    """
    Count every real document beneath the folder,
    including documents in nested subfolders.

    Delimiter="/" is deliberately not used here.
    The full S3 prefix still restricts the count
    to the resolved Building and folder.
    """
    full_prefix = (
        building_root
        + folder_path
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    document_count = 0

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=full_prefix
    ):
        for item in page.get(
            "Contents",
            []
        ):
            key = item["Key"]

            if key.endswith("/"):
                continue

            file_name = (
                key.rsplit("/", 1)[-1]
            )

            if file_name in (
                IGNORED_FILE_NAMES
            ):
                continue

            document_count += 1

    return document_count


def list_building_folder(
    building_root: str,
    folder_path: str
) -> tuple[list[dict], list[dict]]:
    """
    Return only the immediate folders and files
    for the folder currently being viewed.

    Delimiter="/" remains necessary here because
    the UI is navigating one folder level at a
    time.
    """
    full_prefix = (
        building_root
        + folder_path
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    discovered_folders = {}
    files = []

    for page in paginator.paginate(
        Bucket=FILE_BUCKET,
        Prefix=full_prefix,
        Delimiter="/"
    ):
        for folder in page.get(
            "CommonPrefixes",
            []
        ):
            full_folder_path = (
                folder.get("Prefix")
            )

            if not full_folder_path:
                continue

            relative_folder_path = (
                full_folder_path[
                    len(building_root):
                ]
            )

            folder_name = (
                relative_folder_path
                .rstrip("/")
                .rsplit("/", 1)[-1]
            )

            discovered_folders[
                relative_folder_path
            ] = {
                "name": folder_name,
                "path":
                    relative_folder_path
            }

        for item in page.get(
            "Contents",
            []
        ):
            key = item["Key"]

            if key == full_prefix:
                continue

            if key.endswith("/"):
                continue

            filename = (
                key.rsplit("/", 1)[-1]
            )

            if filename in (
                IGNORED_FILE_NAMES
            ):
                continue

            files.append({
                "key": key,
                "name": filename,
                "sizeBytes": item["Size"],
                "lastModified": (
                    item[
                        "LastModified"
                    ].isoformat()
                )
            })

    configured_names = (
        get_configured_child_names(
            folder_path
        )
    )

    merged_folders = {}

    # Always display configured folders,
    # even when S3 has no object beneath them.
    for folder_name in configured_names:
        child_path = (
            folder_path
            + folder_name
            + "/"
        )

        configured_children = (
            get_configured_child_names(
                child_path
            )
        )

        has_child_folders = (
            len(
                configured_children
            )
            > 0
        )

        document_count = (
            count_documents_under_folder(
                building_root,
                child_path
            )
        )

        merged_folders[
            child_path
        ] = {
            "name":
                folder_name,
            "path":
                child_path,
            "isConfigured":
                True,
            "hasContents":
                document_count > 0,
            "documentCount":
                document_count,
            "hasChildFolders":
                has_child_folders,
            "isUploadDestination":
                not has_child_folders
        }

    # Include unexpected folders that physically
    # exist in S3 but are not in the configuration.
    for (
        discovered_path,
        discovered_folder
    ) in discovered_folders.items():

        if (
            discovered_path
            in merged_folders
        ):
            continue

        has_child_folders = (
            folder_has_child_folders(
                building_root,
                discovered_path
            )
        )

        document_count = (
            count_documents_under_folder(
                building_root,
                discovered_path
            )
        )

        merged_folders[
            discovered_path
        ] = {
            "name":
                discovered_folder[
                    "name"
                ],
            "path":
                discovered_path,
            "isConfigured":
                False,
            "hasContents":
                document_count > 0,
            "documentCount":
                document_count,
            "hasChildFolders":
                has_child_folders,
            "isUploadDestination":
                not has_child_folders
        }

    configured_order = {
        name: index
        for index, name
        in enumerate(
            configured_names
        )
    }

    folders = list(
        merged_folders.values()
    )

    folders.sort(
        key=lambda folder: (
            configured_order.get(
                folder["name"],
                9999
            ),
            folder["name"].lower()
        )
    )

    files.sort(
        key=lambda item:
            item["lastModified"],
        reverse=True
    )

    return folders, files


def build_breadcrumbs(
    folder_path: str
) -> list[dict]:
    path_parts = [
        part
        for part
        in folder_path
        .strip("/")
        .split("/")
        if part
    ]

    breadcrumbs = []
    accumulated_parts = []

    for index, part in enumerate(
        path_parts
    ):
        accumulated_parts.append(
            part
        )

        breadcrumb_path = (
            "/".join(
                accumulated_parts
            )
            + "/"
        )

        breadcrumbs.append({
            "key": (
                f"{index}-"
                f"{breadcrumb_path}"
            ),
            "name":
                part,
            "path":
                breadcrumb_path
        })

    return breadcrumbs


def current_folder_name(
    folder_path: str
) -> str:
    path_parts = [
        part
        for part
        in folder_path
        .strip("/")
        .split("/")
        if part
    ]

    if not path_parts:
        return (
            "Compliance Documents"
        )

    return path_parts[-1]


def is_key_in_building_documents(
    key: str,
    building_root: str
) -> bool:
    allowed_prefix = (
        building_root
        + COMPLIANCE_DOCUMENTS_FOLDER
    )

    return key.startswith(
        allowed_prefix
    )


def validate_upload_folder(
    building_root: str,
    supplied_folder_path: str
) -> str:
    folder_path = (
        normalise_folder_path(
            supplied_folder_path
        )
    )

    # Configured final folders are valid upload
    # destinations even when currently empty.
    if (
        is_configured_upload_destination(
            folder_path
        )
    ):
        return folder_path

    # Unconfigured folders must physically exist.
    if not building_folder_exists(
        building_root,
        folder_path
    ):
        raise ValueError(
            "The selected AWS folder "
            "does not exist."
        )

    if folder_has_child_folders(
        building_root,
        folder_path
    ):
        raise ValueError(
            "Open a final document folder "
            "before uploading."
        )

    if (
        folder_path.lower()
        ==
        COMPLIANCE_DOCUMENTS_FOLDER.lower()
    ):
        raise ValueError(
            "Open a final document folder "
            "before uploading."
        )

    return folder_path


def process_work_order_request(
    event: dict,
    raw_path: str
) -> dict:
    work_order_id = (
        get_path_parameter(
            event,
            "workOrderId"
        )
    )

    if not work_order_id:
        return response(400, {
            "error":
                "Missing workOrderId"
        })

    expected_prefix = (
        f"{WORK_ORDER_PREFIX}/"
        f"{work_order_id}/"
    )

    if raw_path.endswith(
        "/open"
    ):
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
                    "The requested object does "
                    "not belong to this Work Order"
                )
            })

        s3.head_object(
            Bucket=FILE_BUCKET,
            Key=key
        )

        return response(200, {
            "url":
                create_presigned_url(
                    key
                ),
            "expiresInSeconds":
                PRESIGNED_URL_SECONDS
        })

    files = list_files(
        expected_prefix
    )

    return response(200, {
        "workOrderId":
            work_order_id,
        "prefix":
            expected_prefix,
        "recordCount":
            len(files),
        "files":
            files
    })


def process_building_request(
    event: dict,
    raw_path: str
) -> dict:
    supplied_prefix = (
        get_query_parameter(
            event,
            "buildingPrefix"
        )
    )

    if not supplied_prefix:
        return response(400, {
            "error":
                "Missing buildingPrefix"
        })

    building_prefix = (
        normalise_building_prefix(
            supplied_prefix
        )
    )

    building_root = (
        find_building_root(
            building_prefix
        )
    )

    if raw_path.endswith(
        "/open"
    ):
        key = get_query_parameter(
            event,
            "key"
        )

        if not key:
            return response(400, {
                "error":
                    "Missing key"
            })

        if not (
            is_key_in_building_documents(
                key,
                building_root
            )
        ):
            return response(403, {
                "error": (
                    "The requested object does not "
                    "belong to this Building's "
                    "Compliance Documents folder"
                )
            })

        s3.head_object(
            Bucket=FILE_BUCKET,
            Key=key
        )

        return response(200, {
            "url":
                create_presigned_url(
                    key
                ),
            "expiresInSeconds":
                PRESIGNED_URL_SECONDS
        })

    supplied_folder_path = (
        get_query_parameter(
            event,
            "folderPath"
        )
    )

    folder_path = (
        normalise_folder_path(
            supplied_folder_path
        )
    )

    if (
        not is_configured_folder(
            folder_path
        )
        and not building_folder_exists(
            building_root,
            folder_path
        )
    ):
        return response(404, {
            "error": (
                "The selected Building folder "
                "was not found"
            )
        })

    folders, files = (
        list_building_folder(
            building_root,
            folder_path
        )
    )

    can_upload = (
        is_configured_upload_destination(
            folder_path
        )
        or (
            folder_path.lower()
            !=
            COMPLIANCE_DOCUMENTS_FOLDER.lower()
            and building_folder_exists(
                building_root,
                folder_path
            )
            and len(folders) == 0
        )
    )

    return response(200, {
        "buildingPrefix":
            building_prefix,
        "buildingRoot":
            building_root,
        "currentPath":
            folder_path,
        "currentFolderName":
            current_folder_name(
                folder_path
            ),
        "recordCount":
            len(files),
        "folders":
            folders,
        "breadcrumbs":
            build_breadcrumbs(
                folder_path
            ),
        "files":
            files,
        "canUpload":
            can_upload
    })


def process_building_upload_request(
    event: dict
) -> dict:
    body = get_json_body(
        event
    )

    supplied_prefix = body.get(
        "buildingPrefix"
    )

    supplied_folder_path = body.get(
        "folderPath"
    )

    file_name = body.get(
        "fileName"
    )

    content_type = (
        body.get(
            "contentType"
        )
        or
        "application/octet-stream"
    )

    if not supplied_prefix:
        return response(400, {
            "error":
                "Missing buildingPrefix"
        })

    if not supplied_folder_path:
        return response(400, {
            "error": (
                "Open a final document folder "
                "before uploading."
            )
        })

    if not file_name:
        return response(400, {
            "error":
                "Missing fileName"
        })

    if not isinstance(
        content_type,
        str
    ):
        return response(400, {
            "error":
                "Invalid contentType"
        })

    content_type = (
        content_type.strip()
    )

    if not content_type:
        content_type = (
            "application/octet-stream"
        )

    building_prefix = (
        normalise_building_prefix(
            supplied_prefix
        )
    )

    building_root = (
        find_building_root(
            building_prefix
        )
    )

    folder_path = (
        validate_upload_folder(
            building_root,
            supplied_folder_path
        )
    )

    safe_file_name = (
        sanitise_file_name(
            file_name
        )
    )

    object_key = (
        building_root
        + folder_path
        + safe_file_name
    )

    if object_exists(
        object_key
    ):
        return response(409, {
            "error": (
                "A file with this name already "
                "exists in the selected folder. "
                "No file was uploaded."
            ),
            "objectKey":
                object_key
        })

    upload_url = (
        create_presigned_upload_url(
            object_key,
            content_type
        )
    )

    return response(200, {
        "uploadUrl":
            upload_url,
        "objectKey":
            object_key,
        "buildingRoot":
            building_root,
        "folderPath":
            folder_path,
        "fileName":
            safe_file_name,
        "contentType":
            content_type,
        "expiresInSeconds":
            PRESIGNED_URL_SECONDS
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

        if raw_path.startswith(
            "/prod/"
        ):
            raw_path = raw_path[
                len("/prod"):
            ]

        print(
            "ROUTING PATH:",
            repr(raw_path)
        )

        if (
            raw_path
            ==
            "/files/buildings/upload-url"
        ):
            return (
                process_building_upload_request(
                    event
                )
            )

        if raw_path.startswith(
            "/files/buildings"
        ):
            return (
                process_building_request(
                    event,
                    raw_path
                )
            )

        if raw_path.startswith(
            "/files/workorders/"
        ):
            return (
                process_work_order_request(
                    event,
                    raw_path
                )
            )

        return response(404, {
            "error":
                "Unsupported file viewer route",
            "rawPath":
                raw_path,
            "path":
                event.get("path"),
            "requestContext": (
                event
                .get(
                    "requestContext",
                    {}
                )
                .get(
                    "http",
                    {}
                )
            )
        })

    except ValueError as error:
        return response(400, {
            "error":
                str(error)
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
            "details":
                str(error)
        })

    except Exception as error:
        return response(500, {
            "error": (
                "Failed to process the S3 "
                "file request"
            ),
            "details":
                str(error)
        })