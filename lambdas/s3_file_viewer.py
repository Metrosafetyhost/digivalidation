import base64
import json
import os
import time
from urllib.parse import unquote

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client('s3')

FILE_BUCKET = os.environ.get('FILE_BUCKET', 'metrosafetyprodfiles')

WORK_ORDER_PREFIX = os.environ.get('WORK_ORDER_PREFIX', 'WorkOrders')

BUILDING_PREFIX = os.environ.get('BUILDING_PREFIX', 'Buildings')

PRESIGNED_URL_SECONDS = int(os.environ.get('PRESIGNED_URL_SECONDS', '300'))

COMPLIANCE_DOCUMENTS_FOLDER = 'Compliance Documents/'

MAX_FILE_NAME_LENGTH = 255

IGNORED_FILE_NAMES = {
    '.textract_ran',
    'textract_ran',
}

STANDARD_CATEGORIES = (
    'Assessment',
    'Testing',
    'Maintenance, Training',
    'Plans, Procedures, Policies etc.',
)

CONFIGURED_FOLDER_STRUCTURE = {
    'Compliance Documents/': (
        'Fire',
        'Legionella',
        'Asbestos',
        'Gas',
        'Electricity',
        'Lifts',
        'Health & Safety',
        'Other',
        'Third Party',
        'ALL',
    ),
    'Compliance Documents/Fire/': STANDARD_CATEGORIES,
    'Compliance Documents/Legionella/': STANDARD_CATEGORIES,
    'Compliance Documents/Asbestos/': STANDARD_CATEGORIES,
    'Compliance Documents/Gas/': STANDARD_CATEGORIES,
    'Compliance Documents/Electricity/': STANDARD_CATEGORIES,
    'Compliance Documents/Lifts/': STANDARD_CATEGORIES,
    'Compliance Documents/Health & Safety/': STANDARD_CATEGORIES,
    'Compliance Documents/Other/': STANDARD_CATEGORIES,
    'Compliance Documents/ALL/': (
        'Summary Service Reports',
        'Plans, Procedures, Policies etc.',
    ),
}


MALFORMED_BUILDING_ROOT_MARKERS = (
    'compliance documents',
    'assessment',
    'testing',
    'maintenance, training',
    'plans, procedures, policies etc.',
)

class BuildingRootNotFoundError(Exception):
    pass

def response(status_code: int, body: dict) -> dict:
    return {'statusCode': status_code, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps(body)}


def log_timing(label: str, started_at: float, **details) -> float:
    elapsed_ms = (time.perf_counter() - started_at) * 1000

    detail_text = ' '.join(f'{key}={value}' for key, value in details.items())

    if detail_text:
        print(f'[PERF] {label}: {elapsed_ms:.2f} ms {detail_text}')
    else:
        print(f'[PERF] {label}: {elapsed_ms:.2f} ms')

    return elapsed_ms


def get_path_parameter(event: dict, parameter_name: str) -> str | None:
    path_parameters = event.get('pathParameters') or {}

    return path_parameters.get(parameter_name)


def get_query_parameter(event: dict, parameter_name: str) -> str | None:
    query_parameters = event.get('queryStringParameters') or {}

    value = query_parameters.get(parameter_name)

    if value is None:
        return None

    return unquote(value)


def get_json_body(event: dict) -> dict:
    body = event.get('body')

    if not body:
        return {}

    if event.get('isBase64Encoded'):
        try:
            body = base64.b64decode(body).decode('utf-8')

        except Exception as error:
            raise ValueError('The encoded request body could not be read') from error

    try:
        parsed_body = json.loads(body)

    except json.JSONDecodeError as error:
        raise ValueError('The request body is not valid JSON') from error

    if not isinstance(parsed_body, dict):
        raise ValueError('The request body must be a JSON object')

    return parsed_body


def create_presigned_url(key: str) -> str:
    return s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': FILE_BUCKET, 'Key': key}, ExpiresIn=PRESIGNED_URL_SECONDS)


def create_presigned_upload_url(key: str, content_type: str) -> str:
    return s3.generate_presigned_url(
        ClientMethod='put_object', Params={'Bucket': FILE_BUCKET, 'Key': key, 'ContentType': content_type}, ExpiresIn=PRESIGNED_URL_SECONDS
    )


def normalise_building_prefix(building_prefix: str) -> str:
    prefix = str(building_prefix).strip()

    if not prefix:
        raise ValueError(
            'The building prefix cannot be blank'
        )

    single_slash_start = f'{BUILDING_PREFIX}/'
    double_slash_start = f'{BUILDING_PREFIX}//'

    if not (
        prefix.startswith(single_slash_start)
        or prefix.startswith(double_slash_start)
    ):
        raise ValueError(
            'The supplied prefix is not a valid Building path'
        )

    return prefix


def normalise_exact_building_root(building_root: str) -> str:
    root = str(building_root).strip()

    if not root:
        raise ValueError('The Building root cannot be blank')

    if not root.startswith(f'{BUILDING_PREFIX}/'):
        raise ValueError('The supplied Building root is not valid')

    return root.rstrip('/') + '/'


def get_boolean_query_parameter(
    event: dict,
    parameter_name: str,
    default: bool,
) -> bool:
    value = get_query_parameter(event, parameter_name)

    if value is None:
        return default

    normalised = str(value).strip().lower()

    if normalised in {'true', '1', 'yes'}:
        return True

    if normalised in {'false', '0', 'no'}:
        return False

    raise ValueError(f'{parameter_name} must be true or false')


def get_building_lookup_token(building_prefix: str) -> str:

    # Extract the stable Building Number token.

    normalised_prefix = normalise_building_prefix(
        building_prefix
    )

    if normalised_prefix.startswith(
        f'{BUILDING_PREFIX}//'
    ):
        remainder = normalised_prefix[
            len(f'{BUILDING_PREFIX}//'):
        ]
    else:
        remainder = normalised_prefix[
            len(f'{BUILDING_PREFIX}/'):
        ]

    separator_index = remainder.find('|')

    if separator_index < 0:
        raise ValueError(
            'The Building lookup value could not be determined'
        )

    building_number = remainder[
        :separator_index
    ].strip()

    if not building_number:
        raise ValueError(
            'The Building lookup value could not be determined'
        )

    return f'{building_number} |'


def get_building_parent_prefixes() -> tuple[str, ...]:

    return (
        f'{BUILDING_PREFIX}/',
        f'{BUILDING_PREFIX}//',
    )


def find_building_matches_under_parent(parent_prefix: str, lookup_token: str) -> set[str]:
    """
    Search only S3 keys that can belong to the
    requested Building Number.

    Previously this listed every immediate folder
    beneath Buildings/ and Buildings// and filtered
    the results in Python. In Production that meant
    walking a very large Building namespace on every
    viewer request.

    S3 already supports prefix filtering, so the
    lookup can start directly at:

        Buildings/<building number> |
        Buildings//<building number> |

    Delimiter="/" still limits the result to candidate
    Building roots while preserving legacy/malformed
    candidates that begin with the same stable token.
    """
    started_at = time.perf_counter()

    search_prefix = parent_prefix + lookup_token

    paginator = s3.get_paginator('list_objects_v2')

    matches = set()
    page_count = 0
    common_prefix_count = 0
    object_count = 0

    for page in paginator.paginate(Bucket=FILE_BUCKET, Prefix=search_prefix, Delimiter='/'):
        page_count += 1

        common_prefixes = page.get('CommonPrefixes', [])
        common_prefix_count += len(common_prefixes)

        # Normally Building roots are represented by
        # CommonPrefixes. Contents is counted only for
        # timing/diagnostics because legacy folder
        # marker objects may also be returned.
        object_count += len(page.get('Contents', []))

        for common_prefix in common_prefixes:
            folder_prefix = common_prefix.get('Prefix', '')

            if not folder_prefix:
                continue

            folder_name = folder_prefix[len(parent_prefix) :]

            if folder_name.startswith(lookup_token):
                matches.add(folder_prefix)

    log_timing(
        'targeted building parent lookup',
        started_at,
        parent=parent_prefix,
        searchPrefix=search_prefix,
        pages=page_count,
        commonPrefixes=common_prefix_count,
        objects=object_count,
        matches=len(matches),
    )

    return matches


def find_building_roots(building_prefix: str) -> set[str]:
    """
    Resolve all immediate S3 Building folders that
    begin with the stable Building Number token.
    """
    lookup_token = get_building_lookup_token(building_prefix)

    all_matches = set()

    for parent_prefix in get_building_parent_prefixes():
        all_matches.update(find_building_matches_under_parent(parent_prefix, lookup_token))

    return all_matches


def get_root_parent_prefix(building_root: str) -> str:
    for parent_prefix in get_building_parent_prefixes():
        if building_root.startswith(parent_prefix):
            return parent_prefix

    return ''


def get_building_root_suffix(building_root: str, lookup_token: str) -> str:
    parent_prefix = get_root_parent_prefix(building_root)

    if not parent_prefix:
        return ''

    folder_name = building_root[len(parent_prefix) :].rstrip('/')

    if not folder_name.startswith(lookup_token):
        return ''

    return folder_name[len(lookup_token) :].strip()


def building_root_priority(building_root: str, lookup_token: str) -> int:

    # Rank candidate Building roots

    parent_prefix = get_root_parent_prefix(building_root)

    score = 0

    if parent_prefix == f'{BUILDING_PREFIX}/':
        score += 20
    elif parent_prefix == f'{BUILDING_PREFIX}//':
        score += 10

    suffix = get_building_root_suffix(building_root, lookup_token)

    suffix_lower = suffix.lower()

    if any(marker in suffix_lower for marker in (MALFORMED_BUILDING_ROOT_MARKERS)):
        score -= 1000
    elif suffix:
        score += 50

    return score


def prefix_contains_real_files(prefix: str) -> bool:
    """
    Return True when a prefix contains at least one
    real document rather than only folder markers or
    ignored processing marker files.
    """
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=FILE_BUCKET, Prefix=prefix):
        for item in page.get('Contents', []):
            key = item.get('Key', '')

            if not key or key.endswith('/'):
                continue

            file_name = key.rsplit('/', 1)[-1]

            if file_name in IGNORED_FILE_NAMES:
                continue

            return True

    return False


def create_missing_building_root(building_prefix: str) -> str:
    """
    Create a zero-byte S3 folder marker when a
    Building does not yet have an S3 root.
    """
    normalised_prefix = normalise_building_prefix(building_prefix)

    building_root = normalised_prefix.rstrip('/') + '/'

    s3.put_object(Bucket=FILE_BUCKET, Key=building_root, Body=b'', ContentType='application/x-directory')

    return building_root


def resolve_building_root(building_prefix: str, create_if_missing: bool = True) -> dict:
    """
    Select the safest Building root and return any
    warnings about lower-priority legacy roots.

    A missing root can be created automatically so
    the configured virtual folder structure remains
    available without relying on Neilon to create
    empty folders first.

    Timing logs are emitted to CloudWatch only.
    The API response remains unchanged.
    """
    total_started_at = time.perf_counter()

    lookup_token = get_building_lookup_token(building_prefix)

    lookup_started_at = time.perf_counter()
    building_roots = find_building_roots(building_prefix)
    log_timing('building root lookup', lookup_started_at, candidates=len(building_roots))

    if not building_roots:
        if not create_if_missing:
            raise BuildingRootNotFoundError(
                'No AWS documents or Building folder were found for this Building.'
            )

        creation_started_at = time.perf_counter()
        created_root = create_missing_building_root(building_prefix)
        log_timing('missing building root creation', creation_started_at)
        log_timing('building root resolution total', total_started_at, selected=created_root, created=True, warnings=0)

        return {'buildingRoot': created_root, 'buildingRootCreated': True, 'warnings': []}

    ranking_started_at = time.perf_counter()
    ranked_roots = sorted(building_roots, key=lambda root: (building_root_priority(root, lookup_token), root), reverse=True)

    highest_score = building_root_priority(ranked_roots[0], lookup_token)

    highest_ranked_roots = [root for root in ranked_roots if building_root_priority(root, lookup_token) == highest_score]
    log_timing('building root ranking', ranking_started_at, candidates=len(ranked_roots), topScore=highest_score)

    if len(highest_ranked_roots) > 1:
        roots_text = ', '.join(sorted(highest_ranked_roots))

        raise ValueError(
            'Multiple equally valid S3 Building folders were found for this Building Number. The system could not safely choose one. Found: ' + roots_text
        )

    selected_root = ranked_roots[0]
    warnings = []

    warning_scan_started_at = time.perf_counter()
    lower_priority_checked = 0

    for lower_priority_root in ranked_roots[1:]:
        lower_priority_checked += 1

        if prefix_contains_real_files(lower_priority_root):
            warnings.append('A lower-priority legacy Building folder also contains files and may require review: ' + lower_priority_root)

    log_timing('legacy root warning scan', warning_scan_started_at, checked=lower_priority_checked, warnings=len(warnings))
    log_timing('building root resolution total', total_started_at, selected=selected_root, created=False, warnings=len(warnings))

    return {'buildingRoot': selected_root, 'buildingRootCreated': False, 'warnings': warnings}


def find_building_root(building_prefix: str) -> str:
    return resolve_building_root(building_prefix, create_if_missing=True)['buildingRoot']


def normalise_folder_path(folder_path: str | None) -> str:
    if not folder_path:
        return COMPLIANCE_DOCUMENTS_FOLDER

    path = unquote(str(folder_path)).strip()

    path = path.replace('\\', '/')

    while path.startswith('/'):
        path = path[1:]

    while '//' in path:
        path = path.replace('//', '/')

    if not path:
        return COMPLIANCE_DOCUMENTS_FOLDER

    path_parts = [part.strip() for part in path.split('/') if part.strip()]

    if any(part in {'.', '..'} for part in path_parts):
        raise ValueError('The supplied folder path is invalid')

    normalised_path = '/'.join(path_parts) + '/'

    compliance_lower = COMPLIANCE_DOCUMENTS_FOLDER.lower()

    path_lower = normalised_path.lower()

    if path_lower != compliance_lower and not path_lower.startswith(compliance_lower):
        raise ValueError('The selected folder is outside Compliance Documents')

    return normalised_path


def sanitise_file_name(file_name: str) -> str:
    name = str(file_name).strip()

    name = name.replace('/', '_')

    name = name.replace('\\', '_')

    name = name.replace('\x00', '')

    name = ''.join(character for character in name if ord(character) >= 32)

    if not name:
        raise ValueError('The file name cannot be blank')

    if name in {'.', '..'}:
        raise ValueError('The file name is invalid')

    if len(name) > MAX_FILE_NAME_LENGTH:
        raise ValueError('The file name is too long')

    return name


def get_configured_child_names(folder_path: str) -> tuple[str, ...]:
    return tuple(CONFIGURED_FOLDER_STRUCTURE.get(folder_path, ()))


def is_configured_folder(folder_path: str) -> bool:
    if folder_path == COMPLIANCE_DOCUMENTS_FOLDER:
        return True

    if folder_path in (CONFIGURED_FOLDER_STRUCTURE):
        return True

    for parent_path, child_names in CONFIGURED_FOLDER_STRUCTURE.items():
        for child_name in child_names:
            child_path = parent_path + child_name + '/'

            if child_path == folder_path:
                return True

    return False


def is_configured_upload_destination(folder_path: str) -> bool:
    if not is_configured_folder(folder_path):
        return False

    return len(get_configured_child_names(folder_path)) == 0


def object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=FILE_BUCKET, Key=key)

        return True

    except ClientError as error:
        error_code = error.response.get('Error', {}).get('Code')

        if error_code in {'NoSuchKey', '404', 'NotFound'}:
            return False

        raise


def s3_prefix_has_contents(prefix: str) -> bool:
    result = s3.list_objects_v2(Bucket=FILE_BUCKET, Prefix=prefix, MaxKeys=1)

    return result.get('KeyCount', 0) > 0


def building_folder_exists(building_root: str, folder_path: str) -> bool:
    return s3_prefix_has_contents(building_root + folder_path)


def folder_has_child_folders(building_root: str, folder_path: str) -> bool:
    full_prefix = building_root + folder_path

    result = s3.list_objects_v2(Bucket=FILE_BUCKET, Prefix=full_prefix, Delimiter='/', MaxKeys=1000)

    return bool(result.get('CommonPrefixes'))


def list_files(prefix: str) -> list[dict]:
    """
    Existing Work Order listing behaviour.

    This intentionally lists all objects beneath
    the supplied Work Order prefix.
    """
    paginator = s3.get_paginator('list_objects_v2')

    files = []

    for page in paginator.paginate(Bucket=FILE_BUCKET, Prefix=prefix):
        for item in page.get('Contents', []):
            key = item['Key']

            if key.endswith('/'):
                continue

            filename = key.rsplit('/', 1)[-1]

            if filename in (IGNORED_FILE_NAMES):
                continue

            files.append({'key': key, 'name': filename, 'sizeBytes': item['Size'], 'lastModified': (item['LastModified'].isoformat())})

    files.sort(key=lambda item: item['lastModified'], reverse=True)

    return files


def count_documents_under_folder(building_root: str, folder_path: str) -> int:
    """
    Count every real document beneath the folder,
    including documents in nested subfolders.

    Delimiter="/" is deliberately not used here.
    The full S3 prefix still restricts the count
    to the resolved Building and folder.

    Timing logs are emitted so we can measure the
    cost of the current recursive counting strategy.
    """
    started_at = time.perf_counter()

    full_prefix = building_root + folder_path

    paginator = s3.get_paginator('list_objects_v2')

    document_count = 0
    page_count = 0
    object_count = 0

    for page in paginator.paginate(Bucket=FILE_BUCKET, Prefix=full_prefix):
        page_count += 1

        for item in page.get('Contents', []):
            object_count += 1
            key = item['Key']

            if key.endswith('/'):
                continue

            file_name = key.rsplit('/', 1)[-1]

            if file_name in (IGNORED_FILE_NAMES):
                continue

            document_count += 1

    log_timing('folder document count', started_at, folder=folder_path, pages=page_count, objects=object_count, documents=document_count)

    return document_count


def list_building_folder(building_root: str, folder_path: str) -> tuple[list[dict], list[dict]]:
    """
    Return the immediate folders and files for the
    folder currently being viewed.

    Performance optimisation:
    S3 is recursively listed once for the current
    folder. The returned keys are then aggregated in
    memory to determine:

    - immediate child folders
    - direct files
    - recursive document counts per child folder
    - whether unexpected child folders have children

    This replaces the previous pattern of one current
    folder listing plus a separate recursive S3 listing
    for every displayed child folder.
    """
    total_started_at = time.perf_counter()

    full_prefix = building_root + folder_path

    paginator = s3.get_paginator('list_objects_v2')

    discovered_folders = {}
    files = []
    document_counts = {}
    unexpected_has_child_folders = {}

    page_count = 0
    object_count = 0
    real_document_count = 0

    listing_started_at = time.perf_counter()

    for page in paginator.paginate(Bucket=FILE_BUCKET, Prefix=full_prefix):
        page_count += 1

        for item in page.get('Contents', []):
            object_count += 1

            key = item.get('Key', '')

            if not key:
                continue

            if not key.startswith(full_prefix):
                continue

            relative_key = key[len(full_prefix) :]

            if not relative_key:
                continue

            # Anything containing "/" is beneath an
            # immediate child folder of the folder being
            # viewed. This also detects S3 folder-marker
            # objects such as "Fire/".
            if '/' in relative_key:
                child_name, remainder = relative_key.split('/', 1)

                if not child_name:
                    continue

                child_path = folder_path + child_name + '/'

                discovered_folders[child_path] = {'name': child_name, 'path': child_path}

                document_counts.setdefault(child_path, 0)

                unexpected_has_child_folders.setdefault(child_path, False)

                # A further slash in the remainder means
                # this immediate child physically contains
                # another subfolder.
                if remainder and '/' in remainder:
                    unexpected_has_child_folders[child_path] = True

                # Folder-marker objects are not documents.
                if key.endswith('/'):
                    continue

                file_name = key.rsplit('/', 1)[-1]

                if file_name in IGNORED_FILE_NAMES:
                    continue

                document_counts[child_path] += 1

                real_document_count += 1
                continue

            # No slash means this file is directly inside
            # the folder currently being viewed.
            if key.endswith('/'):
                continue

            file_name = relative_key

            if file_name in IGNORED_FILE_NAMES:
                continue

            files.append({'key': key, 'name': file_name, 'sizeBytes': item['Size'], 'lastModified': (item['LastModified'].isoformat())})

            real_document_count += 1

    log_timing(
        'single-pass folder tree listing',
        listing_started_at,
        folder=folder_path,
        pages=page_count,
        objects=object_count,
        realDocuments=real_document_count,
        discoveredFolders=len(discovered_folders),
        directFiles=len(files),
    )

    configured_names = get_configured_child_names(folder_path)

    merged_folders = {}

    aggregation_started_at = time.perf_counter()

    # Always display configured folders, even when no
    # corresponding S3 object currently exists.
    for folder_name in configured_names:
        child_path = folder_path + folder_name + '/'

        configured_children = get_configured_child_names(child_path)

        has_child_folders = len(configured_children) > 0

        document_count = document_counts.get(child_path, 0)

        merged_folders[child_path] = {
            'name': folder_name,
            'path': child_path,
            'isConfigured': True,
            'hasContents': document_count > 0,
            'documentCount': document_count,
            'hasChildFolders': has_child_folders,
            'isUploadDestination': not has_child_folders,
        }

    # Include unexpected folders that physically exist
    # in S3 but are not part of the configured structure.
    for discovered_path, discovered_folder in discovered_folders.items():
        if discovered_path in merged_folders:
            continue

        has_child_folders = unexpected_has_child_folders.get(discovered_path, False)

        document_count = document_counts.get(discovered_path, 0)

        merged_folders[discovered_path] = {
            'name': discovered_folder['name'],
            'path': discovered_path,
            'isConfigured': False,
            'hasContents': document_count > 0,
            'documentCount': document_count,
            'hasChildFolders': has_child_folders,
            'isUploadDestination': not has_child_folders,
        }

    log_timing(
        'in-memory folder aggregation',
        aggregation_started_at,
        folder=folder_path,
        configuredFolders=len(configured_names),
        discoveredFolders=len(discovered_folders),
        returnedFolders=len(merged_folders),
    )

    configured_order = {name: index for index, name in enumerate(configured_names)}

    folders = list(merged_folders.values())

    folders.sort(key=lambda folder: (configured_order.get(folder['name'], 9999), folder['name'].lower()))

    files.sort(key=lambda item: item['lastModified'], reverse=True)

    log_timing('building folder listing total', total_started_at, folder=folder_path, returnedFolders=len(folders), returnedFiles=len(files))

    return folders, files


def build_breadcrumbs(folder_path: str) -> list[dict]:
    path_parts = [part for part in folder_path.strip('/').split('/') if part]

    breadcrumbs = []
    accumulated_parts = []

    for index, part in enumerate(path_parts):
        accumulated_parts.append(part)

        breadcrumb_path = '/'.join(accumulated_parts) + '/'

        breadcrumbs.append({'key': (f'{index}-{breadcrumb_path}'), 'name': part, 'path': breadcrumb_path})

    return breadcrumbs


def current_folder_name(folder_path: str) -> str:
    path_parts = [part for part in folder_path.strip('/').split('/') if part]

    if not path_parts:
        return 'Compliance Documents'

    return path_parts[-1]


def is_key_in_building_documents(key: str, building_root: str) -> bool:
    allowed_prefix = building_root + COMPLIANCE_DOCUMENTS_FOLDER

    return key.startswith(allowed_prefix)


def validate_upload_folder(building_root: str, supplied_folder_path: str) -> str:
    folder_path = normalise_folder_path(supplied_folder_path)

    # Configured final folders are valid upload
    # destinations even when currently empty.
    if is_configured_upload_destination(folder_path):
        return folder_path

    # Unconfigured folders must physically exist.
    if not building_folder_exists(building_root, folder_path):
        raise ValueError('The selected AWS folder does not exist.')

    if folder_has_child_folders(building_root, folder_path):
        raise ValueError('Open a final document folder before uploading.')

    if folder_path.lower() == COMPLIANCE_DOCUMENTS_FOLDER.lower():
        raise ValueError('Open a final document folder before uploading.')

    return folder_path


def process_work_order_request(event: dict, raw_path: str) -> dict:
    work_order_id = get_path_parameter(event, 'workOrderId')

    if not work_order_id:
        return response(400, {'error': 'Missing workOrderId'})

    expected_prefix = f'{WORK_ORDER_PREFIX}/{work_order_id}/'

    if raw_path.endswith('/upload-url'):
        body = get_json_body(event)
        file_name = body.get('fileName')
        content_type = body.get('contentType') or 'application/octet-stream'

        if not file_name:
            return response(400, {'error': 'Missing fileName'})

        if not isinstance(content_type, str):
            return response(400, {'error': 'Invalid contentType'})

        content_type = content_type.strip() or 'application/octet-stream'
        safe_file_name = sanitise_file_name(file_name)
        object_key = expected_prefix + safe_file_name

        if object_exists(object_key):
            return response(409, {'error': ('A file with this name already exists for this Work Order. No file was uploaded.'), 'objectKey': object_key})

        return response(
            200,
            {
                'uploadUrl': create_presigned_upload_url(object_key, content_type),
                'objectKey': object_key,
                'fileName': safe_file_name,
                'contentType': content_type,
                'expiresInSeconds': PRESIGNED_URL_SECONDS,
            },
        )

    if raw_path.endswith('/delete'):
        body = get_json_body(event)
        object_key = body.get('objectKey')

        if not object_key:
            return response(400, {'error': 'Missing objectKey'})

        if not isinstance(object_key, str):
            return response(400, {'error': 'Invalid objectKey'})

        object_key = object_key.strip()

        if not object_key.startswith(expected_prefix):
            return response(403, {'error': ('The selected file does not belong to this Work Order')})

        file_name = object_key.rsplit('/', 1)[-1]

        if not file_name or object_key.endswith('/') or file_name in IGNORED_FILE_NAMES:
            return response(400, {'error': ('The selected S3 object cannot be deleted.')})

        if not object_exists(object_key):
            return response(404, {'error': ('The selected file no longer exists in S3.')})

        s3.delete_object(Bucket=FILE_BUCKET, Key=object_key)

        return response(200, {'deleted': True, 'objectKey': object_key, 'fileName': file_name})

    if raw_path.endswith('/open'):
        key = get_query_parameter(event, 'key')

        if not key:
            return response(400, {'error': 'Missing key'})

        if not key.startswith(expected_prefix):
            return response(403, {'error': ('The requested object does not belong to this Work Order')})

        s3.head_object(Bucket=FILE_BUCKET, Key=key)

        return response(200, {'url': create_presigned_url(key), 'expiresInSeconds': PRESIGNED_URL_SECONDS})

    files = list_files(expected_prefix)

    return response(200, {'workOrderId': work_order_id, 'prefix': expected_prefix, 'recordCount': len(files), 'files': files})


def process_building_request(event: dict, raw_path: str) -> dict:
    request_started_at = time.perf_counter()

    supplied_prefix = get_query_parameter(event, 'buildingPrefix')

    if not supplied_prefix:
        return response(400, {'error': 'Missing buildingPrefix'})

    building_prefix = normalise_building_prefix(supplied_prefix)

    supplied_building_root = get_query_parameter(
        event,
        'buildingRoot',
    )

    create_if_missing = get_boolean_query_parameter(
        event,
        'createIfMissing',
        True,
    )

    root_started_at = time.perf_counter()

    if supplied_building_root:
        building_root = normalise_exact_building_root(supplied_building_root)

        if not s3_prefix_has_contents(building_root):
            return response(
                404,
                {'error': 'The supplied Building root was not found'},
            )

        root_resolution = {
            'buildingRoot': building_root,
            'buildingRootCreated': False,
            'warnings': [],
        }

    else:
        try:
            root_resolution = resolve_building_root(
                building_prefix,
                create_if_missing=create_if_missing,
            )

        except BuildingRootNotFoundError as error:
            return response(
                404,
                {'error': str(error)},
            )

        building_root = root_resolution['buildingRoot']

    log_timing(
        'request root resolution',
        root_started_at,
        exactRoot=bool(supplied_building_root),
        createIfMissing=create_if_missing,
    )

    if raw_path.endswith('/open'):
        key = get_query_parameter(event, 'key')

        if not key:
            return response(400, {'error': 'Missing key'})

        if not (is_key_in_building_documents(key, building_root)):
            return response(403, {'error': ("The requested object does not belong to this Building's Compliance Documents folder")})

        head_started_at = time.perf_counter()
        s3.head_object(Bucket=FILE_BUCKET, Key=key)
        log_timing('open file head_object', head_started_at)
        log_timing('building request total', request_started_at, operation='open')

        return response(200, {'url': create_presigned_url(key), 'expiresInSeconds': PRESIGNED_URL_SECONDS})

    supplied_folder_path = get_query_parameter(event, 'folderPath')

    folder_path = normalise_folder_path(supplied_folder_path)

    existence_started_at = time.perf_counter()
    folder_exists_check_needed = not is_configured_folder(folder_path)

    if folder_exists_check_needed and not building_folder_exists(building_root, folder_path):
        log_timing('selected folder existence check', existence_started_at, required=True)
        return response(404, {'error': ('The selected Building folder was not found')})

    log_timing('selected folder existence check', existence_started_at, required=folder_exists_check_needed)

    listing_started_at = time.perf_counter()
    folders, files = list_building_folder(building_root, folder_path)
    log_timing('request folder listing', listing_started_at, folder=folder_path)

    can_upload_started_at = time.perf_counter()
    can_upload = is_configured_upload_destination(folder_path) or (
        folder_path.lower() != COMPLIANCE_DOCUMENTS_FOLDER.lower() and building_folder_exists(building_root, folder_path) and len(folders) == 0
    )
    log_timing('canUpload calculation', can_upload_started_at, canUpload=can_upload)

    result = response(
        200,
        {
            'buildingPrefix': building_prefix,
            'buildingRoot': building_root,
            'currentPath': folder_path,
            'currentFolderName': current_folder_name(folder_path),
            'recordCount': len(files),
            'folders': folders,
            'breadcrumbs': build_breadcrumbs(folder_path),
            'files': files,
            'canUpload': can_upload,
            'buildingRootCreated': root_resolution['buildingRootCreated'],
            'warnings': root_resolution['warnings'],
        },
    )

    log_timing('building request total', request_started_at, operation='list', folder=folder_path, folders=len(folders), files=len(files))

    return result


def process_building_upload_request(event: dict) -> dict:
    body = get_json_body(event)

    supplied_prefix = body.get('buildingPrefix')
    supplied_folder_path = body.get('folderPath')
    file_name = body.get('fileName')
    content_type = body.get('contentType') or 'application/octet-stream'

    if not supplied_prefix:
        return response(400, {'error': 'Missing buildingPrefix'})

    if not supplied_folder_path:
        return response(
            400,
            {'error': 'Open a final document folder before uploading.'},
        )

    if not file_name:
        return response(400, {'error': 'Missing fileName'})

    if not isinstance(content_type, str):
        return response(400, {'error': 'Invalid contentType'})

    content_type = content_type.strip()

    if not content_type:
        content_type = 'application/octet-stream'

    building_prefix = normalise_building_prefix(supplied_prefix)

    # New uploads always use the canonical Building root supplied
    # by Salesforce. Existing legacy/null/double-slash roots remain
    # readable, but new documents are never written back into them.
    building_root = building_prefix.rstrip('/') + '/'

    folder_path = validate_upload_folder(
        building_root,
        supplied_folder_path,
    )

    safe_file_name = sanitise_file_name(file_name)

    object_key = building_root + folder_path + safe_file_name

    if object_exists(object_key):
        return response(
            409,
            {
                'error': (
                    'A file with this name already exists in the '
                    'selected folder. No file was uploaded.'
                ),
                'objectKey': object_key,
            },
        )

    upload_url = create_presigned_upload_url(
        object_key,
        content_type,
    )

    return response(
        200,
        {
            'uploadUrl': upload_url,
            'objectKey': object_key,
            'buildingRoot': building_root,
            'folderPath': folder_path,
            'fileName': safe_file_name,
            'contentType': content_type,
            'expiresInSeconds': PRESIGNED_URL_SECONDS,
        },
    )


def process_building_delete_request(event: dict) -> dict:
    body = get_json_body(event)

    supplied_prefix = body.get('buildingPrefix')
    supplied_building_root = body.get('buildingRoot')
    object_key = body.get('objectKey')

    if not supplied_prefix:
        return response(400, {'error': 'Missing buildingPrefix'})

    if not object_key:
        return response(400, {'error': 'Missing objectKey'})

    if not isinstance(object_key, str):
        return response(400, {'error': 'Invalid objectKey'})

    object_key = object_key.strip()

    building_prefix = normalise_building_prefix(supplied_prefix)

    if supplied_building_root:
        building_root = normalise_exact_building_root(supplied_building_root)
    else:
        building_root = find_building_root(building_prefix)

    if not is_key_in_building_documents(object_key, building_root):
        return response(
            403,
            {'error': "The selected file does not belong to this Building's Compliance Documents folder."},
        )

    file_name = object_key.rsplit('/', 1)[-1]

    if not file_name or file_name in IGNORED_FILE_NAMES:
        return response(
            400,
            {'error': 'The selected S3 object cannot be deleted.'},
        )

    if object_key.endswith('/'):
        return response(
            400,
            {'error': 'Folders cannot be deleted from this viewer.'},
        )

    if not object_exists(object_key):
        return response(
            404,
            {'error': 'The selected file no longer exists in S3.'},
        )

    s3.delete_object(
        Bucket=FILE_BUCKET,
        Key=object_key,
    )

    return response(
        200,
        {
            'deleted': True,
            'objectKey': object_key,
            'fileName': file_name,
        },
    )


def process_building_move_request(event: dict) -> dict:
    body = get_json_body(event)

    supplied_prefix = body.get('buildingPrefix')
    supplied_building_root = body.get('buildingRoot')
    object_key = body.get('objectKey')
    supplied_destination = body.get('destinationFolderPath')

    if not supplied_prefix:
        return response(
            400,
            {'error': 'Missing buildingPrefix'},
        )

    if not object_key:
        return response(
            400,
            {'error': 'Missing objectKey'},
        )

    if not supplied_destination:
        return response(
            400,
            {'error': 'Missing destinationFolderPath'},
        )

    if not isinstance(object_key, str):
        return response(
            400,
            {'error': 'Invalid objectKey'},
        )

    object_key = object_key.strip()

    building_prefix = normalise_building_prefix(supplied_prefix)

    if supplied_building_root:
        building_root = normalise_exact_building_root(supplied_building_root)
    else:
        building_root = find_building_root(building_prefix)

    if not is_key_in_building_documents(object_key, building_root):
        return response(
            403,
            {'error': "The selected file does not belong to this Building's Compliance Documents folder."},
        )

    file_name = object_key.rsplit('/', 1)[-1]

    if not file_name or object_key.endswith('/') or file_name in IGNORED_FILE_NAMES:
        return response(
            400,
            {'error': 'The selected S3 object cannot be moved.'},
        )

    if not object_exists(object_key):
        return response(
            404,
            {'error': 'The selected file no longer exists in S3.'},
        )

    destination_folder_path = validate_upload_folder(
        building_root,
        supplied_destination,
    )

    destination_key = building_root + destination_folder_path + file_name

    if destination_key == object_key:
        return response(
            400,
            {'error': 'The file is already in the selected folder.'},
        )

    if object_exists(destination_key):
        return response(
            409,
            {
                'error': 'A file with this name already exists in the selected folder. No file was moved.',
                'objectKey': destination_key,
            },
        )

    s3.copy_object(
        Bucket=FILE_BUCKET,
        CopySource={
            'Bucket': FILE_BUCKET,
            'Key': object_key,
        },
        Key=destination_key,
    )

    if not object_exists(destination_key):
        return response(
            500,
            {'error': 'AWS did not confirm the copied file. The original file was not removed.'},
        )

    s3.delete_object(
        Bucket=FILE_BUCKET,
        Key=object_key,
    )

    return response(
        200,
        {
            'moved': True,
            'sourceObjectKey': object_key,
            'destinationObjectKey': destination_key,
            'destinationFolderPath': destination_folder_path,
            'fileName': file_name,
        },
    )


def process(event, context):
    try:
        raw_path = event.get('rawPath') or event.get('path') or ''

        print('ORIGINAL RAW PATH:', repr(raw_path))

        if raw_path.startswith('/prod/'):
            raw_path = raw_path[len('/prod') :]

        print('ROUTING PATH:', repr(raw_path))

        if raw_path == '/files/buildings/upload-url':
            return process_building_upload_request(event)

        if raw_path == '/files/buildings/move':
            return process_building_move_request(event)

        if raw_path == '/files/buildings/delete':
            return process_building_delete_request(event)

        if raw_path.startswith('/files/buildings'):
            return process_building_request(event, raw_path)

        if raw_path.startswith('/files/workorders/'):
            return process_work_order_request(event, raw_path)

        return response(
            404,
            {
                'error': 'Unsupported file viewer route',
                'rawPath': raw_path,
                'path': event.get('path'),
                'requestContext': (event.get('requestContext', {}).get('http', {})),
            },
        )

    except ValueError as error:
        return response(400, {'error': str(error)})

    except ClientError as error:
        error_code = error.response.get('Error', {}).get('Code')

        if error_code in {'NoSuchKey', '404', 'NotFound'}:
            return response(404, {'error': ('The requested S3 object was not found')})

        if error_code in {'AccessDenied', '403'}:
            return response(403, {'error': ('The Lambda does not have permission to access this S3 object')})

        return response(500, {'error': ('AWS failed to process the file request'), 'details': str(error)})

    except Exception as error:
        return response(500, {'error': ('Failed to process the S3 file request'), 'details': str(error)})
