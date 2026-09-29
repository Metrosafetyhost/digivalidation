from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote_plus

import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())

S3 = boto3.client('s3')

CUSTOMER_VISIBLE_TAG = 'customer-visible'
PUBLIC_VISIBLE_TAG = 'public-visible'
ALLOWED_PREFIXES = ('Buildings/', 'WorkOrders/')
MAX_S3_TAGS = 10


def required_file_bucket() -> str:
    bucket = os.environ.get('FILE_BUCKET')
    if not bucket:
        raise RuntimeError('FILE_BUCKET must be configured before processing S3 events')
    return bucket


def visibility_defaults(file_name: str) -> dict[str, str]:
    """Return the agreed default visibility tags for a filename."""
    normalised_name = file_name.casefold()
    is_preview = 'preview' in normalised_name
    is_final = '_final' in normalised_name
    is_temporary_vr_final = '_vr' in normalised_name

    customer_visible = not is_preview and (is_final or is_temporary_vr_final)

    return {
        CUSTOMER_VISIBLE_TAG: 'true' if customer_visible else 'false',
        PUBLIC_VISIBLE_TAG: 'false',
    }


def should_process_key(key: str) -> bool:
    """Return True only for real objects under an approved prefix."""
    return bool(key) and not key.endswith('/') and any(key.startswith(prefix) for prefix in ALLOWED_PREFIXES)


def merge_missing_visibility_tags(
    existing_tags: dict[str, str],
    file_name: str,
) -> tuple[dict[str, str], bool]:
    """Add missing defaults without overwriting explicit uploader choices."""
    merged_tags = dict(existing_tags)
    changed = False

    for key, value in visibility_defaults(file_name).items():
        if key not in merged_tags:
            merged_tags[key] = value
            changed = True

    if len(merged_tags) > MAX_S3_TAGS:
        raise ValueError(f"Adding visibility tags would exceed S3's {MAX_S3_TAGS}-tag limit")

    return merged_tags, changed


def _tagging_parameters(
    bucket: str,
    key: str,
    version_id: str | None,
) -> dict[str, str]:
    parameters = {'Bucket': bucket, 'Key': key}
    if version_id:
        parameters['VersionId'] = version_id
    return parameters


def tag_object_if_required(
    bucket: str,
    key: str,
    version_id: str | None = None,
) -> str:
    """Apply missing visibility tags and return tagged or skipped."""
    expected_bucket = required_file_bucket()

    if bucket != expected_bucket:
        LOGGER.warning(
            'Ignoring object from unexpected bucket bucket=%s key=%s',
            bucket,
            key,
        )
        return 'skipped'

    if not should_process_key(key):
        return 'skipped'

    parameters = _tagging_parameters(bucket, key, version_id)
    current = S3.get_object_tagging(**parameters)
    existing_tags = {item['Key']: item['Value'] for item in current.get('TagSet', [])}

    file_name = PurePosixPath(key).name
    merged_tags, changed = merge_missing_visibility_tags(
        existing_tags,
        file_name,
    )

    if not changed:
        LOGGER.info('Visibility tags already set bucket=%s key=%s', bucket, key)
        return 'skipped'

    S3.put_object_tagging(
        **parameters,
        Tagging={'TagSet': [{'Key': tag_key, 'Value': tag_value} for tag_key, tag_value in sorted(merged_tags.items())]},
    )

    LOGGER.info(
        'Applied visibility tags bucket=%s key=%s customer_visible=%s',
        bucket,
        key,
        merged_tags[CUSTOMER_VISIBLE_TAG],
    )
    return 'tagged'


def process_s3_record(record: dict[str, Any]) -> str:
    """Process one native S3 event record."""
    if record.get('eventSource') != 'aws:s3':
        return 'skipped'

    if not str(record.get('eventName', '')).startswith('ObjectCreated:'):
        return 'skipped'

    s3_details = record['s3']
    bucket = s3_details['bucket']['name']
    object_details = s3_details['object']
    key = unquote_plus(object_details['key'])
    version_id = object_details.get('versionId')

    return tag_object_if_required(bucket, key, version_id)


def api_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body),
    }


def recommendation_request(event: dict[str, Any]) -> dict[str, Any]:
    http = event['requestContext']['http']
    method = http.get('method', '')
    path = event.get('rawPath') or http.get('path') or ''
    if path.startswith('/prod/'):
        path = path[len('/prod') :]

    if path != '/files/visibility-recommendation':
        return api_response(404, {'error': 'Unsupported visibility recommendation route'})
    if method != 'POST':
        return api_response(405, {'error': 'Method not allowed'})

    body = event.get('body')
    if event.get('isBase64Encoded'):
        try:
            body = base64.b64decode(body, validate=True).decode('utf-8')
        except (TypeError, ValueError, UnicodeError):
            return api_response(400, {'error': 'The encoded request body could not be read'})

    try:
        parsed_body = json.loads(body)
    except (TypeError, ValueError):
        return api_response(400, {'error': 'The request body is not valid JSON'})

    if not isinstance(parsed_body, dict):
        return api_response(400, {'error': 'The request body must be a JSON object'})

    file_name = parsed_body.get('fileName')
    if not isinstance(file_name, str) or not file_name.strip():
        return api_response(400, {'error': 'fileName must be a nonblank string'})

    defaults = visibility_defaults(file_name)
    return api_response(
        200,
        {
            'customerVisible': defaults[CUSTOMER_VISIBLE_TAG] == 'true',
            'publicVisible': defaults[PUBLIC_VISIBLE_TAG] == 'true',
        },
    )


def process(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Handle HTTP recommendations or native S3 ObjectCreated records."""
    if isinstance(event.get('requestContext'), dict) and isinstance(event['requestContext'].get('http'), dict):
        return recommendation_request(event)

    required_file_bucket()
    counts = {'received': 0, 'tagged': 0, 'skipped': 0}

    for record in event.get('Records', []):
        counts['received'] += 1
        outcome = process_s3_record(record)
        counts[outcome] += 1

    LOGGER.info('Visibility tagging complete counts=%s', counts)
    return counts
