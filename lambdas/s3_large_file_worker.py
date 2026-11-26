import json
import mimetypes
import os
import re
import tempfile

import boto3
import requests
from botocore.exceptions import ClientError

REGION = os.getenv('AWS_REGION', 'eu-west-2')
SF_OAUTH_SECRET_ARN = os.getenv('SF_OAUTH_SECRET_ARN')
FILE_BUCKET = os.getenv('FILE_BUCKET')
MAX_FILE_SIZE_BYTES = 1024 * 1024 * 1024  # 1 GiB; ensure Lambda /tmp is sized for this.
PROCESSING_ENABLED = os.getenv('PROCESSING_ENABLED', 'false').lower() == 'true'
REGISTRATION_MARKER = 'NEILON registration attempted; verify S3-Link before retry'
ENTITY_LOOKUPS = {'WorkOrder': 'WorkOrder__c', 'Opportunity': 'NEILON__Opportunity__c'}

secretsmanager = boto3.client('secretsmanager', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


def _get_json_secret(secret_arn):
    if not secret_arn:
        raise ValueError('SF_OAUTH_SECRET_ARN must be configured')

    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    return json.loads(response['SecretString'])


def _authenticate_salesforce():
    oauth = _get_json_secret(SF_OAUTH_SECRET_ARN)
    response = requests.post(
        oauth['token_url'],
        data={
            'grant_type': 'client_credentials',
            'client_id': oauth['client_id'],
            'client_secret': oauth['client_secret'],
        },
        timeout=10,
        allow_redirects=False,
    )
    response.raise_for_status()
    authentication = response.json()
    return authentication['access_token'], authentication['instance_url'].rstrip('/')


def _get_pending_requests(access_token, instance_url):
    query = (
        'SELECT Id, Content_Version_Id__c, Content_Document_Id__c, '
        'Entity_Name__c, Record_Id__c, S3_Key__c, Size_Bytes__c, Error__c '
        "FROM S3_Large_File_Request__c WHERE Status__c = 'Pending' "
        'ORDER BY CreatedDate LIMIT 10'
    )
    response = requests.get(
        f'{instance_url}/services/data/v68.0/query',
        headers={'Authorization': f'Bearer {access_token}'},
        params={'q': query},
        timeout=10,
        allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 200:
        raise RuntimeError('Salesforce query did not return HTTP 200')
    return response.json()['records']


def _parse_destination(destination):
    if not FILE_BUCKET:
        raise ValueError('FILE_BUCKET must explicitly configure the allowed bucket')
    if not isinstance(destination, str) or '/' not in destination:
        raise ValueError('S3_Key__c must include a bucket and object key')

    bucket, key = destination.split('/', 1)
    if bucket != FILE_BUCKET:
        raise ValueError('Destination bucket is not allowed')
    if (
        not key
        or key != key.strip()
        or any(segment in ('', '.', '..') for segment in key.split('/'))
        or '\\' in key
        or any(ord(character) < 32 or ord(character) == 127 for character in key)
        or len(key.encode('utf-8')) > 1024
    ):
        raise ValueError('Invalid S3 object key')
    return bucket, key


def _download_content_version(access_token, instance_url, version_id, expected_size, output):
    url = f'{instance_url}/services/data/v68.0/sobjects/ContentVersion/{version_id}/VersionData'
    with requests.get(
        url,
        headers={'Authorization': f'Bearer {access_token}'},
        stream=True,
        timeout=(10, 60),
        allow_redirects=False,
    ) as response:
        response.raise_for_status()
        if response.status_code != 200:
            raise RuntimeError('Expected a complete HTTP 200 ContentVersion download')

        downloaded_size = 0
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            downloaded_size += len(chunk)
            if downloaded_size > expected_size:
                raise ValueError('Downloaded file size does not match Size_Bytes__c')
            output.write(chunk)

        if downloaded_size != expected_size:
            raise ValueError('Downloaded file size does not match Size_Bytes__c')
        return response.headers.get('Content-Type', 'application/octet-stream')


def _upload_request_file(request, access_token, instance_url):
    bucket, key = _parse_destination(request['S3_Key__c'])
    request_id = request['Id']
    version_id = request['Content_Version_Id__c']
    expected_size = request['Size_Bytes__c']
    if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?', request_id):
        raise ValueError('Invalid request ID')
    if not isinstance(version_id, str) or not re.fullmatch(r'068[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?', version_id):
        raise ValueError('Invalid ContentVersion ID')
    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, (int, float))
        or expected_size <= 0
        or expected_size != int(expected_size)
    ):
        raise ValueError('Size_Bytes__c must be a positive whole number')
    expected_size = int(expected_size)
    if expected_size > MAX_FILE_SIZE_BYTES:
        raise ValueError('File exceeds the 1 GiB maximum transfer size')
    metadata = {'salesforce-request-id': request_id, 'salesforce-content-version-id': version_id}

    try:
        existing = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response['Error']['Code'] not in ('404', 'NoSuchKey', 'NotFound'):
            raise
        existing = None

    if existing is not None:
        if existing.get('ContentLength') != expected_size or any(existing.get('Metadata', {}).get(name) != value for name, value in metadata.items()):
            raise ValueError('Existing S3 object conflicts with this request or ContentVersion')
        return {'bucket': bucket, 'key': key, 'already_uploaded': True}

    with tempfile.TemporaryFile() as output:
        content_type = _download_content_version(access_token, instance_url, version_id, expected_size, output)
        if content_type == 'application/octet-stream':
            content_type = mimetypes.guess_type(key)[0] or content_type
        output.seek(0)
        # Conditional PUT prevents overwriting even if an object appears after the HEAD check.
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=output,
            ContentLength=expected_size,
            ContentType=content_type,
            Metadata=metadata,
            IfNoneMatch='*',
        )
    return {'bucket': bucket, 'key': key, 'already_uploaded': False}



def _update_request(access_token, instance_url, request_id, status, error=None):
    response = requests.patch(
        f'{instance_url}/services/data/v68.0/sobjects/S3_Large_File_Request__c/{request_id}',
        headers={'Authorization': f'Bearer {access_token}'},
        json={'Status__c': status, 'Error__c': error},
        timeout=10,
        allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 204:
        raise RuntimeError('Salesforce did not confirm the request update')



def _verify_content_version(request, access_token, instance_url):
    version_id = request['Content_Version_Id__c']
    if not isinstance(version_id, str) or not re.fullmatch(r'068[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?', version_id):
        raise ValueError('Invalid ContentVersion ID')
    response = requests.get(
        f'{instance_url}/services/data/v68.0/sobjects/ContentVersion/{version_id}',
        headers={'Authorization': f'Bearer {access_token}'},
        params={'fields': 'ContentDocumentId,ContentSize'},
        timeout=(10, 60),
        allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 200:
        raise RuntimeError('ContentVersion metadata was not returned')
    metadata = response.json()
    if (
        metadata.get('ContentDocumentId') != request['Content_Document_Id__c']
        or metadata.get('ContentSize') != request['Size_Bytes__c']
    ):
        raise ValueError('ContentVersion metadata does not match the request')

def _get_work_order_work_type(request, access_token, instance_url):
    # Match NeilonFileBuilder: Work_Type__c is set only for non-Maintenance WorkOrders.
    record_id = request['Record_Id__c']
    query = f"SELECT WorkTypeId, RecordType.Name FROM WorkOrder WHERE Id = '{record_id}' LIMIT 1"
    response = requests.get(
        f'{instance_url}/services/data/v68.0/query',
        headers={'Authorization': f'Bearer {access_token}'},
        params={'q': query},
        timeout=10,
        allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 200:
        raise RuntimeError('WorkOrder metadata query was not successful')
    records = response.json()['records']
    if len(records) != 1:
        raise RuntimeError('Related WorkOrder could not be verified')

    work_order = records[0]
    record_type = work_order.get('RecordType') or {}
    return None if record_type.get('Name') == 'Maintenance' else work_order.get('WorkTypeId')


def _s3_link_fields(request, bucket, key, work_type_id=None):
    # Use the destination/name already built by Salesforce; don't rebuild its paths.
    lookup = ENTITY_LOOKUPS.get(request.get('Entity_Name__c'))
    if not lookup:
        raise RuntimeError('S3-Link registration mapping is not verified for this entity')

    name = key.rsplit('/', 1)[-1]
    extension = '.' + name.rsplit('.', 1)[-1].lower() if '.' in name else ''
    fields = {
        'Name': name,
        'NEILON__Bucket_Name__c': bucket,
        'NEILON__Amazon_File_Key__c': key,
        'NEILON__Size__c': request['Size_Bytes__c'],
        'NEILON__Bucket_Region__c': REGION,
        'NEILON__Content_Document_Id__c': request['Content_Document_Id__c'],
        'NEILON__Extension__c': extension,
        'Report_Source_Id__c': request['Record_Id__c'],
        'Public__c': False,
        lookup: request['Record_Id__c'],
    }
    if request['Entity_Name__c'] == 'WorkOrder' and work_type_id:
        fields['Work_Type__c'] = work_type_id
    # Match NeilonFileBuilder's existing MIME mapping, including its DOCX value.
    if extension == '.pdf':
        fields['NEILON__Content_Type__c'] = 'application/pdf'
    elif extension == '.docx':
        fields['NEILON__Content_Type__c'] = 'application/msword'
    return fields


def _find_s3_link(request, access_token, instance_url, work_type_id=None):
    document_id = request['Content_Document_Id__c']
    if not isinstance(document_id, str) or not re.fullmatch(r'069[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?', document_id):
        raise ValueError('Invalid ContentDocument ID')
    bucket, key = _parse_destination(request['S3_Key__c'])
    expected = _s3_link_fields(request, bucket, key, work_type_id)
    query = (
        'SELECT Id, Name, NEILON__Bucket_Name__c, NEILON__Amazon_File_Key__c, '
        'NEILON__Content_Document_Id__c, NEILON__Size__c, NEILON__Folder__c, '
        'NEILON__Bucket_Region__c, NEILON__Extension__c, NEILON__Content_Type__c, '
        'WorkOrder__c, NEILON__Opportunity__c, Report_Source_Id__c, Public__c, Work_Type__c '
        "FROM NEILON__File__c WHERE NEILON__Content_Document_Id__c = '"
        + document_id + "' LIMIT 101"
    )
    response = requests.get(
        f'{instance_url}/services/data/v68.0/query',
        headers={'Authorization': f'Bearer {access_token}'},
        params={'q': query},
        timeout=(10, 60),
        allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 200:
        raise RuntimeError('S3-Link query did not return HTTP 200')
    result = response.json()
    if not result.get('done', True) or len(result['records']) > 100:
        raise RuntimeError('Too many S3-Link records to reconcile safely')

    matches = [file for file in result['records']
               if file.get('NEILON__Bucket_Name__c') == bucket
               and file.get('NEILON__Amazon_File_Key__c') == key]
    if len(matches) > 1:
        raise RuntimeError('Duplicate S3-Link records require review')
    if not matches:
        return None

    file = matches[0]
    if not file.get('NEILON__Folder__c') or any(file.get(field) != value for field, value in expected.items()):
        raise RuntimeError('S3-Link metadata does not match the request')
    return file['Id']


def _register_s3_link(request, bucket, key, access_token, instance_url, work_type_id=None):
    # The documented NEILON endpoint registers existing S3 objects from metadata.
    if '?' in key or any(len(part) > 80 for part in key.split('/')):
        raise ValueError('S3 destination exceeds documented NEILON REST path limitations')

    payload = [_s3_link_fields(request, bucket, key, work_type_id)]
    response = requests.post(
        f'{instance_url}/services/apexrest/NEILON/S3Link/v1/creates3files/',
        headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'},
        json=payload,
        timeout=(10, 30),
        allow_redirects=False,
    )
    response.raise_for_status()
    result = response.json()
    if response.status_code != 200 or result.get('status') != 'OK' or str(result.get('code')) != '200':
        raise RuntimeError('NEILON did not confirm successful registration')
    files = result.get('data')
    if isinstance(files, str):
        files = json.loads(files)
    if not isinstance(files, list) or len(files) != 1 or not files[0].get('Id'):
        raise RuntimeError('NEILON returned an unexpected registration result')
    return files[0]['Id']


def _verify_s3_upload(request):
    bucket, key = _parse_destination(request['S3_Key__c'])
    result = s3.head_object(Bucket=bucket, Key=key)
    metadata = result.get('Metadata', {})
    if (
        result.get('ContentLength') != request['Size_Bytes__c']
        or metadata.get('salesforce-request-id') != request['Id']
        or metadata.get('salesforce-content-version-id') != request['Content_Version_Id__c']
    ):
        raise RuntimeError('Uploaded S3 object verification failed')


def _process_request(request, access_token, instance_url):
    # Validate supported entity/record before transferring any bytes.
    if request.get('Entity_Name__c') not in ENTITY_LOOKUPS:
        raise RuntimeError('S3-Link registration mapping is not verified for this entity')
    record_id = request.get('Record_Id__c')
    if not isinstance(record_id, str) or not re.fullmatch(r'[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?', record_id):
        raise ValueError('Invalid related record ID')

    _, key = _parse_destination(request['S3_Key__c'])
    if '?' in key or any(len(part) > 80 for part in key.split('/')):
        raise ValueError('S3 destination exceeds documented NEILON REST path limitations')

    work_type_id = None
    if request['Entity_Name__c'] == 'WorkOrder':
        work_type_id = _get_work_order_work_type(request, access_token, instance_url)

    # Stage names provide safe Production diagnostics without exposing file names,
    # record IDs, Salesforce responses or authentication details.
    print('Large-file stage: verify_content_version')
    _verify_content_version(request, access_token, instance_url)
    print('Large-file stage: find_s3_link')
    link_id = _find_s3_link(request, access_token, instance_url, work_type_id)
    previous_attempt = request.get('Error__c') == REGISTRATION_MARKER
    if previous_attempt and not link_id:
        # The previous POST may have succeeded without returning a response. Never repeat it blindly.
        raise RuntimeError('Uncertain NEILON registration requires manual reconciliation')

    print('Large-file stage: upload_content_version')
    _upload_request_file(request, access_token, instance_url)
    print('Large-file stage: verify_s3_upload')
    _verify_s3_upload(request)

    if not link_id:
        # Persist the uncertainty marker BEFORE the non-idempotent NEILON POST.
        print('Large-file stage: mark_registration_attempt')
        _update_request(access_token, instance_url, request['Id'], 'Pending', REGISTRATION_MARKER)
        request['Error__c'] = REGISTRATION_MARKER
        print('Large-file stage: register_s3_link')
        created_id = _register_s3_link(
            request, *_parse_destination(request['S3_Key__c']), access_token, instance_url, work_type_id,
        )
        print('Large-file stage: verify_registered_s3_link')
        link_id = _find_s3_link(request, access_token, instance_url, work_type_id)
        if link_id != created_id:
            raise RuntimeError('Registered S3-Link could not be verified')

    print('Large-file stage: mark_done')
    _update_request(access_token, instance_url, request['Id'], 'Done')
    return link_id


def process(event, context):
    try:
        access_token, instance_url = _authenticate_salesforce()
        pending_requests = _get_pending_requests(access_token, instance_url)
    except Exception as error:
        # Do not log response bodies, credentials, request keys or document names.
        print(f'Failed to retrieve Pending large-file requests ({type(error).__name__})')
        raise RuntimeError('Failed to retrieve Pending large-file requests') from None

    print(f'Retrieved {len(pending_requests)} Pending large-file requests')
    if not PROCESSING_ENABLED:
        return {'pending_count': len(pending_requests)}

    # Initial Production pilot: fail closed unless a manual invocation identifies
    # exactly one Pending request. A scheduled/empty event can never process files.
    request_id = event.get('request_id') if isinstance(event, dict) else None
    if (
        not isinstance(event, dict)
        or event.get('action') != 'process_one'
        or not isinstance(request_id, str)
        or not re.fullmatch(r'[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?', request_id)
    ):
        return {'pending_count': len(pending_requests), 'processing_skipped': 'specific_manual_request_required'}

    # The read-only query is intentionally bounded at ten requests. If the
    # selected ID isn't in that batch, don't touch any other request.
    selected = [request for request in pending_requests if request['Id'] == request_id]
    if len(selected) != 1:
        return {'pending_count': len(pending_requests), 'processing_skipped': 'request_not_in_pending_batch'}
    if not FILE_BUCKET:
        raise RuntimeError('FILE_BUCKET must be configured before processing requests')

    try:
        _process_request(selected[0], access_token, instance_url)
    except Exception as error:
        # Pilot mode: never mark a request Failed automatically. A registration
        # uncertainty marker written by _process_request still protects retries.
        print(f'Selected large-file request could not be completed ({type(error).__name__})')
        return {'pending_count': len(pending_requests), 'completed_count': 0, 'failed_count': 1}
    return {'pending_count': len(pending_requests), 'completed_count': 1, 'failed_count': 0}
