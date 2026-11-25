import importlib
import json
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
import requests
from botocore.exceptions import ClientError

SECRET_ARN = 'arn:aws:secretsmanager:eu-west-2:123456789012:secret:test'
TOKEN = 'test-access-token'
INSTANCE_URL = 'https://example.my.salesforce.com'
OAUTH = {
    'token_url': 'https://login.salesforce.com/services/oauth2/token',
    'client_id': 'test-client',
    'client_secret': 'test-secret',
}


@pytest.fixture
def setup_lambda(monkeypatch):
    monkeypatch.setenv('SF_OAUTH_SECRET_ARN', SECRET_ARN)
    monkeypatch.setenv('AWS_REGION', 'eu-west-2')
    secretsmanager = Mock()
    secretsmanager.get_secret_value.return_value = {'SecretString': json.dumps(OAUTH)}
    clients = {'secretsmanager': secretsmanager, 's3': Mock()}

    with (
        patch('boto3.client', side_effect=lambda service, **kwargs: clients[service]),
        patch('requests.post') as post,
        patch('requests.get') as get,
    ):
        from lambdas import s3_large_file_worker

        module = importlib.reload(s3_large_file_worker)
        post.return_value.json.return_value = {'access_token': TOKEN, 'instance_url': INSTANCE_URL}
        get.return_value.json.return_value = {'records': []}
        get.return_value.status_code = 200
        yield module, secretsmanager, post, get


def test_authentication_uses_existing_oauth_secret_pattern(setup_lambda):
    module, secretsmanager, post, get = setup_lambda
    assert module._authenticate_salesforce() == (TOKEN, INSTANCE_URL)
    secretsmanager.get_secret_value.assert_called_once_with(SecretId=SECRET_ARN)
    post.assert_called_once_with(
        OAUTH['token_url'],
        data={'grant_type': 'client_credentials', 'client_id': OAUTH['client_id'], 'client_secret': OAUTH['client_secret']},
        timeout=10,
        allow_redirects=False,
    )
    post.return_value.raise_for_status.assert_called_once()
    get.assert_not_called()


def test_missing_secret_configuration_stops_before_authentication(setup_lambda, monkeypatch):
    module, secretsmanager, post, _ = setup_lambda
    monkeypatch.setattr(module, 'SF_OAUTH_SECRET_ARN', None)
    with pytest.raises(ValueError, match='SF_OAUTH_SECRET_ARN'):
        module._authenticate_salesforce()
    secretsmanager.get_secret_value.assert_not_called()
    post.assert_not_called()


def test_query_selects_requested_fields_and_only_ten_pending_rows(setup_lambda):
    module, _, _, get = setup_lambda
    records = [{'Id': 'test-request', 'Content_Version_Id__c': 'test-version'}]
    get.return_value.json.return_value = {'records': records}
    assert module._get_pending_requests(TOKEN, INSTANCE_URL) == records
    get.assert_called_once_with(
        INSTANCE_URL + '/services/data/v68.0/query',
        headers={'Authorization': f'Bearer {TOKEN}'},
        params={
            'q': (
                'SELECT Id, Content_Version_Id__c, Content_Document_Id__c, '
                'Entity_Name__c, Record_Id__c, S3_Key__c, Size_Bytes__c, Error__c '
                "FROM S3_Large_File_Request__c WHERE Status__c = 'Pending' "
                'ORDER BY CreatedDate LIMIT 10'
            )
        },
        timeout=10,
        allow_redirects=False,
    )
    get.return_value.raise_for_status.assert_called_once()


@pytest.mark.parametrize('records', [[], [{'Id': 'private-id', 'S3_Key__c': 'private-document.pdf'}]])
def test_process_logs_only_a_safe_count(setup_lambda, capsys, records):
    module, _, _, get = setup_lambda
    get.return_value.json.return_value = {'records': records}
    assert module.process({}, None) == {'pending_count': len(records)}
    output = capsys.readouterr().out
    assert output == f'Retrieved {len(records)} Pending large-file requests\n'
    assert TOKEN not in output and OAUTH['client_secret'] not in output
    assert 'private-id' not in output and 'private-document.pdf' not in output
    module.s3.head_object.assert_not_called()
    module.s3.put_object.assert_not_called()


def test_authentication_failure_does_not_query_or_log_sensitive_details(setup_lambda, capsys):
    module, _, post, get = setup_lambda
    post.return_value.raise_for_status.side_effect = requests.HTTPError('private authentication details')
    with pytest.raises(RuntimeError, match='Failed to retrieve Pending'):
        module.process({}, None)
    get.assert_not_called()
    assert capsys.readouterr().out == 'Failed to retrieve Pending large-file requests (HTTPError)\n'


def test_query_failure_is_not_reported_as_an_empty_batch(setup_lambda, capsys):
    module, _, _, get = setup_lambda
    get.return_value.raise_for_status.side_effect = requests.HTTPError('private response details')
    with pytest.raises(RuntimeError, match='Failed to retrieve Pending'):
        module.process({}, None)
    assert capsys.readouterr().out == 'Failed to retrieve Pending large-file requests (HTTPError)\n'


BUCKET = 'metrosafetysandboxfiles'
KEY = 'WorkOrders/synthetic/test file.pdf'
REQUEST_ID = 'a9dR500000000fxIAA'
VERSION_ID = '068R500000f7cddIAA'
PDF = b'%PDF-test'


@pytest.fixture
def transfer_setup(setup_lambda, monkeypatch, tmp_path):
    module, _, _, get = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    response = MagicMock(status_code=200)
    response.__enter__.return_value = response
    response.headers = {'Content-Type': 'application/pdf'}
    response.iter_content.return_value = iter([b'%PDF-', b'', b'test'])
    get.return_value = response
    module.s3.head_object.side_effect = ClientError({'Error': {'Code': '404'}}, 'HeadObject')

    temporary_files = []
    create_temporary_file = module.tempfile.TemporaryFile

    def tracked_temporary_file():
        output = create_temporary_file(dir=tmp_path)
        temporary_files.append(output)
        return output

    monkeypatch.setattr(module.tempfile, 'TemporaryFile', tracked_temporary_file)
    request = {'Id': REQUEST_ID, 'Content_Version_Id__c': VERSION_ID, 'S3_Key__c': BUCKET + '/' + KEY, 'Size_Bytes__c': len(PDF)}
    yield module, request, get, response, temporary_files
    assert all(output.closed for output in temporary_files)
    assert list(tmp_path.iterdir()) == []


def test_stream_download_and_conditional_file_backed_upload(transfer_setup):
    module, request, get, response, temporary_files = transfer_setup
    uploaded = []

    def put_object(**kwargs):
        assert not isinstance(kwargs['Body'], (bytes, str))
        assert not kwargs['Body'].closed
        assert kwargs['Body'].tell() == 0
        uploaded.append(kwargs['Body'].read())

    module.s3.put_object.side_effect = put_object
    # Reading Response.content would buffer the entire file and must never happen.
    with patch.object(type(response), 'content', new_callable=PropertyMock, create=True, side_effect=AssertionError('Do not buffer the response')):
        result = module._upload_request_file(request, TOKEN, INSTANCE_URL)

    assert result == {'bucket': BUCKET, 'key': KEY, 'already_uploaded': False}
    assert uploaded == [PDF]
    get.assert_called_once_with(
        f'{INSTANCE_URL}/services/data/v68.0/sobjects/ContentVersion/{VERSION_ID}/VersionData',
        headers={'Authorization': f'Bearer {TOKEN}'},
        stream=True,
        timeout=(10, 60),
        allow_redirects=False,
    )
    response.iter_content.assert_called_once_with(chunk_size=1024 * 1024)
    response.__exit__.assert_called_once()
    module.s3.put_object.assert_called_once_with(
        Bucket=BUCKET,
        Key=KEY,
        Body=temporary_files[0],
        ContentLength=len(PDF),
        ContentType='application/pdf',
        Metadata={'salesforce-request-id': REQUEST_ID, 'salesforce-content-version-id': VERSION_ID},
        IfNoneMatch='*',
    )


@pytest.mark.parametrize('size', [len(PDF) - 1, len(PDF) + 1])
def test_size_mismatch_prevents_upload_and_cleans_up(transfer_setup, size):
    module, request, _, response, _ = transfer_setup
    request['Size_Bytes__c'] = size
    with pytest.raises(ValueError, match='size does not match'):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    module.s3.put_object.assert_not_called()
    response.__exit__.assert_called_once()


@pytest.mark.parametrize(
    'destination',
    [
        'metrosafetyprodfiles/' + KEY,
        'missing-bucket-prefix',
        BUCKET + '/',
        BUCKET + '//file.pdf',
        BUCKET + '/WorkOrders/../file.pdf',
        BUCKET + '/WorkOrders/./file.pdf',
        BUCKET + '/WorkOrders//file.pdf',
        BUCKET + '/WorkOrders/file.pdf/',
        BUCKET + '/WorkOrders/file.pdf\n',
        BUCKET + '/WorkOrders\\file.pdf',
    ],
)
def test_rejected_destination_does_not_download_or_use_s3(transfer_setup, destination):
    module, request, get, _, temporary_files = transfer_setup
    request['S3_Key__c'] = destination
    with pytest.raises(ValueError):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    get.assert_not_called()
    module.s3.head_object.assert_not_called()
    module.s3.put_object.assert_not_called()
    assert temporary_files == []


def test_allowed_bucket_must_be_explicitly_configured(transfer_setup, monkeypatch):
    module, request, get, _, _ = transfer_setup
    monkeypatch.setattr(module, 'FILE_BUCKET', None)
    with pytest.raises(ValueError, match='FILE_BUCKET'):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    get.assert_not_called()
    module.s3.head_object.assert_not_called()


def test_existing_matching_object_skips_download_and_upload(transfer_setup):
    module, request, get, _, temporary_files = transfer_setup
    module.s3.head_object.side_effect = None
    module.s3.head_object.return_value = {
        'ContentLength': len(PDF),
        'Metadata': {'salesforce-request-id': REQUEST_ID, 'salesforce-content-version-id': VERSION_ID},
    }
    assert module._upload_request_file(request, TOKEN, INSTANCE_URL) == {'bucket': BUCKET, 'key': KEY, 'already_uploaded': True}
    module.s3.head_object.assert_called_once_with(Bucket=BUCKET, Key=KEY)
    get.assert_not_called()
    module.s3.put_object.assert_not_called()
    assert temporary_files == []


@pytest.mark.parametrize(
    'metadata,size',
    [
        ({}, len(PDF)),
        ({'salesforce-request-id': 'different', 'salesforce-content-version-id': VERSION_ID}, len(PDF)),
        ({'salesforce-request-id': REQUEST_ID, 'salesforce-content-version-id': 'different'}, len(PDF)),
        ({'salesforce-request-id': REQUEST_ID, 'salesforce-content-version-id': VERSION_ID}, len(PDF) + 1),
    ],
)
def test_existing_unverified_object_is_a_conflict(transfer_setup, metadata, size):
    module, request, get, _, _ = transfer_setup
    module.s3.head_object.side_effect = None
    module.s3.head_object.return_value = {'ContentLength': size, 'Metadata': metadata}
    with pytest.raises(ValueError, match='conflicts'):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    get.assert_not_called()
    module.s3.put_object.assert_not_called()


@pytest.mark.parametrize('failure', ['http', 'stream', 'redirect'])
def test_download_failure_never_uploads_and_cleans_up(transfer_setup, failure):
    module, request, _, response, _ = transfer_setup
    if failure == 'http':
        response.raise_for_status.side_effect = requests.HTTPError('download failed')
    elif failure == 'stream':

        def broken_stream(**kwargs):
            yield b'%PDF-'
            raise requests.ConnectionError('stream interrupted')

        response.iter_content.side_effect = broken_stream
    else:
        response.status_code = 302
    with pytest.raises((requests.RequestException, RuntimeError)):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    module.s3.put_object.assert_not_called()
    response.__exit__.assert_called_once()


def test_head_access_denied_is_not_treated_as_a_missing_object(transfer_setup):
    module, request, get, _, _ = transfer_setup
    module.s3.head_object.side_effect = ClientError({'Error': {'Code': '403'}}, 'HeadObject')
    with pytest.raises(ClientError):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    get.assert_not_called()
    module.s3.put_object.assert_not_called()


def test_conditional_upload_failure_cleans_up_without_overwrite_retry(transfer_setup):
    module, request, _, _, temporary_files = transfer_setup
    module.s3.put_object.side_effect = ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
    with pytest.raises(ClientError):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    module.s3.put_object.assert_called_once()
    assert module.s3.put_object.call_args.kwargs['IfNoneMatch'] == '*'
    assert temporary_files[0].closed


@pytest.mark.parametrize(
    'field,value',
    [
        ('Content_Version_Id__c', '../other'),
        ('Id', ''),
        ('Size_Bytes__c', 0),
        ('Size_Bytes__c', 1.5),
        ('Size_Bytes__c', True),
    ],
)
def test_invalid_request_metadata_is_rejected_before_transfer(transfer_setup, field, value):
    module, request, get, _, _ = transfer_setup
    request[field] = value
    with pytest.raises(ValueError):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    get.assert_not_called()
    module.s3.head_object.assert_not_called()


def test_file_larger_than_maximum_is_rejected_before_io(transfer_setup):
    module, request, get, _, _ = transfer_setup
    request['Size_Bytes__c'] = module.MAX_FILE_SIZE_BYTES + 1
    with pytest.raises(ValueError, match='maximum transfer size'):
        module._upload_request_file(request, TOKEN, INSTANCE_URL)
    get.assert_not_called()
    module.s3.head_object.assert_not_called()
    module.s3.put_object.assert_not_called()


@pytest.mark.parametrize('header', ['application/octet-stream', None])
def test_pdf_mime_type_falls_back_to_file_extension(transfer_setup, header):
    module, request, _, response, _ = transfer_setup
    response.headers = {'Content-Type': header} if header else {}
    module._upload_request_file(request, TOKEN, INSTANCE_URL)
    assert module.s3.put_object.call_args.kwargs['ContentType'] == 'application/pdf'

# Completion tests: everything below is mocked; no Salesforce or S3 access.
DOCUMENT_ID = '069R500000f7cddIAA'
WORK_ORDER_ID = '0WOR5000008gUTJOA2'
OPPORTUNITY_ID = '006R50000000abcdAA'
FILE_ID = 'a5LR5000002KierMAC'


def _pending_request():
    return {
        'Id': REQUEST_ID,
        'Content_Version_Id__c': VERSION_ID,
        'Content_Document_Id__c': DOCUMENT_ID,
        'Entity_Name__c': 'WorkOrder',
        'Record_Id__c': WORK_ORDER_ID,
        'S3_Key__c': BUCKET + '/' + KEY,
        'Size_Bytes__c': len(PDF),
        'Error__c': None,
    }


def _linked_file():
    return {
        'Id': FILE_ID,
        'Name': 'test file.pdf',
        'NEILON__Bucket_Name__c': BUCKET,
        'NEILON__Amazon_File_Key__c': KEY,
        'NEILON__Content_Document_Id__c': DOCUMENT_ID,
        'NEILON__Size__c': len(PDF),
        'NEILON__Bucket_Region__c': 'eu-west-2',
        'NEILON__Folder__c': 'a0PR5000001FolderAA',
        'NEILON__Extension__c': '.pdf',
        'NEILON__Content_Type__c': 'application/pdf',
        'WorkOrder__c': WORK_ORDER_ID,
        'NEILON__Opportunity__c': None,
        'Report_Source_Id__c': WORK_ORDER_ID,
        'Public__c': False,
    }


def test_update_request_uses_salesforce_patch(setup_lambda):
    module, _, _, _ = setup_lambda
    with patch.object(module.requests, 'patch') as update:
        update.return_value.status_code = 204
        module._update_request(TOKEN, INSTANCE_URL, REQUEST_ID, 'Done')
        update.assert_called_once_with(
            f'{INSTANCE_URL}/services/data/v68.0/sobjects/S3_Large_File_Request__c/{REQUEST_ID}',
            headers={'Authorization': f'Bearer {TOKEN}'},
            json={'Status__c': 'Done', 'Error__c': None},
            timeout=10,
            allow_redirects=False,
        )
        update.return_value.raise_for_status.assert_called_once()


def test_update_request_rejects_unconfirmed_success(setup_lambda):
    module, _, _, _ = setup_lambda
    with patch.object(module.requests, 'patch') as update:
        update.return_value.status_code = 200
        with pytest.raises(RuntimeError, match='did not confirm'):
            module._update_request(TOKEN, INSTANCE_URL, REQUEST_ID, 'Done')


def test_find_existing_link_verifies_exact_metadata(setup_lambda, monkeypatch):
    module, _, _, get = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    get.return_value.json.return_value = {'records': [_linked_file()], 'done': True}
    assert module._find_s3_link(_pending_request(), TOKEN, INSTANCE_URL) == FILE_ID
    assert "WHERE NEILON__Content_Document_Id__c = '" + DOCUMENT_ID + "'" in get.call_args.kwargs['params']['q']
    assert 'NEILON__Amazon_File_Key__c' not in get.call_args.kwargs['params']['q'].split('WHERE', 1)[1]


def test_find_existing_link_rejects_duplicate_and_incorrect_metadata(setup_lambda, monkeypatch):
    module, _, _, get = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    get.return_value.json.return_value = {'records': [_linked_file(), _linked_file()]}
    with pytest.raises(RuntimeError, match='Duplicate'):
        module._find_s3_link(_pending_request(), TOKEN, INSTANCE_URL)
    bad = _linked_file()
    bad['WorkOrder__c'] = '0WOR5000000OtherAA'
    get.return_value.json.return_value = {'records': [bad]}
    with pytest.raises(RuntimeError, match='does not match'):
        module._find_s3_link(_pending_request(), TOKEN, INSTANCE_URL)


def test_neilon_registration_uses_documented_json_payload(setup_lambda):
    module, _, post, _ = setup_lambda
    post.return_value.status_code = 200
    post.return_value.json.return_value = {
        'status': 'OK', 'code': '200', 'data': json.dumps([{'Id': FILE_ID}]),
    }
    request = _pending_request()
    assert module._register_s3_link(request, BUCKET, KEY, TOKEN, INSTANCE_URL) == FILE_ID
    post.assert_called_once_with(
        INSTANCE_URL + '/services/apexrest/NEILON/S3Link/v1/creates3files/',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json=[{
            'Name': 'test file.pdf',
            'NEILON__Bucket_Name__c': BUCKET,
            'NEILON__Amazon_File_Key__c': KEY,
            'NEILON__Size__c': len(PDF),
            'NEILON__Bucket_Region__c': 'eu-west-2',
            'NEILON__Content_Document_Id__c': DOCUMENT_ID,
            'NEILON__Extension__c': '.pdf',
            'Report_Source_Id__c': WORK_ORDER_ID,
            'Public__c': False,
            'WorkOrder__c': WORK_ORDER_ID,
            'NEILON__Content_Type__c': 'application/pdf',
        }],
        timeout=(10, 30),
        allow_redirects=False,
    )


@pytest.mark.parametrize('result', [
    {'status': 'ERROR', 'code': '200', 'data': '[]'},
    {'status': 'OK', 'code': '200', 'data': '[]'},
    {'status': 'OK', 'code': '200', 'data': 'not-json'},
])
def test_neilon_registration_does_not_trust_bad_response(setup_lambda, result):
    module, _, post, _ = setup_lambda
    post.return_value.status_code = 200
    post.return_value.json.return_value = result
    with pytest.raises((RuntimeError, ValueError)):
        module._register_s3_link(_pending_request(), BUCKET, KEY, TOKEN, INSTANCE_URL)


def test_verify_s3_upload_checks_identity_and_size(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    module.s3.head_object.return_value = {
        'ContentLength': len(PDF),
        'Metadata': {
            'salesforce-request-id': REQUEST_ID,
            'salesforce-content-version-id': VERSION_ID,
        },
    }
    module._verify_s3_upload(_pending_request())
    module.s3.head_object.return_value['Metadata']['salesforce-content-version-id'] = 'some-other-version'
    with pytest.raises(RuntimeError, match='verification failed'):
        module._verify_s3_upload(_pending_request())


def test_process_request_marks_registration_attempt_before_post(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, '_get_work_order_work_type', lambda *args: None)
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    actions = []
    with (
        patch.object(module, '_verify_content_version', side_effect=lambda *args: actions.append('verify_version')),
        patch.object(module, '_find_s3_link', side_effect=[None, FILE_ID]) as find,
        patch.object(module, '_upload_request_file', side_effect=lambda *args: actions.append('upload')),
        patch.object(module, '_verify_s3_upload', side_effect=lambda *args: actions.append('verify_s3')),
        patch.object(module, '_update_request', side_effect=lambda *args: actions.append(('update', args[3]))),
        patch.object(module, '_register_s3_link', side_effect=lambda *args: (actions.append('register'), FILE_ID)[1]),
    ):
        assert module._process_request(request, TOKEN, INSTANCE_URL) == FILE_ID
    assert find.call_count == 2
    assert actions == ['verify_version', 'upload', 'verify_s3', ('update', 'Pending'), 'register', ('update', 'Done')]
    assert request['Error__c'] == module.REGISTRATION_MARKER


def test_previous_uncertain_registration_never_posts_again(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, '_get_work_order_work_type', lambda *args: None)
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    request['Error__c'] = module.REGISTRATION_MARKER
    with (
        patch.object(module, '_verify_content_version'),
        patch.object(module, '_find_s3_link', return_value=None),
        patch.object(module, '_upload_request_file') as upload,
        patch.object(module, '_register_s3_link') as register,
        patch.object(module, '_update_request') as update,
    ):
        with pytest.raises(RuntimeError, match='manual reconciliation'):
            module._process_request(request, TOKEN, INSTANCE_URL)
    upload.assert_not_called()
    register.assert_not_called()
    update.assert_not_called()


def test_retry_with_verified_existing_link_only_marks_done(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, '_get_work_order_work_type', lambda *args: None)
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    request['Error__c'] = module.REGISTRATION_MARKER
    with (
        patch.object(module, '_verify_content_version'),
        patch.object(module, '_find_s3_link', return_value=FILE_ID),
        patch.object(module, '_upload_request_file') as upload,
        patch.object(module, '_verify_s3_upload') as verify,
        patch.object(module, '_register_s3_link') as register,
        patch.object(module, '_update_request') as update,
    ):
        assert module._process_request(request, TOKEN, INSTANCE_URL) == FILE_ID
    upload.assert_called_once()
    verify.assert_called_once()
    register.assert_not_called()
    update.assert_called_once_with(TOKEN, INSTANCE_URL, REQUEST_ID, 'Done')


def test_failed_registration_preserves_uncertainty_marker(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, '_get_work_order_work_type', lambda *args: None)
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    with (
        patch.object(module, '_verify_content_version'),
        patch.object(module, '_find_s3_link', return_value=None),
        patch.object(module, '_upload_request_file'),
        patch.object(module, '_verify_s3_upload'),
        patch.object(module, '_register_s3_link', side_effect=requests.Timeout('post response lost')),
        patch.object(module, '_update_request') as update,
    ):
        with pytest.raises(requests.Timeout):
            module._process_request(request, TOKEN, INSTANCE_URL)
    assert request['Error__c'] == module.REGISTRATION_MARKER
    update.assert_called_once_with(TOKEN, INSTANCE_URL, REQUEST_ID, 'Pending', module.REGISTRATION_MARKER)


def test_unsupported_entity_does_not_upload_or_register(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    request['Entity_Name__c'] = 'Account'
    with patch.object(module, '_upload_request_file') as upload, patch.object(module, '_register_s3_link') as register:
        with pytest.raises(RuntimeError, match='not verified'):
            module._process_request(request, TOKEN, INSTANCE_URL)
    upload.assert_not_called()
    register.assert_not_called()


def test_opportunity_registration_uses_builder_lookup(setup_lambda):
    module, _, post, _ = setup_lambda
    post.return_value.status_code = 200
    post.return_value.json.return_value = {
        'status': 'OK', 'code': '200', 'data': json.dumps([{'Id': FILE_ID}]),
    }
    request = _pending_request()
    request['Entity_Name__c'] = 'Opportunity'
    request['Record_Id__c'] = OPPORTUNITY_ID
    assert module._register_s3_link(request, BUCKET, KEY, TOKEN, INSTANCE_URL) == FILE_ID
    fields = post.call_args.kwargs['json'][0]
    assert fields['NEILON__Opportunity__c'] == OPPORTUNITY_ID
    assert fields['Report_Source_Id__c'] == OPPORTUNITY_ID
    assert 'WorkOrder__c' not in fields
    assert fields['NEILON__Extension__c'] == '.pdf'
    assert fields['NEILON__Content_Type__c'] == 'application/pdf'
    assert fields['Public__c'] is False


def test_opportunity_link_verification_uses_correct_relationship(setup_lambda, monkeypatch):
    module, _, _, get = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    request['Entity_Name__c'] = 'Opportunity'
    request['Record_Id__c'] = OPPORTUNITY_ID
    link = _linked_file()
    link['WorkOrder__c'] = None
    link['NEILON__Opportunity__c'] = OPPORTUNITY_ID
    link['Report_Source_Id__c'] = OPPORTUNITY_ID
    get.return_value.json.return_value = {'records': [link], 'done': True}
    assert module._find_s3_link(request, TOKEN, INSTANCE_URL) == FILE_ID
    link['NEILON__Opportunity__c'] = WORK_ORDER_ID
    with pytest.raises(RuntimeError, match='does not match'):
        module._find_s3_link(request, TOKEN, INSTANCE_URL)


@pytest.mark.parametrize('filename,extension,content_type', [
    ('report.pdf', '.pdf', 'application/pdf'),
    ('report.docx', '.docx', 'application/msword'),
    ('recording.mp4', '.mp4', None),
    ('email.msg', '.msg', None),
])
def test_file_metadata_matches_existing_neilon_builder(setup_lambda, filename, extension, content_type):
    module, _, _, _ = setup_lambda
    fields = module._s3_link_fields(_pending_request(), BUCKET, 'WorkOrders/test/' + filename)
    assert fields['Name'] == filename
    assert fields['NEILON__Extension__c'] == extension
    assert fields.get('NEILON__Content_Type__c') == content_type
    assert fields['Report_Source_Id__c'] == WORK_ORDER_ID
    assert 'Work_Type__c' not in fields  # Not available on the hand-off request.


def test_process_enabled_requires_explicit_single_request(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    monkeypatch.setattr(module, 'PROCESSING_ENABLED', True)
    first = _pending_request()
    second = _pending_request()
    second['Id'] = 'a9dR500000000hZIAQ'
    with patch.object(module, '_get_pending_requests', return_value=[first, second]):
        with patch.object(module, '_process_request', return_value=FILE_ID) as complete:
            result = module.process({'action': 'process_one', 'request_id': second['Id']}, None)
    assert result == {'pending_count': 2, 'completed_count': 1, 'failed_count': 0}
    complete.assert_called_once_with(second, TOKEN, INSTANCE_URL)


@pytest.mark.parametrize('event', [
    {},
    {'action': 'process_one'},
    {'request_id': REQUEST_ID},
    {'action': 'process_one', 'request_id': 'not-an-id'},
    {'source': 'aws.events', 'request_id': REQUEST_ID},
    None,
])
def test_processing_enabled_without_valid_manual_event_never_writes(setup_lambda, monkeypatch, event):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    monkeypatch.setattr(module, 'PROCESSING_ENABLED', True)
    with patch.object(module, '_get_pending_requests', return_value=[_pending_request()]):
        with patch.object(module, '_process_request') as complete:
            result = module.process(event, None)
    assert result['processing_skipped'] == 'specific_manual_request_required'
    complete.assert_not_called()
    module.s3.put_object.assert_not_called()


def test_selected_request_not_in_pending_batch_never_writes(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    monkeypatch.setattr(module, 'PROCESSING_ENABLED', True)
    with patch.object(module, '_get_pending_requests', return_value=[_pending_request()]):
        with patch.object(module, '_process_request') as complete:
            result = module.process({'action': 'process_one', 'request_id': 'a9dR500000000hZIAQ'}, None)
    assert result['processing_skipped'] == 'request_not_in_pending_batch'
    complete.assert_not_called()


def test_processing_disabled_even_with_manual_event_never_writes(setup_lambda):
    module, _, _, _ = setup_lambda
    with patch.object(module, '_get_pending_requests', return_value=[_pending_request()]):
        with patch.object(module, '_process_request') as complete:
            assert module.process({'action': 'process_one', 'request_id': REQUEST_ID}, None) == {'pending_count': 1}
    complete.assert_not_called()


def test_invalid_request_is_left_pending_for_manual_review(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    monkeypatch.setattr(module, 'PROCESSING_ENABLED', True)
    request = _pending_request()
    with patch.object(module, '_get_pending_requests', return_value=[request]):
        with patch.object(module, '_process_request', side_effect=ValueError('some private input')):
            with patch.object(module, '_update_request') as update:
                result = module.process({'action': 'process_one', 'request_id': request['Id']}, None)
    assert result == {'pending_count': 1, 'completed_count': 0, 'failed_count': 1}
    update.assert_not_called()


def test_uncertain_registration_not_marked_failed(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    monkeypatch.setattr(module, 'PROCESSING_ENABLED', True)
    request = _pending_request()
    request['Error__c'] = module.REGISTRATION_MARKER
    with patch.object(module, '_get_pending_requests', return_value=[request]):
        with patch.object(module, '_process_request', side_effect=ValueError('private bad response')):
            with patch.object(module, '_update_request') as update:
                assert module.process({'action': 'process_one', 'request_id': request['Id']}, None)['failed_count'] == 1
    update.assert_not_called()


def test_verify_source_version_matches_document_and_size(setup_lambda):
    module, _, _, get = setup_lambda
    get.return_value.json.return_value = {'ContentDocumentId': DOCUMENT_ID, 'ContentSize': len(PDF)}
    module._verify_content_version(_pending_request(), TOKEN, INSTANCE_URL)
    get.assert_called_once_with(
        f'{INSTANCE_URL}/services/data/v68.0/sobjects/ContentVersion/{VERSION_ID}',
        headers={'Authorization': f'Bearer {TOKEN}'},
        params={'fields': 'ContentDocumentId,ContentSize'},
        timeout=10,
        allow_redirects=False,
    )
    get.return_value.json.return_value['ContentDocumentId'] = '069R5000000OtherAA'
    with pytest.raises(ValueError, match='does not match'):
        module._verify_content_version(_pending_request(), TOKEN, INSTANCE_URL)


def test_opportunity_request_can_complete_using_existing_link(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    request['Entity_Name__c'] = 'Opportunity'
    request['Record_Id__c'] = OPPORTUNITY_ID
    with (
        patch.object(module, '_verify_content_version'),
        patch.object(module, '_find_s3_link', return_value=FILE_ID),
        patch.object(module, '_upload_request_file') as upload,
        patch.object(module, '_verify_s3_upload'),
        patch.object(module, '_register_s3_link') as register,
        patch.object(module, '_update_request') as update,
    ):
        assert module._process_request(request, TOKEN, INSTANCE_URL) == FILE_ID
    upload.assert_called_once()
    register.assert_not_called()
    update.assert_called_once_with(TOKEN, INSTANCE_URL, REQUEST_ID, 'Done')


def test_neilon_link_requires_report_source_and_extension_match(setup_lambda, monkeypatch):
    module, _, _, get = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    link = _linked_file()
    link['Report_Source_Id__c'] = None
    get.return_value.json.return_value = {'records': [link], 'done': True}
    with pytest.raises(RuntimeError, match='does not match'):
        module._find_s3_link(_pending_request(), TOKEN, INSTANCE_URL)
    link['Report_Source_Id__c'] = WORK_ORDER_ID
    link['NEILON__Extension__c'] = '.docx'
    with pytest.raises(RuntimeError, match='does not match'):
        module._find_s3_link(_pending_request(), TOKEN, INSTANCE_URL)


# WorkType behavior is taken from S3CDLFileCreatorQueueable and NeilonFileBuilder.
WORK_TYPE_ID = '08qR50000000012AAA'


@pytest.mark.parametrize('record_type,work_type_id,expected', [
    ('Repair', WORK_TYPE_ID, WORK_TYPE_ID),
    ('Maintenance', WORK_TYPE_ID, None),
    ('Repair', None, None),
])
def test_work_order_type_follows_existing_apex_builder(setup_lambda, record_type, work_type_id, expected):
    module, _, _, get = setup_lambda
    get.return_value.json.return_value = {
        'records': [{'WorkTypeId': work_type_id, 'RecordType': {'Name': record_type}}],
    }
    result = module._get_work_order_work_type(_pending_request(), TOKEN, INSTANCE_URL)
    assert result == expected
    get.assert_called_once_with(
        INSTANCE_URL + '/services/data/v68.0/query',
        headers={'Authorization': f'Bearer {TOKEN}'},
        params={'q': (
            "SELECT WorkTypeId, RecordType.Name FROM WorkOrder "
            f"WHERE Id = '{WORK_ORDER_ID}' LIMIT 1"
        )},
        timeout=10,
        allow_redirects=False,
    )


def test_inaccessible_related_work_order_fails_before_file_transfer(setup_lambda, monkeypatch):
    module, _, _, get = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    get.return_value.json.return_value = {'records': []}
    with patch.object(module, '_upload_request_file') as upload:
        with pytest.raises(RuntimeError, match='could not be verified'):
            module._process_request(_pending_request(), TOKEN, INSTANCE_URL)
    upload.assert_not_called()


def test_non_maintenance_work_order_registration_includes_work_type(setup_lambda):
    module, _, post, _ = setup_lambda
    post.return_value.status_code = 200
    post.return_value.json.return_value = {
        'status': 'OK', 'code': '200', 'data': json.dumps([{'Id': FILE_ID}]),
    }
    module._register_s3_link(_pending_request(), BUCKET, KEY, TOKEN, INSTANCE_URL, WORK_TYPE_ID)
    assert post.call_args.kwargs['json'][0]['Work_Type__c'] == WORK_TYPE_ID


def test_work_type_is_verified_for_existing_s3_link(setup_lambda, monkeypatch):
    module, _, _, get = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    link = _linked_file()
    link['Work_Type__c'] = WORK_TYPE_ID
    get.return_value.json.return_value = {'records': [link], 'done': True}
    assert module._find_s3_link(_pending_request(), TOKEN, INSTANCE_URL, WORK_TYPE_ID) == FILE_ID
    assert 'Work_Type__c' in get.call_args.kwargs['params']['q']
    link['Work_Type__c'] = None
    with pytest.raises(RuntimeError, match='does not match'):
        module._find_s3_link(_pending_request(), TOKEN, INSTANCE_URL, WORK_TYPE_ID)


def test_process_passes_work_type_into_registration_and_verification(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    with (
        patch.object(module, '_get_work_order_work_type', return_value=WORK_TYPE_ID) as get_type,
        patch.object(module, '_verify_content_version'),
        patch.object(module, '_find_s3_link', side_effect=[None, FILE_ID]) as find,
        patch.object(module, '_upload_request_file'),
        patch.object(module, '_verify_s3_upload'),
        patch.object(module, '_update_request'),
        patch.object(module, '_register_s3_link', return_value=FILE_ID) as register,
    ):
        assert module._process_request(request, TOKEN, INSTANCE_URL) == FILE_ID
    get_type.assert_called_once_with(request, TOKEN, INSTANCE_URL)
    assert find.call_args_list[0].args[3] == WORK_TYPE_ID
    assert find.call_args_list[1].args[3] == WORK_TYPE_ID
    assert register.call_args.args[-1] == WORK_TYPE_ID


def test_opportunity_still_never_queries_work_order_metadata(setup_lambda, monkeypatch):
    module, _, _, _ = setup_lambda
    monkeypatch.setattr(module, 'FILE_BUCKET', BUCKET)
    request = _pending_request()
    request['Entity_Name__c'] = 'Opportunity'
    request['Record_Id__c'] = OPPORTUNITY_ID
    with (
        patch.object(module, '_get_work_order_work_type') as get_type,
        patch.object(module, '_verify_content_version'),
        patch.object(module, '_find_s3_link', return_value=FILE_ID),
        patch.object(module, '_upload_request_file'),
        patch.object(module, '_verify_s3_upload'),
        patch.object(module, '_update_request'),
    ):
        assert module._process_request(request, TOKEN, INSTANCE_URL) == FILE_ID
    get_type.assert_not_called()
