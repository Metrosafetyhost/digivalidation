import importlib
import json
from datetime import UTC, datetime

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def viewer(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    module = importlib.import_module('lambdas.s3_file_viewer')
    monkeypatch.setattr(module, 'object_exists', lambda key: False)
    return module


def upload_event(kind, body):
    if kind == 'workorder':
        path = '/files/workorders/42/upload-url'
        body = {'fileName': 'report.pdf', 'contentType': 'application/pdf', **body}
        path_parameters = {'workOrderId': '42'}
    else:
        path = '/files/buildings/upload-url'
        body = {
            'buildingPrefix': 'Buildings/123 | Example/',
            'folderPath': 'Compliance Documents/Fire/Assessment/',
            'fileName': 'report.pdf',
            'contentType': 'application/pdf',
            **body,
        }
        path_parameters = {}
    return {'rawPath': path, 'pathParameters': path_parameters, 'body': json.dumps(body)}


@pytest.mark.parametrize('kind', ['workorder', 'building'])
@pytest.mark.parametrize(('visible', 'expected'), [(True, 'true'), (False, 'false')])
def test_explicit_visibility_is_signed_and_returned(viewer, monkeypatch, kind, visible, expected):
    calls = []

    def presign(**kwargs):
        calls.append(kwargs)
        return 'https://example.test/upload'

    monkeypatch.setattr(viewer.s3, 'generate_presigned_url', presign)
    result = viewer.process(upload_event(kind, {'customerVisible': visible}), None)
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    tagging = f'customer-visible={expected}&public-visible=false'
    assert calls == [
        {
            'ClientMethod': 'put_object',
            'Params': {
                'Bucket': viewer.FILE_BUCKET,
                'Key': body['objectKey'],
                'ContentType': 'application/pdf',
                'Tagging': tagging,
            },
            'ExpiresIn': viewer.PRESIGNED_URL_SECONDS,
        }
    ]
    assert body['taggingHeader'] == tagging
    assert body['uploadUrl'] == 'https://example.test/upload'
    assert body['fileName'] == 'report.pdf'
    assert body['contentType'] == 'application/pdf'
    assert body['expiresInSeconds'] == viewer.PRESIGNED_URL_SECONDS
    if kind == 'building':
        assert body['buildingRoot'] == 'Buildings/123 | Example/'
        assert body['folderPath'] == 'Compliance Documents/Fire/Assessment/'
    else:
        assert body['objectKey'] == 'WorkOrders/42/report.pdf'


@pytest.mark.parametrize('kind', ['workorder', 'building'])
def test_legacy_upload_has_no_tagging(viewer, monkeypatch, kind):
    calls = []

    def presign(**kwargs):
        calls.append(kwargs)
        return 'https://example.test/upload'

    monkeypatch.setattr(viewer.s3, 'generate_presigned_url', presign)
    result = viewer.process(upload_event(kind, {}), None)
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert 'taggingHeader' not in body
    assert 'Tagging' not in calls[0]['Params']
    assert body['uploadUrl'] == 'https://example.test/upload'
    assert body['fileName'] == 'report.pdf'
    assert body['contentType'] == 'application/pdf'
    assert body['expiresInSeconds'] == viewer.PRESIGNED_URL_SECONDS
    if kind == 'building':
        assert set(body) == {'uploadUrl', 'objectKey', 'buildingRoot', 'folderPath', 'fileName', 'contentType', 'expiresInSeconds'}
    else:
        assert set(body) == {'uploadUrl', 'objectKey', 'fileName', 'contentType', 'expiresInSeconds'}


@pytest.mark.parametrize('kind', ['workorder', 'building'])
@pytest.mark.parametrize('invalid', [None, 'true', 1, 0, [], {}])
def test_invalid_visibility_is_rejected(viewer, monkeypatch, kind, invalid):
    def no_presign(**kwargs):
        raise AssertionError('Invalid uploads must not be presigned')

    monkeypatch.setattr(viewer.s3, 'generate_presigned_url', no_presign)
    result = viewer.process(upload_event(kind, {'customerVisible': invalid}), None)
    assert result['statusCode'] == 400
    assert json.loads(result['body']) == {'error': 'customerVisible must be a JSON Boolean'}


def file_item(key):
    return {'Key': key, 'Size': 7, 'LastModified': datetime(2026, 1, 1, tzinfo=UTC)}


def work_order_event(scope=None, *, open_key=None):
    path = '/files/workorders/42' + ('/open' if open_key is not None else '')
    query = {} if scope is None else {'visibilityScope': scope}
    if open_key is not None:
        query['key'] = open_key
    return {'rawPath': path, 'pathParameters': {'workOrderId': '42'}, 'queryStringParameters': query}


def building_event(scope=None, *, open_key=None):
    path = '/files/buildings' + ('/open' if open_key is not None else '')
    query = {'buildingPrefix': 'Buildings/123 | Main/'}
    if scope is not None:
        query['visibilityScope'] = scope
    if open_key is not None:
        query['key'] = open_key
    return {'rawPath': path, 'queryStringParameters': query}


@pytest.fixture
def tagged_s3(viewer, monkeypatch):
    class Paginator:
        def __init__(self, owner):
            self.owner = owner

        def paginate(self, **kwargs):
            self.owner.list_calls.append(kwargs)
            yield {'Contents': [file_item(key) for key in self.owner.keys if key.startswith(kwargs['Prefix'])]}

    class FakeS3:
        def __init__(self):
            self.keys = []
            self.tags = {}
            self.tag_calls = []
            self.list_calls = []
            self.head_calls = []
            self.presign_calls = []

        def get_paginator(self, name):
            assert name == 'list_objects_v2'
            return Paginator(self)

        def list_objects_v2(self, **kwargs):
            return {'KeyCount': sum(key.startswith(kwargs['Prefix']) for key in self.keys)}

        def get_object_tagging(self, **kwargs):
            self.tag_calls.append(kwargs)
            return {'TagSet': self.tags.get(kwargs['Key'], [])}

        def head_object(self, **kwargs):
            self.head_calls.append(kwargs)
            return {}

        def generate_presigned_url(self, **kwargs):
            self.presign_calls.append(kwargs)
            return 'https://example.test/download'

    fake = FakeS3()
    monkeypatch.setattr(viewer, 's3', fake)
    return fake


@pytest.mark.parametrize('scope', [None, 'staff'])
def test_staff_work_order_lists_and_opens_every_file_without_tag_reads(viewer, tagged_s3, scope):
    tagged_s3.keys = ['WorkOrders/42/internal.pdf', 'WorkOrders/42/visible.pdf', 'WorkOrders/42/folder/']
    result = viewer.process(work_order_event(scope), None)
    body = json.loads(result['body'])
    assert result['statusCode'] == 200
    assert body['recordCount'] == 2
    assert {item['key'] for item in body['files']} == set(tagged_s3.keys[:2])
    opened = viewer.process(work_order_event(scope, open_key=tagged_s3.keys[0]), None)
    assert opened['statusCode'] == 200
    assert json.loads(opened['body']) == {'url': 'https://example.test/download', 'expiresInSeconds': viewer.PRESIGNED_URL_SECONDS}
    assert tagged_s3.tag_calls == []


@pytest.mark.parametrize(('scope', 'required_tag'), [('customer', 'customer-visible'), ('public', 'public-visible')])
def test_work_order_filters_exact_tag_and_rechecks_open(viewer, tagged_s3, scope, required_tag):
    root = 'WorkOrders/42/'
    tagged_s3.keys = [root + name for name in ('visible.pdf', 'false.pdf', 'missing.pdf', 'malformed.pdf')]
    tagged_s3.tags = {
        tagged_s3.keys[0]: [{'Key': required_tag, 'Value': 'true'}],
        tagged_s3.keys[1]: [{'Key': required_tag, 'Value': 'false'}],
        tagged_s3.keys[3]: [{'Key': required_tag, 'Value': 'TRUE'}],
    }
    listed = viewer.process(work_order_event(scope), None)
    assert listed['statusCode'] == 200
    assert json.loads(listed['body'])['recordCount'] == 1
    assert [item['key'] for item in json.loads(listed['body'])['files']] == tagged_s3.keys[:1]
    assert len(tagged_s3.tag_calls) == 4

    denied = viewer.process(work_order_event(scope, open_key=tagged_s3.keys[1]), None)
    assert denied['statusCode'] == 404
    assert tagged_s3.presign_calls == []

    # A formerly visible object is checked again rather than trusted from the list.
    tagged_s3.tags[tagged_s3.keys[0]] = [{'Key': required_tag, 'Value': 'false'}]
    denied_after_change = viewer.process(work_order_event(scope, open_key=tagged_s3.keys[0]), None)
    assert denied_after_change['statusCode'] == 404
    assert tagged_s3.presign_calls == []
    tagged_s3.tags[tagged_s3.keys[0]] = [{'Key': required_tag, 'Value': 'true'}]
    opened = viewer.process(work_order_event(scope, open_key=tagged_s3.keys[0]), None)
    assert opened['statusCode'] == 200
    assert tagged_s3.presign_calls[-1]['Params']['Key'] == tagged_s3.keys[0]
    assert len(tagged_s3.tag_calls) == 7

    outside = viewer.process(work_order_event(scope, open_key='WorkOrders/other/visible.pdf'), None)
    assert outside['statusCode'] == 403
    assert len(tagged_s3.tag_calls) == 7


@pytest.mark.parametrize('scope', ['customer', 'public'])
def test_tag_failure_returns_no_listing_or_download(viewer, tagged_s3, monkeypatch, scope):
    key = 'WorkOrders/42/report.pdf'
    tagged_s3.keys = [key]

    def fail(**kwargs):
        raise ClientError({'Error': {'Code': 'InternalError', 'Message': 'tag lookup failed'}}, 'GetObjectTagging')

    monkeypatch.setattr(tagged_s3, 'get_object_tagging', fail)
    assert viewer.process(work_order_event(scope), None)['statusCode'] == 500
    assert viewer.process(work_order_event(scope, open_key=key), None)['statusCode'] == 500
    assert tagged_s3.presign_calls == []


@pytest.mark.parametrize('scope', ['customer', 'public'])
def test_building_counts_folders_and_open_use_visible_tags(viewer, tagged_s3, monkeypatch, scope):
    root = 'Buildings/123 | Main/'
    base = root + 'Compliance Documents/'
    visible = base + 'visible.pdf'
    nested_visible = base + 'Fire/Assessment/nested.pdf'
    hidden = base + 'hidden.pdf'
    hidden_folder = base + 'Secret/hidden.pdf'
    tagged_s3.keys = [visible, nested_visible, hidden, hidden_folder, base + 'Secret/']
    required_tag = 'customer-visible' if scope == 'customer' else 'public-visible'
    tagged_s3.tags = {
        visible: [{'Key': required_tag, 'Value': 'true'}],
        nested_visible: [{'Key': required_tag, 'Value': 'true'}],
        hidden: [{'Key': required_tag, 'Value': 'false'}],
        hidden_folder: [{'Key': required_tag, 'Value': 'false'}],
    }
    monkeypatch.setattr(viewer, 'find_building_roots', lambda prefix: {root, 'Buildings//123 | Old/'})
    monkeypatch.setattr(viewer, 'prefix_contains_real_files', lambda prefix: (_ for _ in ()).throw(AssertionError('hidden root scan')))
    result = viewer.process(building_event(scope), None)
    body = json.loads(result['body'])
    assert result['statusCode'] == 200
    assert body['buildingRoot'] == root
    assert body['recordCount'] == 1
    assert [item['key'] for item in body['files']] == [visible]
    assert body['warnings'] == []
    assert body['canUpload'] is False
    assert body['breadcrumbs'] == viewer.build_breadcrumbs('Compliance Documents/')
    folders = {folder['name']: folder for folder in body['folders']}
    assert 'Secret' not in folders
    assert folders['Fire']['documentCount'] == 1
    assert folders['Fire']['hasContents'] is True
    assert folders['Gas']['documentCount'] == 0
    assert folders['Gas']['hasContents'] is False
    assert tagged_s3.tag_calls and all(call['Bucket'] == viewer.FILE_BUCKET for call in tagged_s3.tag_calls)

    monkeypatch.setattr(viewer, 's3_prefix_has_contents', lambda prefix: True)
    denied = viewer.process(building_event(scope, open_key=hidden), None)
    assert denied['statusCode'] == 404
    assert tagged_s3.presign_calls == []
    opened = viewer.process(building_event(scope, open_key=visible), None)
    assert opened['statusCode'] == 200
    assert tagged_s3.presign_calls[-1]['Params']['Key'] == visible

    hidden_folder_request = building_event(scope)
    hidden_folder_request['queryStringParameters']['folderPath'] = 'Compliance Documents/Secret/'
    hidden_folder_result = viewer.process(hidden_folder_request, None)
    assert hidden_folder_result['statusCode'] == 404


def test_listing_cache_is_request_local(viewer, tagged_s3, monkeypatch):
    key = 'WorkOrders/42/visible.pdf'
    tagged_s3.tags[key] = [{'Key': 'customer-visible', 'Value': 'true'}]

    class RepeatedPaginator:
        def paginate(self, **kwargs):
            yield {'Contents': [file_item(key)]}
            yield {'Contents': [file_item(key)]}

    monkeypatch.setattr(tagged_s3, 'get_paginator', lambda name: RepeatedPaginator())
    viewer.process(work_order_event('customer'), None)
    assert len(tagged_s3.tag_calls) == 1
    viewer.process(work_order_event('customer'), None)
    assert len(tagged_s3.tag_calls) == 2


def test_staff_building_keeps_hidden_folders_counts_and_warnings(viewer, tagged_s3, monkeypatch):
    root = 'Buildings/123 | Main/'
    base = root + 'Compliance Documents/'
    tagged_s3.keys = [base + 'Secret/internal.pdf', base + 'internal.pdf']
    monkeypatch.setattr(viewer, 'find_building_roots', lambda prefix: {root, 'Buildings//123 | Old/'})
    monkeypatch.setattr(viewer, 'prefix_contains_real_files', lambda prefix: True)
    result = viewer.process(building_event(), None)
    body = json.loads(result['body'])
    assert result['statusCode'] == 200
    assert body['recordCount'] == 1
    assert body['warnings'] and 'Old' in body['warnings'][0]
    assert {folder['name']: folder['documentCount'] for folder in body['folders']}['Secret'] == 1
    opened = viewer.process(building_event(open_key=base + 'internal.pdf'), None)
    assert opened['statusCode'] == 200
    assert tagged_s3.tag_calls == []


def test_staff_building_keeps_double_slash_root_support(viewer, tagged_s3):
    root = 'Buildings//123 | Legacy/'
    key = root + 'Compliance Documents/report.pdf'
    tagged_s3.keys = [key]
    event = building_event()
    event['queryStringParameters']['buildingRoot'] = root
    result = viewer.process(event, None)
    body = json.loads(result['body'])
    assert result['statusCode'] == 200
    assert body['buildingRoot'] == root
    assert [item['key'] for item in body['files']] == [key]
    assert tagged_s3.tag_calls == []


@pytest.mark.parametrize('scope', ['', 'Staff', 'internal', 'false', 1, None, []])
def test_invalid_visibility_scope_returns_400_before_s3(viewer, tagged_s3, scope):
    event = work_order_event()
    event['queryStringParameters'] = {'visibilityScope': scope}
    result = viewer.process(event, None)
    assert result['statusCode'] == 400
    assert tagged_s3.list_calls == []
    assert tagged_s3.tag_calls == []


def test_invalid_building_scope_returns_400_before_root_resolution(viewer, monkeypatch):
    monkeypatch.setattr(viewer, 'resolve_building_root', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('root resolved')))
    event = building_event('invalid')
    assert viewer.process(event, None)['statusCode'] == 400


def test_existing_delete_and_move_routes_ignore_visibility_scope(viewer, monkeypatch):
    class MutatingS3:
        def __init__(self):
            self.deleted = []
            self.copied = []

        def delete_object(self, **kwargs):
            self.deleted.append(kwargs)

        def copy_object(self, **kwargs):
            self.copied.append(kwargs)

        def get_object_tagging(self, **kwargs):
            raise AssertionError('Mutation must not fetch tags')

    fake = MutatingS3()
    monkeypatch.setattr(viewer, 's3', fake)
    monkeypatch.setattr(viewer, 'object_exists', lambda key: True)
    work_order_key = 'WorkOrders/42/report.pdf'
    deleted = viewer.process(
        {
            'rawPath': '/files/workorders/42/delete',
            'pathParameters': {'workOrderId': '42'},
            'queryStringParameters': {'visibilityScope': 'customer'},
            'body': json.dumps({'objectKey': work_order_key}),
        },
        None,
    )
    assert deleted['statusCode'] == 200
    assert fake.deleted == [{'Bucket': viewer.FILE_BUCKET, 'Key': work_order_key}]

    root = 'Buildings/123 | Main/'
    source = root + 'Compliance Documents/Fire/Assessment/report.pdf'
    monkeypatch.setattr(viewer, 'object_exists', lambda key: key == source or bool(fake.copied))
    monkeypatch.setattr(viewer, 'find_building_root', lambda prefix: root)
    monkeypatch.setattr(viewer, 'validate_upload_folder', lambda building_root, folder: 'Compliance Documents/Gas/Assessment/')
    moved = viewer.process(
        {
            'rawPath': '/files/buildings/move',
            'queryStringParameters': {'visibilityScope': 'public'},
            'body': json.dumps(
                {
                    'buildingPrefix': root,
                    'objectKey': source,
                    'destinationFolderPath': 'Compliance Documents/Gas/Assessment/',
                }
            ),
        },
        None,
    )
    assert moved['statusCode'] == 200
    destination = root + 'Compliance Documents/Gas/Assessment/report.pdf'
    assert fake.copied[-1]['Key'] == destination
    assert fake.deleted[-1] == {'Bucket': viewer.FILE_BUCKET, 'Key': source}

    building_deleted = viewer.process(
        {
            'rawPath': '/files/buildings/delete',
            'queryStringParameters': {'visibilityScope': 'customer'},
            'body': json.dumps({'buildingPrefix': root, 'objectKey': source}),
        },
        None,
    )
    assert building_deleted['statusCode'] == 200
    assert fake.deleted[-1] == {'Bucket': viewer.FILE_BUCKET, 'Key': source}


def test_existing_upload_duplicate_check_is_unchanged(viewer, monkeypatch):
    monkeypatch.setattr(viewer, 'object_exists', lambda key: True)
    result = viewer.process(upload_event('workorder', {'customerVisible': False}), None)
    assert result['statusCode'] == 409
    assert json.loads(result['body'])['objectKey'] == 'WorkOrders/42/report.pdf'
