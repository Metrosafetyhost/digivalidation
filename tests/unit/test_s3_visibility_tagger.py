import base64
import importlib
import json
from urllib.parse import quote_plus

import pytest


@pytest.fixture
def tagger(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.setenv('FILE_BUCKET', 'metrosafetyprodfiles')
    return importlib.import_module('lambdas.s3_visibility_tagger')


def http_event(body, *, method='POST', path='/files/visibility-recommendation', encoded=False):
    if encoded:
        body = base64.b64encode(body.encode('utf-8')).decode('ascii')
    return {
        'version': '2.0',
        'rawPath': path,
        'requestContext': {'http': {'method': method, 'path': path}},
        'body': body,
        'isBase64Encoded': encoded,
    }


def s3_record(key, *, bucket='metrosafetyprodfiles', version_id=None):
    obj = {'key': quote_plus(key)}
    if version_id is not None:
        obj['versionId'] = version_id
    return {
        'eventSource': 'aws:s3',
        'eventName': 'ObjectCreated:Put',
        's3': {'bucket': {'name': bucket}, 'object': obj},
    }


@pytest.mark.parametrize(
    ('name', 'expected'),
    [
        ('report_final.pdf', True),
        ('report_VR.pdf', True),
        ('REPORT_FINAL.PDF', True),
        ('report_vr.PDF', True),
        ('report_final_Preview.pdf', False),
        ('report_VR_PREVIEW.pdf', False),
        ('quote.pdf', False),
    ],
)
def test_recommendation_uses_filename_rule(tagger, monkeypatch, name, expected):
    class NoS3:
        def __getattr__(self, method):
            raise AssertionError(f'HTTP recommendation must not use S3: {method}')

    monkeypatch.setattr(tagger, 'S3', NoS3())
    monkeypatch.delenv('FILE_BUCKET')
    result = tagger.process(http_event(json.dumps({'fileName': name})), None)
    assert result['statusCode'] == 200
    assert result['headers'] == {'Content-Type': 'application/json'}
    assert json.loads(result['body']) == {'customerVisible': expected, 'publicVisible': False}
    assert isinstance(json.loads(result['body'])['customerVisible'], bool)
    assert isinstance(json.loads(result['body'])['publicVisible'], bool)


def test_base64_encoded_recommendation(tagger):
    result = tagger.process(http_event(json.dumps({'fileName': 'example_final.pdf'}), encoded=True), None)
    assert json.loads(result['body']) == {'customerVisible': True, 'publicVisible': False}


@pytest.mark.parametrize(
    'body',
    [
        '{}',
        '{"fileName": null}',
        '{"fileName": true}',
        '{"fileName": 1}',
        '{"fileName": []}',
        '{"fileName": {}}',
        '{"fileName": "  "}',
        '[]',
        'not-json',
        None,
    ],
)
def test_invalid_recommendation_body(tagger, body):
    result = tagger.process(http_event(body), None)
    assert result['statusCode'] == 400
    assert 'error' in json.loads(result['body'])


def test_invalid_base64_body(tagger):
    result = tagger.process(http_event('not-base64', encoded=False) | {'isBase64Encoded': True}, None)
    assert result['statusCode'] == 400


def test_unexpected_http_route_and_method(tagger):
    assert tagger.process(http_event('{}', path='/files/elsewhere'), None)['statusCode'] == 404
    assert tagger.process(http_event('{}', method='GET'), None)['statusCode'] == 405


def test_s3_uses_same_rule_and_preserves_explicit_tags(tagger, monkeypatch):
    class FakeS3:
        def __init__(self):
            self.puts = []
            self.gets = []

        def get_object_tagging(self, **params):
            self.gets.append(params)
            return {
                'TagSet': [
                    {'Key': 'customer-visible', 'Value': 'false'},
                    {'Key': 'other', 'Value': 'kept'},
                ]
            }

        def put_object_tagging(self, **params):
            self.puts.append(params)

    fake = FakeS3()
    monkeypatch.setattr(tagger, 'S3', fake)
    counts = tagger.process({'Records': [s3_record('Buildings/123/report_final.pdf', version_id='v1')]}, None)
    assert counts == {'received': 1, 'tagged': 1, 'skipped': 0}
    assert fake.gets == [{'Bucket': 'metrosafetyprodfiles', 'Key': 'Buildings/123/report_final.pdf', 'VersionId': 'v1'}]
    tags = {tag['Key']: tag['Value'] for tag in fake.puts[0]['Tagging']['TagSet']}
    assert tags == {'customer-visible': 'false', 'public-visible': 'false', 'other': 'kept'}


@pytest.mark.parametrize(
    ('name', 'expected'),
    [
        ('report_final.pdf', 'true'),
        ('report_VR.pdf', 'true'),
        ('report_final_Preview.pdf', 'false'),
        ('quote.pdf', 'false'),
    ],
)
def test_s3_and_http_use_exact_same_rule(tagger, monkeypatch, name, expected):
    class FakeS3:
        def __init__(self):
            self.tags = None

        def get_object_tagging(self, **params):
            return {'TagSet': []}

        def put_object_tagging(self, **params):
            self.tags = {tag['Key']: tag['Value'] for tag in params['Tagging']['TagSet']}

    fake = FakeS3()
    monkeypatch.setattr(tagger, 'S3', fake)
    tagger.process({'Records': [s3_record(f'WorkOrders/42/{name}')]}, None)
    api = tagger.process(http_event(json.dumps({'fileName': name})), None)
    assert fake.tags == tagger.visibility_defaults(name)
    assert json.loads(api['body']) == {
        'customerVisible': expected == 'true',
        'publicVisible': False,
    }


def test_s3_explicit_true_and_false_and_multiple_records(tagger, monkeypatch):
    class FakeS3:
        def __init__(self):
            self.puts = []

        def get_object_tagging(self, **params):
            return {
                'TagSet': [
                    {'Key': 'customer-visible', 'Value': 'true'},
                    {'Key': 'public-visible', 'Value': 'false'},
                ]
            }

        def put_object_tagging(self, **params):
            self.puts.append(params)

    fake = FakeS3()
    monkeypatch.setattr(tagger, 'S3', fake)
    counts = tagger.process(
        {
            'Records': [
                s3_record('WorkOrders/42/quote.pdf'),
                s3_record('Buildings/1/'),
                s3_record('Other/report_final.pdf'),
                s3_record('Buildings/1/quote.pdf', bucket='other'),
                {'eventSource': 'aws:sns'},
            ]
        },
        None,
    )
    assert counts == {'received': 5, 'tagged': 0, 'skipped': 5}
    assert fake.puts == []


def test_s3_preserves_explicit_public_true(tagger, monkeypatch):
    class FakeS3:
        def __init__(self):
            self.tags = None

        def get_object_tagging(self, **params):
            return {'TagSet': [{'Key': 'public-visible', 'Value': 'true'}]}

        def put_object_tagging(self, **params):
            self.tags = {tag['Key']: tag['Value'] for tag in params['Tagging']['TagSet']}

    fake = FakeS3()
    monkeypatch.setattr(tagger, 'S3', fake)
    tagger.process({'Records': [s3_record('Buildings/123/quote.pdf')]}, None)
    assert fake.tags == {'customer-visible': 'false', 'public-visible': 'true'}


def test_http_and_s3_call_same_rule_function(tagger, monkeypatch):
    called_with = []
    original = tagger.visibility_defaults

    def tracking_rule(file_name):
        called_with.append(file_name)
        return original(file_name)

    class FakeS3:
        def get_object_tagging(self, **params):
            return {'TagSet': []}

        def put_object_tagging(self, **params):
            pass

    monkeypatch.setattr(tagger, 'visibility_defaults', tracking_rule)
    monkeypatch.setattr(tagger, 'S3', FakeS3())
    tagger.process({'Records': [s3_record('Buildings/123/report_final.pdf')]}, None)
    tagger.process(http_event(json.dumps({'fileName': 'report_final.pdf'})), None)
    assert called_with == ['report_final.pdf', 'report_final.pdf']
