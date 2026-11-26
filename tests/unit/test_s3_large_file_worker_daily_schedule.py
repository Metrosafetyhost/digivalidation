"""Daily schedule guardrail tests for the existing large-file worker."""

import importlib
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def worker(monkeypatch):
    with patch('boto3.client', return_value=Mock()):
        module = importlib.import_module('lambdas.s3_large_file_worker')
        module = importlib.reload(module)
    monkeypatch.setattr(module, 'PROCESSING_ENABLED', True)
    monkeypatch.setattr(module, 'FILE_BUCKET', 'metrosafetyprodfiles')
    monkeypatch.setattr(module, '_authenticate_salesforce', lambda: ('token', 'https://example.invalid'))
    yield module


def event():
    return {'action': 'process_pending', 'source': 'metro-safety-daily-scheduler'}


def requests(count):
    return [{'Id': f'a9dR500000000f{i}IAA'} for i in range(count)]


def test_daily_schedule_does_nothing_when_empty(worker, monkeypatch):
    monkeypatch.setattr(worker, '_get_pending_requests', lambda *_: [])
    with patch.object(worker, '_process_request') as process:
        result = worker.process(event(), None)
    assert result == {'pending_count': 0, 'completed_count': 0, 'failed_count': 0}
    process.assert_not_called()


def test_daily_schedule_processes_only_oldest_request(worker, monkeypatch):
    items = requests(3)
    monkeypatch.setattr(worker, '_get_pending_requests', lambda *_: items)
    with patch.object(worker, '_process_request') as process:
        result = worker.process(event(), None)
    assert result == {'pending_count': 3, 'completed_count': 1, 'failed_count': 0}
    process.assert_called_once_with(items[0], 'token', 'https://example.invalid')


@pytest.mark.parametrize('untrusted', [
    {}, None, {'action': 'process_pending'},
    {'action': 'process_pending', 'source': 'aws.events'},
    {'action': 'process_one'},
    {'action': 'unknown', 'source': 'metro-safety-daily-scheduler'},
])
def test_non_authorized_event_never_processes(worker, monkeypatch, untrusted):
    monkeypatch.setattr(worker, '_get_pending_requests', lambda *_: requests(1))
    with patch.object(worker, '_process_request') as process:
        result = worker.process(untrusted, None)
    assert result['processing_skipped'] == 'specific_manual_request_required'
    process.assert_not_called()


def test_processing_disabled_still_checks_queue_but_does_not_write(worker, monkeypatch):
    monkeypatch.setattr(worker, 'PROCESSING_ENABLED', False)
    monkeypatch.setattr(worker, '_get_pending_requests', lambda *_: requests(1))
    with patch.object(worker, '_process_request') as process:
        assert worker.process(event(), None) == {'pending_count': 1}
    process.assert_not_called()


def test_manual_single_request_still_works(worker, monkeypatch):
    items = requests(2)
    monkeypatch.setattr(worker, '_get_pending_requests', lambda *_: items)
    with patch.object(worker, '_process_request') as process:
        result = worker.process({'action': 'process_one', 'request_id': items[1]['Id']}, None)
    assert result == {'pending_count': 2, 'completed_count': 1, 'failed_count': 0}
    process.assert_called_once_with(items[1], 'token', 'https://example.invalid')


def test_scheduler_failure_leaves_status_to_existing_worker_logic(worker, monkeypatch):
    monkeypatch.setattr(worker, '_get_pending_requests', lambda *_: requests(2))
    with patch.object(worker, '_process_request', side_effect=RuntimeError('test failure')) as process:
        with patch.object(worker, '_update_request') as update:
            result = worker.process(event(), None)
    assert result == {'pending_count': 2, 'completed_count': 0, 'failed_count': 1}
    process.assert_called_once()
    update.assert_not_called()


def test_missing_bucket_stops_before_processing(worker, monkeypatch):
    monkeypatch.setattr(worker, 'FILE_BUCKET', None)
    monkeypatch.setattr(worker, '_get_pending_requests', lambda *_: requests(1))
    with patch.object(worker, '_process_request') as process:
        with pytest.raises(RuntimeError, match='FILE_BUCKET'):
            worker.process(event(), None)
    process.assert_not_called()
