"""Regression tests for operational and protocol boundaries."""
import concurrent.futures
import socket

import pytest

from api.server import create_app
from common import protocol
from tests.helpers import RawWorker, wait_for
from worker.tasks.email import send_email
from worker.tasks.reports import generate_report


@pytest.mark.parametrize('body', [[], 'bad', None, {'task_type': 'x', 'priority': 'high'},
    {'task_type': 'x', 'priority': True}, {'task_type': 'x', 'priority': 2**80},
    {'task_type': 'x', 'payload': []}, {'task_type': 'x', 'payload': None},
    {'task_type': 'x', 'max_attempts': 0}, {'task_type': 'x', 'max_attempts': '3'},
    {'task_type': 'x', 'max_attempts': True}, {'task_type': ' '}])
def test_invalid_submissions_are_client_errors(api, body):
    assert api.post('/jobs', json=body).status_code == 400


@pytest.mark.parametrize('query', ['limit=0', 'limit=-1', 'limit=201', 'offset=-1', 'offset=no', 'status=bad'])
def test_invalid_job_queries(api, query):
    assert api.get('/jobs?' + query).status_code == 400


def test_pagination_and_search(api, store):
    ids = {store.create_job('email', {})['id'] for _ in range(4)}
    first = api.get('/jobs?limit=2').get_json()
    second = api.get('/jobs?limit=2&offset=2').get_json()
    assert first['has_more'] is True and second['has_more'] is False
    assert {j['job_id'] for j in first['jobs'] + second['jobs']} == ids
    job_id = next(iter(ids))
    assert api.get('/jobs?search=' + job_id[:12]).get_json()['jobs'][0]['job_id'] == job_id
    assert api.get('/jobs?search=%25').get_json()['count'] == 0


def test_auth_probes_and_metrics(server):
    api = create_app(server, api_key='test-key').test_client()
    assert api.get('/health').status_code == 200
    assert api.get('/ready').status_code == 200
    assert api.get('/stats').status_code == 401
    assert api.post('/jobs', json={'task_type': 'x'}).status_code == 401
    assert api.get('/stats', headers={'Authorization': 'Bearer wrong'}).status_code == 401
    assert api.get('/stats', headers={'Authorization': 'Bearer test-key'}).status_code == 200
    metrics = api.get('/metrics', headers={'Authorization': 'Bearer test-key'})
    assert 'djqs_jobs{status="queued"}' in metrics.text
    server.stop()
    assert api.get('/ready').status_code == 503


def test_body_limit(api):
    response = api.post('/jobs', json={'task_type': 'x', 'payload': {'large': 'x' * 300000}})
    assert response.status_code == 413
    assert 'error' in response.get_json()


@pytest.mark.parametrize('limit', [1, 5])
def test_per_job_retry_budget(store, limit):
    job = store.create_job('x', {}, max_attempts=limit)
    for attempt in range(1, limit + 1):
        store.claim_next(['x'], 'w', now='2099-01-01 00:00:00.000000')
        outcome = store.fail_job(job['id'], 'w', 'failed')
        assert outcome['dead_letter'] == (attempt == limit)
    assert store.get_job(job['id'])['status'] == 'dead-letter'


def test_worker_cannot_steal_identity(server, store):
    owner = RawWorker(server.host, server.port, capabilities=['x'])
    other = RawWorker(server.host, server.port, capabilities=['x'])
    try:
        job = store.create_job('x', {})
        assert owner.fetch() == job['id']
        protocol.send(other.sock, protocol.ack(owner.worker_id, job['id'], 'stolen'))
        assert protocol.recv_message(other.sock)['type'] == 'error'
        assert store.get_job(job['id'])['status'] == 'running'
        owner.ack(job['id'], 'real')
        assert protocol.recv_message(owner.sock)['applied'] is True
    finally:
        owner.close()
        other.close()


def test_repeated_fetch_preserves_owned_job(server, store):
    raw = RawWorker(server.host, server.port, capabilities=['x'])
    try:
        first = store.create_job('x', {})
        second = store.create_job('x', {})
        assert raw.fetch() == first['id']
        protocol.send(raw.sock, protocol.fetch(raw.worker_id))
        assert protocol.recv_message(raw.sock)['type'] == 'error'
        assert store.get_job(second['id'])['status'] == 'queued'
    finally:
        raw.close()
    assert wait_for(lambda: store.get_job(first['id'])['status'] == 'queued')


def test_duplicate_register_does_not_orphan_job(server, store):
    raw = RawWorker(server.host, server.port, capabilities=['x'])
    job = store.create_job('x', {})
    try:
        raw.fetch()
        protocol.send(raw.sock, protocol.register('again', 1, ['x']))
        assert protocol.recv_message(raw.sock)['type'] == 'error'
        assert wait_for(lambda: store.get_job(job['id'])['status'] == 'queued')
    finally:
        raw.close()


@pytest.mark.parametrize('message', [[], None, 1, {'type': []}])
def test_nonobject_protocol_frames_rejected(message):
    a, b = socket.socketpair()
    try:
        a.sendall(protocol.pack_message(message))
        with pytest.raises(protocol.ProtocolError):
            protocol.recv_message(b)
    finally:
        a.close()
        b.close()


def test_outbox_deduplicates_concurrent_delivery(tmp_path, monkeypatch):
    monkeypatch.setenv('DJQ_OUTPUT_DIR', str(tmp_path))
    ctx = {'job_id': 'same-job', 'worker_id': 'w', 'attempt': 1}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: send_email({'delay_secs': 0}, ctx), range(16)))
    assert sum(not r['duplicate'] for r in results) == 1
    assert all(r['message_id'] == 'same-job' for r in results)


@pytest.mark.parametrize('report_id', ['../escape', '/tmp/escape', 'a/b', 'a\\b'])
def test_reports_cannot_escape_output_directory(report_id):
    with pytest.raises(ValueError, match='report_id'):
        generate_report({'report_id': report_id}, {'job_id': 'x', 'attempt': 1})


def test_empty_capabilities_do_not_claim_arbitrary_work(store):
    store.create_job('x', {})
    assert store.claim_next([], 'w') is None
