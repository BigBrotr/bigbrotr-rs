#!/usr/bin/env python3
"""Real HTTP acceptance/logout, then separately killed/restarted installed Control."""
from http.cookies import SimpleCookie
import hashlib
from http.client import HTTPConnection
import json
import signal
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from signed_publication_oracle import EVENT


def record(name, value):
    (http.ROOT / ('executor-' + name + '.json')).write_text(json.dumps(value, indent=2) + '\n')


def wait(name, server):
    end = time.monotonic() + 25
    while not (http.ROOT / ('executor-' + name)).exists():
        assert server.process.poll() is None, server.error.read_text()
        if time.monotonic() >= end:
            raise TimeoutError(name)
        time.sleep(.01)


def accept(revision=3, keys=('d3',), label='signed-accept'):
    server = http.Server('executor-' + label)
    try:
        ready = server.ready(revision)
        assert len(ready['listeners']) == 1
        status, _, headers = http.request('POST', '/management/v1/sessions',
            dict(username='alice', password='Independent owner password 2026'))
        assert status == 201
        cookies = SimpleCookie()
        for name, value in headers:
            if name.lower() == 'set-cookie':
                cookies.load(value)
        auth = {'Cookie': 'BigBrotrLocalSession=' + cookies['BigBrotrLocalSession'].value,
                'X-CSRF-Token': cookies['BigBrotrLocalCsrf'].value}
        body = dict(kind='export', format='signed-events-jsonl',
            query=dict(version=1, resource='event_history', mode='snapshot',
                scope=dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1'),
                filters=[dict(ids=[EVENT['id']])]),
            allocation=dict(catalog_id='5a'*16, frontier_revision='1'),
            destination=dict(id='e1'*16, binding_sha256=hashlib.sha256(
                Path('/out/signed_normal/export-destination/destination.json').read_bytes()).hexdigest()),
            expected_config_revision='1', expected_policy_revision=str(revision))
        receipts = []
        for prefix in keys:
            key = prefix * 16
            status, receipt, _ = http.request('POST', '/management/v1/jobs', body,
                {**auth, 'Idempotency-Key': key})
            assert status == 202 and receipt['created'] and receipt['execution'] == 'unobserved', (status, receipt)
            assert http.request('POST', '/management/v1/jobs', body,
                {**auth, 'Idempotency-Key': key})[:2] == (200, {**receipt, 'created': False})
            receipts.append(receipt)
        # Logout is a204 with no JSON body; use the actual HTTP protocol directly.
        connection = HTTPConnection('127.0.0.1', 18080, timeout=8)
        connection.request('DELETE', '/management/v1/sessions/current', headers={
            **auth, 'Origin': 'http://' + http.ADDRESS})
        response = connection.getresponse()
        assert response.status == 204 and response.read() == b''
        connection.close()
        assert http.request('GET', '/management/v1/me', auth=auth)[0] == 401
        server.stop()
        result = dict(status='PASS', command=body, logged_out=True)
        result.update({'receipt': receipts[0]} if len(receipts) == 1 else {'receipts': receipts})
        record(label, result)
    finally:
        server.cleanup()


def runtime(crash):
    name = 'crash' if crash else 'restart'
    server = http.Server('executor-' + name)
    try:
        ready = server.ready(4)
        assert ready['listeners'] == []
        record(name + '-ready', ready)
        if crash:
            wait('crash-freeze', server)
            server.process.send_signal(signal.SIGSTOP)
            deadline = time.monotonic() + 2
            while True:
                state = next(line for line in Path(f'/proc/{server.process.pid}/status').read_text().splitlines() if line.startswith('State:'))
                if state.split()[1] == 'T':
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('parent SIGSTOP observation')
                time.sleep(.005)
            record('crash-frozen', dict(pid=server.process.pid, state=state, observed_ns=time.monotonic_ns()))
        wait(name + ('-now' if crash else '-stop'), server)
        if crash:
            started = time.monotonic()
            server.process.kill()
            assert server.process.wait(timeout=10) == -9
            record(name + '-killed', dict(status='PASS', exit=-9,
                elapsed_ms=round((time.monotonic()-started)*1000, 3)))
        else:
            server.stop()
            record(name + '-stopped', dict(status='PASS', exit=0))
    finally:
        server.cleanup()


if __name__ == '__main__':
    {'accept': accept, 'accept-commits': lambda: accept(5, ('d5', 'd6'), 'commit-accept'), 'crash': lambda: runtime(True), 'restart': lambda: runtime(False)}[sys.argv[1]]()
