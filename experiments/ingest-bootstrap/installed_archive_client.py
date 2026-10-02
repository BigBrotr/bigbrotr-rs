#!/usr/bin/env python3
"""Independent HTTP/CLI and real signed peer, retained through Archive publication."""
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from installed_sync_client import publish, relay, relay_listener


def run(directory):
    origin = 'http://127.0.0.1:18081'
    def request(method, path, body=None, headers=None):
        connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=3)
        try:
            connection.request(method, path, None if body is None else json.dumps(body),
                {'Origin': origin, 'Content-Type': 'application/json', **(headers or {})})
            response = connection.getresponse()
            raw = response.read(65537)
            assert len(raw) <= 65536
            return response.status, json.loads(raw)
        finally:
            connection.close()
    def cli(*args):
        result = subprocess.run(['/build/bigbrotr', 'access', *args], env={}, capture_output=True, timeout=5)
        assert result.returncode == 0 and not result.stderr, 'private CLI command refused'
        return json.loads(result.stdout)
    password = directory / 'password.private'
    with os.fdopen(os.open(password, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write('Independent owner password 2026')
    session = directory / 'session.private'
    logged_in = cli('login', '--origin', origin, '--username', 'alice',
        '--password-ref', 'file:' + str(password), '--session-output', str(session))
    saved = json.loads(session.read_text())
    assert saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == origin
    assert session.stat().st_mode & 0o777 == 0o600
    auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
    status, me = request('GET', '/management/v1/me', headers=auth)
    assert status == 200 and me['principal_id'] == '10'*16
    actions = ['policy.change', 'roles.control', 'status.read']
    if directory.name == 'archive-export':
        actions = ['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read']
    assert me['stored_actions'] == actions
    observations, statuses, errors = [], [], []
    def call(path, value, key):
        status, answer = request('POST', '/management/v1/' + path, value, {**auth, 'Idempotency-Key': key})
        observations.append(dict(path=path, input=value, key=key, status=status, body=answer))
        assert status == 202 and answer['created'] and answer['command_key'] == key
        return answer
    def barrier(name, seconds=15, poll=None):
        publish(directory, name + '.json', observations)
        deadline, next_poll = time.monotonic() + seconds, 0
        while not (directory / (name + '.continue')).exists():
            assert not errors and time.monotonic() < deadline, name
            if poll is not None and time.monotonic() >= next_poll:
                statuses.append(poll())
                next_poll = time.monotonic() + .5
            time.sleep(.01)
    visibility = json.loads((directory / 'visibility.json').read_text())
    assert visibility['ordinal'] == '3'
    revisions = dict(expected_config_revision='1', expected_policy_revision='1')
    endpoint = call('relay-endpoints', dict(url='WS://127.0.0.1:7447/a/../installed-fixture', **revisions), '91'*16)
    enrollment = call('relay-enrollments', dict(endpoint_id=endpoint['endpoint_id'], visibility=visibility,
        route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True, expected_generation=None, **revisions), '92'*16)
    assert enrollment['generation'] == '1' and enrollment['enabled'] is True
    scope = call('acquisition-scopes', dict(endpoint_id=endpoint['endpoint_id'], access_scope_id=enrollment['access_scope_id'],
        visibility=visibility, selector=dict(version=1, filters=[{'kinds': [1]}], history_since=0, ephemeral='retain'), **revisions), 'a1'*16)
    listener = relay_listener()
    finished = threading.Event()
    worker = threading.Thread(target=relay, args=(directory, listener, finished, errors, True))
    worker.start()
    try:
        barrier('prepared')
        receipt = call('jobs', dict(kind='acquisition', scope_id=scope['scope_id'], deadline_us='120000000', **revisions), 'b1'*16)
        assert receipt['execution'] == 'unobserved' and receipt['authority'] == 'acceptance-only'
        def status():
            code, observed = request('GET', '/management/v1/jobs/' + receipt['job_id'], headers=auth)
            assert code == 200
            assert observed['state'] in ('accepted', 'queued')
            assert observed == dict(format='bigbrotr-job-status/1', job_id=receipt['job_id'], kind='acquisition',
                state=observed['state'], job_revision='1' if observed['state'] == 'accepted' else '2',
                checkpoint_revision='0', created_at_us=receipt['accepted_at_us'], completed_at_us=None,
                observed_at_us=observed['observed_at_us'], reason_code=None, authority='observation-only')
            return observed
        barrier('submitted', seconds=65, poll=status)
        final_status = status()
        assert final_status['state'] == 'queued'
        statuses.append(final_status)
        publish(directory, 'status-observations.json', statuses)
        logged_out = cli('logout', '--session-ref', 'file:' + str(session))
        assert json.loads(session.read_text()) == saved
        assert request('GET', '/management/v1/me', headers=auth)[0] == 401
        barrier('commands-finished')
    finally:
        finished.set()
        worker.join(4)
        assert not worker.is_alive(), 'independent relay did not join'
    assert not errors
    return dict(login=logged_in, logout=logged_out, observations=observations, job_status=final_status)


if __name__ == '__main__':
    sys.dont_write_bytecode = True
    directory = Path(sys.argv[1])
    assert directory.is_absolute()
    terminal = dict(exit_code=1)
    try:
        result = run(directory)
        publish(directory, 'client-result.json', dict(status='PASS', **result))
        terminal['exit_code'] = 0
    finally:
        publish(directory, 'client-exit.json', terminal)
