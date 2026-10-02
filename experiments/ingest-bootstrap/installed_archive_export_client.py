"""Independent ordinary Control client after the initial publisher has exited."""
import hashlib
import http.client
import json
from pathlib import Path
import subprocess
import sys
import time


def run(home):
    expected = json.loads((home / 'export-input.json').read_text())
    event = json.loads((home / 'event.jsonl').read_text())
    origin = 'http://127.0.0.1:18081'
    observations = []

    def record(case, **fields):
        observations.append(dict(case=case, **fields))
        (home / 'export-observations.json').write_text(json.dumps(observations, indent=2) + '\n')

    def request(method, path, body=None, headers=None):
        connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=6)
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

    session = home / 'export-session.private'
    cli('login', '--origin', origin, '--username', 'alice',
        '--password-ref', 'file:' + str(home / 'password.private'), '--session-output', str(session))
    saved = json.loads(session.read_text())
    auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
    status, me = request('GET', '/management/v1/me', headers=auth)
    assert status == 200 and me['principal_id'] == '10'*16
    assert me['stored_actions'] == ['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read']
    query = dict(version=1, resource='event_history', mode='snapshot', scope=expected['scope'],
        filters=[dict(ids=[event['id']])])
    allocation = dict(catalog_id=expected['catalog_id'], frontier_revision='4')
    marker = (home / 'destination/destination.json').read_bytes()
    body = dict(kind='export', format='signed-events-jsonl', query=query, allocation=allocation,
        destination=dict(id=expected['destination_id'], binding_sha256=hashlib.sha256(marker).hexdigest()),
        expected_config_revision='1', expected_policy_revision='1')
    keyed = {**auth, 'Idempotency-Key': 'd1'*16}
    assert request('POST', '/management/v1/jobs', body, keyed)[0] == 403
    assert request('POST', '/v1/resources/event_history', query, auth)[0] == 403
    record('no-implicit-scope-authority')
    for revision, action in enumerate(('history.read', 'export', 'private-export'), 1):
        command = dict(principal_id='10'*16, action=action, enabled=True, scope=expected['scope'],
            expected_config_revision='1', expected_policy_revision='1',
            expected_grant_revision=str(revision), allocation=allocation)
        key = format(0xc0 + revision, '02x')*16
        headers = {**auth, 'Idempotency-Key': key}
        status, proof = request('POST', '/management/v1/sessions/current/reauthenticate',
            dict(password='Independent owner password 2026', command=command), headers)
        record('reauthenticate-' + action, status=status)
        assert status == 201, (status, proof)
        status, receipt = request('PUT', '/management/v1/principals/' + '10'*16 + '/grants',
            dict(command=command, proof_id=proof['proof_id']), headers)
        record('enable-' + action, command=command, key=key, status=status, receipt=receipt)
        assert status == 200 and receipt['created'], (status, receipt)
        assert receipt['scope_generation'] == '1' and receipt['account_grant_revision'] == str(revision+1)
        if revision < 3:
            assert request('POST', '/management/v1/jobs', body, keyed)[0] == 403
    status, history = request('POST', '/v1/resources/event_history', query, auth)
    record('signed-history', status=status, response=history)
    assert status == 200 and history['items'] == [event] and history['next_cursor'] is None
    assert history['view_stamp']['input_generations'] == [dict(kind='archive', **allocation, snapshot_id='5')]
    status, receipt = request('POST', '/management/v1/jobs', body, keyed)
    record('accept-export', status=status, command=body, key='d1'*16, receipt=receipt)
    assert status == 202 and receipt['created'] and receipt['execution'] == 'unobserved'
    assert request('POST', '/management/v1/jobs', body, keyed) == (200, dict(receipt, created=False))
    deadline = time.monotonic() + 12
    while True:
        status, observed = request('GET', '/management/v1/jobs/' + receipt['job_id'], headers=auth)
        record('export-status', status=status, response=observed)
        assert status == 200 and observed['kind'] == 'export'
        if observed['state'] == 'succeeded':
            break
        assert observed['state'] in ('accepted', 'queued', 'running'), observed
        assert time.monotonic() < deadline, 'actual signed export did not complete'
        time.sleep(.25)
    assert observed['checkpoint_revision'] == '1'
    assert request('POST', '/management/v1/jobs', body, keyed) == (200, dict(receipt, created=False))
    record('logout', response=cli('logout', '--session-ref', 'file:' + str(session)))
    assert request('GET', '/management/v1/me', headers=auth)[0] == 401
    return dict(status='PASS', job_id=receipt['job_id'], history=history, completed=observed)


if __name__ == '__main__':
    home = Path(sys.argv[1])
    result = run(home)
    (home / 'export-client-result.json').write_text(json.dumps(result, indent=2) + '\n')
