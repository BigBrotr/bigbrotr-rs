"""Independent ordinary Control client after the initial publisher has exited."""
import hashlib
import http.client
import json
from pathlib import Path
import subprocess
import sys
import time
from installed_archive_profile import load as load_profile, password_file

READINESS_MODE = 'ordinary-readiness/1'
READINESS_PHASES = ('setup', 'positive', 'missing', 'narrow', 'restored')
PHASE_SECONDS = 30
JOURNEY_SECONDS = 40
CLIENT_SECONDS = JOURNEY_SECONDS + len(READINESS_PHASES) * PHASE_SECONDS


def phase(home, name, action=None):
    """One closed added phase, including its action, with an absolute deadline."""
    assert name in READINESS_PHASES
    started = time.monotonic_ns()
    deadline = started + PHASE_SECONDS * 1_000_000_000
    payload = action() if action else None
    marker = home / ('ordinary-' + name + '.json')
    assert not marker.exists()
    temporary = marker.with_suffix('.tmp')
    temporary.write_text(json.dumps(dict(phase=name, started_ns=started,
        deadline_ns=deadline, payload=payload)) + '\n')
    temporary.rename(marker)
    release = home / ('ordinary-' + name + '.continue')
    while not release.exists():
        assert time.monotonic_ns() < deadline, 'ordinary readiness phase timed out: ' + name
        time.sleep(.05)
    finished = time.monotonic_ns()
    assert finished < deadline, 'ordinary readiness phase exceeded deadline: ' + name
    return dict(phase=name, started_ns=started, finished_ns=finished, limit_seconds=PHASE_SECONDS)


def run(home):
    started = time.monotonic_ns()
    profile = load_profile(home)
    binary = profile['product_binary'] if profile else '/build/bigbrotr'
    principal = profile['expected_principal_id'] if profile else '10'*16
    actions = profile['expected_actions'] if profile else ['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read']
    password_ref = profile['password_ref'] if profile else 'file:' + str(home / 'password.private')
    expected = json.loads((home / 'export-input.json').read_text())
    mode = expected.get('readiness_mode')
    assert mode in (None, READINESS_MODE), 'unknown ordinary fixture mode'
    phases = []
    event = json.loads((home / 'event.jsonl').read_text())
    origin = 'http://127.0.0.1:18081'
    observations = []

    def record(case, **fields):
        observations.append(dict(case=case, **fields))
        (home / 'export-observations.json').write_text(json.dumps(observations, indent=2) + '\n')

    def active_ns():
        return time.monotonic_ns() - started - sum(p['finished_ns'] - p['started_ns'] for p in phases)

    def barrier(name, action=None):
        assert active_ns() <= JOURNEY_SECONDS * 1_000_000_000, 'original ordinary journey exceeded40s'
        phases.append(phase(home, name, action))

    def request(method, path, body=None, headers=None):
        connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=6)
        try:
            connection.request(method, path, None if body is None else json.dumps(body),
                {'Origin': origin, 'Content-Type': 'application/json', **(headers or {})})
            response = connection.getresponse()
            maximum = 131072 if path == '/management/v1/roles' else 65536
            raw = response.read(maximum + 1)
            assert len(raw) <= maximum
            return response.status, json.loads(raw)
        finally:
            connection.close()

    def cli(*args):
        result = subprocess.run([binary, 'access', *args], env={}, capture_output=True, timeout=5)
        assert result.returncode == 0 and not result.stderr, 'private CLI command refused'
        return json.loads(result.stdout)

    session = home / 'export-session.private'
    cli('login', '--origin', origin, '--username', 'alice',
        '--password-ref', password_ref, '--session-output', str(session))
    saved = json.loads(session.read_text())
    auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
    status, me = request('GET', '/management/v1/me', headers=auth)
    assert status == 200 and me['principal_id'] == principal
    assert me['stored_actions'] == actions
    if mode:
        barrier('setup')
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
        command = dict(principal_id=principal, action=action, enabled=True, scope=expected['scope'],
            expected_config_revision='1', expected_policy_revision='1',
            expected_grant_revision=str(revision), allocation=allocation)
        key = format(0xc0 + revision, '02x')*16
        headers = {**auth, 'Idempotency-Key': key}
        password = password_file(password_ref) if profile else 'Independent owner password 2026'
        status, proof = request('POST', '/management/v1/sessions/current/reauthenticate',
            dict(password=password, command=command), headers)
        record('reauthenticate-' + action, status=status)
        assert status == 201, (status, proof)
        status, receipt = request('PUT', '/management/v1/principals/' + principal + '/grants',
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
    if mode:
        barrier('positive')
        barrier('missing')
        def narrow():
            status, repeated = request('POST', '/v1/resources/event_history', query, auth)
            record('narrow-history-after-full-failure', status=status, response=repeated)
            assert status == 200 and repeated['items'] == [event] and repeated['next_cursor'] is None
            assert int(repeated['view_stamp']['visibility_as_of_us']) >= int(history['view_stamp']['visibility_as_of_us'])
            comparable = dict(repeated, view_stamp=dict(repeated['view_stamp'],
                visibility_as_of_us=history['view_stamp']['visibility_as_of_us']))
            assert comparable == history, 'original narrow history changed'
            # Observe immediately: a later full-probe failure must not hide a
            # transient erroneous Ready promotion caused by this narrow read.
            status, roles = request('GET', '/management/v1/roles', headers=auth)
            record('roles-immediately-after-narrow-history', status=status, response=roles)
            assert status == 200
            return roles
        barrier('narrow', narrow)
        barrier('restored')
    record('logout', response=cli('logout', '--session-ref', 'file:' + str(session)))
    assert request('GET', '/management/v1/me', headers=auth)[0] == 401
    if mode:
        active = active_ns()
        assert active <= JOURNEY_SECONDS * 1_000_000_000, 'original ordinary journey exceeded40s'
        (home / 'ordinary-timing.json').write_text(json.dumps(dict(mode=mode,
            original_limit_seconds=JOURNEY_SECONDS, export_limit_seconds=12,
            overall_limit_seconds=CLIENT_SECONDS, original_active_ns=active, phases=phases), indent=2) + '\n')
    return dict(status='PASS', job_id=receipt['job_id'], history=history, completed=observed)


if __name__ == '__main__':
    home = Path(sys.argv[1])
    result = run(home)
    (home / 'export-client-result.json').write_text(json.dumps(result, indent=2) + '\n')
