"""Actual registration adapter COMMIT schedules; the common host owns lab resources."""
import hashlib
import json
from pathlib import Path
import time
import lab
from fault_cases import Process

CASES = {
    'register-pass': ('pass', 'accept', [4, 4, 4, 4, 0], True),
    'register-before': ('before-forward', 'unknown', [4, 3, 3, 3, 1], False),
    'register-after': ('after-complete', 'unknown', [4, 4, 4, 3, 1], True),
    'register-cancel-close': ('hold', 'cancel', [4, 3, 3, 3, 1], False),
    'register-cancel-release': ('hold', 'cancel', [4, 4, 4, 4, 1], True),
    'register-owner-expiry': ('hold', 'owner-expiry', [4, 4, 3, 3, 1], False),
    'register-session-expiry': ('hold', 'session-expiry', [4, 4, 3, 3, 1], False),
}
KEY = '\\x' + '31' * 16
ACTOR = '\\x' + '10' * 16
URL = b'ws://127.0.0.1:7447/installed-fixture'
DOMAIN_TABLES = {'management_commands', 'management_audit', 'relay_registration_commands'}


def event(process, wanted, seconds=10):
    deadline = time.monotonic() + seconds
    with process.condition:
        while True:
            assert not process.overflow, 'probe output overflow'
            end = process.stdout.find(b'\n', process.offset)
            if end >= 0:
                value = json.loads(bytes(process.stdout[process.offset:end]))
                process.offset = end + 1
                assert value.get('event') == wanted, value
                return value
            assert time.monotonic() < deadline and (
                process.process.poll() is None or process.threads[0].is_alive()), wanted
            process.condition.wait(.02)


def observe(pg, database, ready):
    value = json.loads(lab.sql(pg, database, f"""
        SELECT json_build_object('pid',a.pid,'start',a.backend_start::text,'epoch',o.epoch,
            'owner_expired',o.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            'session_expired',LEAST(s.expires_at_us,s.idle_expires_at_us)<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            'session',encode(s.session_id,'hex'),'session_xmin',s.xmin::text,
            'fenced',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid AND locktype='advisory'
                AND classid=1111651442::oid AND objid=1129599564::oid AND objsubid=2
                AND mode='ExclusiveLock' AND granted))
        FROM pg_stat_activity a JOIN control.owner_epochs o
            ON o.control_backend_pid=a.pid AND o.control_backend_start=a.backend_start
        CROSS JOIN control.browser_sessions s WHERE a.pid={ready['pid']} AND a.datname='{database}'
            AND o.state='active' AND s.principal_id=decode(repeat('10',16),'hex')
            AND s.expires_at_us={ready['session_expires']} AND s.revoked_at_us IS NULL
    """))
    assert value and value['pid'] == ready['pid'] and value['fenced']
    return value


def receipt(state, ready):
    commands = [r for r in state['management_commands'] if r['actor_id'] == ACTOR and r['request_key'] == KEY]
    if not commands:
        assert not any(r['request_key'] == KEY for r in state['relay_registration_commands'])
        return None
    assert len(commands) == 1
    command = commands[0]
    detail, = [r for r in state['relay_registration_commands'] if r['actor_id'] == ACTOR and r['request_key'] == KEY]
    endpoint, = [r for r in state['relay_endpoints'] if r['endpoint_id'] == detail['endpoint_id']]
    audit, = [r for r in state['management_audit'] if r['audit_id'] == command['audit_id']]
    owner, = [r for r in state['owner_epochs'] if r['resource_id'] == detail['owner_resource_id'] and r['epoch'] == detail['owner_epoch']]
    account, = [r for r in state['local_accounts'] if r['principal_id'] == ACTOR]
    session, = [r for r in state['browser_sessions'] if r['session_id'] == detail['session_id']]
    dataset = state['datasets'][0]['dataset_id']
    digest = hashlib.sha256(b'bigbrotr/relay-registration/1\0' + bytes.fromhex(dataset[2:] + '10'*16 + '31'*16)
        + (1).to_bytes(8, 'big') * 2 + (1).to_bytes(2, 'big') + len(URL).to_bytes(4, 'big') + URL).hexdigest()
    assert command['request_digest'] == '\\x' + digest and command['action'] == 'relay.endpoint.register'
    assert command['expected_config_revision'] == command['policy_revision'] == 1
    assert endpoint['url_bytes'] == '\\x' + URL.hex() and endpoint['normalization_version'] == 1
    assert command['dataset_id'] == detail['dataset_id'] == endpoint['dataset_id'] == dataset
    assert command['xmin'] == detail['xmin'] == audit['xmin']
    assert audit['actor_id'] == ACTOR and audit['subject_id'] == KEY and audit['dataset_id'] == dataset
    assert audit['action'] == command['action'] and audit['outcome'] == 'accepted'
    assert audit['created_at_us'] == command['accepted_at_us'] and audit['config_revision'] == audit['policy_revision'] == 1
    assert owner['epoch'] == ready['epoch'] and owner['control_backend_pid'] == ready['pid']
    assert owner['process_incarnation'] == detail['owner_incarnation'] and detail['owner_role'] == 'control'
    assert session['principal_id'] == ACTOR and session['expires_at_us'] == ready['session_expires']
    for revision in ('credential_revision', 'authentication_revision', 'grant_revision'):
        assert detail['actor_' + revision] == account[revision]
    return dict(command_key=KEY[2:], endpoint_id=endpoint['endpoint_id'][2:], accepted_at_us=command['accepted_at_us'])


def effects(before, after, committed):
    assert before.keys() == after.keys()
    changed = {t for t in before if before[t] != after[t]}
    assert changed == (DOMAIN_TABLES if committed else set()), changed
    for table in changed:
        assert all(row in after[table] for row in before[table])
        assert len(after[table]) == len(before[table]) + 1


def run(pg, client, output, database, name, rows):
    proxy_mode, mode, expected_wire, committed = CASES[name]
    directory = output / name
    directory.mkdir(mode=0o700)
    original = rows(database)
    assert len(original) == 68 and [r['epoch'] for r in original['owner_epochs'] if r['role'] == 'control'] == [1]
    endpoint, = [r for r in original['relay_endpoints'] if r['url_bytes'] == '\\x' + URL.hex()]
    assert endpoint['owner_epoch'] == 1 and receipt(original, {}) is None
    documents = [original[t][0]['document'] for t in ('config_revisions', 'policy_revisions')]
    assert documents[0] != documents[1]
    for table, document in zip(('config_revisions', 'policy_revisions'), documents):
        encoded = json.dumps(document, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        assert document['format'] == 'bigbrotr-product-policy/2'
        assert original[table][0]['document_digest'] == '\\x' + hashlib.sha256(b'bigbrotr/product-policy/2\0' + encoded).hexdigest()
    lab.write_json(directory / 'policies.json', documents)
    processes = []
    result = dict(name=name, status='RUNNING', preexisting_endpoint=endpoint, committed_expected=committed)

    def capture(label):
        value = rows(database)
        lab.write_json(directory / (label + '.json'), value)
        return value

    def app(operation, nonce, port=5432):
        token = f'{name}/{nonce}'
        process = Process(['docker', 'exec', client, '/build/relay_registration_fault_probe',
            database, '/run/secrets/application', '/lab/' + name + '/policies.json',
            operation, str(port), str(nonce), '/lab/' + token])
        processes.append((str(nonce), process))
        return process, token

    def go(token, suffix='go'):
        (output / (token + '.' + suffix)).touch(exist_ok=False)

    def finish(process):
        answer = event(process, 'done')
        assert process.wait(10) == 0 and not process.stderr and answer['driver_joined']
        return answer['outcome']

    try:
        proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
            '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
            '--mode', proxy_mode, '--commit-number', '4', '--deadline-seconds', '40'], input_pipe=True)
        processes.append(('proxy', proxy))
        proxy_ready = proxy.event('ready', 10)
        assert proxy_ready['proxy_sha256'] == hashlib.sha256(
            (Path(__file__).resolve().parents[1] / 'control-migrations/fault_proxy.py').read_bytes()).hexdigest()
        process, token = app(mode, 11, proxy_ready['listen_port'])
        ready = event(process, 'ready')
        assert ready['epoch'] == 2 and ready['config'] == ready['policy'] == 1
        before = capture('before')
        observations = [observe(pg, database, ready)]
        assert not observations[0]['owner_expired'] and not observations[0]['session_expired']
        go(token)
        if proxy_mode == 'hold':
            proxy.event('held', 10)
            assert rows(database) == before, 'provisional writes visible'
            if mode == 'cancel':
                assert event(process, 'cancelled', 5)['commit_started']
            else:
                field = 'owner_expired' if mode == 'owner-expiry' else 'session_expired'
                deadline = time.monotonic() + 5
                while True:
                    observations.append(observe(pg, database, ready))
                    if observations[-1][field]:
                        break
                    assert time.monotonic() < deadline, 'expiry not observed'
                    time.sleep(.05)
                other = 'session_expired' if field == 'owner_expired' else 'owner_expired'
                assert not observations[-1][other], 'expiry cause not isolated'
            contender, _ = app('busy', 12)
            assert event(contender, 'busy')['commit_started'] is False
            assert contender.wait(10) == 0 and not contender.stderr and rows(database) == before
            proxy.send('close' if name == 'register-cancel-close' else 'release')
            if mode == 'cancel':
                go(token, 'finish')
        outcome = finish(process)
        wire = proxy.event('receipt', 10)
        assert proxy.wait(5) == 0 and not proxy.stderr
        assert [wire[k] for k in ('commits_seen', 'commits_forwarded', 'server_commit_complete',
            'client_commit_complete', 'faults')] == expected_wire
        after = capture('after')
        effects(before, after, committed)
        accepted = receipt(after, ready)
        assert (accepted is not None) == committed
        if mode == 'accept':
            assert outcome == dict(receipt=dict(accepted, created=True), commit_started=True)
        recovery, recovery_token = app('accept', 13)
        recovered_owner = event(recovery, 'ready')
        assert recovered_owner['epoch'] == 3
        recovery_before = capture('recovery-before')
        go(recovery_token)
        recovered = finish(recovery)
        recovery_after = capture('recovery-after')
        effects(recovery_before, recovery_after, not committed)
        durable = receipt(recovery_after, ready if committed else recovered_owner)
        assert durable is not None and (not committed or durable == accepted)
        assert recovered == dict(receipt=dict(durable, created=not committed), commit_started=not committed)
        assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        result.update(status='PASS', ready=ready, observations=observations, outcome=outcome, wire=wire,
            recovery_owner=recovered_owner, recovered=recovered, durable=durable, complete_control_tables=68)
    except BaseException as error:
        result.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
        raise
    finally:
        cleanup_errors = []
        for label, process in processes:
            try:
                process.stop()
            except BaseException as error:
                cleanup_errors.append(label + '-' + type(error).__name__)
            (directory / (label + '.stdout')).write_bytes(process.stdout)
            (directory / (label + '.stderr')).write_bytes(process.stderr)
        result['cleanup_errors'] = cleanup_errors
        if cleanup_errors:
            result['status'] = 'INCOMPLETE_CLEANUP'
        lab.write_json(directory / 'result.json', result)
    assert result['status'] == 'PASS'
    return result
