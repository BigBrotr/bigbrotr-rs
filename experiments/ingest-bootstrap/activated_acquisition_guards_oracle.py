"""Literal SQL guard expectations over the frozen, genuinely closed off source.

The temporary Control is a new real backend, not a revived historical actor.
Expected gap/job rows are constructed independently; failed attempts must retain
all seventy-two durable tables and every original native/input byte.
"""
import copy
import hashlib
import json
import math
import re

import activated_acquisition_oracle as acquisition
from initial_succession_oracle import only, same
from installed_empty_oracle import fresh_xmin, window
from installed_live_oracle import instant

CASES = ('complete-rollback', 'wrong-producer', 'wrong-source', 'wrong-reason',
    'registrar-not-activator', 'wrong-cut', 'wrong-digest', 'gap-only-commit',
    'requested-only-commit', 'immediate-before-insert', 'terminal-job-update',
    'terminal-gap-update', 'terminal-gap-delete', 'terminal-gap-truncate')
MODE = 'activated-acquisition-guards'
GRANT = 'GRANT UPDATE,DELETE,TRUNCATE ON control.acquisition_gaps TO bigbrotr_control;\n'
REVOKE = 'REVOKE UPDATE,DELETE,TRUNCATE ON control.acquisition_gaps FROM bigbrotr_control;\n'
# Full scripts include the independently reviewed fresh-Control helper and the
# frozen source/job fields. Database names are separate psql arguments.
SQL_SHA256 = {
    'complete-rollback': '6777f7ea5a081056ea18f9096a34e6b09bf098398c7a9789f0e5e483c5ca8d06',
    'wrong-producer': '0385795327dc63822c3ceea36d68ed2d65f7480b6509b128d7d2fa9f896cb127',
    'wrong-source': '49e030df1e33161d6d6c50d29338f440cd1b01152e207217774615f1cb93da5a',
    'wrong-reason': '2b8a1ed721a50a335729633cef515933ce48b28163a3af8a1ec099183416ae43',
    'registrar-not-activator': '4f6fdc23fa1d29743d4800090dc39cd1373e73774684738cfaffc932e3579923',
    'wrong-cut': 'fcf94a44b3ac4a055870a2f85e0412653dde431e34d957aa6d66fa1d78d4ca67',
    'wrong-digest': 'c29fa3e1bb50b0d86e4c6aa172bfa1c65c74114fdc0bf4e2f94c07c3ddb1cc50',
    'gap-only-commit': '6c7b9e64b8fb2e005a8e1a97d44d8cf1f1291d699e4c8458bd7e2e06b34f6475',
    'requested-only-commit': 'd3adbfbdbf451b2c2ca2a02dc7ad2e8e4acf4f625895cd07c0710c2c1a53035b',
    'immediate-before-insert': '46260e904a926e467317f87d43342b9d252e315be3164a00c6620f4cfdd1e1b5',
    'terminal-job-update': 'e4c394da5e8f6c9fc8856b8d9ba5b328e5c5ca055ae639cb3c9790d8108b028b',
    'terminal-gap-update': '7a2f3deb98d75f70ce5cc92224142f6a6410176b01b9e97bd7a3c75c9e0792c9',
    'terminal-gap-delete': '6110ae6d5c52a78e26679c62455653ed24234138386dcd274ee153a0d9747bb4',
    'terminal-gap-truncate': 'da82e4ce0c6b9c511e3ac147165fc14be13de093e832c4bfbb0a7a3c2e823de8',
}

ACTIVATOR = ('activated acquisition gap requires its activating Control', 'control.guard_acquisition_gap()')
SOURCE = ('acquisition gap requires its closed resolved initial source', 'control.guard_acquisition_gap()')
PARTIAL = ('whole-intent gap and terminal job must commit atomically', 'control.require_acquisition_gap_complete()')
# These unchanged 0001 functions include control in their own search_path,
# so PostgreSQL's error context renders their names without a schema prefix.
HISTORY = ('immutable control history cannot be changed', 'reject_history_mutation()')
ERRORS = dict(zip(CASES[1:], (SOURCE, ACTIVATOR, ACTIVATOR, ACTIVATOR, SOURCE, SOURCE,
    PARTIAL, PARTIAL, PARTIAL, ('invalid finite job transition', 'guard_job_update()'),
    HISTORY, HISTORY, HISTORY), strict=True))


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(mode, kind):
    return sha(('bigbrotr/empty-sql-fixture/1\0' + MODE + '-' + mode + '\0' + kind).encode())[:32]


def checked_shared(seed, setup, grants, baseline, native, files, clock):
    assert seed['name'] == 'off'
    known = acquisition.seed_checked(seed)
    same(baseline, acquisition.migrated(seed, setup, clock))
    assert native == seed['native'] and len(baseline) == 72
    assert grants['statement'] == GRANT
    answer = grants['response']
    assert answer == dict(returncode=0, stdout='', stderr='', outcome='PROCESS_EXITED')
    assert grants['before'] == setup['catalog_after']
    expected = copy.deepcopy(grants['before'])
    expected['acquisition']['privileges'].update(gap_update=True, gap_delete=True, gap_truncate=True)
    assert grants['after'] == expected
    assert files and all(isinstance(name, str) and not name.startswith('/') and '..' not in name.split('/') for name in files)
    assert len({r['job_id'] for r in baseline['acquisition_gaps']}) == len(baseline['acquisition_gaps'])
    assert all(r['state'] == 'released' for r in baseline['owner_epochs'] if r['role'] == 'control')
    return known


def parsed_events(stdout):
    def unique(pairs):
        value = dict(pairs)
        assert len(value) == len(pairs), 'duplicate SQL evidence field'
        return value
    return [json.loads(line, object_pairs_hook=unique) for line in stdout.splitlines() if line.strip()]


def absence_checked(value, backend, database):
    assert value['status'] == 'OBSERVED_ABSENT' and value['database'] == database
    assert value['role'] == 'bigbrotr_control' and value['exact_backend'] == backend
    assert value['retained_control_tuples_checked'] is True
    elapsed = 0
    for sample in value['samples']:
        assert type(sample['seconds']) in (int, float) and math.isfinite(sample['seconds'])
        assert elapsed <= sample['seconds'] <= 5
        elapsed = sample['seconds']
        identities = []
        for row in sample['present']:
            assert set(row) == {'pid', 'backend_start', 'database', 'user'}
            assert type(row['pid']) is int and row['pid'] > 0 and instant(row['backend_start']) > 0
            assert row['database'] == database and row['user'] == 'bigbrotr_control'
            identities.append((row['pid'], row['backend_start']))
        assert len(identities) == len(set(identities))
    assert value['samples'] and value['samples'][-1]['present'] == []


def verify_case(case, *, seed, setup, grants, baseline, native, files):
    mode = case['mode']
    assert mode in CASES
    clock = case['server_window']
    window(clock)
    known = checked_shared(seed, setup, grants, baseline, native, files, clock)
    assert case['capture_errors'] == []
    assert sha(case['statement'].encode()) == case['statement_sha256'] == SQL_SHA256[mode]
    same(case['before'], baseline)
    same(case['after'], baseline)
    assert case['catalog_before'] == case['catalog_after'] == grants['after']
    assert case['files_before'] == case['files_after'] == files
    assert case['inputs_before'] == case['inputs_after'] == seed['inputs']
    assert case['contents_before'] == case['contents_after'] == seed['contents']
    database = case['database']
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    command = case['command']
    assert len(command) == 19 and isinstance(command[4], str) and command[4]
    assert command == ['exec', '-i', '--env', 'PGCONNECT_TIMEOUT=5', command[4],
        'psql', '-X', '-A', '-t', '-q', '--no-password', '-v', 'ON_ERROR_STOP=1',
        '-v', 'VERBOSITY=verbose', '-U', 'bigbrotr_control', '-d', database]
    response = case['response']
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds']) and 0 <= response['seconds'] <= 35
    events = parsed_events(response['stdout'])
    assert events == case['events']
    backend = events[0]
    previous = max((r for r in baseline['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    at = backend['fixture_time']
    assert type(at) is int and clock['before_us'] <= at <= clock['after_us']
    assert type(backend['pid']) is int and backend['pid'] > 0
    assert clock['before_us'] <= instant(backend['backend_start']) <= at
    assert backend == dict(event='backend', pid=backend['pid'], backend_start=backend['backend_start'],
        database=database, user='bigbrotr_control', role='bigbrotr_control', superuser=False,
        isolation='read committed', control_epoch=previous['epoch']+1, incarnation=identity(mode, 'incarnation'),
        role_instance=identity(mode, 'role'), replication_role='origin', fixture_time=at)
    incarnation, role_id = '\\x' + backend['incarnation'], '\\x' + backend['role_instance']
    assert not any(r['process_incarnation'] == incarnation for r in baseline['owner_epochs'])
    assert not any(r['role_instance_id'] == role_id for r in baseline['role_instances'])
    stages = ['baseline']
    if mode in ('gap-only-commit', 'requested-only-commit', 'complete-rollback') or mode.startswith('terminal-'):
        stages.append('gap')
    if mode in ('requested-only-commit', 'complete-rollback') or mode.startswith('terminal-'):
        stages.append('requested')
    if mode == 'complete-rollback' or mode.startswith('terminal-'):
        stages += ['complete', 'checked']
    states = [e for e in events if e['event'] == 'state']
    assert [e['stage'] for e in states] == stages
    xid = states[0]['transaction_id']
    assert isinstance(xid, str) and xid.isdecimal() and 0 < int(xid) < 1 << 64
    xmin = str(int(xid) % (1 << 32))
    fresh_xmin(xmin, baseline)
    owner = dict(previous, epoch=previous['epoch']+1, role_instance_id=role_id, process_incarnation=incarnation,
        acquired_at_us=at, lease_expires_at_us=at+20_000_000, state='active', released_at_us=None,
        exclusion_method=None, exclusion_digest=None, archive_frontier_protocol=None, control_fence_version=1,
        control_backend_pid=backend['pid'], control_backend_start=backend['backend_start'], xmin=xmin)
    role = dict(role_instance_id=role_id, dataset_id=previous['dataset_id'], role='control',
        created_at_us=at, retired_at_us=None, xmin=xmin)
    job = copy.deepcopy(known['job'])
    activation = known['activation']
    gap = dict(job_id=job['job_id'], dataset_id=job['dataset_id'], producer_slot_id=activation['producer_slot_id'],
        producer_epoch=2, shard_id=0, source_owner_epoch=2, last_complete_sequence=5,
        source_closure_digest='\\x'+known['digest'], previous_job_revision=2,
        previous_owner_resource=activation['activated_by_resource'], previous_owner_epoch=8,
        previous_owner_incarnation=activation['activated_by_incarnation'], issuer_resource=owner['resource_id'],
        issuer_epoch=owner['epoch'], issuer_incarnation=incarnation, owner_role='control',
        abandoned_at_us=at, reason=acquisition.REASON, xmin=xmin)
    assert at >= max(known['source']['closed_at_us'], job['created_at_us'])
    observed_at = at
    for state in states:
        stamp = state['observed_us']
        assert type(stamp) is int and observed_at <= stamp <= clock['after_us']
        assert stamp < at + 20_000_000, 'raw schedule outlived actual Control lease'
        observed_at = stamp
        phase = state['stage']
        wanted_job = copy.deepcopy(job)
        if phase in ('requested', 'complete', 'checked'):
            wanted_job.update(state='cancel_requested', job_revision=3, reason_code=acquisition.REASON,
                owner_resource_id=owner['resource_id'], owner_epoch=owner['epoch'], owner_incarnation=incarnation, xmin=xmin)
        if phase in ('complete', 'checked'):
            wanted_job.update(state='cancelled', job_revision=4, completed_at_us=at)
        assert state == dict(event='state', stage=phase, transaction_id=xid, observed_us=stamp,
            control=owner, role=role, job=wanted_job, gap=None if phase == 'baseline' else gap)
    expected_events = [backend, *states]
    if mode == 'immediate-before-insert':
        expected_events.append(dict(event='immediate'))
    elif mode in ('gap-only-commit', 'requested-only-commit'):
        expected_events.append(dict(event='before-commit'))
    elif mode == 'complete-rollback':
        expected_events.append(dict(event='rolled-back'))
    assert events == expected_events
    if mode == 'complete-rollback':
        assert response['returncode'] == 0 and response['stderr'] == ''
    else:
        message, function = ERRORS[mode]
        assert response['returncode'] == 3
        assert len(re.findall(r'ERROR:', response['stderr'])) == 1
        assert re.search(r'ERROR:\s+23514: ' + re.escape(message) + r'\s*\n', response['stderr'])
        assert 'function ' + function + ' line ' in response['stderr']
    absence_checked(case['backend_absence'], backend, database)
    return dict(name=mode, status='PASS', observed_states=len(states), sqlstate='00000' if mode == CASES[0] else '23514',
        all_control_rows_xmin_unchanged=True, actual_backend_absent=True, native_authority=False)


def verify(*, seed, setup, grants, baseline, native, files, cases, native_after, restored):
    assert [case['mode'] for case in cases] == list(CASES)
    shared = dict(seed=seed, setup=setup, grants=grants, baseline=baseline, native=native, files=files)
    checked = [verify_case(case, **shared) for case in cases]
    assert len({case['database'] for case in cases}) == 1
    assert all(a['server_window']['after_us'] <= b['server_window']['before_us'] for a, b in zip(cases, cases[1:]))
    assert native_after == native == seed['native']
    assert restored == dict(statement=REVOKE, response=dict(returncode=0, stdout='', stderr='', outcome='PROCESS_EXITED'),
        catalog=grants['before'], errors=[])
    return dict(status='PASS', attempts=len(checked), positive=1, refusals=13,
        observed_states=sum(c['observed_states'] for c in checked), control_tables=72,
        native_rows=70, native_sequence=85, inputs=6, catalog_restored=True, cases=checked)


def negative_controls(**context):
    verify(**context)
    checks = []
    def reject(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        try:
            verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            checks.append(name)
            return
        raise AssertionError('raw guard counterexample accepted: ' + name)
    def case(c, mode=CASES[0]):
        return only(c['cases'], mode=mode)
    def state(c, phase='checked'):
        return only(case(c)['events'], event='state', stage=phase)
    def edit_events(c, change):
        selected = case(c)
        change(selected['events'])
        selected['response']['stdout'] = ''.join(json.dumps(e) + '\n' for e in selected['events'])
    reject('missing-attempt', lambda c: c['cases'].pop())
    reject('unknown-outcome', lambda c: case(c)['response'].update(returncode=None, outcome='UNKNOWN_TIMEOUT'))
    reject('missing-clock', lambda c: case(c)['server_window'].pop('after_us'))
    reject('lost-terminal-capture', lambda c: case(c)['capture_errors'].append('after:TimeoutExpired'))
    reject('native-write', lambda c: c['native_after'].update(sequence=86))
    reject('old-input-changed', lambda c: case(c)['contents_after'].update({next(iter(c['seed']['contents'])):'00'}))
    reject('old-source-changed', lambda c: only(case(c)['after']['source_streams'], owner_epoch=1).update(xmin='999999'))
    reject('browser-session-changed', lambda c: case(c)['after']['browser_sessions'][0].update(revoked_at_us=1))
    reject('retained-file-changed', lambda c: case(c)['files_after'].pop(next(iter(c['files']))))
    reject('unrestored-grant', lambda c: c['restored']['catalog']['acquisition']['privileges'].update(gap_update=True))
    reject('trigger-disabled', lambda c: case(c)['catalog_after']['triggers'][0].update(enabled='D'))
    reject('wrong-database-command', lambda c: case(c)['command'].__setitem__(-1, 'wrong_database'))
    reject('backend-still-live', lambda c: case(c)['backend_absence']['samples'][-1]['present'].append(
        dict(pid=1, backend_start=case(c)['events'][0]['backend_start'], database=case(c)['database'], user='bigbrotr_control')))
    reject('replica-invoker', lambda c: edit_events(c, lambda e: e[0].update(replication_role='replica')))
    reject('superuser-invoker', lambda c: edit_events(c, lambda e: e[0].update(superuser=True)))
    reject('gap-attributed-registrar', lambda c: edit_events(c, lambda e: e[-2]['gap'].update(previous_owner_epoch=3)))
    reject('split-terminal-transaction', lambda c: edit_events(c, lambda e: e[-2]['job'].update(xmin='999999')))
    reject('incomplete-positive', lambda c: edit_events(c, lambda e: e[-2]['job'].update(state='cancel_requested', job_revision=3)))
    reject('generic-error', lambda c: case(c, 'wrong-cut')['response'].update(stderr='ERROR: 23514: unrelated guard\n'))
    reject('missing-commit-boundary', lambda c: case(c, 'gap-only-commit')['events'].pop())
    def unreviewed(c):
        selected = case(c)
        selected['statement'] += '-- changed\n'
        selected['statement_sha256'] = sha(selected['statement'].encode())
    reject('rehashed-unreviewed-sql', unreviewed)
    return dict(status='PASS', rejected=len(checks), controls=checks)
