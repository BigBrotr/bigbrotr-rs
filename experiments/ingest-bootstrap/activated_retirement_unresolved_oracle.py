"""Genuine zero-request acquisition ownership blocks activated retirement.

All72 tables are checked. The authentication/claim reducer is a literal expected
model of the fixed typed commands, not a projection into a network transcript.
The prepared native bytes and four original inputs are independently pinned by
the frozen f12 fixture. No Event, source request, gap or workflow completion is
invented. Control-only projections validate every removed lifecycle field first.
"""
import copy
import json
import math
import re
import tomllib

import activated_recovery_oracle as recovery
import initial_live_oracle as live
from activated_recovery_faults_oracle import failed_control
from activated_retirement_import_oracle import upgrades, response_checked
from initial_live_routes_oracle import backend_checked as finite_backend
from initial_succession_oracle import binary, claim, ended, only, prepared, renewed, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import catalog_identity, instant

CASES = ('installed-activated-retirement-zero-claim',)
PROBE = 'successor-live-acquisition-zero-claim'
ACTOR = '\\x' + '10' * 16
REQUEST_KEY = '\\x' + 'a6' * 16
SCOPE = '\\xa39723d7c49af2c99498fb2f59c03b16'


def authenticated_job(before, after, control, clock):
    """One actual login/accept/claim; no endpoint, scope or workflow mutation."""
    refreshed = copy.deepcopy(before)
    changed = only(after['owner_epochs'], role='control', epoch=8)
    refreshed['owner_epochs'][refreshed['owner_epochs'].index(control)] = changed
    control = renewed(before, refreshed, control, only(after['management_commands'], actor_id=ACTOR, request_key=REQUEST_KEY)['accepted_at_us'])
    fresh_xmin(control['xmin'], before)
    refreshed_at = control['lease_expires_at_us'] - 20_000_000
    live.clock(refreshed_at, clock)
    wanted = copy.deepcopy(refreshed)
    ds = only(before['datasets'])['dataset_id']
    session = only(after['browser_sessions'])
    sid, stx, issued = session['session_id'], session['xmin'], session['issued_at_us']
    assert len(binary(sid)) == 16 and sid not in {r['session_id'] for r in before['browser_sessions']}
    assert all(r['revoked_at_us'] is not None or r['expires_at_us'] <= issued for r in before['browser_sessions'])
    fresh_xmin(stx, before); live.clock(issued, clock)
    activation = only(before[live.ACTIVATIONS])['activated_at_us']
    assert activation <= issued <= refreshed_at
    for key in ('token_digest', 'csrf_digest'):
        assert len(binary(session[key])) == 32 and any(binary(session[key]))
        assert session[key] not in {r[key] for r in before['browser_sessions']}
    assert session['token_digest'] != session['csrf_digest']
    wanted['browser_sessions'] = [dict(dataset_id=ds, session_id=sid, principal_id=ACTOR,
        credential_revision=1, authentication_revision=1, token_digest=session['token_digest'],
        csrf_digest=session['csrf_digest'], issued_at_us=issued, expires_at_us=issued + 28_800_000_000,
        idle_window_us=1_800_000_000, last_seen_at_us=issued, idle_expires_at_us=issued + 1_800_000_000,
        revoked_at_us=None, xmin=stx)]
    bucket = only(after['login_buckets'], kind='account')
    at, btx = bucket['window_started_at_us'], bucket['xmin']
    fresh_xmin(btx, before); live.clock(at, clock)
    assert activation <= at <= issued and btx != stx
    wanted['login_buckets'] = [dict(dataset_id=ds, kind=kind, bucket=n, window_started_at_us=at,
        attempts=1, xmin=btx) for kind, n in [('account', 97), ('peer', 13)]]

    def audit(action, subject, tx, at=None, aid=None):
        actual = only(after['management_audit'], action=action, subject_id=subject)
        value = actual['created_at_us'] if at is None else at
        assert len(binary(actual['audit_id'])) == 16
        assert actual['audit_id'] not in {r['audit_id'] for r in wanted['management_audit']}
        live.clock(value, clock)
        wanted['management_audit'].append(dict(dataset_id=ds, actor_id=ACTOR,
            audit_id=actual['audit_id'] if aid is None else aid, action=action, outcome='accepted',
            subject_id=subject, created_at_us=value, config_revision=1, policy_revision=1,
            reason=None, requester_database_role=None, requester_os_uid=None, xmin=tx))
        return value

    assert issued <= audit('browser.session.issue', sid, stx) <= refreshed_at
    command = only(after['management_commands'], actor_id=ACTOR, request_key=REQUEST_KEY)
    tx, at = command['xmin'], command['accepted_at_us']
    fresh_xmin(tx, before); live.clock(at, clock)
    assert refreshed_at <= at < control['lease_expires_at_us']
    digest = live.sha(b'bigbrotr/acquisition-job/1\0' + binary(ds) + binary(ACTOR)
        + binary(REQUEST_KEY) + binary(SCOPE) + live.be(30_000_000) + live.be(1) * 2)
    wanted['management_commands'].append(dict(dataset_id=ds, actor_id=ACTOR, request_key=REQUEST_KEY,
        request_digest=live.bytea(digest), action='job.acquisition.accept', accepted_at_us=at,
        expected_config_revision=1, policy_revision=1, audit_id=command['audit_id'],
        authority_mode=None, document_digest=None, xmin=tx))
    audit('job.acquisition.accept', REQUEST_KEY, tx, at, command['audit_id'])
    job = only(after['jobs'], request_key=REQUEST_KEY)
    assert len(binary(job['job_id'])) == 16 and job['job_id'] not in {r['job_id'] for r in before['jobs']}
    fresh_xmin(job['xmin'], before)
    assert len({btx, stx, tx, job['xmin'], control['xmin']}) == 5
    owner = dict(owner_resource_id=control['resource_id'], owner_epoch=8,
        owner_role='control', owner_incarnation=control['process_incarnation'])
    wanted['jobs'].append(dict(only(before['jobs'], kind='acquisition'), job_id=job['job_id'],
        request_key=REQUEST_KEY, request_digest=live.bytea(digest), state='queued', job_revision=2,
        config_revision=1, policy_revision=1, created_at_us=at, completed_at_us=None,
        reason_code=None, xmin=job['xmin'], **owner))
    wanted['job_admissions'].append(dict(job_id=job['job_id'],
        admission_ordinal=max(r['admission_ordinal'] for r in before['job_admissions']) + 1, xmin=tx))
    wanted['acquisition_jobs'].append(dict(only(before['acquisition_jobs']), job_id=job['job_id'],
        request_key=REQUEST_KEY, scope_id=SCOPE, duration_us=30_000_000, deadline_at_us=at + 30_000_000,
        session_id=sid, actor_credential_revision=1, actor_authentication_revision=1, actor_grant_revision=4, xmin=tx))
    same(wanted, after)
    return control


def target_empty(rows, control):
    activation = only(rows[live.ACTIVATIONS])
    assert activation['activated_by_epoch'] == 8
    assert (activation['activated_by_resource'], activation['activated_by_incarnation']) == (
        control['resource_id'], control['process_incarnation'])
    producer = only(rows['producer_epochs'], producer_epoch=2)
    assert producer['reserved_through_request'] == 0 and producer['state'] == 'open'
    stream = only(rows['source_streams'], owner_epoch=2)
    assert stream['state'] == 'open' and stream['last_complete_sequence'] is None
    for table in ('producer_requests', 'producer_outcomes', 'producer_input_bindings',
            'producer_checkpoints', 'acquisition_gaps'):
        assert not any(r['producer_epoch'] == 2 for r in rows[table]), table
    claimed = [r for r in rows['jobs'] if (r['owner_resource_id'], r['owner_epoch'], r['owner_incarnation'])
        == (control['resource_id'], 8, control['process_incarnation'])]
    assert claimed == [only(rows['jobs'], request_key=REQUEST_KEY)]
    assert claimed[0]['kind'] == 'acquisition' and claimed[0]['state'] == 'queued'


def seed_state(events, original, native, inputs, *, retired, server_window, backend, response,
        after, native_after, inputs_after, input_contents, catalog_before, catalog_after):
    live.check_window(server_window)
    live.migrated(retired, original, native, inputs, server_window)
    catalog_identity(catalog_before)
    assert catalog_before == catalog_after
    assert response == dict(returncode=0, stdout=response['stdout'], stderr='')
    assert [json.loads(line) for line in response['stdout'].splitlines()] == events
    observed = only(events, event='successor-plan')
    live.clock(observed['created_at_us'], server_window)
    plan, old_rows, prepared_rows = live.minted_plan(original, native, inputs, observed)
    capture = live.Capture(events, original, old_rows, inputs)
    assert capture.take('successor-control', 'initial', 'claim-planned') == dict(event='successor-control',
        stage='initial', phase='claim-planned', role='d3' * 16, incarnation='dd' * 16, lease_ttl_us='20000000')
    assert plan['target_role'] != 'd3' * 16 and plan['target_incarnation'] != 'dd' * 16
    same(original, capture.snapshot('initial-before-claim'))
    claimed = capture.take('successor-control', 'initial', 'claimed')
    current = capture.snapshot('initial-after-claim')
    control = claim(original, current, 1)
    assert control['epoch'] == 8
    assert claimed == dict(event='successor-control', stage='initial', phase='claimed',
        resource=control['resource_id'][2:], epoch=8, role='d3' * 16, incarnation='dd' * 16)
    for at in (control['acquired_at_us'], instant(control['control_backend_start']),
            only(current['role_instances'], role_instance_id=control['role_instance_id'])['created_at_us']):
        live.clock(at, server_window)
    assert capture.take('successor-plan') == plan
    following = capture.snapshot('before-prepare')
    control = renewed(current, following, control, plan['created_at_us'])
    fresh_xmin(control['xmin'], current)
    live.clock(control['lease_expires_at_us'] - 20_000_000, server_window)
    current = following
    capture.native, capture.sequence = prepared_rows, 62
    following = capture.snapshot('prepared')
    # The old preparation reducer remains71-table strict; independently require
    # the additive activation table empty before this sole projection.
    assert current[live.ACTIVATIONS] == following[live.ACTIVATIONS] == []
    prepared({k: v for k, v in current.items() if k != live.ACTIVATIONS},
        {k: v for k, v in following.items() if k != live.ACTIVATIONS}, plan, control)
    current = following
    physical = capture.take('successor-backend', 'initial')
    assert physical == dict(event='successor-backend', stage='initial', **backend['exact']['expected'], present=True)
    assert capture.take('live-activation-start') == dict(event='live-activation-start', mode=PROBE, control_epoch=8)
    result = capture.take('live-activation-result')
    assert result == dict(event='live-activation-result', writer=True, error=None, activated_at_us=result['activated_at_us'])
    following = capture.snapshot('activated-zero')
    live.activated(current, following, plan, control, server_window, result['activated_at_us'])
    current = following
    same(current, capture.snapshot('zero-claim-before'))
    for phase in ('begin', 'verify', 'finish'):
        assert capture.take('live-zero-claim-login', phase=phase) == dict(event='live-zero-claim-login', phase=phase, error=None)
    refreshed = capture.take('live-zero-claim-refresh')
    assert refreshed == dict(event='live-zero-claim-refresh', error=None, policy=dict(
        config_revision=1, policy_revision=1, config_digest=plan['config_digest'], policy_digest=plan['policy_digest']))
    after_login = capture.snapshot('zero-claim-refreshed')
    accepted = capture.take('live-zero-claim-acceptance')
    after_accept = capture.snapshot('zero-claim-accepted')
    claimed = capture.take('live-zero-claim-claim')
    following = capture.snapshot('zero-claim-queued')
    control = authenticated_job(current, following, control, server_window)
    target_empty(following, control)
    job = only(following['jobs'], request_key=REQUEST_KEY)
    command = only(following['management_commands'], actor_id=ACTOR, request_key=REQUEST_KEY)
    at, tx = command['accepted_at_us'], command['xmin']
    assert accepted == dict(event='live-zero-claim-acceptance', request_key=REQUEST_KEY[2:], error=None,
        commit_started=True, receipt=dict(command_key=REQUEST_KEY[2:], job_id=job['job_id'][2:], accepted_at_us=at, created=True))
    assert claimed == dict(event='live-zero-claim-claim', error=None, commit_started=True, job=dict(
        id=job['job_id'][2:], scope=SCOPE[2:], state='queued', job_revision=2, checkpoint_revision=0,
        accepted_at_us=at, deadline_at_us=at + 30_000_000,
        owner=dict(resource=control['resource_id'][2:], epoch=8, incarnation=control['process_incarnation'][2:])))
    expected_accept = copy.deepcopy(following)
    accepted_job = only(expected_accept['jobs'], request_key=REQUEST_KEY)
    accepted_job.update(state='accepted', job_revision=1, owner_resource_id=None,
        owner_epoch=None, owner_incarnation=None, xmin=tx)
    same(expected_accept, after_accept)
    expected_login = copy.deepcopy(expected_accept)
    for table in ('jobs', 'job_admissions', 'acquisition_jobs'):
        expected_login[table] = [r for r in expected_login[table] if r['job_id'] != job['job_id']]
    expected_login['management_commands'].remove(command)
    expected_login['management_audit'] = [r for r in expected_login['management_audit']
        if not (r['action'] == 'job.acquisition.accept' and r['subject_id'] == REQUEST_KEY)]
    same(expected_login, after_login)
    current = following
    originals = [dict(name=name, bytes=input_contents[name]) for name in sorted(inputs)]
    assert set(input_contents) == set(inputs) and inputs_after == inputs and len(inputs) == 4
    for name, value in input_contents.items():
        raw = bytes.fromhex(value)
        assert inputs[name] == dict(bytes=len(raw), sha256=live.digest(raw))
    assert capture.take('live-zero-claim-state') == dict(event='live-zero-claim-state', error=None,
        native_rows=52, native_sequence=62, inputs_before=originals, inputs_after=originals, executions=0, passed=True)
    same(current, capture.snapshot('final-before-control-end'))
    assert capture.take('live-zero-claim-release') == dict(event='live-zero-claim-release', attempted=True, error=None)
    def driver(stage):
        assert capture.take('live-zero-claim-driver', stage) == dict(event='live-zero-claim-driver',
            stage=stage, joined=True, failed=False, aborted=False, error=None)
    driver('control')
    assert capture.take('successor-control', 'final', 'released-and-joined') == dict(event='successor-control',
        stage='final', phase='released-and-joined', resource=control['resource_id'][2:], epoch=8)
    assert capture.take('live-zero-claim-backend-absence') == dict(event='live-zero-claim-backend-absence', error=None)
    assert capture.take('successor-backend', 'initial') == dict(physical, present=False)
    following = capture.snapshot('final-after-control-end')
    ended(current, following, control, True)
    released = only(following['owner_epochs'], role='control', epoch=8)
    fresh_xmin(released['xmin'], current)
    live.clock(released['released_at_us'], server_window)
    assert at <= released['released_at_us'] < released['lease_expires_at_us']
    live.clock(only(following['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    current = following
    same(current, capture.snapshot('final'))
    driver('observer')
    assert capture.take('live-complete') == dict(event='live-complete', mode=PROBE, native='closed',
        control='released-and-joined', writer=False, admission=False, passed=True)
    assert capture.position == len(events)
    same(current, after)
    assert recovery.native(native_after) == prepared_rows and native_after['sequence'] == 62
    finite_backend(backend, released)
    assert math.isfinite(backend['absence']['seconds']) and 0 <= backend['absence']['seconds'] <= 120
    slot = bytes.fromhex(plan['producer_slot'])
    digest = live.digest(b'bigbrotr/activated-initial-source-closure/1\0'
        + live.sha(bytes.fromhex(plan['handoff'])) + live.sha(bytes.fromhex(plan['target_manifest']))
        + slot + live.be(2) + live.be(0) * 2)
    closed = dict(prepared_rows)
    closed['receipts', b'\x01\x31' + slot + live.be(2)] = b'\x02\x31' + live.be(0) * 3 + b'\x01'
    return dict(plan=plan, rows=prepared_rows, closed=closed, digest=digest, job=job['job_id']), capture.snapshots


def verify_seed(**context):
    known, count = seed_state(**context)
    return dict(status='PASS', snapshots=count, control_tables=72, native_rows=52, native_sequence=62,
        retained_inputs=4, issued=0, reserved=0, source_sequence=0, genuine_claims=1, claim_epoch=8,
        job_state='queued', job_revision=2, checkpoint=0, job=known['job'],
        source_closure_sha256=known['digest'], request_execution=False, session_revoked=False)


def named_veto(value, expected, clock):
    assert value['backend'] == expected and value['returncode'] == 0
    assert value['observed_sqlstate'] in (None, '23514')
    assert instant(value['since']) == clock['before_us'] and instant(value['until']) == clock['after_us']
    error, = value['errors']
    context, = value['contexts']
    prefix = r'\[' + str(expected['pid']) + r'\] '
    match = re.search(prefix + r'ERROR:  (?:(?P<code>23514): )?activated producer workflow claims remain unresolved$', error)
    assert match and value['observed_sqlstate'] == match.group('code')
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function (?:control\.)?lock_activated_initial_retirement\(bytea\) line [1-9][0-9]* at RAISE', context)
    for line in (error, context):
        recovery.inside_window(instant(line.split()[0].replace('Z', '+00:00')), clock)


def verify(seed, setup, attempts):
    known, snapshots = seed_state(**seed)
    assert len(attempts) == 2 and [a['phase'] for a in attempts] == ['source', 'retirement']
    expected = upgrades(seed, setup, attempts[0]['server_window'])
    previous_time = seed['server_window']['after_us']
    raw = seed['native_after']
    config = setup['configuration']
    assert set(config) == {'path', 'text', 'sha256'} and live.digest(config['text'].encode()) == config['sha256']
    actual = tomllib.loads(config['text'])
    database = seed['backend']['absence']['database']
    assert config['path'] == '/lab/' + database + '.toml'
    assert actual['postgres']['control'] == dict(actual['postgres']['control'], database=database,
        endpoint='127.0.0.1:5432', user='bigbrotr_control', password_ref='file:/run/secrets/application')
    assert actual['storage']['ingest_root'] == '/lab/' + CASES[0] + '/store'
    ds = only(seed['original']['datasets'])
    assert actual['topology']['dataset_id'] == ds['dataset_id'][2:] and actual['topology']['instance_id'] == ds['instance_id'][2:]
    for k, v in dict(lease_ttl='20s', authorization_ttl='5s', renewal_timeout='1s', graceful_shutdown_timeout='2s').items():
        assert actual['runtime'][k] == v
    controls = [(seed['backend']['exact']['expected']['pid'], seed['backend']['exact']['expected']['backend_start'])]
    processes = []
    for index, item in enumerate(attempts):
        phase = item['phase']
        assert item['mode'] == CASES[0] + '-' + phase
        before, after, clock = item['before'], item['after'], item['server_window']
        window(clock)
        assert previous_time <= clock['before_us']
        same(expected, before['control'])
        assert len(after['control']) == len(expected) == 72
        assert before['native'] == raw
        assert before['inputs'] == after['inputs'] == seed['inputs_after']
        assert before['contents'] == after['contents'] == seed['input_contents']
        assert item['catalog_before'] == item['catalog_after'] == setup['retirement']['catalog_after']
        wanted = copy.deepcopy(expected)
        if phase == 'retirement':
            projected, control = failed_control(expected, after['control'], 'close', clock)
            assert after['native'] == raw
        else:
            projected, control = control_delta(expected, after['control'], 'normal', clock)
            assert native(after['native']) == known['closed'] and after['native']['sequence'] == 63
            producer = only(wanted['producer_epochs'], producer_epoch=2)
            observed = only(projected['producer_epochs'], producer_epoch=2)
            role = only(after['control']['role_instances'], role_instance_id=control['role_instance_id'])
            fresh_xmin(observed['xmin'], expected)
            assert observed['xmin'] not in (control['xmin'], role['xmin'])
            producer.update(state='closing', xmin=observed['xmin'])
            stream = only(wanted['source_streams'], owner_epoch=2)
            observed = only(projected['source_streams'], owner_epoch=2)
            fresh_xmin(observed['xmin'], expected)
            assert observed['xmin'] not in (control['xmin'], role['xmin'], producer['xmin'])
            at = observed['closed_at_us']
            assert control['lease_expires_at_us'] - 20_000_000 <= at <= control['released_at_us'] < control['lease_expires_at_us']
            stream.update(state='closed', last_complete_sequence=0, closure_digest='\\x' + known['digest'], closed_at_us=at, xmin=observed['xmin'])
        assert control['epoch'] == 9 + index
        same(wanted, projected)
        report = dict(format='bigbrotr-ingest-recovery/1', state='activated-source-closed',
            dataset_id=known['plan']['dataset'], owner_resource_id=known['plan']['resource'],
            producer_slot_id=known['plan']['producer_slot'], producer_epoch='2', issued_through='0',
            reserved_through='0', resolved_requests='0', source_closure=dict(last_complete_sequence='0', sha256=known['digest']),
            native='closed', owner_release='committed', ready=False)
        response_checked(item['response'], item['mode'], phase, report)
        recovery.backend_checked(item['backend'], control, item['response'], clock)
        assert item['response']['terminal']['config'] == config['path']
        process = (item['response']['terminal']['pid'], item['response']['terminal']['start_ticks'])
        assert process not in processes
        processes.append(process)
        assert item['backend']['absence']['database'] == database
        identity = (control['control_backend_pid'], control['control_backend_start'])
        assert identity not in controls and previous_time <= instant(identity[1])
        controls.append(identity)
        if phase == 'retirement':
            named_veto(item['diagnostic'], item['backend']['exact']['expected'], clock)
        else:
            assert item['diagnostic'] is None
        previous_time, expected, raw = clock['after_us'], after['control'], after['native']
    return dict(status='PASS', seed_snapshots=snapshots, cli_invocations=2, observed_states=4, control_tables=72,
        genuine_claims=1, claim_epoch=8, claim_requests=0, job_unchanged=True, source_closed=True,
        issued=0, reserved=0, source_cut=0, native_rows=52, native_sequence=63, retained_inputs=4,
        producer_retired=False, refusal='activated producer workflow claims remain unresolved')


def negative_controls(**context):
    verify(**context)
    rejected = []
    def reject(name, edit):
        changed = copy.deepcopy(context)
        edit(changed)
        changed['seed']['response']['stdout'] = ''.join(json.dumps(e) + '\n' for e in changed['seed']['events'])
        try:
            verify(**changed)
        except (AssertionError, KeyError, IndexError, TypeError, ValueError, StopIteration):
            rejected.append(name)
            return
        raise AssertionError('zero-request retirement oracle accepted: ' + name)
    def pg(c, stage, edit):
        event = only(c['seed']['events'], event='successor-state', stage=stage)
        rows = {name: json.loads(raw) for name, raw in event['state']['control_tables']}
        edit(rows)
        event['state']['control_tables'] = [[name, live.encoded(rows[name]).decode()] for name in sorted(rows)]
        event['state']['control_sha256'] = live.digest(live.encoded(event['state']['control_tables']))
    reject('historical-claim', lambda c: pg(c, 'zero-claim-queued',
        lambda rows: only(rows['jobs'], request_key=REQUEST_KEY).update(owner_epoch=3)))
    reject('unclaimed-accepted', lambda c: pg(c, 'zero-claim-queued',
        lambda rows: only(rows['jobs'], request_key=REQUEST_KEY).update(state='accepted', owner_epoch=None)))
    reject('wrong-zero-selector', lambda c: pg(c, 'zero-claim-queued',
        lambda rows: only(rows['producer_epochs'], producer_epoch=2).update(reserved_through_request=1)))
    reject('accepted-before-claim-shape', lambda c: pg(c, 'zero-claim-accepted',
        lambda rows: only(rows['jobs'], request_key=REQUEST_KEY).update(state='queued')))
    reject('login-without-session', lambda c: pg(c, 'zero-claim-refreshed', lambda rows: rows['browser_sessions'].clear()))
    reject('session-revoked', lambda c: pg(c, 'zero-claim-queued',
        lambda rows: only(rows['browser_sessions']).update(revoked_at_us=c['seed']['server_window']['after_us'])))
    reject('claim-extra-field', lambda c: only(c['seed']['events'], event='live-zero-claim-claim')['job'].update(executed=True))
    reject('commit-not-started', lambda c: only(c['seed']['events'], event='live-zero-claim-claim').update(commit_started=False))
    reject('native-dispatch', lambda c: only(c['seed']['events'], event='live-zero-claim-state').update(executions=1))
    reject('admission-claim', lambda c: only(c['seed']['events'], event='live-complete').update(admission=True))
    reject('failed-control-driver', lambda c: only(c['seed']['events'], event='live-zero-claim-driver', stage='control').update(failed=True))
    reject('unjoined-observer', lambda c: only(c['seed']['events'], event='live-zero-claim-driver', stage='observer').update(joined=False))
    reject('seed-extra-native-write', lambda c: c['seed']['native_after'].update(sequence=63))
    reject('seed-original-input', lambda c: c['seed']['input_contents'].update({next(iter(c['seed']['inputs'])): '00'}))
    reject('seed-backend-present', lambda c: c['seed']['backend']['exact'].update(present=True))
    reject('workflow-falsely-completed', lambda c: only(c['attempts'][-1]['after']['control']['jobs'], request_key=REQUEST_KEY).update(state='cancelled'))
    reject('zero-claim-deleted', lambda c: c['attempts'][-1]['after']['control']['jobs'].remove(
        only(c['attempts'][-1]['after']['control']['jobs'], request_key=REQUEST_KEY)))
    reject('named-veto-missing', lambda c: c['attempts'][-1]['diagnostic'].update(errors=[]))
    reject('wrong-veto-kind', lambda c: c['attempts'][-1]['diagnostic'].update(errors=[
        c['attempts'][-1]['diagnostic']['errors'][0].replace('workflow', 'import')]))
    reject('wrong-veto-function', lambda c: c['attempts'][-1]['diagnostic'].update(contexts=[
        c['attempts'][-1]['diagnostic']['contexts'][0].replace('lock_activated_initial_retirement', 'require_control')]))
    reject('contradictory-sqlstate', lambda c: c['attempts'][-1]['diagnostic'].update(
        observed_sqlstate=None if c['attempts'][-1]['diagnostic']['observed_sqlstate'] else '23514'))
    reject('wrong-veto-backend', lambda c: c['attempts'][-1]['diagnostic']['backend'].update(pid=1))
    reject('generic-error', lambda c: c['attempts'][-1]['response'].update(stderr='error: producer operation unavailable: Control(Unavailable)\n'))
    reject('partial-retirement', lambda c: only(c['attempts'][-1]['after']['control']['producer_epochs'], producer_epoch=2).update(state='retired'))
    reject('source-not-closed', lambda c: only(c['attempts'][0]['after']['control']['source_streams'], owner_epoch=2).update(state='open'))
    reject('source-wrong-cut', lambda c: only(c['attempts'][0]['after']['control']['source_streams'], owner_epoch=2).update(last_complete_sequence=1))
    reject('source-digest', lambda c: only(c['attempts'][0]['after']['control']['source_streams'], owner_epoch=2).update(closure_digest='\\x' + '00' * 32))
    reject('missing-native-barrier', lambda c: c['attempts'][0]['after']['native'].update(rows=c['seed']['native_after']['rows']))
    reject('extra-native-write', lambda c: c['attempts'][-1]['after']['native'].update(sequence=64))
    reject('missing-input', lambda c: c['attempts'][-1]['after']['contents'].pop(next(iter(c['seed']['inputs']))))
    reject('catalog-disabled', lambda c: c['attempts'][-1]['catalog_after'].update(session_replication_role='replica'))
    reject('backend-present', lambda c: c['attempts'][-1]['backend']['exact'].update(present=True))
    reject('unknown-process', lambda c: c['attempts'][-1]['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('unjoined-process', lambda c: c['attempts'][-1]['response']['terminal'].update(reaped=False))
    reject('wrong-config', lambda c: c['setup']['configuration'].update(text=c['setup']['configuration']['text'].replace('127.0.0.1:5432', '127.0.0.1:5433')))
    reject('missing-upgrade', lambda c: c['setup']['retirement']['after']['schema_versions'].remove(
        only(c['setup']['retirement']['after']['schema_versions'], migration=37)))
    return dict(status='PASS', rejected=len(rejected), controls=rejected)
