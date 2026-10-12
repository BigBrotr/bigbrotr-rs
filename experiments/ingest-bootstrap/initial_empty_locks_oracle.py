"""Independent current-Control claim/retirement lock qualification.

Every fixture job field and phase is checked before projecting those exact rows
out for the existing full-history oracle. Capture hashes are checked before that
projection. Process execution and PostgreSQL observations belong to the harness.
"""
import copy
from datetime import datetime, timezone
import json
import re

import initial_empty_oracle as empty
from initial_history_oracle import inside_window
from initial_succession_oracle import digest, encoded, only, same


MODES = ('empty-claim-commit', 'empty-claim-rollback',
    'empty-retirement-first', 'empty-control-expiry')
JOB = '\\x' + 'e1' * 16
PRINCIPAL = '\\x' + 'e2' * 16
REQUEST = '\\x' + 'e3' * 16
DIGEST = '\\x' + 'e4' * 32
ROLE = 'bigbrotr_control'
PROXY_SHA256 = '6e737de8223818d0b05b47cb25f16038233d43181b12aec97bd13e3b44e3f829'
CONTROL_WAIT = ('SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea '
    'AND epoch=$2::text::numeric FOR UPDATE')


def tables(event):
    """Do not repair a checksum before checking the original observation."""
    rows = event['state']['control_tables']
    assert event['state']['control_sha256'] == digest(encoded(rows)), 'original SQL capture hash'
    answer = {name: json.loads(value) for name, value in rows}
    assert len(rows) == len(answer) == 71
    assert list(answer) == sorted(answer)
    return answer


def snapshot(events, suffix):
    return only(events, event='successor-state', stage='empty-' + suffix)


def sql_text(value):
    assert isinstance(value, str)
    return ' '.join(value.strip().rstrip(';').split())


def timestamp(value):
    assert isinstance(value, str)
    parsed = datetime.fromisoformat(value)
    assert parsed.tzinfo is not None
    return parsed


def microseconds(value):
    delta = timestamp(value) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def identity(actual, expected):
    assert type(actual['pid']) is int and actual['pid'] > 0
    assert actual['pid'] == expected['pid']
    assert timestamp(actual['backend_start']) == timestamp(expected['backend_start'])
    assert actual['database'] == expected['database']


def app_identity(observed, backend):
    assert observed == dict(pid=backend['pid'], database=backend['database'], user=ROLE, role=ROLE,
        superuser=False, session_replication_role='origin', isolation='read committed')


def guards(observed):
    assert observed['session_replication_role'] == 'origin'
    assert observed['triggers'] and all(t['enabled'] in ('O', 'A') for t in observed['triggers'])
    assert observed['constraints'] and all(c['validated'] is True for c in observed['constraints'])
    selected = {(t['table'], t['name']): t for t in observed['triggers']}
    required = {
        ('jobs', 'job_creation_guard'), ('jobs', 'job_update_guard'),
        ('jobs', 'job_input_complete'), ('jobs', 'job_admission_lock'),
        ('jobs', 'capture_job_admission'), ('jobs', 'job_admission_complete'),
        ('job_admissions', 'job_admission_assign'), ('job_admissions', 'immutable_job_admission'),
        ('source_streams', 'initial_empty_source_complete'),
        ('producer_epochs', 'initial_empty_producer_complete'),
        ('allocation_domains', 'initial_empty_domain_complete'),
        ('owner_epochs', 'initial_empty_owner_complete'),
        ('role_instances', 'initial_empty_role_complete'),
    }
    assert required <= set(selected)
    assert all(not selected[key]['internal'] for key in required)


def setup_rows(original, setup, seed, server_window):
    before, after = setup['before'], setup['after']
    same(empty.migration(seed['control'], before, setup['server_window']), before)
    same(original, after)
    window = setup['server_window']
    assert set(window) == {'before_us', 'after_us'}
    assert all(type(v) is int for v in window.values())
    assert 0 < window['before_us'] <= window['after_us'] <= server_window['before_us']
    assert window['after_us'] - window['before_us'] <= 30_000_000
    inside_window(setup['created_at_us'], window)
    assert (setup['job_id'], setup['principal_id'], setup['request_key'], setup['request_digest']) == (
        JOB, PRINCIPAL, REQUEST, DIGEST)
    backend = setup['backend']
    assert backend['user'] == ROLE and backend['isolation'] == 'read committed'
    assert backend['synchronous_commit'] == 'on' and backend['application']
    identity(backend, backend)
    app_identity(setup['identity'], backend)
    assert setup['status'] == 'CAPTURED' and not setup['cleanup_errors']
    guards(setup['guards_before'])
    assert setup['guards_after'] == setup['guards_before']
    assert setup['statement'] == [
        'BEGIN ISOLATION LEVEL READ COMMITTED;',
        "INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,"
        "kind,config_revision,policy_revision,created_at_us) SELECT "
        f"decode('{JOB[2:]}','hex'),dataset_id,decode('{PRINCIPAL[2:]}','hex'),"
        f"decode('{REQUEST[2:]}','hex'),decode('{DIGEST[2:]}','hex'),'import',"
        f"config_revision,policy_revision,{setup['created_at_us']} "
        "FROM control.datasets RETURNING to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text);",
        f"UPDATE control.jobs j SET input_complete=true WHERE job_id=decode('{JOB[2:]}','hex') "
        "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",
        'COMMIT;',
    ], 'guarded setup statements differ'
    assert setup['transactions']
    assert all(r['sqlstate'] == '00000' for r in setup['transactions'])
    dataset = only(before['datasets'])
    assert not any(r['job_id'] == JOB or r['principal_id'] == PRINCIPAL
        or r['request_key'] == REQUEST for r in before['jobs'])
    row = only(after['jobs'], job_id=JOB)
    tx = row['xmin']
    assert tx.isdecimal() and all(tx != r['xmin'] for rows in before.values() for r in rows)
    wanted_job = dict(job_id=JOB, dataset_id=dataset['dataset_id'], principal_id=PRINCIPAL,
        request_key=REQUEST, request_digest=DIGEST, kind='import', config_revision=1, policy_revision=1,
        created_at_us=setup['created_at_us'], state='accepted', job_revision=1, checkpoint_revision=0,
        reason_code=None, result_manifest_digest=None, owner_resource_id=None, owner_epoch=None,
        owner_role='control', owner_incarnation=None, completed_at_us=None, input_topology_generation=None,
        input_complete=True, acquisition_job_protocol=None, export_job_protocol=None, xmin=tx)
    wanted_admission = dict(job_id=JOB,
        admission_ordinal=max(r['admission_ordinal'] for r in before['job_admissions']) + 1, xmin=tx)
    assert row == wanted_job
    inserted, sealed = setup['transactions'][1:3]
    assert len(setup['transactions']) == len(setup['statement']) == 4
    assert inserted['rows'] == sealed['rows'] == 1
    assert [json.loads(line) for line in inserted['lines']] == [dict(wanted_job, input_complete=False)]
    assert [json.loads(line) for line in sealed['lines']] == [wanted_job]
    wanted = copy.deepcopy(before)
    wanted['jobs'].append(wanted_job)
    wanted['job_admissions'].append(wanted_admission)
    same(wanted, after)
    return wanted_job, wanted_admission


def gate_order(events, schedule, mode, server_window):
    expected = [('before-retirement', 'waiting'), ('before-retirement', 'released')]
    if mode == 'empty-retirement-first':
        expected += [('after-retirement', 'waiting'), ('after-retirement', 'released')]
    actual = [e for e in events if e['event'] == 'successor-empty-gate']
    assert actual == [dict(event='successor-empty-gate', gate=gate, phase=phase)
        for gate, phase in expected]
    for gate in {gate for gate, _ in expected}:
        first = events.index(only(actual, gate=gate, phase='waiting'))
        assert events[first + 1] == only(actual, gate=gate, phase='released')
        if gate == 'before-retirement':
            assert events[first - 1] == snapshot(events, 'proof')
        else:
            assert events[first - 1] == only(events, event='successor-empty-retirement', attempt='initial')
        assert events[first + 2] == snapshot(events, gate)
    observations = schedule['gates']
    assert [(g['gate'], g['phase']) for g in observations] == expected
    previous = server_window['before_us']
    for observation in observations:
        inside_window(observation['server_us'], server_window)
        assert observation['server_us'] >= previous
        previous = observation['server_us']
    return observations


def claimed_job(job, control, tx):
    return dict(job, state='queued', job_revision=2, owner_resource_id=control['resource_id'],
        owner_epoch=control['epoch'], owner_incarnation=control['process_incarnation'], xmin=tx)


def project(events, original, job, admission, claimed, mode):
    """Remove only rows already proven exact at each independently named phase."""
    projected = copy.deepcopy(events)
    after_dispatch = False
    for event in projected:
        if event['event'] != 'successor-state':
            continue
        pg = tables(event)
        after_dispatch |= event['stage'] == 'empty-after-retirement'
        wanted = claimed if after_dispatch and mode in ('empty-claim-commit', 'empty-retirement-first') else job
        assert only(pg['jobs'], job_id=JOB) == wanted, 'fixture job phase or field differs'
        assert only(pg['job_admissions'], job_id=JOB) == admission, 'fixture admission changed'
        pg['jobs'].remove(wanted)
        pg['job_admissions'].remove(admission)
        event['state']['control_tables'] = [[name, encoded(pg[name]).decode()] for name in sorted(pg)]
        event['state']['control_sha256'] = digest(encoded(event['state']['control_tables']))
    before = copy.deepcopy(original)
    before['jobs'].remove(job)
    before['job_admissions'].remove(admission)
    completed = only(projected, event='successor-empty-complete')
    assert completed['command'] == mode
    completed['command'] = 'empty-retire'
    return [e for e in projected if e['event'] != 'successor-empty-gate'], before


def statement(control, expiry):
    resource = "decode('" + control['resource_id'][2:] + "','hex')"
    incarnation = "decode('" + control['process_incarnation'][2:] + "','hex')"
    if expiry:
        return ("SELECT to_jsonb(o)||jsonb_build_object('xmin',xmin::text) "
            f"FROM control.owner_epochs o WHERE resource_id={resource} AND epoch={control['epoch']} "
            f"AND process_incarnation={incarnation} AND role='control' AND state='active' FOR SHARE")
    return ("UPDATE control.jobs j SET state='queued',job_revision=2,"
        f"owner_resource_id={resource},owner_epoch={control['epoch']},owner_incarnation={incarnation} "
        f"WHERE job_id=decode('{JOB[2:]}','hex') AND state='accepted' AND job_revision=1 "
        "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text)")


def transport_and_veto(schedule, backend, mode, server_window):
    wire = schedule['wire']
    if mode == 'empty-retirement-first':
        assert wire['status'] == 'PASS' and wire['exit_code'] == wire['wrapper_exit'] == 0
        assert not wire['forced_wrapper_stop'] and not wire['stderr_nonempty']
        assert wire['proxy_sha256'] == PROXY_SHA256
        ready, held, released, receipt = wire['events']
        assert [e['event'] for e in wire['events']] == ['ready', 'held', 'released', 'receipt']
        assert (ready, held, released, receipt) == tuple(wire[k] for k in ('ready', 'held', 'released', 'receipt'))
        fields = ('commits_seen', 'commits_forwarded', 'server_commit_complete', 'client_commit_complete', 'faults')
        assert [ready[k] for k in fields] == [0, 0, 0, 0, 0]
        assert [held[k] for k in fields] == [6, 5, 5, 5, 1]
        assert [released[k] for k in fields] == [6, 5, 5, 5, 1]
        assert [receipt[k] for k in fields] == [8, 8, 8, 8, 1]
        assert ready['listen_host'] == '127.0.0.1' and ready['upstream_port'] == 5432
        assert type(ready['listen_port']) is int and 0 < ready['listen_port'] <= 65535
        assert ready['proxy_sha256'] == PROXY_SHA256 and ready['deadline_seconds'] == 30
        assert ready['held'] is False and ready['hold_action'] is None
        assert held['held'] is True and held['hold_action'] is None
        assert released['held'] is False and released['hold_action'] == 'release'
        assert receipt['held'] is False and receipt['hold_action'] == 'release'
        assert receipt['reason'] in ('frontend-eof', 'backend-eof')
        previous = 0
        for event in wire['events']:
            assert event['format'] == 'bigbrotr-pg-fault-proxy/1'
            assert event['mode'] == 'hold' and event['commit_number'] == 6
            assert type(event['elapsed_ns']) is int and previous <= event['elapsed_ns'] < 30_000_000_000
            previous = event['elapsed_ns']
        assert 0 < receipt['declared_wire_bytes'] <= ready['max_wire_bytes']
        assert 0 < sum(receipt['frames'].values()) <= ready['max_messages']
    else:
        assert wire == dict(status='NOT_RUN')
    observed = schedule['claim_guard']
    if mode != 'empty-claim-commit':
        assert observed is None
        return
    identity(observed['backend'], backend)
    assert microseconds(observed['since']) == server_window['before_us']
    assert microseconds(observed['until']) == server_window['after_us']
    error, = observed['errors']
    context, = observed['contexts']
    for line in (error, context):
        inside_window(microseconds(line.split()[0]), server_window)
        assert f'[{backend["pid"]}] ' in line
    matched = re.search(r'\[' + str(backend['pid']) + r'\] ERROR:  '
        r'(?:(?P<code>[0-9A-Z]{5}): )?empty attempt Control workflow claims remain unresolved$', error)
    assert matched is not None and observed['observed_sqlstate'] == matched.group('code')
    assert observed['observed_sqlstate'] in (None, '23514')
    assert f'[{backend["pid"]}] CONTEXT:  ' in context
    assert re.search(r'function (?:control\.)?require_initial_empty_claims\(control.initial_ingest_successions\)', context)


def verify(events, original, native, inputs, mode, *, seed, server_window, setup, schedule):
    assert mode in MODES and schedule['mode'] == mode
    assert schedule['status'] == 'CAPTURED' and not schedule['cleanup_errors']
    job, admission = setup_rows(original, setup, seed, server_window)
    gates = gate_order(events, schedule, mode, server_window)
    proof = tables(snapshot(events, 'proof'))
    control = only(proof['owner_epochs'], role='control', state='active')
    assert control['epoch'] == 10 and schedule['control'] == control
    backend = only(events, event='successor-backend', present=True)
    assert schedule['control_backend'] == backend
    identity(backend, dict(pid=control['control_backend_pid'],
        backend_start=control['control_backend_start'], database=backend['database']))
    claimant = schedule['claimant']
    assert claimant['database'] == backend['database'] == setup['backend']['database']
    assert claimant['pid'] != backend['pid'] and claimant['user'] == ROLE
    assert claimant['isolation'] == 'read committed' and claimant['synchronous_commit'] == 'on'
    assert claimant['application'] and claimant['application'] != setup['backend']['application']
    app_identity(schedule['claimant_identity'], claimant)
    app_identity(schedule['transaction_identity'], claimant)
    same(schedule['before_holder'], proof)
    same(schedule['held'], proof)
    assert sql_text(schedule['claim_statement']) == statement(control, mode == 'empty-control-expiry')
    answer = schedule['claim_result']
    assert answer['sqlstate'] == '00000' and answer['rows'] == 1 and len(answer['lines']) == 1
    observed_claim = json.loads(answer['lines'][0])
    if mode == 'empty-control-expiry':
        assert observed_claim == control
        claimed = None
    else:
        tx = observed_claim['xmin']
        assert tx.isdecimal() and all(tx != r['xmin'] for rows in proof.values() for r in rows)
        claimed = claimed_job(job, control, tx)
        assert observed_claim == claimed, 'claim may change only revision/state/current owner/xmin'
    end = schedule['transaction_end']
    assert end['sqlstate'] == '00000' and end['rows'] == 0 and end['lines'] == []
    assert sql_text(schedule['transaction_end_statement']) == (
        'COMMIT' if mode in ('empty-claim-commit', 'empty-retirement-first') else 'ROLLBACK')
    blocks = schedule['blocks']
    assert blocks
    waiter, holder = (claimant, backend) if mode == 'empty-retirement-first' else (backend, claimant)
    last_time = gates[1]['server_us']
    previous_seconds = 0
    for sample in blocks:
        inside_window(sample['observed_at_us'], server_window)
        assert sample['observed_at_us'] >= last_time
        last_time = sample['observed_at_us']
        assert type(sample['sample_seconds']) in (float, int)
        assert previous_seconds <= sample['sample_seconds'] <= 2
        previous_seconds = sample['sample_seconds']
        identity(sample['waiter'], waiter)
        identity(sample['holder'], holder)
        assert sample['waiter']['user'] == sample['holder']['user'] == ROLE
    blocked = blocks[-1]
    assert blocked['waiter']['state'] == 'active' and blocked['waiter']['wait_event_type'] == 'Lock'
    assert holder['pid'] in blocked['waiter']['blockers']
    assert blocked['holder']['state'] == 'idle in transaction'
    assert blocked['holder']['backend_xid'].isdecimal()
    locks = blocked['waiting_locks']
    assert locks and all(lock['granted'] is False for lock in locks)
    assert any(lock['type'] == 'transactionid' and lock['mode'] == 'ShareLock'
        and lock['transaction'] == blocked['holder']['backend_xid'] for lock in locks)
    query = schedule['claim_statement'] if mode == 'empty-retirement-first' else CONTROL_WAIT
    assert sql_text(blocked['waiter']['query']) == sql_text(query)
    if mode != 'empty-control-expiry':
        actor = blocked['waiter'] if mode == 'empty-retirement-first' else blocked['holder']
        assert actor['backend_xid'] == claimed['xmin']
        assert actor['application'] == claimant['application']
    if mode == 'empty-control-expiry':
        expiry = schedule['expiry']
        lease = control['lease_expires_at_us']
        assert expiry['lease_expires_at_us'] == lease
        age = expiry['pre_age_observations']
        assert age and age[0]['remaining_us'] > 2_000_000
        previous = gates[0]['server_us']
        for sample in age:
            inside_window(sample['server_us'], server_window)
            assert previous <= sample['server_us'] < lease
            assert sample['remaining_us'] == lease - sample['server_us']
            previous = sample['server_us']
        assert 0 < age[-1]['remaining_us'] <= 2_000_000
        assert age[-1]['server_us'] <= gates[1]['server_us'] <= expiry['lock_started_us']
        assert expiry['lock_started_us'] == blocked['observed_at_us'] < lease
        assert lease < expiry['expired_server_us'] <= expiry['released_server_us']
        inside_window(expiry['released_server_us'], server_window)
        assert expiry['released_server_us'] - expiry['lock_started_us'] < 5_000_000
    else:
        assert schedule['expiry'] is None
    projected, projected_original = project(events, original, job, admission, claimed, mode)
    base_mode = 'empty-normal' if mode in ('empty-claim-rollback', 'empty-retirement-first') else mode
    report = empty.verify(projected, projected_original, native, inputs, base_mode,
        seed=seed, server_window=server_window)
    transport_and_veto(schedule, backend, mode, server_window)
    if mode == 'empty-retirement-first':
        assert schedule['after_holder_phase'] == 'after-claim-before-postdispatch-release'
        same(schedule['after_holder'], tables(snapshot(events, 'after-retirement')))
        assert blocked['holder']['backend_xid'] == report['retirement_transaction']
        assert report['retired_at_us'] <= blocked['observed_at_us']
        assert blocked['observed_at_us'] <= gates[2]['server_us'] <= gates[3]['server_us']
    else:
        assert schedule['after_holder_phase'] == 'after-process'
        same(schedule['after_holder'], tables(snapshot(events, 'final')))
    report.update(mode=mode, fixture_job=JOB, fixture_job_claim_committed=mode in (
        'empty-claim-commit', 'empty-retirement-first'), actual_control_block_observed=True,
        current_control_epoch=10, control_expired_during_wait=mode == 'empty-control-expiry',
        historical_preparer_claim='NOT_RUN', qualification=(
            'guarded generic import SQL and observed PostgreSQL custody; installed import execution is not qualified'))
    report['final']['control_sha256'] = empty.control_digest(tables(snapshot(events, 'final')))
    return report


def negative_controls(events, original, native, inputs, mode, **context):
    """Preserve checked fixture deltas while attacking the independent evidence."""
    verify(events, original, native, inputs, mode, **context)
    setup, schedule = context['setup'], context['schedule']
    job = only(setup['after']['jobs'], job_id=JOB)
    admission = only(setup['after']['job_admissions'], job_id=JOB)
    claimed = None if mode == 'empty-control-expiry' else json.loads(schedule['claim_result']['lines'][0])
    projected, baseline = project(events, original, job, admission, claimed, mode)
    base_mode = 'empty-normal' if mode in ('empty-claim-rollback', 'empty-retirement-first') else mode
    checks = empty.negative_controls(projected, baseline, native, inputs, base_mode,
        seed=context['seed'], server_window=context['server_window'])
    rejected = ['history-' + name for name in checks['rejected']]

    def check(name, mutate):
        candidate = copy.deepcopy(dict(events=events, original=original, native=native,
            inputs=inputs, mode=mode, **context))
        mutate(candidate)
        try:
            verify(**candidate)
        except (AssertionError, KeyError, ValueError, TypeError):
            rejected.append(name)
        else:
            raise AssertionError('lock oracle accepted perturbation: ' + name)

    def edit_sql(candidate, suffix, mutate):
        event = snapshot(candidate['events'], suffix)
        pg = tables(event)
        mutate(pg)
        event['state']['control_tables'] = [[name, encoded(pg[name]).decode()] for name in sorted(pg)]
        event['state']['control_sha256'] = digest(encoded(event['state']['control_tables']))

    check('unverified-original-capture-hash', lambda c: snapshot(c['events'], 'proof')['state']
        .update(control_sha256='00' * 32))
    check('setup-privileged-writer', lambda c: c['setup']['identity'].update(superuser=True))
    check('setup-declared-replica-bypass', lambda c: c['setup']['statement'].__setitem__(
        0, "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL session_replication_role='replica';"))
    check('claim-replica-bypass', lambda c: c['schedule']['transaction_identity']
        .update(session_replication_role='replica'))
    check('fixture-job-request-binding', lambda c: edit_sql(c, 'final', lambda pg:
        only(pg['jobs'], job_id=JOB).update(input_topology_generation=1)))
    check('fixture-admission-xmin', lambda c: edit_sql(c, 'final', lambda pg:
        only(pg['job_admissions'], job_id=JOB).update(xmin='4294967294')))
    check('fixture-checkpoint-advance', lambda c: edit_sql(c, 'final', lambda pg:
        only(pg['jobs'], job_id=JOB).update(checkpoint_revision=1)))
    check('unchecked-job-addition', lambda c: edit_sql(c, 'final', lambda pg:
        pg['jobs'].append(dict(only(pg['jobs'], job_id=JOB), job_id='\\x' + 'e5' * 16))))
    check('wrong-waiter-incarnation', lambda c: c['schedule']['blocks'][-1]['waiter']
        .update(backend_start='2000-01-01T00:00:00+00:00'))
    check('wrong-holder-database', lambda c: c['schedule']['blocks'][-1]['holder']
        .update(database='another_database'))
    check('missing-real-blocker', lambda c: c['schedule']['blocks'][-1]['waiter'].update(blockers=[]))
    check('no-waiting-lock', lambda c: c['schedule']['blocks'][-1].update(waiting_locks=[]))
    check('unrelated-blocked-query', lambda c: c['schedule']['blocks'][-1]['waiter'].update(query='SELECT 1'))
    check('missing-before-dispatch-gate', lambda c: c['events'].remove(only(c['events'],
        event='successor-empty-gate', gate='before-retirement', phase='waiting')))
    check('wrong-current-Control-epoch', lambda c: c['schedule']['control'].update(epoch=8))
    check('transaction-not-completed', lambda c: c['schedule']['transaction_end'].update(sqlstate='23514'))
    check('wrong-claim-transaction-ending', lambda c: c['schedule'].update(
        transaction_end_statement='ROLLBACK;' if mode in ('empty-claim-commit', 'empty-retirement-first') else 'COMMIT;'))
    if mode != 'empty-control-expiry':
        def historical_claim(candidate):
            result = candidate['schedule']['claim_result']
            claimed = json.loads(result['lines'][0])
            claimed['owner_epoch'] = 8
            result['lines'] = [encoded(claimed).decode()]
            for event in candidate['events']:
                if event['event'] == 'successor-state':
                    def replace(pg):
                        row = only(pg['jobs'], job_id=JOB)
                        if row['state'] == 'queued':
                            row['owner_epoch'] = 8
                    edit_sql(candidate, event['stage'][6:], replace)
            row = only(candidate['schedule']['after_holder']['jobs'], job_id=JOB)
            if row['state'] == 'queued':
                row['owner_epoch'] = 8
        check('consistently-historical-claim-attribution', historical_claim)
        check('claim-xmin-not-holder-transaction', lambda c: c['schedule']['blocks'][-1][
            'waiter' if mode == 'empty-retirement-first' else 'holder'].update(backend_xid='4294967294'))
    else:
        check('lease-not-expired-at-release', lambda c: c['schedule']['expiry'].update(
            expired_server_us=c['schedule']['control']['lease_expires_at_us'] - 1))
        check('lock-acquired-after-expiry', lambda c: c['schedule']['expiry'].update(
            lock_started_us=c['schedule']['control']['lease_expires_at_us'] + 1))
        check('expiry-crossing-lock-timeout', lambda c: c['schedule']['expiry'].update(
            released_server_us=c['schedule']['expiry']['lock_started_us'] + 5_000_000))
        check('pre-age-fictional-remaining-time', lambda c: c['schedule']['expiry']['pre_age_observations'][-1]
            .update(remaining_us=1))
    if mode == 'empty-retirement-first':
        check('held-transaction-not-retirement', lambda c: c['schedule']['blocks'][-1]['holder']
            .update(backend_xid='4294967294'))
        check('claim-observed-before-retirement-release', lambda c: c['schedule']['gates'][2]
            .update(server_us=c['schedule']['blocks'][-1]['observed_at_us'] - 1))
        def wire_field(candidate, phase, **changes):
            wire = candidate['schedule']['wire']
            wire[phase].update(changes)
            only(wire['events'], event=phase).update(changes)
        check('COMMIT-forwarded-before-lock-observation', lambda c:
            wire_field(c, 'held', commits_forwarded=6))
        check('held-COMMIT-never-released', lambda c: wire_field(c, 'released', hold_action='close'))

        def terminal_after_block(candidate):
            time = candidate['schedule']['blocks'][-1]['observed_at_us'] + 1
            role = only(candidate['original']['initial_ingest_successions'])['target_role_instance_id']
            def change(pg):
                only(pg['producer_epochs'], producer_epoch=2)['retired_at_us'] = time
                only(pg['source_streams'], owner_epoch=2)['closed_at_us'] = time
                only(pg['owner_epochs'], role='ingest', epoch=2)['released_at_us'] = time
                only(pg['role_instances'], role_instance_id=role)['retired_at_us'] = time
                for row in pg['allocation_domains']:
                    if row['owner_epoch'] == 2:
                        row['closed_at_us'] = time
            active = False
            for event in candidate['events']:
                if event['event'] == 'successor-empty-retirement':
                    event['retired_at_us'] = time
                if event['event'] == 'successor-state':
                    active |= event['stage'] == 'empty-after-retirement'
                    if active:
                        edit_sql(candidate, event['stage'][6:], change)
            change(candidate['schedule']['after_holder'])
        check('consistently-terminal-time-after-held-transaction', terminal_after_block)
    if mode == 'empty-claim-commit':
        check('generic-Backend-without-named-veto', lambda c: c['schedule']['claim_guard'].update(errors=[]))
        check('named-veto-from-another-backend', lambda c: c['schedule']['claim_guard']['backend'].update(pid=1))
    return dict(status='PASS', rejected=rejected)
