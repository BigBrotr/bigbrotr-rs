"""Bounded 64-intent limit, combined retained count and batch atomicity.

Expected intent/gap rows and literal science are independent of product reducers.
Fresh-connection recovery is distinguished from raw guarded operator preparation.
"""
import copy
import json
import re

import activated_acquisition_oracle as abandonment
import activated_acquisition_claims_oracle as claims
import activated_source_durable_oracle as durable
import activated_source_closure_oracle as closure
import activated_recovery_oracle as recovery
import initial_live_oracle as live
from activated_source_durable_oracle import authenticated_job, durable_science, admitted
from activated_recovery_faults_oracle import failed_control, bounded_number
from initial_live_routes_oracle import backend_checked as finite_backend
from initial_succession_oracle import binary, claim, ended, only, prepared, renewed, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window, exclusion
from installed_live_oracle import catalog_identity, instant

CASES = ('installed-acquisition-limit64', 'installed-acquisition-limit65',
    'installed-acquisition-combined65', 'installed-acquisition-unsupported-last')
FENCE = CASES[1] + '-fence'

def limit_seed_state(events, original, native, inputs, *, retired, server_window, backend, response,
        after, native_after, inputs_after, input_contents, catalog_before, catalog_after, receive_clock, total):
    assert total in (64, 65)
    probe = 'successor-live-acquisition-limit' + str(total)
    live.check_window(server_window)
    live.migrated(retired, original, native, inputs, server_window)
    catalog_identity(catalog_before)
    assert catalog_before == catalog_after
    assert response['returncode'] == 0 and response['stderr'] == ''
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
    live.clock(control['acquired_at_us'], server_window)
    live.clock(instant(control['control_backend_start']), server_window)
    live.clock(only(current['role_instances'], role_instance_id=control['role_instance_id'])['created_at_us'], server_window)
    assert capture.take('successor-plan') == plan
    following = capture.snapshot('before-prepare')
    control = renewed(current, following, control, plan['created_at_us'])
    fresh_xmin(control['xmin'], current)
    live.clock(control['lease_expires_at_us'] - 20_000_000, server_window)
    current = following
    capture.native, capture.sequence = prepared_rows, 62
    following = capture.snapshot('prepared')
    # Only the independently checked empty activation table is excluded from
    # this older71-table preparation reducer; all72 tables remain captured.
    assert current[live.ACTIVATIONS] == following[live.ACTIVATIONS] == []
    prepared({k: v for k, v in current.items() if k != live.ACTIVATIONS},
        {k: v for k, v in following.items() if k != live.ACTIVATIONS}, plan, control)
    current = following
    physical = capture.take('successor-backend', 'initial')
    assert physical == dict(event='successor-backend', stage='initial',
        **backend['exact']['expected'], present=True)
    assert capture.take('live-activation-start') == dict(event='live-activation-start', mode=probe, control_epoch=8)
    result = capture.take('live-activation-result')
    assert result == dict(event='live-activation-result', writer=True, error=None,
        activated_at_us=result['activated_at_us'])
    following = capture.snapshot('activated-zero')
    live.activated(current, following, plan, control, server_window, result['activated_at_us'])
    current = following
    job = capture.take('live-durable-job')
    following = capture.snapshot('durable-job-claimed')
    control = authenticated_job(current, following, job, control, server_window)
    current = following
    wire = capture.take('live-durable-wire')
    subscription = wire['request'][1]
    assert re.fullmatch(r'[0-9a-f]{32}0000000000000001', subscription)
    assert int(subscription[:32], 16) != 0
    assert wire == dict(event='live-durable-wire', request=['REQ', subscription, dict(kinds=[1], limit=1)],
        signed_event=durable.SIGNED, events=1, eose=True)
    assert capture.take('live-durable-peer-closed') == dict(event='live-durable-peer-closed', eof=True)
    same(current, capture.snapshot('durable-received'))
    observation = capture.take('live-durable-input')
    assert observation['wire_request'] == subscription
    assert job['accepted_at_us'] <= observation['received_at_us'] < job['deadline_at_us']
    science, filename, raw, receipt = durable_science(plan, prepared_rows, inputs, input_contents,
        observation, receive_clock, server_window)
    capture.native, capture.sequence, capture.inputs = science['rows'], 64, science['inputs']
    following = capture.snapshot('already-durable')
    admitted(current, following, science, raw, receipt, control, observation, server_window)
    current = following
    assert capture.take('live-already-durable') == dict(event='live-already-durable', request=1,
        receipt=receipt.hex(), dependencies=10, source_cut=0, peer_joined=True, job_complete=False, session_revoked=False)
    same(current, capture.snapshot('limits-before'))
    receipts = []
    policy = dict(config_revision=1, policy_revision=1,
        config_digest=only(current['config_revisions'], revision=1)['document_digest'][2:],
        policy_digest=only(current['policy_revisions'], revision=1)['document_digest'][2:])
    for ordinal in range(2, total + 1):
        assert capture.take('live-limit-refresh') == dict(event='live-limit-refresh',
            ordinal=ordinal, policy=policy, error=None)
        receipts.append((capture.take('live-limit-acceptance'), capture.take('live-limit-claim')))
    following = capture.snapshot('limits-after')
    wanted = copy.deepcopy(current)
    last = receipts[-1][0]['receipt']['accepted_at_us']
    # Only the final refresh is retained in SQL. Intermediate calls report their
    # policy result, not fabricated historical lease rows or transactions.
    updated = only(following['owner_epochs'], role='control', epoch=8)
    fresh_xmin(updated['xmin'], current)
    sampled = updated['lease_expires_at_us'] - 20_000_000
    live.clock(sampled, server_window)
    assert receipts[-2][0]['receipt']['accepted_at_us'] <= sampled <= last
    only(wanted['owner_epochs'], role='control', epoch=8).update(
        lease_expires_at_us=updated['lease_expires_at_us'], xmin=updated['xmin'])
    txs = {updated['xmin']}
    earliest = only(current['producer_outcomes'], producer_epoch=2)['recorded_at_us']
    for ordinal, (acceptance, claimed) in enumerate(receipts, 2):
        key = '\\x' + ('b0' * 15) + f'{ordinal:02x}'
        row = only(following['jobs'], request_key=key)
        at = row['created_at_us']
        live.clock(at, server_window)
        assert earliest <= at <= last < updated['lease_expires_at_us']
        earliest = at
        command = only(following['management_commands'], request_key=key)
        tx = command['xmin']
        claim_tx = row['xmin']
        fresh_xmin(tx, current)
        fresh_xmin(claim_tx, current)
        assert tx not in txs and claim_tx not in txs and tx != claim_tx
        txs.update((tx, claim_tx))
        # Reuse the separately defined literal acceptance/claim comparisons on
        # independently assembled intermediate expected rows, never project the
        # actual transcript into an older mode.
        accepted_rows = copy.deepcopy(wanted)
        digest = live.bytea(live.sha(b'bigbrotr/acquisition-job/1\0' + binary(row['dataset_id'])
            + binary(durable.ACTOR) + binary(key) + binary(durable.SCOPE)
            + live.be(30_000_000) + live.be(1) * 2))
        audit = only(following['management_audit'], action='job.acquisition.accept', subject_id=key)
        assert len(binary(audit['audit_id'])) == 16 and not any(r['audit_id'] == audit['audit_id'] for r in wanted['management_audit'])
        assert len(binary(row['job_id'])) == 16 and not any(r['job_id'] == row['job_id'] for r in wanted['jobs'])
        accepted_rows['management_audit'].append(dict(only(current['management_audit'], action='job.acquisition.accept',
            subject_id=durable.REQUEST_KEY), audit_id=audit['audit_id'], subject_id=key, created_at_us=at, xmin=tx))
        accepted_rows['management_commands'].append(dict(only(current['management_commands'], request_key=durable.REQUEST_KEY),
            request_key=key, request_digest=digest, accepted_at_us=at, audit_id=audit['audit_id'], xmin=tx))
        job = dict(only(current['jobs'], request_key=durable.REQUEST_KEY), job_id=row['job_id'], request_key=key,
            request_digest=digest, state='accepted', job_revision=1, created_at_us=at, owner_resource_id=None,
            owner_epoch=None, owner_incarnation=None, xmin=tx)
        accepted_rows['jobs'].append(job)
        accepted_rows['acquisition_jobs'].append(dict(only(current['acquisition_jobs'], request_key=durable.REQUEST_KEY),
            job_id=row['job_id'], request_key=key, deadline_at_us=at + 30_000_000, xmin=tx))
        accepted_rows['job_admissions'].append(dict(job_id=row['job_id'],
            admission_ordinal=max(r['admission_ordinal'] for r in wanted['job_admissions']) + 1, xmin=tx))
        assert acceptance == dict(event='live-limit-acceptance', ordinal=ordinal, request_key=key[2:],
            receipt=dict(command_key=key[2:], job_id=row['job_id'][2:], accepted_at_us=at, created=True),
            error=None, commit_started=True)
        job.update(state='queued', job_revision=2, owner_resource_id=control['resource_id'], owner_epoch=8,
            owner_incarnation=control['process_incarnation'], xmin=claim_tx)
        assert claimed == dict(event='live-limit-claim', ordinal=ordinal, error=None, commit_started=True,
            job=dict(id=row['job_id'][2:], scope=durable.SCOPE[2:], state='queued', job_revision=2,
                checkpoint_revision=0, accepted_at_us=at, deadline_at_us=at + 30_000_000,
                owner=dict(resource=control['resource_id'][2:], epoch=8, incarnation=control['process_incarnation'][2:])))
        wanted = accepted_rows
    same(wanted, following)
    current, control = following, updated
    originals = [dict(name=k, bytes=input_contents[k]) for k in sorted(input_contents)]
    assert capture.take('live-limit-intents') == dict(event='live-limit-intents', total_jobs=total,
        created_claims=total - 1, extra_executions=0, native_rows=53, native_sequence=64,
        inputs_before=originals, inputs_after=originals, passed=True)
    same(current, capture.snapshot('final-before-control-end'))
    assert capture.take('live-limit-release') == dict(event='live-limit-release', attempted=True, error=None)
    assert capture.take('live-limit-driver', 'control') == dict(event='live-limit-driver', stage='control',
        joined=True, failed=False, aborted=False, error=None)
    assert capture.take('successor-control', 'final', 'released-and-joined') == dict(event='successor-control',
        stage='final', phase='released-and-joined', resource=control['resource_id'][2:], epoch=8)
    assert capture.take('live-limit-backend-absence') == dict(event='live-limit-backend-absence', error=None)
    assert capture.take('successor-backend', 'initial') == dict(physical, present=False)
    following = capture.snapshot('final-after-control-end')
    ended(current, following, control, True)
    released = only(following['owner_epochs'], role='control', epoch=8)
    fresh_xmin(released['xmin'], current)
    live.clock(released['released_at_us'], server_window)
    assert last <= released['released_at_us']
    live.clock(only(following['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    same(following, capture.snapshot('final'))
    assert capture.take('live-limit-driver', 'observer') == dict(event='live-limit-driver', stage='observer',
        joined=True, failed=False, aborted=False, error=None)
    assert capture.take('live-complete') == dict(event='live-complete', mode=probe, native='closed',
        control='released-and-joined', writer=False, admission=True, passed=True)
    assert capture.position == len(events)
    same(following, after)
    assert native_after == dict(event='dump-state', rows=only(events, event='successor-state', stage='final')['rows'], sequence=64)
    assert inputs_after == science['inputs'] and len(inputs_after) == 5
    for name, raw in input_contents.items():
        value = bytes.fromhex(raw)
        assert inputs_after[name] == dict(bytes=len(value), sha256=live.digest(value))
    finite_backend(backend, released)
    bounded_number(backend['absence']['seconds'], 120)
    return science, capture.snapshots


def seed_state(context):
    if 'total' in context:
        return limit_seed_state(**context)
    return claims.seed_state(**context)


def verify_seed(**context):
    science, count = seed_state(context)
    return dict(status='PASS', snapshots=count, claims=context.get('total', 2), native_rows=53,
        native_sequence=64, inputs=5, request_count=1, source_cut=0, source_digest=science['digest'])


def selected_jobs(rows):
    activation = only(rows[live.ACTIVATIONS])
    return sorted((r for r in rows['jobs'] if r['kind'] == 'acquisition' and
        (r['owner_resource_id'], r['owner_epoch'], r['owner_incarnation']) ==
        (activation['activated_by_resource'], 8, activation['activated_by_incarnation'])), key=lambda r: binary(r['job_id']))


def gap_rows(wanted, observed, science, current, keys, clock):
    activation = only(wanted[live.ACTIVATIONS])
    source = only(wanted['source_streams'], owner_epoch=2)
    xids = set()
    before = copy.deepcopy(wanted)
    for key in keys:
        job = only(wanted['jobs'], job_id=key)
        gap = only(observed['acquisition_gaps'], job_id=key)
        fresh_xmin(gap['xmin'], before)
        xids.add(gap['xmin'])
        at = gap['abandoned_at_us']
        live.clock(at, clock)
        assert max(source['closed_at_us'], job['created_at_us'], current['lease_expires_at_us'] - 20_000_000) <= at
        assert at < current['lease_expires_at_us'] and at <= current['released_at_us']
        assert job['job_revision'] == 2 and job['checkpoint_revision'] == 0 and job['state'] == 'queued'
        assert gap == dict(job_id=key, dataset_id=job['dataset_id'], producer_slot_id=live.bytea(science['slot']),
            producer_epoch=2, shard_id=0, source_owner_epoch=2, last_complete_sequence=0, source_closure_digest='\\x' + science['digest'],
            previous_job_revision=2, previous_owner_resource=activation['activated_by_resource'], previous_owner_epoch=8,
            previous_owner_incarnation=activation['activated_by_incarnation'], issuer_resource=current['resource_id'],
            issuer_epoch=current['epoch'], issuer_incarnation=current['process_incarnation'], owner_role='control',
            abandoned_at_us=at, reason=abandonment.REASON, xmin=gap['xmin'])
        wanted['acquisition_gaps'].append(gap)
        job.update(state='cancelled', job_revision=4, reason_code=abandonment.REASON, completed_at_us=at,
            owner_resource_id=current['resource_id'], owner_epoch=current['epoch'], owner_incarnation=current['process_incarnation'], xmin=gap['xmin'])
    assert len(xids) == 1
    return next(iter(xids))


def expected_operator(before, kind, label):
    """The established fixture prefix owns actual-backend setup, not expectations.

The gap/job/release body and every resulting row are independently constrained.
No product SQL function or receipt encoder supplies expected terminal rows.
    """
    from initial_empty_guards import _setup
    jobs = selected_jobs(before)
    if kind == 'unsupported-last':
        job = jobs[-1]['job_id'][2:]
        return ("BEGIN;\nSET LOCAL session_replication_role=replica;\n"
            "UPDATE control.jobs SET checkpoint_revision=1 WHERE job_id=decode('" + job + "','hex')\n"
            "RETURNING (to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text))::text;\nCOMMIT;\n")
    assert kind == 'retained-gap'
    intent = only(before['initial_ingest_successions'])
    source = only(before['source_streams'], owner_epoch=2)
    prefix, identity = _setup(before, intent, label, 'bigbrotr_control')
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    resource = previous['resource_id'][2:]
    inc = identity['incarnation']
    epoch = identity['control_epoch']
    job = jobs[0]
    jid = job['job_id'][2:]
    ds = job['dataset_id'][2:]
    slot = intent['producer_slot_id'][2:]
    old = only(before[live.ACTIVATIONS])
    oldinc = old['activated_by_incarnation'][2:]
    proof = live.digest(b'bigbrotr/control-exclusion/1\0' + binary(job['dataset_id']) + binary(previous['resource_id'])
        + live.be(epoch) + bytes.fromhex(inc) * 2)
    return prefix + f"""SELECT control.require_activated_initial_source(decode('{intent['owner_resource_id'][2:]}','hex'));
INSERT INTO control.acquisition_gaps
 (job_id,dataset_id,producer_slot_id,producer_epoch,shard_id,source_owner_epoch,
 last_complete_sequence,source_closure_digest,previous_job_revision,previous_owner_resource,
 previous_owner_epoch,previous_owner_incarnation,issuer_resource,issuer_epoch,issuer_incarnation,
 abandoned_at_us,reason)
VALUES (decode('{jid}','hex'),decode('{ds}','hex'),decode('{slot}','hex'),2,0,2,0,
 decode('{source['closure_digest'][2:]}','hex'),2,decode('{resource}','hex'),8,decode('{oldinc}','hex'),
 decode('{resource}','hex'),{epoch},decode('{inc}','hex'),:fixture_time,'activated_initial_producer_abandoned');
UPDATE control.jobs SET state='cancel_requested',job_revision=3,
 owner_resource_id=decode('{resource}','hex'),owner_epoch={epoch},owner_incarnation=decode('{inc}','hex'),
 reason_code='activated_initial_producer_abandoned' WHERE job_id=decode('{jid}','hex');
UPDATE control.jobs SET state='cancelled',job_revision=4,completed_at_us=:fixture_time WHERE job_id=decode('{jid}','hex');
SET CONSTRAINTS ALL IMMEDIATE;
UPDATE control.owner_epochs SET state='released',released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
 exclusion_method='storage_fenced',exclusion_digest=decode('{proof}','hex')
 WHERE resource_id=decode('{resource}','hex') AND epoch={epoch};
UPDATE control.role_instances SET retired_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
 WHERE role_instance_id=decode('{identity['role_instance']}','hex');
SELECT json_build_object('event','operator-complete','transaction_id',pg_current_xact_id()::text);
COMMIT;
"""


def operator_checked(operator, expected, raw, inputs, contents, science, clock):
    assert operator['kind'] in ('retained-gap', 'unsupported-last')
    assert operator['capture_errors'] == []
    before, after = operator['before'], operator['after']
    same(before['control'], expected)
    for state in (before, after):
        assert state['native'] == raw and state['inputs'] == inputs and state['contents'] == contents
    assert operator['catalog_before'] == operator['catalog_after']
    window(operator['server_window'])
    assert operator['server_window']['after_us'] <= clock['before_us']
    response = operator['response']
    assert response['outcome'] == 'PROCESS_EXITED'
    assert response['returncode'] == 0 and response['stderr'] == ''
    bounded_number(response['seconds'], 35)
    assert operator['statement'] == expected_operator(expected, operator['kind'], operator['label'])
    assert live.digest(operator['statement'].encode()) == operator['statement_sha256']
    events = [json.loads(line) for line in response['stdout'].splitlines() if line.startswith('{')]
    wanted = copy.deepcopy(expected)
    jobs = selected_jobs(wanted)
    if operator['kind'] == 'unsupported-last':
        assert len(jobs) == 2
        changed = only(after['control']['jobs'], job_id=jobs[-1]['job_id'])
        fresh_xmin(changed['xmin'], expected)
        jobs[-1].update(checkpoint_revision=1, xmin=changed['xmin'])
        same(wanted, after['control'])
        assert events == [changed]
        assert operator['backend'] is None and operator['command'][-4:] == ['-U', 'postgres', '-d', operator['database']]
    else:
        assert len(jobs) == 65 and only(expected['source_streams'], owner_epoch=2)['state'] == 'closed'
        projected, current = control_delta(expected, after['control'], 'normal', operator['server_window'])
        tx = gap_rows(wanted, after['control'], science, current, [jobs[0]['job_id']], operator['server_window'])
        assert tx == current['xmin'] == only(after['control']['role_instances'], role_instance_id=current['role_instance_id'])['xmin']
        same(wanted, projected)
        backend, complete = events
        assert backend['event'] == 'backend' and backend['superuser'] is False and backend['replication_role'] == 'origin'
        assert backend['user'] == backend['role'] == 'bigbrotr_control' and backend['isolation'] == 'read committed'
        assert backend['database'] == operator['database'] and backend['pid'] == current['control_backend_pid']
        assert instant(backend['backend_start']) == instant(current['control_backend_start'])
        assert backend['control_epoch'] == current['epoch'] and backend['fixture_time'] == current['acquired_at_us']
        assert backend['incarnation'] == current['process_incarnation'][2:] and backend['role_instance'] == current['role_instance_id'][2:]
        assert complete == dict(event='operator-complete', transaction_id=complete['transaction_id'])
        assert int(complete['transaction_id']) % (1<<32) == int(tx)
        assert operator['backend'] == backend and operator['command'][-4:] == ['-U', 'bigbrotr_control', '-d', operator['database']]
    absence = operator['absence']
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['database'] == operator['database']
    assert absence['attempts'][-1]['count'] == 0
    return after['control']


def response_checked(response, mode, report, error):
    if mode == FENCE:
        closure.response_checked(response, mode, report, None)
        bounded_number(response['seconds'], 120)
        return
    if error is None:
        abandonment.response_checked(response, mode, report)
        return
    assert response['outcome'] == 'PROCESS_EXITED' and response['returncode'] == 4 and response['stdout'] == ''
    assert response['stderr'] == 'error: producer operation unavailable: ' + error + '\n'
    assert not any(k in response for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error', 'holder_error'))
    bounded_number(response['seconds'], 120)
    terminal = response['terminal']
    assert terminal['exit_code'] == 4 and terminal['reaped'] is True and terminal['forced'] is False
    assert type(terminal['pid']) is int and terminal['pid'] > 0 and type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    bounded_number(terminal['elapsed_seconds'], 50)
    assert terminal['prefix'] == '/lab/' + mode and terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--abandon-activated-acquisition']


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents, response, mode, *,
        seed_context, setup, server_window, backend, operator=None, predecessor=None):
    assert mode in (*CASES, CASES[0] + '-retry', FENCE)
    window(server_window)
    science, _ = seed_state(seed_context)
    expected = claims.migrated(seed_context, setup, server_window)
    assert seed_context['server_window']['after_us'] <= server_window['before_us']
    raw = seed_context['native_after']
    prior = seed_context
    if predecessor is not None:
        verify(**predecessor)
        expected = predecessor['after']
        raw = predecessor['native_after']
        prior = predecessor
        assert predecessor['seed_context'] == seed_context and predecessor['setup'] == setup
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
    if mode in (CASES[0], CASES[1], CASES[3]):
        assert predecessor is None
    if mode == CASES[0] + '-retry':
        assert predecessor['mode'] == CASES[0]
    if mode == FENCE:
        assert predecessor['mode'] == CASES[1]
    if mode == CASES[2]:
        assert predecessor['mode'] == FENCE
    assert seed_context.get('total', 2) == (64 if mode.startswith(CASES[0]) else 2 if mode == CASES[3] else 65)
    if operator is not None:
        assert mode in (CASES[2], CASES[3])
        assert operator['kind'] == ('retained-gap' if mode == CASES[2] else 'unsupported-last')
        assert operator['catalog_before'] == setup['catalog_before']
        expected = operator_checked(operator, expected, raw, inputs_before, input_contents, science, server_window)
    else:
        assert mode not in (CASES[2], CASES[3])
    same(expected, before)
    assert native_before == raw
    assert inputs_before == inputs_after == seed_context['inputs_after'] and input_contents == seed_context['input_contents']
    failed = mode in CASES[1:]
    if failed:
        projected, current = failed_control(before, after, 'abandonment', server_window)
    else:
        projected, current = control_delta(before, after, 'fresh-completion' if mode == FENCE else 'normal', server_window)
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    source = only(wanted['source_streams'], owner_epoch=2)
    closed = dict(science['rows'])
    closed['receipts', b'\x01\x31' + science['slot'] + live.be(2)] = b'\x02\x31' + live.be(1) + live.be(0) * 2 + b'\x01'
    assert native(native_after) == closed and native_after['sequence'] == 65
    closes = source['state'] == 'open'
    if closes:
        assert producer['state'] == 'open'
        changed = only(projected['producer_epochs'], producer_epoch=2)
        fresh_xmin(changed['xmin'], before)
        producer.update(state='closing', xmin=changed['xmin'])
        changed = only(projected['source_streams'], owner_epoch=2)
        fresh_xmin(changed['xmin'], before)
        live.clock(changed['closed_at_us'], server_window)
        assert current['acquired_at_us'] <= changed['closed_at_us'] <= current['lease_expires_at_us'] - 20_000_000
        source.update(state='closed', closed_at_us=changed['closed_at_us'], last_complete_sequence=0, closure_digest='\\x' + science['digest'], xmin=changed['xmin'])
        assert producer['xmin'] != source['xmin']
    jobs = selected_jobs(before)
    if mode == CASES[0]:
        assert len(jobs) == 64
        tx = gap_rows(wanted, projected, science, current, [j['job_id'] for j in jobs], server_window)
        assert len({tx, producer['xmin'], source['xmin'], current['xmin']}) == 4
    elif mode == CASES[1]:
        assert len(jobs) == 65 and not any(r['producer_epoch'] == 2 for r in before['acquisition_gaps'])
    elif mode == CASES[2]:
        assert len(jobs) == 64 and len([r for r in before['acquisition_gaps'] if r['producer_epoch'] == 2]) == 1
    elif mode == CASES[3]:
        assert len(jobs) == 2 and jobs[-1]['checkpoint_revision'] == 1 and jobs[0]['checkpoint_revision'] == 0
    same(wanted, projected)
    error = ('Control(InvalidMetadata)' if mode == CASES[3] else 'Native(InsufficientReservation)') if failed else None
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-source-closed' if mode == FENCE else 'activated-acquisition-abandoned',
        dataset_id=science['ds'].hex(), owner_resource_id=science['plan']['resource'], producer_slot_id=science['slot'].hex(),
        producer_epoch='2', issued_through='1', reserved_through='1', resolved_requests='1',
        source_closure=dict(last_complete_sequence='0', sha256=science['digest']), native='closed', owner_release='committed', ready=False)
    if mode != FENCE:
        report['acquisition_abandonment'] = dict(jobs='64', reason=abandonment.REASON, coverage='unknown')
    response_checked(response, mode, report, error)
    recovery.backend_checked(backend, current, response, server_window)
    if mode != CASES[2]:
        assert backend['absence']['database'] == prior['backend']['absence']['database']
    else:
        assert backend['absence']['database'] == operator['database']
    old = prior['backend']['exact']['expected']
    assert (current['control_backend_pid'], current['control_backend_start']) != (old['pid'], old['backend_start'])
    assert prior['server_window']['after_us'] <= instant(current['control_backend_start'])
    if operator is not None:
        assert operator['server_window']['after_us'] <= instant(current['control_backend_start'])
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72, selected=len(jobs),
        retained_gaps=len([r for r in before['acquisition_gaps'] if r['producer_epoch'] == 2]),
        new_gaps=64 if mode == CASES[0] else 0, native_rows=53, native_sequence=65, native_writes=int(closes),
        original_inputs=5, source_closed=True, refusal=error, source_fencing=mode == FENCE)


def seed_negative_controls(**context):
    if 'total' not in context:
        return claims.seed_negative_controls(**context)
    verify_seed(**context)
    rejected = []
    def check(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        value['response']['stdout'] = ''.join(json.dumps(e) + '\n' for e in value['events'])
        try:
            verify_seed(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            rejected.append(name)
            return
        raise AssertionError('accepted count seed perturbation: ' + name)
    def sql(c, mutate):
        e = only(c['events'], event='successor-state', stage='limits-after')
        pg = {k:json.loads(v) for k, v in e['state']['control_tables']}
        mutate(pg)
        e['state']['control_tables'] = [[k, live.encoded(pg[k]).decode()] for k in sorted(pg)]
        e['state']['control_sha256'] = live.digest(live.encoded(e['state']['control_tables']))
    check('missing-extra-claim', lambda c: c['events'].remove(only(c['events'], event='live-limit-claim', ordinal=2)))
    check('false-count', lambda c: only(c['events'], event='live-limit-intents').update(total_jobs=63))
    check('old-owner-claim', lambda c: sql(c, lambda p: only(p['jobs'], request_key='\\x' + 'b0' * 15 + '02').update(owner_epoch=3)))
    check('unsealed-intent', lambda c: sql(c, lambda p: only(p['jobs'], request_key='\\x' + 'b0' * 15 + '02').update(input_complete=False)))
    check('new-native-effect', lambda c: only(c['events'], event='live-limit-intents').update(native_sequence=65, extra_executions=1))
    check('wrong-policy', lambda c: only(c['events'], event='live-limit-refresh', ordinal=2)['policy'].update(policy_revision=2))
    check('unknown-acceptance', lambda c: only(c['events'], event='live-limit-acceptance', ordinal=2).update(commit_started=False))
    check('unjoined-control', lambda c: only(c['events'], event='live-limit-driver', stage='control').update(joined=False))
    check('aborted-observer', lambda c: only(c['events'], event='live-limit-driver', stage='observer').update(aborted=True))
    check('absence-unobserved', lambda c: only(c['events'], event='live-limit-backend-absence').update(error='TimeoutOutcomeUnknown'))
    check('input-rewritten', lambda c: only(c['events'], event='live-limit-intents')['inputs_after'][0].update(bytes='00'))
    check('old-gap-rewritten', lambda c: sql(c, lambda p: only(p['acquisition_gaps'], producer_epoch=1).update(xmin='999999')))
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def negative_controls(**context):
    verify(**context)
    rejected = []
    def check(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        try:
            verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            rejected.append(name)
            return
        raise AssertionError('accepted count boundary perturbation: ' + name)
    check('native-extra-write', lambda c: c['native_after'].update(sequence=66))
    check('old-gap-rewritten', lambda c: only(c['after']['acquisition_gaps'], producer_epoch=1).update(xmin='999999'))
    check('new-request', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(reserved_through_request=2))
    check('input-original-lost', lambda c: c['input_contents'].pop(next(iter(c['input_contents']))))
    check('session-revoked', lambda c: only(c['after']['browser_sessions']).update(revoked_at_us=1))
    check('original-outcome-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2).update(recorder_epoch=99))
    check('source1-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=1).update(xmin='999999'))
    check('backend-present', lambda c: c['backend']['exact'].update(present=True))
    check('unjoined-child', lambda c: c['response']['terminal'].update(reaped=False))
    check('catalog-disabled', lambda c: c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    check('unknown-clock', lambda c: c['response'].update(clock_error='TimeoutExpired'))
    if context['mode'].startswith(CASES[0]):
        def missing(c):
            gap = next(r for r in c['after']['acquisition_gaps'] if r['producer_epoch'] == 2)
            c['after']['acquisition_gaps'].remove(gap)
        check('missing-one-of64', missing)
        check('terminal-count63', lambda c: c['response'].update(stdout=c['response']['stdout'].replace('"64"', '"63"')))
    elif context['mode'] in CASES[1:]:
        check('partial-job-cancel', lambda c: only(c['after']['jobs'], job_id=selected_jobs(c['before'])[0]['job_id']).update(state='cancelled', job_revision=4))
        check('wrong-refusal-taxonomy', lambda c: c['response'].update(stderr='error: producer operation unavailable: Control(Backend)\n'))
    if context['operator'] is not None:
        def altered_sql(c):
            c['operator']['statement'] = 'SET session_replication_role=replica;\n' + c['operator']['statement']
            c['operator']['statement_sha256'] = live.digest(c['operator']['statement'].encode())
        check('coherent-operator-bypass', altered_sql)
        check('operator-clock-missing', lambda c: c['operator']['server_window'].pop('after_us'))
        if context['mode'] == CASES[2]:
            check('retained-gap-reissued', lambda c: next(r for r in c['after']['acquisition_gaps'] if r['producer_epoch'] == 2).update(issuer_epoch=12))
        else:
            check('unsupported-first-not-last', lambda c: only(c['before']['jobs'], job_id=selected_jobs(c['before'])[0]['job_id']).update(checkpoint_revision=1))
    return dict(status='PASS', count=len(rejected), rejected=rejected)
