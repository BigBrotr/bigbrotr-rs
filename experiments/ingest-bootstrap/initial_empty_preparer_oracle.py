"""Independent live-preparer claim and replacement-process retirement refusal.

The retired f12 seed is frozen. A new prepared state is accepted only through its
complete preparation transcript, exact guarded claim and positively ended owner.
The earlier ac644 seed oracle remains unchanged and strictly pinned.
"""
import copy
import json

import initial_empty_oracle as empty
from initial_empty_locks_oracle import app_identity, guards, identity, tables, transport_and_veto
from initial_history_oracle import claim, inside_window, refresh, retained_expected, rows_dump
from initial_succession_oracle import digest, encoded, ended, expected, native_rows, only, prepared, same


MODE = 'empty-preparer-claim'
PREPARER = 'successor-preparer-claim'
SEED_CONTROL = '87ff8c09c1eec90dbbed8b333761ef9816828273ac6bd53beae2028e02b6c21a'
SEED_NATIVE = '91b64595e263c5d297e0d3a67b82fdf834777ade7f3e93524f0cc972eeff3b1d'
SEED_INPUTS = '5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea'
MIGRATION30 = '55049328c3f0be13a22af199f1599af157ce02e5a74760a296909469693be1f4'
JOB, PRINCIPAL, REQUEST, DIGEST = ('\\x' + value * size for value, size in
    (('e6', 16), ('e7', 16), ('e8', 16), ('e9', 32)))


def window(value):
    assert set(value) == {'before_us', 'after_us'}
    assert all(type(v) is int and v > 0 for v in value.values())
    assert 0 <= value['after_us'] - value['before_us'] <= 120_000_000


class Capture:
    """Ordered raw observations; hashes are checked before interpreting rows."""
    def __init__(self, events, inputs, native, sequence):
        self.events, self.inputs = events, inputs
        self.native, self.sequence = native, sequence
        self.position = self.snapshots = 0

    def take(self, name, stage=None, phase=None):
        assert self.position < len(self.events), (name, stage, 'missing event')
        event = self.events[self.position]
        self.position += 1
        assert (event['event'], event.get('stage'), event.get('phase')) == (name, stage, phase), event
        return event

    def snapshot(self, stage):
        event = self.take('successor-state', stage)
        assert set(event) == {'event', 'stage', 'state', 'rows'}
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence',
            'control_sha256', 'control_tables', 'originals'}
        assert native_rows(event['rows']) == self.native
        assert state['native_rows'] == len(event['rows'])
        assert state['native_sha256'] == digest(encoded(event['rows']))
        assert type(state['sequence']) is int and state['sequence'] == self.sequence
        assert state['originals'] == [[k, self.inputs[k]] for k in sorted(self.inputs)]
        self.snapshots += 1
        return tables(event)

    def finish(self):
        assert self.position == len(self.events), 'unclassified trailing event'


def backend(value, control, stage, database=None):
    name = value['database']
    assert isinstance(name, str) and 1 <= len(name) <= 63
    assert name.isascii() and name.replace('_', '').isalnum()
    assert database is None or name == database
    assert value == dict(event='successor-backend', stage=stage, database=name,
        pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
        control_epoch=control['epoch'], present=True)
    return value


def migrated(seed, original, server_window):
    assert set(seed) == {'control', 'native', 'inputs', 'sequence'} and seed['sequence'] == 58
    assert len(seed['control']) == 70 and len(seed['native']['rows']) == 49
    assert empty.control_digest(seed['control']) == SEED_CONTROL
    assert digest(encoded(seed['native'])) == SEED_NATIVE
    assert digest(encoded(seed['inputs'])) == SEED_INPUTS
    wanted = copy.deepcopy(seed['control'])
    wanted['initial_ingest_successions'] = []
    row = only(original['schema_versions'], migration=30)
    assert row['xmin'].isdecimal() and all(row['xmin'] != r['xmin']
        for value in wanted.values() for r in value)
    assert type(row['applied_at_us']) is int
    assert max(r['applied_at_us'] for r in wanted['schema_versions']) <= row['applied_at_us'] <= server_window['before_us']
    assert row == dict(name='0030_initial_succession.sql', xmin=row['xmin'],
        engine='postgres-control', checksum='\\x' + MIGRATION30, migration=30,
        applied_by='bigbrotr_migrator', reader_max=1, reader_min=1, writer_floor=1,
        applied_at_us=row['applied_at_us'])
    wanted['schema_versions'].append(row)
    same(empty.migration(wanted, original, server_window), original)


def claim_statements(control, created):
    return [
        'BEGIN ISOLATION LEVEL READ COMMITTED;',
        "INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,"
        "kind,config_revision,policy_revision,created_at_us) SELECT "
        f"decode('{JOB[2:]}','hex'),dataset_id,decode('{PRINCIPAL[2:]}','hex'),"
        f"decode('{REQUEST[2:]}','hex'),decode('{DIGEST[2:]}','hex'),'import',"
        f"config_revision,policy_revision,{created} "
        "FROM control.datasets RETURNING to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text);",
        f"UPDATE control.jobs j SET input_complete=true WHERE job_id=decode('{JOB[2:]}','hex') "
        "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",
        "UPDATE control.jobs j SET state='queued',job_revision=2,"
        f"owner_resource_id=decode('{control['resource_id'][2:]}','hex'),owner_epoch={control['epoch']},"
        f"owner_incarnation=decode('{control['process_incarnation'][2:]}','hex') "
        f"WHERE job_id=decode('{JOB[2:]}','hex') AND state='accepted' AND job_revision=1 "
        "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",
        'COMMIT;',
    ]


def claimed(before, control, observed_backend, setup, server_window):
    same(before, setup['before'])
    assert setup['control'] == control and setup['control_backend'] == observed_backend
    assert control['epoch'] == 8 and control['state'] == 'active'
    assert (setup['job_id'], setup['principal_id'], setup['request_key'], setup['request_digest']) == (
        JOB, PRINCIPAL, REQUEST, DIGEST)
    actor = setup['backend']
    identity(actor, actor)
    assert actor['database'] == observed_backend['database'] and actor['pid'] != observed_backend['pid']
    assert actor['user'] == 'bigbrotr_control' and actor['isolation'] == 'read committed'
    assert actor['synchronous_commit'] == 'on' and actor['application']
    app_identity(setup['identity'], actor)
    assert setup['status'] == 'CAPTURED' and setup['cleanup_errors'] == []
    guards(setup['guards_before'])
    assert setup['guards_after'] == setup['guards_before']
    window(setup['server_window'])
    start, end = setup['server_window']['before_us'], setup['server_window']['after_us']
    inside_window(start, server_window)
    inside_window(end, server_window)
    assert start <= setup['created_at_us'] <= end < control['lease_expires_at_us']
    assert type(setup['created_at_us']) is int
    gates = setup['gates']
    assert [(g['gate'], g['phase']) for g in gates] == [
        ('preparer-claim', 'waiting'), ('preparer-claim', 'released')]
    assert server_window['before_us'] <= gates[0]['server_us'] <= start <= end <= gates[1]['server_us'] <= server_window['after_us']
    assert 0 <= gates[1]['server_us'] - gates[0]['server_us'] < 10_000_000
    assert setup['statement'] == claim_statements(control, setup['created_at_us'])
    answers = setup['transactions']
    assert len(answers) == 5 and all(r['sqlstate'] == '00000' for r in answers)
    assert all(answers[i]['rows'] == 0 and answers[i]['lines'] == [] for i in (0, 4))
    assert not any(r['job_id'] == JOB or r['principal_id'] == PRINCIPAL
        or r['request_key'] == REQUEST for r in before['jobs'])
    job = only(setup['after']['jobs'], job_id=JOB)
    tx = job['xmin']
    assert tx.isdecimal() and all(tx != r['xmin'] for value in before.values() for r in value)
    accepted = dict(job_id=JOB, dataset_id=control['dataset_id'], principal_id=PRINCIPAL,
        request_key=REQUEST, request_digest=DIGEST, kind='import', config_revision=1, policy_revision=1,
        created_at_us=setup['created_at_us'], state='accepted', job_revision=1, checkpoint_revision=0,
        reason_code=None, result_manifest_digest=None, owner_resource_id=None, owner_epoch=None,
        owner_role='control', owner_incarnation=None, completed_at_us=None, input_topology_generation=None,
        input_complete=False, acquisition_job_protocol=None, export_job_protocol=None, xmin=tx)
    sealed = dict(accepted, input_complete=True)
    queued = dict(sealed, state='queued', job_revision=2, owner_resource_id=control['resource_id'],
        owner_epoch=8, owner_incarnation=control['process_incarnation'])
    assert job == queued
    for response, row in zip(answers[1:4], (accepted, sealed, queued)):
        assert response['rows'] == 1 and [json.loads(line) for line in response['lines']] == [row]
    wanted = copy.deepcopy(before)
    wanted['jobs'].append(queued)
    wanted['job_admissions'].append(dict(job_id=JOB,
        admission_ordinal=max(r['admission_ordinal'] for r in before['job_admissions']) + 1, xmin=tx))
    same(wanted, setup['after'])
    return wanted


def preparation_checked(context, seed):
    original, inputs = context['original'], context['inputs']
    server_window = context['server_window']
    window(server_window)
    migrated(seed, original, server_window)
    assert context['native'] == seed['native'] and inputs == seed['inputs']
    observed = only(context['events'], event='successor-plan')
    inside_window(observed['created_at_us'], server_window)
    plan, previous_native, prepared_native = expected(original, context['native'], inputs, observed['created_at_us'])
    c = Capture(context['events'], inputs, previous_native, 58)
    planned = c.take('successor-control', 'initial', 'claim-planned')
    assert planned == dict(event='successor-control', stage='initial', phase='claim-planned',
        role='d3' * 16, incarnation='dd' * 16, lease_ttl_us='20000000')
    same(original, c.snapshot('initial-before-claim'))
    acquired = c.take('successor-control', 'initial', 'claimed')
    current = c.snapshot('initial-after-claim')
    control = claim(original, current, planned, server_window)
    assert control['epoch'] == 8
    assert acquired == dict(event='successor-control', stage='initial', phase='claimed',
        resource=control['resource_id'][2:], epoch=8, role='d3' * 16, incarnation='dd' * 16)
    assert c.take('successor-plan') == plan
    after = c.snapshot('before-prepare')
    control = refresh(current, after, control, server_window)
    assert control['lease_expires_at_us'] - 20_000_000 <= plan['created_at_us']
    current = after
    for stage in ('prepared', 'same-control-retry'):
        assert c.take('successor-attempt', stage) == dict(event='successor-attempt', stage=stage,
            role=plan['target_role'], control_epoch=8)
        assert c.take('successor-result', stage) == dict(event='successor-result', stage=stage,
            control_epoch=8, validations=4, error=None)
        c.native, c.sequence = prepared_native, 62
        after = c.snapshot(stage)
        if stage == 'prepared':
            prepared(current, after, plan, control)
        else:
            same(current, after)
        current = after
    present = backend(c.take('successor-backend', 'initial'), control, 'initial')
    for phase in ('waiting', 'released'):
        assert c.take('successor-preparer-gate', phase=phase) == dict(
            event='successor-preparer-gate', gate='preparer-claim', phase=phase)
    current = claimed(current, control, present, context['claim'], server_window)
    same(current, c.snapshot('final-before-control-end'))
    assert c.take('successor-control', 'final', 'released-and-joined') == dict(
        event='successor-control', stage='final', phase='released-and-joined',
        resource=control['resource_id'][2:], epoch=8)
    after = c.snapshot('final-after-control-end')
    ended(current, after, control, True)
    released = only(after['owner_epochs'], role='control', epoch=8)
    retired = only(after['role_instances'], role_instance_id=control['role_instance_id'])
    inside_window(released['released_at_us'], server_window)
    inside_window(retired['retired_at_us'], server_window)
    assert context['claim']['gates'][1]['server_us'] <= released['released_at_us']
    assert c.take('successor-driver', 'final') == dict(event='successor-driver', stage='final', failed=False, joined=True)
    assert c.take('successor-backend', 'initial') == dict(present, present=False)
    same(after, c.snapshot('final'))
    assert c.take('successor-complete') == dict(event='successor-complete', mode=PREPARER,
        native='closed', control='released-and-joined', admission=False)
    c.finish()
    # The dynamic seed is already fully verified. Reconstruct all four original
    # receipts independently without weakening the older frozen-seed helper.
    known = retained_expected(after, rows_dump(prepared_native), inputs)
    return after, prepared_native, known, present, c.snapshots


def verify(events, original, native, inputs, mode, *, seed, server_window, preparation):
    assert mode == MODE
    window(server_window)
    current, prepared_native, known, preparer_backend, preparation_snapshots = preparation_checked(preparation, seed)
    assert preparation['server_window']['after_us'] <= server_window['before_us']
    same(current, original)
    assert native == rows_dump(prepared_native) and inputs == seed['inputs']
    c = Capture(events, inputs, prepared_native, 62)
    planned = c.take('successor-control', 'empty', 'claim-planned')
    assert set(planned) == {'event', 'stage', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['lease_ttl_us'] == '20000000'
    same(current, c.snapshot('empty-before-claim'))
    acquired = c.take('successor-control', 'empty', 'claimed')
    policy = c.take('successor-policy', 'empty', 'acquired')
    present = c.take('successor-backend', 'empty')
    after = c.snapshot('empty-after-claim')
    control = claim(current, after, planned, server_window)
    assert control['epoch'] == 9
    assert acquired == dict(event='successor-control', stage='empty', phase='claimed',
        resource=control['resource_id'][2:], epoch=9, role=planned['role'], incarnation=planned['incarnation'])
    backend(present, control, 'empty', preparer_backend['database'])
    assert (present['pid'], present['backend_start']) != (preparer_backend['pid'], preparer_backend['backend_start'])
    assert policy == dict(event='successor-policy', stage='empty', phase='acquired', sequence=None,
        control_epoch=9, config_revision=1, policy_revision=1,
        config_digest=only(current['config_revisions'], revision=1)['document_digest'][2:],
        policy_digest=only(current['policy_revisions'], revision=1)['document_digest'][2:])
    current = after
    same(current, c.snapshot('empty-before-read'))
    assert c.take('successor-empty-result', 'empty-load') == dict(event='successor-empty-result',
        stage='empty-load', sequence=None, error='Control(Backend)')
    assert c.take('successor-empty-control-reuse') == dict(event='successor-empty-control-reuse',
        available=False, error='Unavailable')
    for stage in ('after-reuse', 'after-read', 'before-control-end'):
        same(current, c.snapshot('empty-' + stage))
    assert c.take('successor-driver', 'empty') == dict(event='successor-driver', stage='empty', failed=False, joined=True)
    assert c.take('successor-backend', 'empty') == dict(present, present=False)
    assert c.take('successor-control', 'empty', 'dropped-and-joined') == dict(
        event='successor-control', stage='empty', phase='dropped-and-joined',
        resource=control['resource_id'][2:], epoch=9)
    for stage in ('after-control-end', 'final'):
        same(current, c.snapshot('empty-' + stage))
    assert c.take('successor-empty-complete') == dict(event='successor-empty-complete', command='empty-retire',
        native='closed', control='dropped-and-joined', admission=False, success=False)
    c.finish()
    # Reuse only the existing exact named-veto observation, not its job reducer.
    transport_and_veto(dict(wire=dict(status='NOT_RUN'), claim_guard=preparation['claim_guard']),
        present, 'empty-claim-commit', server_window)
    assert only(current['jobs'], job_id=JOB)['owner_epoch'] == 8
    return dict(status='PASS', mode=mode, processes=2, snapshots=c.snapshots + preparation_snapshots,
        preparation_snapshots=preparation_snapshots, refusal_snapshots=c.snapshots,
        control_tables=71, original_inputs=4, original_receipts_verified=len(known['receipts']),
        original_rows_and_xmin_preserved=True, immutable_intent_preserved=True,
        preparation_native_sequence_delta=4, retirement_native_sequence_delta=0,
        prepared_by_epoch=8, claim_owner_epoch=8, retirement_control_epoch=9,
        historical_registrar=known['registration']['control_epoch'], resolved_requests=[],
        result_error='Control(Backend)', admission=False,
        qualification='live guarded relational import claim; installed import execution is not exercised',
        final=dict(control_sha256=empty.control_digest(current), native_sha256=digest(encoded(native)),
            inputs_sha256=digest(encoded(inputs)), sequence=62, control_epoch=9, control_state='active'))


def negative_controls(events, original, native, inputs, mode, **context):
    """Reject semantic changes even when the altered full captures are rehashed."""
    baseline = dict(events=events, original=original, native=native, inputs=inputs, mode=mode, **context)
    verify(**baseline)
    rejected = []

    def check(name, change):
        candidate = copy.deepcopy(baseline)
        change(candidate)
        try:
            verify(**candidate)
        except (AssertionError, KeyError, ValueError, TypeError):
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted perturbation: ' + name)

    def observation(candidate, stage, preparation=False):
        source = candidate['preparation'] if preparation else candidate
        return only(source['events'], event='successor-state', stage=stage)

    def sql(candidate, stage, change, preparation=False):
        event = observation(candidate, stage, preparation)
        pg = tables(event)
        change(pg)
        event['state']['control_tables'] = [[name, encoded(pg[name]).decode()] for name in sorted(pg)]
        event['state']['control_sha256'] = digest(encoded(event['state']['control_tables']))

    def native_change(candidate, stage, preparation=False, close=False):
        event = observation(candidate, stage, preparation)
        if close:
            slot = only(candidate['original']['producer_slots'])['producer_slot_id'][2:]
            row = only(event['rows'], cf='receipts', key='0131' + slot + '0000000000000002')
            row['value'] = '0231' + '00' * 24 + '01'
            event['state']['sequence'] = 63
        else:
            row = only(event['rows'], cf='default', key='0101')
            value = bytearray.fromhex(row['value'])
            value[-1] ^= 1
            row['value'] = value.hex()
        event['state']['native_sha256'] = digest(encoded(event['rows']))

    check('frozen-original-control', lambda c: c['seed']['control']['producer_slots'][0]
        .update(registered_by_epoch=8))
    check('migration31-checksum', lambda c: only(c['preparation']['original']['schema_versions'], migration=31)
        .update(checksum='\\x' + '00' * 32))
    check('preparation-original-capture-hash', lambda c: observation(c, 'prepared', True)['state']
        .update(control_sha256='00' * 32))
    check('preparation-native-byte-rehashed', lambda c: native_change(c, 'prepared', True))
    check('prepared-original-outcome-xmin', lambda c: sql(c, 'prepared',
        lambda pg: pg['producer_outcomes'][0].update(xmin='4294967294'), True))
    check('prepared-intent-attribution', lambda c: sql(c, 'prepared',
        lambda pg: pg['initial_ingest_successions'][0].update(prepared_by_epoch=7), True))
    check('same-Control-retry-writes', lambda c: sql(c, 'same-control-retry',
        lambda pg: pg['initial_ingest_successions'][0].update(xmin='4294967294'), True))
    check('preparation-receipt-validation-missing', lambda c: only(c['preparation']['events'],
        event='successor-result', stage='prepared').update(validations=0))
    check('preparation-plan-original-digest', lambda c: only(c['preparation']['events'],
        event='successor-plan').update(closure_digest='00' * 32))
    check('claim-superuser', lambda c: c['preparation']['claim']['identity'].update(superuser=True))
    check('claim-replica-mode', lambda c: c['preparation']['claim']['identity'].update(session_replication_role='replica'))
    check('claim-declared-replica-bypass', lambda c: c['preparation']['claim']['statement'].__setitem__(
        0, "BEGIN; SET LOCAL session_replication_role='replica';"))
    check('claim-not-committed', lambda c: c['preparation']['claim']['transactions'][-1].update(sqlstate='23514'))
    check('claim-after-preparer-release', lambda c: c['preparation']['claim']['gates'][1]
        .update(server_us=c['server_window']['after_us']))
    check('claim-wrong-owner', lambda c: only(c['preparation']['claim']['after']['jobs'], job_id=JOB)
        .update(owner_epoch=9))
    check('claim-checkpoint-invented', lambda c: only(c['preparation']['claim']['after']['jobs'], job_id=JOB)
        .update(checkpoint_revision=1))
    check('claim-input-binding-invented', lambda c: only(c['preparation']['claim']['after']['jobs'], job_id=JOB)
        .update(input_topology_generation=1))
    check('claim-admission-xmin', lambda c: only(c['preparation']['claim']['after']['job_admissions'], job_id=JOB)
        .update(xmin='4294967294'))
    check('missing-preparer-gate', lambda c: c['preparation']['events'].remove(only(
        c['preparation']['events'], event='successor-preparer-gate', phase='waiting')))
    check('preparer-driver-not-joined', lambda c: only(c['preparation']['events'], event='successor-driver')
        .update(joined=False))
    check('preparer-backend-absence-another-tuple', lambda c: only(c['preparation']['events'],
        event='successor-backend', present=False).update(backend_start='another backend'))
    check('preparer-backend-still-present', lambda c: only(c['preparation']['events'],
        event='successor-backend', present=False).update(present=True))
    check('refusal-original-capture-hash', lambda c: observation(c, 'empty-final')['state']
        .update(control_sha256='00' * 32))
    check('refusal-native-byte-rehashed', lambda c: native_change(c, 'empty-final'))
    check('refusal-closes-native', lambda c: native_change(c, 'empty-final', close=True))
    check('refusal-changes-input', lambda c: observation(c, 'empty-final')['state']['originals'][0][1]
        .update(sha256='00' * 32))
    check('refusal-rewrites-original', lambda c: sql(c, 'empty-final',
        lambda pg: pg['producer_outcomes'][0].update(xmin='4294967294')))
    check('refusal-releases-target', lambda c: sql(c, 'empty-final',
        lambda pg: only(pg['owner_epochs'], role='ingest', epoch=2).update(state='released')))
    check('refusal-drops-claim', lambda c: sql(c, 'empty-final',
        lambda pg: pg['jobs'].remove(only(pg['jobs'], job_id=JOB))))
    check('refusal-reassigns-claim', lambda c: sql(c, 'empty-final',
        lambda pg: only(pg['jobs'], job_id=JOB).update(owner_epoch=9)))
    check('refusal-wrong-phase', lambda c: only(c['events'], event='successor-empty-result').update(stage='retirement'))
    check('refusal-wrong-error', lambda c: only(c['events'], event='successor-empty-result').update(error='Control(Unavailable)'))
    check('refusal-session-reusable', lambda c: only(c['events'], event='successor-empty-control-reuse').update(available=True))
    check('refusal-wrong-policy', lambda c: only(c['events'], event='successor-policy').update(policy_revision=2))
    check('refusal-driver-not-joined', lambda c: only(c['events'], event='successor-driver').update(joined=False))
    check('refusal-backend-absence-another-tuple', lambda c: only(c['events'],
        event='successor-backend', present=False).update(backend_start='another backend'))
    check('refusal-claims-admission', lambda c: only(c['events'], event='successor-empty-complete').update(admission=True))
    check('generic-Backend-without-named-veto', lambda c: c['preparation']['claim_guard'].update(errors=[]))
    check('named-veto-from-preparer', lambda c: c['preparation']['claim_guard']['backend']
        .update(pid=c['preparation']['claim']['control_backend']['pid']))
    return dict(status='PASS', rejected=rejected)
