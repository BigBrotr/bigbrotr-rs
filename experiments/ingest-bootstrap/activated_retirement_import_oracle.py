"""Exact live import attribution, complete source/job effects and named veto.

The import and the shifted HTTP admission ordinal are checked before a narrow
projection into the unchanged installed-live science/HTTP oracle. No lifecycle,
authority, native bytes, clock or outcome is normalized to make that oracle pass.
"""
import copy
import hashlib
import json
import math
import re
import tomllib

import activated_acquisition_oracle as acquisition
import activated_recovery_oracle as recovery
import activated_retirement_oracle as retirement
import activated_source_closure_oracle as source
import installed_live_oracle as live
from activated_recovery_faults_oracle import failed_control
from initial_empty_locks_oracle import app_identity, guards
from initial_empty_preparer_oracle import claim_statements, JOB, PRINCIPAL, REQUEST, DIGEST
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window

CASES = ('installed-activated-retirement-import',)
PHASES = ('source', 'acquisition', 'retirement')


def claim_checked(preparation):
    value = preparation['fixture']['after_activated']
    active = preparation['snapshots'][0]
    assert value['status'] == 'CAPTURED' and value['cleanup_errors'] == [] and 'failure' not in value
    window(value['server_window'])
    start, end = (value['server_window'][k] for k in ('before_us', 'after_us'))
    assert active['observed_by_us'] <= start <= value['created_at_us'] <= end <= value['observed_by_us']
    assert value['observed_by_us'] <= preparation['snapshots'][1]['server_us']
    current = only(value['before']['owner_epochs'], role='control', epoch=8)
    assert value['control'] == current and current['state'] == 'active'
    assert end < current['lease_expires_at_us']
    expected = dict(pid=current['control_backend_pid'], backend_start=current['control_backend_start'],
        database=preparation['backend']['absence']['database'], user='bigbrotr_control')
    assert value['live_before'] == value['live_after'] == expected
    assert live.instant(expected['backend_start']) <= current['acquired_at_us'] <= start
    actor = value['backend']
    assert actor['database'] == expected['database'] and actor['user'] == 'bigbrotr_control'
    assert actor['isolation'] == 'read committed' and actor['synchronous_commit'] == 'on'
    assert actor['application'] == 'retirement_import_claim' and actor['pid'] != expected['pid']
    assert type(actor['pid']) is int and actor['pid'] > 0
    assert start <= live.instant(actor['backend_start']) <= value['created_at_us']
    app_identity(value['identity'], actor)
    assert value['actor_absence'] == '0' and type(value['actor_exit_code']) is int
    assert value['inputs_before'] == value['inputs_after'] == preparation['inputs_before']
    guards(value['guards_before'])
    assert value['guards_after'] == value['guards_before'] == preparation['setup']['guards_after']
    assert value['statements'] == claim_statements(current, value['created_at_us'])
    answers = value['transactions']
    assert len(answers) == 5 and all(r['sqlstate'] == '00000' for r in answers)
    assert all(answers[i] == dict(sqlstate='00000', rows=0, lines=[]) for i in (0, 4))
    wanted = copy.deepcopy(active['control'])
    live.renewed(wanted, value['before'], active['control'], dict(
        before_us=preparation['server_window']['before_us'], after_us=start))
    same(wanted, value['before'])
    assert not any(r['job_id'] == JOB or r['request_key'] == REQUEST or r['principal_id'] == PRINCIPAL
        for r in wanted['jobs'])
    job = only(value['after']['jobs'], job_id=JOB)
    tx = job['xmin']
    fresh_xmin(tx, wanted)
    assert all(r['xmin'] != tx for table, rows in value['after'].items() for r in rows
        if table not in ('jobs', 'job_admissions') or r.get('job_id') != JOB)
    accepted = dict(job_id=JOB, dataset_id=current['dataset_id'], principal_id=PRINCIPAL,
        request_key=REQUEST, request_digest=DIGEST, kind='import', config_revision=2, policy_revision=2,
        created_at_us=value['created_at_us'], state='accepted', job_revision=1, checkpoint_revision=0,
        reason_code=None, result_manifest_digest=None, owner_resource_id=None, owner_epoch=None,
        owner_role='control', owner_incarnation=None, completed_at_us=None, input_topology_generation=None,
        input_complete=False, acquisition_job_protocol=None, export_job_protocol=None, xmin=tx)
    sealed = dict(accepted, input_complete=True)
    queued = dict(sealed, state='queued', job_revision=2, owner_resource_id=current['resource_id'],
        owner_epoch=8, owner_incarnation=current['process_incarnation'])
    assert job == queued
    for answer, row in zip(answers[1:4], (accepted, sealed, queued), strict=True):
        assert answer == dict(sqlstate='00000', rows=1, lines=answer['lines'])
        assert [json.loads(line) for line in answer['lines']] == [row]
    assert max(r['admission_ordinal'] for r in wanted['job_admissions']) == 2
    admission = dict(job_id=JOB, admission_ordinal=3, xmin=tx)
    wanted['jobs'].append(job)
    wanted['job_admissions'].append(admission)
    live.renewed(wanted, value['after'], value['before'], dict(
        before_us=preparation['server_window']['before_us'], after_us=value['observed_by_us']))
    same(wanted, value['after'])
    return job, admission


def preparation_checked(preparation):
    assert preparation['mode'] == 'installed-live-off'
    job, admission = claim_checked(preparation)
    projected = copy.deepcopy(preparation)
    projected['fixture'].pop('after_activated')
    assert preparation['fixture']['renewal']['current'] == preparation['snapshots'][3]

    def project(rows, phase):
        assert only(rows['jobs'], job_id=JOB) == job
        assert only(rows['job_admissions'], job_id=JOB) == admission
        for table in ('job_input_bounds', 'producer_input_bindings', 'producer_checkpoints', 'acquisition_jobs'):
            assert not any(r.get('job_id') == JOB for r in rows[table]), table
        assert not any(r.get('acquisition_job_id') == JOB for r in rows['producer_requests'])
        assert not any(r['producer_epoch'] == 2 for r in rows['producer_input_bindings'] + rows['producer_checkpoints'])
        rows['jobs'].remove(job)
        rows['job_admissions'].remove(admission)
        if phase != 'prepared':
            acquired = only(rows['jobs'], request_key='\\x' + 'f4' * 16)
            assigned = only(rows['job_admissions'], job_id=acquired['job_id'])
            assert assigned['admission_ordinal'] == 4
            assigned['admission_ordinal'] = 3

    for snapshot in projected['snapshots'][1:]:
        project(snapshot['control'], snapshot['stage'])
    project(projected['after'], 'final')
    projected['fixture']['renewal']['current'] = copy.deepcopy(projected['snapshots'][3])
    # The full original captures remain untouched and are persisted by caller.
    checked = live.verify(**projected)
    assert checked['native_rows'] == 70 and checked['native_sequence'] == 84
    assert checked['final_inputs'] == 6
    return checked


def ledger(before, migration, version, filename, sha, upper):
    assert migration['before'] == before
    assert not any(k in migration for k in ('outcome', 'incomplete_response'))
    assert len(migration['responses']) == 2 and all(r['returncode'] == 0 and not r['stderr'] for r in migration['responses'])
    row = only(migration['after']['schema_versions'], migration=version)
    fresh_xmin(row['xmin'], before)
    assert max(r['applied_at_us'] for r in before['schema_versions']) <= row['applied_at_us'] <= upper
    assert row == dict(only(before['schema_versions'], migration=version - 1), migration=version,
        name=filename, checksum='\\x' + sha, applied_at_us=row['applied_at_us'], xmin=row['xmin'])
    wanted = copy.deepcopy(before)
    wanted['schema_versions'].append(row)
    same(wanted, migration['after'])
    same(wanted, migration['reapplied'])
    assert migration['physical_before'] == migration['physical_after'] and set(migration['physical_before']) == set(wanted)
    return wanted


def upgrades(preparation, setup, clock):
    # Reuse exact additive33/34 checks without a fabricated producer identity.
    wanted = recovery.migrated(dict(control=preparation['after']), dict(migration=setup['recovery'],
        migration_sha256=recovery.MIGRATION_SHA256, correction_sha256=recovery.CORRECTION_SHA256), clock['before_us'])
    step = setup['source']
    wanted = ledger(wanted, step, 35, '0035_activated_source_closure.sql', source.MIGRATION, clock['before_us'])
    assert step['catalog_before'] == setup['recovery']['catalog_after']
    expected = copy.deepcopy(step['catalog_before'])
    expected['functions'].append(dict(name=source.SOURCE_FUNCTION, execute=False, definition=''))
    for row in expected['functions']:
        if row['name'] in source.FUNCTIONS:
            observed = only(step['catalog_ungranted']['functions'], name=row['name'])
            assert set(observed) == {'name', 'definition', 'execute'}
            assert recovery.sha(observed['definition'].encode()) == source.FUNCTIONS[row['name']]
            row['definition'] = observed['definition']
    expected['functions'].sort(key=lambda r: r['name'])
    assert step['catalog_ungranted'] == expected
    grant = setup['source_grant']
    assert grant['statement'] == source.GRANT and grant['before'] == expected
    assert grant['response'] == dict(returncode=0, stdout='', stderr='', outcome='PROCESS_EXITED')
    only(expected['functions'], name=source.SOURCE_FUNCTION)['execute'] = True
    assert grant['after'] == expected
    step = setup['acquisition']
    wanted = ledger(wanted, step, 36, '0036_activated_acquisition_abandonment.sql', acquisition.MIGRATION, clock['before_us'])
    assert step['grant_statements'] == []
    acquisition.catalog_old(step['catalog_before'], expected)
    acquisition.catalog_migrated(step['catalog_before'], step['catalog_after'])
    upgrade = setup['retirement']
    old = copy.deepcopy(upgrade['catalog_before'])
    extra = old.pop('retirement')
    assert old == step['catalog_after']
    assert extra['privileges'] == {name: dict(select=True, update=True, delete=False, truncate=False)
        for name in ('producer_epochs', 'source_streams', 'allocation_domains', 'owner_epochs', 'role_instances')}
    row, = extra['functions']
    assert row['name'] == 'control.guard_producer_epoch()' and row['execute'] is True
    assert recovery.sha(row['definition'].encode()) == retirement.OLD_PRODUCER_GUARD
    return retirement.migrated(dict(control=wanted, catalog=upgrade['catalog_before']), dict(
        migration=upgrade, migration_sha256=retirement.MIGRATION,
        catalog_before=upgrade['catalog_after'], catalog_after=upgrade['catalog_after']), clock)


def science(preparation):
    rows = native(preparation['native_after'])
    intent = only(preparation['after']['initial_ingest_successions'])
    slot = bytes.fromhex(intent['producer_slot_id'][2:])
    handoff, manifest = (bytes.fromhex(intent[k][2:]) for k in ('handoff', 'target_manifest'))
    digest = hashlib.sha256(b'bigbrotr/activated-initial-source-closure/1\0'
        + hashlib.sha256(handoff).digest() + hashlib.sha256(manifest).digest()
        + slot + (2).to_bytes(8, 'big') + (2).to_bytes(8, 'big'))
    for n in (1, 2):
        original = bytes.fromhex(preparation['input_contents'][slot.hex() + '-0000000000000002-' + f'{n:016x}.cbor'])
        receipt = rows['receipts', b'\x01\x30' + slot + (2).to_bytes(8, 'big') + n.to_bytes(8, 'big')]
        digest.update(n.to_bytes(8, 'big') + hashlib.sha256(original).digest() + hashlib.sha256(receipt).digest())
    digest.update((5).to_bytes(8, 'big'))
    key = ('receipts', b'\x01\x31' + slot + (2).to_bytes(8, 'big'))
    assert rows[key] == b'\x01\x31' + (2).to_bytes(8, 'big') + bytes(16)
    rows[key] = b'\x02\x31' + (2).to_bytes(8, 'big') + bytes(16) + b'\x01'
    return dict(intent=intent, digest=digest.hexdigest(), closed=rows)


def response_checked(response, mode, phase, report):
    assert response['outcome'] == 'PROCESS_EXITED'
    assert not any(k in response for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error', 'holder_error'))
    code = 4 if phase == 'retirement' else 0
    assert type(response['returncode']) is int and response['returncode'] == code
    assert response['stderr'] == ('error: producer operation unavailable: Control(Backend)\n' if code else '')
    assert math.isfinite(response['seconds']) and 0 <= response['seconds'] <= 120
    terminal = response['terminal']
    assert terminal['exit_code'] == code and terminal['reaped'] is True and terminal['forced'] is False
    assert type(terminal['pid']) is int and terminal['pid'] > 0 and type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    flag = {'source': '--close-activated-source', 'acquisition': '--abandon-activated-acquisition',
        'retirement': '--retire-activated-initial'}[phase]
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], flag]
    if code:
        assert response['stdout'] == ''
    else:
        def unique(pairs):
            assert len(pairs) == len(dict(pairs))
            return dict(pairs)
        assert len(response['stdout'].splitlines()) == 1
        assert json.loads(response['stdout'], object_pairs_hook=unique) == report


def named_veto(value, expected, clock):
    assert value['backend'] == expected and value['returncode'] == 0
    assert value['observed_sqlstate'] in (None, '23514')
    assert live.instant(value['since']) == clock['before_us'] and live.instant(value['until']) == clock['after_us']
    error, = value['errors']
    context, = value['contexts']
    prefix = r'\[' + str(expected['pid']) + r'\] '
    matched = re.search(prefix + r'ERROR:  (?:(?P<code>23514): )?activated producer import claims remain unresolved$', error)
    assert matched and value['observed_sqlstate'] == matched.group('code')
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function (?:control\.)?lock_activated_initial_retirement\(bytea\) line [1-9][0-9]* at RAISE', context)
    for line in (error, context):
        recovery.inside_window(live.instant(line.split()[0].replace('Z', '+00:00')), clock)


def verify(preparation, setup, attempts):
    live_result = preparation_checked(preparation)
    assert len(attempts) == 3 and [a['phase'] for a in attempts] == list(PHASES)
    known = science(preparation)
    expected = upgrades(preparation, setup, attempts[0]['server_window'])
    previous_time = preparation['server_window']['after_us']
    raw = preparation['native_after']
    contents, inputs = preparation['input_contents'], preparation['inputs_after']
    config = setup['configuration']
    actual = tomllib.loads(config['text'])
    database = attempts[0]['backend']['absence']['database']
    assert config['path'] == '/lab/' + database + '.toml'
    assert actual['postgres']['control'] == dict(actual['postgres']['control'],
        database=database, endpoint='127.0.0.1:5432', user='bigbrotr_control', password_ref='file:/run/secrets/application')
    assert actual['storage']['ingest_root'] == '/lab/installed-live-off/store'
    ds = only(preparation['seed']['control']['datasets'])
    assert actual['topology']['dataset_id'] == ds['dataset_id'][2:] and actual['topology']['instance_id'] == ds['instance_id'][2:]
    for key, value in dict(lease_ttl='20s', authorization_ttl='5s', renewal_timeout='1s', graceful_shutdown_timeout='2s').items():
        assert actual['runtime'][key] == value
    serve = setup['serve_configuration']
    assert serve['path'] == preparation['response']['terminal']['config'] == config['path']
    served = tomllib.loads(serve['text'])
    assert served['runtime']['lease_ttl'] == '6s' and served['storage']['ingest_root'] == actual['storage']['ingest_root']
    assert served['postgres']['control'] == actual['postgres']['control']
    controls, processes = [], []
    for index, item in enumerate(attempts):
        phase = item['phase']
        assert item['mode'] == CASES[0] + '-' + phase
        before, after, clock = item['before'], item['after'], item['server_window']
        window(clock)
        assert previous_time <= clock['before_us']
        same(expected, before['control'])
        assert before['native'] == raw and before['inputs'] == after['inputs'] == inputs
        assert before['contents'] == after['contents'] == contents
        assert item['catalog_before'] == item['catalog_after'] == setup['retirement']['catalog_after']
        wanted = copy.deepcopy(expected)
        if phase == 'retirement':
            projected, control = failed_control(expected, after['control'], 'close', clock)
        else:
            projected, control = control_delta(expected, after['control'], 'normal', clock)
        assert control['epoch'] == 9 + index
        role = only(after['control']['role_instances'], role_instance_id=control['role_instance_id'])
        if phase == 'source':
            assert native(after['native']) == known['closed'] and after['native']['sequence'] == 85
            producer = only(wanted['producer_epochs'], producer_epoch=2)
            observed = only(projected['producer_epochs'], producer_epoch=2)
            fresh_xmin(observed['xmin'], expected)
            assert observed['xmin'] not in (control['xmin'], role['xmin'])
            producer.update(state='closing', xmin=observed['xmin'])
            stream = only(wanted['source_streams'], owner_epoch=2)
            observed = only(projected['source_streams'], owner_epoch=2)
            fresh_xmin(observed['xmin'], expected)
            assert observed['xmin'] not in (control['xmin'], role['xmin'], producer['xmin'])
            at = observed['closed_at_us']
            assert control['lease_expires_at_us'] - 20_000_000 <= at <= control['released_at_us'] < control['lease_expires_at_us']
            stream.update(state='closed', last_complete_sequence=5, closure_digest='\\x' + known['digest'], closed_at_us=at, xmin=observed['xmin'])
        else:
            assert after['native'] == raw
        if phase == 'acquisition':
            job = only(wanted['jobs'], request_key='\\x' + 'f4' * 16)
            activation = only(wanted['initial_ingest_activations'])
            gap = only(projected['acquisition_gaps'], producer_epoch=2)
            fresh_xmin(gap['xmin'], expected)
            assert gap['xmin'] not in (control['xmin'], role['xmin'])
            at = gap['abandoned_at_us']
            assert max(only(wanted['source_streams'], owner_epoch=2)['closed_at_us'], job['created_at_us'],
                control['lease_expires_at_us'] - 20_000_000) <= at <= control['released_at_us'] < control['lease_expires_at_us']
            assert gap == dict(job_id=job['job_id'], dataset_id=job['dataset_id'],
                producer_slot_id=activation['producer_slot_id'], producer_epoch=2, shard_id=0, source_owner_epoch=2,
                last_complete_sequence=5, source_closure_digest='\\x' + known['digest'], previous_job_revision=2,
                previous_owner_resource=activation['activated_by_resource'], previous_owner_epoch=8,
                previous_owner_incarnation=activation['activated_by_incarnation'], issuer_resource=control['resource_id'],
                issuer_epoch=10, issuer_incarnation=control['process_incarnation'], owner_role='control',
                abandoned_at_us=at, reason=acquisition.REASON, xmin=gap['xmin'])
            wanted['acquisition_gaps'].append(gap)
            job.update(state='cancelled', job_revision=4, reason_code=acquisition.REASON, completed_at_us=at,
                owner_resource_id=control['resource_id'], owner_epoch=10, owner_incarnation=control['process_incarnation'], xmin=gap['xmin'])
        same(wanted, projected)
        report = dict(format='bigbrotr-ingest-recovery/1',
            state='activated-source-closed' if phase == 'source' else 'activated-acquisition-abandoned',
            dataset_id=known['intent']['dataset_id'][2:], owner_resource_id=known['intent']['owner_resource_id'][2:],
            producer_slot_id=known['intent']['producer_slot_id'][2:], producer_epoch='2',
            issued_through='2', reserved_through='2', resolved_requests='2',
            source_closure=dict(last_complete_sequence='5', sha256=known['digest']),
            native='closed', owner_release='committed', ready=False)
        if phase == 'acquisition':
            report['acquisition_abandonment'] = dict(jobs='1', reason=acquisition.REASON, coverage='unknown')
        response_checked(item['response'], item['mode'], phase, report)
        recovery.backend_checked(item['backend'], control, item['response'], clock)
        assert item['response']['terminal']['config'] == config['path']
        process = (item['response']['terminal']['pid'], item['response']['terminal']['start_ticks'])
        assert process not in processes
        processes.append(process)
        assert item['backend']['absence']['database'] == database
        if phase == 'retirement':
            named_veto(item['diagnostic'], item['backend']['exact']['expected'], clock)
        else:
            assert item['diagnostic'] is None
        identity = (control['control_backend_pid'], control['control_backend_start'])
        assert identity not in controls
        assert previous_time <= live.instant(identity[1])
        controls.append(identity)
        previous_time, expected, raw = clock['after_us'], after['control'], after['native']
    return dict(status='PASS', serve=live_result, cli_invocations=3, observed_states=6,
        control_tables=72, import_claim_epoch=8, import_requests=0, import_unchanged=True,
        issued=2, source_cut=5, source_closed=True, acquisition_abandoned=1,
        native_rows=70, native_sequence=85, original_inputs=6, producer_retired=False,
        refusal='activated producer import claims remain unresolved')


def negative_controls(**context):
    verify(**context)
    rejected = []
    def reject(name, edit):
        changed = copy.deepcopy(context)
        edit(changed)
        try:
            verify(**changed)
        except (AssertionError, KeyError, IndexError, TypeError, ValueError, StopIteration):
            rejected.append(name)
            return
        raise AssertionError('import retirement oracle accepted: ' + name)
    reject('historical-claim', lambda c: c['preparation']['fixture']['after_activated']['live_after'].update(pid=1))
    reject('replica-claim', lambda c: c['preparation']['fixture']['after_activated']['identity'].update(session_replication_role='replica'))
    reject('wrong-sql', lambda c: c['preparation']['fixture']['after_activated']['statements'].__setitem__(0, 'BEGIN; SET LOCAL session_replication_role=replica;'))
    reject('unjoined-claimant', lambda c: c['preparation']['fixture']['after_activated'].update(actor_absence='1'))
    reject('wrong-live-admission', lambda c: only(c['preparation']['after']['job_admissions'], job_id=JOB).update(admission_ordinal=4))
    reject('projected-extra-job', lambda c: c['preparation']['after']['jobs'].append(copy.deepcopy(c['preparation']['after']['jobs'][0])))
    reject('import-bound-request', lambda c: only(c['preparation']['after']['producer_requests'], producer_epoch=2, request_sequence=2).update(acquisition_job_id=JOB))
    reject('import-falsely-completed', lambda c: only(c['attempts'][-1]['after']['control']['jobs'], job_id=JOB).update(state='cancelled'))
    reject('named-veto-missing', lambda c: c['attempts'][-1]['diagnostic'].update(errors=[]))
    reject('contradictory-sqlstate', lambda c: c['attempts'][-1]['diagnostic'].update(
        observed_sqlstate=None if c['attempts'][-1]['diagnostic']['observed_sqlstate'] else '23514'))
    reject('wrong-veto-backend', lambda c: c['attempts'][-1]['diagnostic']['backend'].update(pid=1))
    reject('generic-error', lambda c: c['attempts'][-1]['response'].update(stderr='error: producer operation unavailable: Control(Unavailable)\n'))
    reject('outcome-repair', lambda c: only(c['attempts'][-1]['after']['control']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=11))
    reject('partial-retirement', lambda c: only(c['attempts'][-1]['after']['control']['producer_epochs'], producer_epoch=2).update(state='retired'))
    reject('source-digest', lambda c: only(c['attempts'][0]['after']['control']['source_streams'], owner_epoch=2).update(closure_digest='\\x' + '00' * 32))
    reject('gap-attribution', lambda c: only(c['attempts'][1]['after']['control']['acquisition_gaps'], producer_epoch=2).update(previous_owner_epoch=3))
    reject('native-write', lambda c: c['attempts'][-1]['after']['native'].update(sequence=86))
    reject('missing-input', lambda c: c['attempts'][-1]['after']['contents'].pop(next(iter(c['attempts'][-1]['after']['contents']))))
    reject('catalog-disabled', lambda c: c['attempts'][-1]['catalog_after'].update(session_replication_role='replica'))
    reject('backend-present', lambda c: c['attempts'][-1]['backend']['exact'].update(present=True))
    reject('unknown-process', lambda c: c['attempts'][-1]['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('unjoined-process', lambda c: c['attempts'][-1]['response']['terminal'].update(reaped=False))
    reject('wrong-config', lambda c: c['setup']['configuration'].update(text=c['setup']['configuration']['text'].replace('127.0.0.1:5432', '127.0.0.1:5433')))
    reject('missing-upgrade', lambda c: c['setup']['retirement']['after']['schema_versions'].remove(only(c['setup']['retirement']['after']['schema_versions'], migration=37)))
    return dict(status='PASS', rejected=len(rejected), controls=rejected)
