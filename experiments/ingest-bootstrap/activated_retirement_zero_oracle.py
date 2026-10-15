"""Zero issued and rejected source-zero are different retained positive proofs.

The frozen source-closure captures and literal rejected input determine the
expectation. Retirement shares only the independently checked twelve-row reducer;
no product receipt, nullable SQL column or reported count classifies an Event.
"""
import copy
import json
import math
import tomllib

import activated_acquisition_oracle as acquisition
import activated_retirement_oracle as retirement
import activated_source_closure_oracle as source
from activated_recovery_faults_oracle import failed_control
from activated_recovery_oracle import encoded, sha, backend_checked
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, native, window
from installed_live_oracle import instant

CASES = ('installed-activated-retirement-zero', 'installed-activated-retirement-rejected')
SEEDS = {
    'zero': '64d2da59fc8b1fdfdef9c8c0fb769e50034612d457db8eb604c6c5544c5be76f',
    'rejected': 'b6889707eb2d07cbf9eb3342d71f547ba0bcea3d8c59df05b52890943a0658f0',
}
REJECTED_INPUT = bytes.fromhex(
    '62696762726f74722f70726f64756365722d726571756573742f3100848350d0f0592de2d29a4e3d5c610317a0c0710201'
    '6561646d69740186018450309d9507c5b5c39e9947e1406482d51904508b0202d228b4ab60c5cc83e8907321e5038758'
    '20a669974f5d6f76e4668b0be874435f14be799c04f0e70e5ec3d0967f1babf6bb582079be667ef9dcbbac55a06295ce'
    '870b07029bfcdb2dce28d959f2815b16f817980101848161788261786082617462c3a982617462c3a9781c73616d652065'
    '76656e740a77697468206f726465726564207461677358409544d1fab8095f14d43137f841a400f0648cd614515f53bb5545'
    '749e373056381ce2963d43fdb5d10d9294846da6fc895d1fdfaf3417399a5c2699414877c18066696d706f72748450309d'
    '9507c5b5c39e9947e1406482d5190950141c2e90c8bc3621ad7695b877ad170801f6')


def selected(mode):
    if mode in (CASES[0], CASES[0] + '-retry'):
        return 'zero', mode.endswith('-retry')
    assert mode == CASES[1], 'unselected zero retirement case'
    return 'rejected', False


def seed_checked(seed):
    assert set(seed) == {'name', 'verification', 'control', 'native', 'inputs', 'contents', 'catalog'}
    name, retained = seed['name'], seed['verification']
    assert sha(encoded(retained)) == SEEDS[name]
    assert retained['mode'] == ('installed-source-zero-legacy-closed' if name == 'zero' else 'installed-source-rejected-retry')
    same(seed['control'], retained['after'])
    assert seed['native'] == retained['native_after']
    assert seed['inputs'] == retained['inputs_after'] and seed['contents'] == retained['input_contents']
    known = source.science(retained['seed'])
    issued = int(name == 'rejected')
    assert known['issued'] == issued and known['cut'] == 0
    assert native(seed['native']) == known['closed'] and len(known['closed']) == 52 + issued
    assert seed['native']['sequence'] == known['final_sequence'] == 63 + issued * 2
    assert len(seed['inputs']) == 4 + issued
    assert set(seed['inputs']) == set(seed['contents'])
    for filename, value in seed['contents'].items():
        raw = bytes.fromhex(value)
        assert seed['inputs'][filename] == dict(bytes=len(raw), sha256=sha(raw))
    rows = seed['control']
    assert len(rows) == 72
    assert max(r['migration'] for r in rows['schema_versions'] if r['migration'] < 100) == 35
    stream = only(rows['source_streams'], owner_epoch=2)
    assert stream['state'] == 'closed' and stream['last_complete_sequence'] == 0
    assert stream['closure_digest'] == '\\x' + known['digest']
    owner, role, producer, domains = retirement.target_rows(rows)
    assert owner['state'] == 'active' and role['retired_at_us'] is None
    assert producer['state'] == 'closing' and producer['reserved_through_request'] == issued
    assert producer['retired_at_us'] is None and producer['closure_digest'] is None
    assert all(r['state'] == 'active' and r['closed_at_us'] is None for r in domains)
    previous = only(rows['owner_epochs'], role='control', epoch=12)
    assert max(r['epoch'] for r in rows['owner_epochs'] if r['role'] == 'control') == 12
    assert previous['state'] == ('active' if name == 'zero' else 'released')
    activation = only(rows['initial_ingest_activations'])
    assert activation['activated_by_epoch'] == 8
    claimed = (activation['activated_by_resource'], 8, activation['activated_by_incarnation'])
    assert not any((r['owner_resource_id'], r['owner_epoch'], r['owner_incarnation']) == claimed for r in rows['jobs'])
    for table in ('acquisition_gaps', 'producer_input_bindings', 'producer_checkpoints'):
        assert not any(r['producer_epoch'] == 2 for r in rows[table])
    requests = [r for r in rows['producer_requests'] if r['producer_epoch'] == 2]
    outcomes = [r for r in rows['producer_outcomes'] if r['producer_epoch'] == 2]
    assert len(requests) == len(outcomes) == issued
    if issued:
        request, = requests
        outcome, = outcomes
        slot = bytes.fromhex(known['intent']['producer_slot_id'][2:])
        dataset = bytes.fromhex(known['intent']['dataset_id'][2:])
        original = seed['contents'][slot.hex() + '-0000000000000002-0000000000000001.cbor']
        assert original == REJECTED_INPUT.hex() and len(REJECTED_INPUT) == 327
        assert request['request_sequence'] == 1 and request['input_bytes'] == 327 and request['input_profile'] == 1
        assert request['request_digest'] == '\\x' + sha(REJECTED_INPUT)
        assert all(request[k] is None for k in ('relay_endpoint_id', 'relay_access_scope_id',
            'relay_enrollment_generation', 'relay_route_id', 'acquisition_scope_id', 'acquisition_job_id'))
        rejection = (b'bigbrotr/rejection/1\0' + slot + (2).to_bytes(8, 'big') + (1).to_bytes(8, 'big')
            + bytes.fromhex(sha(REJECTED_INPUT)) + dataset + (1).to_bytes(8, 'big') + b'\x04')
        assert outcome['outcome_kind'] == 3 and outcome['outcome_bytes'] == '\\x' + rejection.hex()
        assert outcome['outcome_sha256'] == '\\x' + sha(rejection)
        assert known['closed']['receipts', b'\x01\x30' + slot + (2).to_bytes(8, 'big') + (1).to_bytes(8, 'big')] == rejection
    old = copy.deepcopy(seed['catalog'])
    extra = old.pop('retirement')
    acquisition.catalog_old(old, retained['setup']['catalog_after'])
    row, = extra['functions']
    assert row['name'] == 'control.guard_producer_epoch()' and row['execute'] is True
    assert set(row) == {'name', 'execute', 'definition'} and sha(row['definition'].encode()) == retirement.OLD_PRODUCER_GUARD
    assert set(extra) == {'functions', 'privileges'}
    assert set(extra['privileges']) == {'producer_epochs', 'source_streams', 'allocation_domains', 'owner_epochs', 'role_instances'}
    assert all(r == dict(select=True, update=True, delete=False, truncate=False) for r in extra['privileges'].values())
    return dict(known, source=stream)


def migrated(seed, setup, clock):
    old_catalog = copy.deepcopy(seed['catalog'])
    old_catalog.pop('retirement')
    first = setup['acquisition_upgrade']
    wanted = acquisition.migrated(dict(seed, catalog=old_catalog), dict(
        migration_sha256=setup['acquisition_migration_sha256'], migration=first,
        catalog_before=first['catalog_after'], catalog_after=first['catalog_after']), clock)
    # Only the unchanged retirement catalog extension is added between the two
    # established additive-migration checkers. Neither old rowset is projected.
    catalog36 = dict(first['catalog_after'], retirement=seed['catalog']['retirement'])
    return retirement.migrated(dict(seed, control=wanted, catalog=catalog36), setup, clock)


def configuration_checked(setup, seed, response, backend, mode):
    config = setup['configuration']
    assert set(config) == {'path', 'text', 'sha256'} and sha(config['text'].encode()) == config['sha256']
    assert config['path'] == response['terminal']['config'] == '/lab/' + backend['absence']['database'] + '.toml'
    actual = tomllib.loads(config['text'])
    pg = actual['postgres']['control']
    assert pg['database'] == backend['absence']['database'] and pg['endpoint'] == '127.0.0.1:5432'
    assert pg['user'] == 'bigbrotr_control' and pg['password_ref'] == 'file:/run/secrets/application'
    dataset = only(seed['control']['datasets'])
    assert actual['topology']['dataset_id'] == dataset['dataset_id'][2:]
    assert actual['topology']['instance_id'] == dataset['instance_id'][2:]
    assert actual['storage']['ingest_root'] == '/lab/' + mode.removesuffix('-retry') + '/store'
    assert actual['runtime']['lease_ttl'] == '20s' and actual['runtime']['authorization_ttl'] == '5s'
    assert actual['runtime']['renewal_timeout'] == '1s' and actual['runtime']['graceful_shutdown_timeout'] == '2s'


def refused(response, mode):
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int and response['returncode'] == 4
    assert response['stdout'] == '' and response['stderr'] == 'error: producer operation unavailable: Outcome(Conflict)\n'
    assert not any(k in response for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error', 'holder_error'))
    assert math.isfinite(response['seconds']) and 0 <= response['seconds'] <= 120
    terminal = response['terminal']
    assert terminal['exit_code'] == 4 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--retire-activated-initial']


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, predecessor=None):
    name, retry = selected(mode)
    assert seed['name'] == name
    window(server_window)
    known = seed_checked(seed)
    expected = migrated(seed, setup, server_window)
    assert seed['verification']['server_window']['after_us'] <= server_window['before_us']
    if retry:
        assert predecessor is not None and predecessor['mode'] == CASES[0]
        assert predecessor['seed'] == seed and predecessor['setup'] == setup
        verify(**predecessor)
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        expected = predecessor['after']
    else:
        assert predecessor is None
    same(before, expected)
    assert len(before) == len(after) == 72
    assert native_before == native_after == seed['native']
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    wanted = copy.deepcopy(before)
    if name == 'zero':
        projected, current = control_delta(before, after, 'normal' if retry else 'fresh-completion', server_window)
        assert current['epoch'] == (14 if retry else 13)
        if not retry:
            at, xid = retirement.retire_targets(wanted, after, known, current, server_window, released=True)
            assert xid != only(after['owner_epochs'], role='control', epoch=12)['xmin'], 'retirement is not the claim/fence transaction'
        else:
            at = only(wanted['producer_epochs'], producer_epoch=2)['retired_at_us']
        intent = known['intent']
        report = dict(format='bigbrotr-ingest-recovery/1', state='activated-initial-retired',
            dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
            producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2', issued_through='0',
            reserved_through='0', resolved_requests='0',
            source_closure=dict(last_complete_sequence='0', sha256=known['digest']),
            ingest_retirement=dict(allocation_domains='9', acquisition_gaps='0', owner_release='committed',
                native_acknowledged='0', native_retired='0'), native='closed', owner_release='committed', ready=False)
        retirement.response_checked(response, mode, report)
    else:
        projected, current = failed_control(before, after, 'retirement', server_window)
        assert current['epoch'] == 13
        at = None
        refused(response, mode)
    same(wanted, projected)
    configuration_checked(setup, seed, response, backend, mode)
    backend_checked(backend, current, response, server_window)
    if retry:
        assert backend['absence']['database'] == predecessor['backend']['absence']['database']
        previous = predecessor['response']['terminal']
        assert (response['terminal']['pid'], response['terminal']['start_ticks']) != (previous['pid'], previous['start_ticks'])
        old = predecessor['backend']['exact']['expected']
        assert (current['control_backend_pid'], current['control_backend_start']) != (old['pid'], old['backend_start'])
        assert predecessor['server_window']['after_us'] <= instant(current['control_backend_start'])
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72,
        issued=known['issued'], source_cut=0, native_rows=len(native(native_after)), native_sequence=native_after['sequence'],
        original_inputs=len(inputs_after), target_rows_changed=12 if name == 'zero' and not retry else 0,
        native_writes=0, source_unchanged=True, jobs_unchanged=True, retained_acquisition_gaps=0,
        producer_retired=name == 'zero', retired_at_us=at, event_without_job_refused=name == 'rejected', ready=False)


def negative_controls(**context):
    checks = []

    def reject(label, edit):
        altered = copy.deepcopy(context)
        edit(altered)
        try:
            verify(**altered)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError):
            checks.append(label)
            return
        raise AssertionError('accepted corrupted retirement evidence: ' + label)

    def target(c):
        return retirement.target_rows(c['after'])

    reject('seed-native-is-not-portable-authority', lambda c: c['seed']['verification']['native_after'].update(sequence=0))
    reject('source-cut-changed', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(last_complete_sequence=1))
    reject('source-row-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin='999999999'))
    reject('predecessor-source-changed', lambda c: only(c['after']['source_streams'], owner_epoch=1).update(closure_digest='\\x' + '00' * 32))
    reject('native-write', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('lost-retained-input', lambda c: c['input_contents'].pop(next(iter(c['input_contents']))))
    reject('unrelated-job-changed', lambda c: c['after']['jobs'][0].update(job_revision=99))
    reject('changed-activation', lambda c: only(c['after']['initial_ingest_activations']).update(activated_by_epoch=3))
    reject('role-identity-rewritten', lambda c: target(c)[1].update(created_at_us=0))
    reject('disabled-role-guard', lambda c: only(c['setup']['catalog_after']['triggers'], name='activated_retirement_role_guard', table='role_instances').update(enabled='D'))
    reject('new-runtime-grant', lambda c: c['setup']['migration']['grant_statements'].append('GRANT ALL'))
    reject('missing-migration36', lambda c: c['setup']['acquisition_upgrade']['after']['schema_versions'].remove(
        only(c['setup']['acquisition_upgrade']['after']['schema_versions'], migration=36)))
    reject('wrong-action', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--retire-empty-attempt'))
    reject('unjoined-child', lambda c: c['response']['terminal'].update(reaped=False))
    reject('unknown-outcome', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('clock-unavailable', lambda c: c['response'].update(clock_error='TimeoutExpired'))
    reject('backend-remains', lambda c: c['backend']['exact'].update(present=True))
    reject('wrong-database', lambda c: c['backend']['absence'].update(database='wrong_database'))
    def different_store(c):
        config = c['setup']['configuration']
        config['text'] = config['text'].replace('/lab/' + c['mode'].removesuffix('-retry') + '/store', '/lab/foreign/store')
        config['sha256'] = sha(config['text'].encode())
    reject('rehashed-foreign-native-path', different_store)
    if selected(context['mode'])[0] == 'zero':
        reject('partial-retirement', lambda c: target(c)[3][0].update(state='active', closed_at_us=None, closure_digest=None))
        reject('split-terminal-time', lambda c: target(c)[1].update(retired_at_us=target(c)[2]['retired_at_us'] + 1))
        reject('terminal-digest-changed', lambda c: target(c)[0].update(exclusion_digest='\\x' + '00' * 32))
        reject('zero-reported-as-one', lambda c: c['response'].update(stdout=json.dumps(dict(json.loads(c['response']['stdout']), issued_through='1')) + '\n'))
        if selected(context['mode'])[1]:
            reject('retry-rewrites-terminal-row', lambda c: target(c)[2].update(xmin='999999999'))
        else:
            reject('departed-control-not-fenced', lambda c: c['after']['owner_epochs'].__setitem__(
                c['after']['owner_epochs'].index(only(c['after']['owner_epochs'], role='control', epoch=12)),
                copy.deepcopy(only(c['before']['owner_epochs'], role='control', epoch=12))))
            def retime(c, at, xid):
                owner, role, producer, domains = target(c)
                owner.update(released_at_us=at, xmin=xid)
                role.update(retired_at_us=at, xmin=xid)
                producer.update(retired_at_us=at, xmin=xid)
                for row in domains:
                    row.update(closed_at_us=at, xmin=xid)
            reject('coherent-target-claim-transaction-alias', lambda c: retime(c, target(c)[2]['retired_at_us'],
                only(c['after']['owner_epochs'], role='control', epoch=12)['xmin']))
            def expired_retirement(c):
                current = only(c['after']['owner_epochs'], role='control', epoch=13)
                at = current['lease_expires_at_us']
                retime(c, at, target(c)[2]['xmin'])
                current['released_at_us'] = at + 1
                only(c['after']['role_instances'], role_instance_id=current['role_instance_id'])['retired_at_us'] = at + 2
                c['server_window']['after_us'] = at + 3
            reject('coherent-retirement-after-authority-expiry', expired_retirement)
    else:
        reject('generic-backend-error', lambda c: c['response'].update(stderr='error: producer operation unavailable: Control(Backend)\n'))
        reject('rejected-copy-repaired', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2).update(recorder_epoch=13))
        reject('issued-erased-as-empty', lambda c: target(c)[2].update(reserved_through_request=0))
        reject('event-job-fabricated', lambda c: only(c['after']['producer_requests'], producer_epoch=2).update(acquisition_job_id='\\x' + 'ff' * 16))
        reject('refusal-changes-producer', lambda c: target(c)[2].update(state='retired', retired_at_us=c['server_window']['before_us']))
    return dict(status='PASS', rejected=len(checks), controls=checks)
