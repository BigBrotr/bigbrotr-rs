"""Independent installed activated recovery over three frozen, qualified seeds.

Only the target producer marker and a missing PostgreSQL outcome copy may change,
apart from explicitly checked current Control lifetime and fixture policy/schema.
The old scientific bytes are golden inputs, not expectations read from this run.
This first wave does not qualify corruptions or ambiguous COMMIT recovery.
"""
import copy
import hashlib
import json
import math
import re

from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import catalog_identity, instant
from initial_history_oracle import inside_window
from initial_succession_oracle import only, same


CASES = ('installed-activated-off', 'installed-activated-on', 'installed-activated-zero')
MIGRATION_SHA256 = '4f22eb019e831126bf3f5c712d0ddf1e8213dfdfcdf9afaf5b38c80418cb5804'
FUNCTION_DIGESTS = {
    'control.require_activated_initial_recovery(bytea)': '8d0697cedacb7d21395d4429d076337217df8214eaac6f1547159c73f1105a23',
    'control.require_complete_initial_empty_retirement()': 'e930dbd823b7fa2a0f9af1f6f3fec2fbddc6b0a76213f87b16160118dd59801b',
}
GRANT = 'GRANT EXECUTE ON FUNCTION control.require_activated_initial_recovery(bytea) TO bigbrotr_control;'
# Canonical JSON hashes of the qualified, untouched seed captures. File hashes
# are separately pinned by the harness. No new recovery output feeds these pins.
SEEDS = {
    'off': dict(control='fca4878e5e12804fbff92cdc52c2b5fd69c9d6692de92289f3d2af899a3ddbb1',
        native='16eff648a6ae5e970fc18a0938c6fb39287af4c8b04678ab64717a0a6470f153',
        inputs='28bb113a743f8327830392c13fff29edfdcea38b66d82b332020d20c63a0618b',
        contents='04b56110c9b63a7df69df67b81bed4e3bd5c4a57c989b484d6a37e474debf6a1'),
    'on': dict(control='02cce88b93bdaa227fe2622945b265922335bd35ecc6ade28db9e3db101360ff',
        native='20ac7277bd06d88e408315801a9948debee72a810646b1479616cea099b1b03b',
        inputs='9b014b8bad3c997e584a29002211ff0c5c1217c3fc93bbcaab209352dbf34d0f',
        contents='399669dc8dc6e858743b249d4fada4e535f69625135eadcb2cea0db0b39f930a'),
    'zero': dict(control='f137d3c217a7cb747a090ae138ad184121cf014949d3b5ff7bb283b6f1d6ffd2',
        native='6d5196cd14b7aba5422009074fc4520e3805ea253563060a13ea750ef2da8f13',
        inputs='5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea',
        contents='d84cec99e2aee1d5b60fbdf13141ab7e5e0a5822cb68c17302d56bfa680d379b'),
}
REMOVE_COPY = ('BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM '
    'control.producer_outcomes WHERE producer_epoch=2 AND request_sequence=2 RETURNING '
    "(to_jsonb(producer_outcomes)||jsonb_build_object('xmin',xmin::text))::text; COMMIT;")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def selected(mode):
    for name in CASES:
        if mode == name:
            return name.removeprefix('installed-activated-'), 'first'
        for suffix in ('retry', 'repair'):
            if mode == name + '-' + suffix:
                assert suffix != 'repair' or name != CASES[-1]
                return name.removeprefix('installed-activated-'), suffix
    raise AssertionError('unselected installed recovery mode')


def known(seed, label):
    assert set(seed) == {'name', 'control', 'native', 'inputs', 'contents'}
    assert seed['name'] == label
    assert all(sha(encoded(seed[field])) == expected for field, expected in SEEDS[label].items())
    assert len(seed['control']) == 72
    expected_count, expected_seq, issued, source = {
        'off': (70, 84, 2, 5), 'on': (84, 98, 2, 9), 'zero': (52, 62, 0, 0)}[label]
    rows = native(seed['native'])
    assert len(rows) == expected_count and seed['native']['sequence'] == expected_seq
    intent = only(seed['control']['initial_ingest_successions'])
    slot = intent['producer_slot_id'][2:]
    key = ('receipts', bytes.fromhex('0131' + slot + '0000000000000002'))
    opened = bytes.fromhex('0131') + issued.to_bytes(8, 'big') + bytes(16)
    assert rows[key] == opened
    source_key = ('default', bytes.fromhex('0102' + intent['dataset_id'][2:] + '000000000000000000000002'))
    assert rows[source_key] == bytes.fromhex('0102') + source.to_bytes(8, 'big')
    # Literal version2 marker preserves issued/ack/retired and adds CLOSING=1.
    closed = dict(rows)
    closed[key] = bytes.fromhex('0231') + issued.to_bytes(8, 'big') + bytes(16) + b'\x01'
    return dict(opened=rows, closed=closed, sequence=expected_seq, issued=issued, intent=intent)


def migrated(seed, setup, before_clock):
    migration = setup['migration']
    assert migration['before'] == seed['control']
    wanted = copy.deepcopy(seed['control'])
    added, = [r for r in migration['after']['schema_versions'] if r not in wanted['schema_versions']]
    assert added['migration'] == 33 and added['name'] == '0033_activated_initial_recovery.sql'
    assert added['checksum'] == '\\x' + setup['migration_sha256']
    assert setup['migration_sha256'] == MIGRATION_SHA256
    assert added['engine'] == 'postgres-control' and added['applied_by'] == 'bigbrotr_migrator'
    assert added['reader_min'] == added['reader_max'] == added['writer_floor'] == 1
    assert type(added['applied_at_us']) is int and 0 < added['applied_at_us'] <= before_clock
    fresh_xmin(added['xmin'], wanted)
    assert added == dict(only(wanted['schema_versions'], migration=32), migration=33,
        name='0033_activated_initial_recovery.sql', checksum='\\x' + MIGRATION_SHA256,
        applied_at_us=added['applied_at_us'], xmin=added['xmin'])
    wanted['schema_versions'].append(added)
    same(wanted, migration['after'])
    assert migration['after'] == migration['reapplied']
    assert migration['physical_before'] == migration['physical_after']
    assert set(migration['physical_before']) == set(wanted)
    for response in migration['responses']:
        assert response['returncode'] == 0 and response['stderr'] == ''
    assert len(migration['responses']) == 2
    # The fully qualified old schema32 catalog is independent of this upgrade.
    catalog_identity(migration['catalog_before'])
    assert migration['grant']['statement'] == GRANT
    answer = migration['grant']['response']
    assert answer['returncode'] == 0 and answer['stdout'] == answer['stderr'] == ''
    previous, current = migration['catalog_before'], migration['catalog_after']
    assert set(current) == set(previous)
    assert all(current[k] == previous[k] for k in previous if k != 'functions')
    expected = {row['name']: row for row in previous['functions']}
    observed = {row['name']: row for row in current['functions']}
    assert len(expected) == len(previous['functions']) == 14
    assert len(observed) == len(current['functions']) == 15
    assert observed.keys() == expected.keys() | {'control.require_activated_initial_recovery(bytea)'}
    for name, row in observed.items():
        assert set(row) == {'name', 'definition', 'execute'} and row['execute'] is True
        if name in FUNCTION_DIGESTS:
            # Pin the complete expected PG16 definition, including language,
            # search_path, invoker semantics and exact reviewed SQL body.
            assert sha(row['definition'].encode()) == FUNCTION_DIGESTS[name]
        else:
            assert row == expected[name]
    return wanted


def policy(wanted, setup, label, before_clock):
    advanced = setup['policy']
    if label == 'zero':
        assert advanced is None
        return wanted
    assert advanced['authority'] == 'privileged-isolated-fixture'
    assert advanced['product_policy_activation'] is False and advanced['status'] == 'APPLIED'
    assert sha(advanced['sql'].encode()) == 'fe79b46a6b938e2f68082c0b16258420c253a27a2ff3e72fc9203ef4bd39202b'
    same(wanted, advanced['before'])
    window(advanced['server_window'])
    assert advanced['server_window']['after_us'] <= before_clock
    assert advanced['catalog_before'] == advanced['catalog_after'] == setup['migration']['catalog_after']
    answer = advanced['response']
    assert answer['returncode'] == 0 and answer['stderr'] == ''
    assert answer['stdout'].strip().isdecimal()
    actual = advanced['after']
    tx = only(actual['config_revisions'], revision=3)['xmin']
    fresh_xmin(tx, wanted)
    assert int(answer['stdout'].strip()) % (1 << 32) == int(tx)
    for table in ('config_revisions', 'policy_revisions'):
        previous = only(wanted[table], revision=2)
        observed = only(actual[table], revision=3)
        at = observed['accepted_at_us']
        inside_window(at, advanced['server_window'])
        value = dict(previous, revision=3, previous_revision=2, accepted_at_us=at, xmin=tx)
        if table == 'policy_revisions':
            value['config_revision'] = 3
        assert previous['document_digest'] == '\\x' + sha(b'bigbrotr/product-policy/2\0' + encoded(previous['document']))
        wanted[table].append(value)
    wanted['datasets'] = [dict(only(wanted['datasets']), config_revision=3, policy_revision=3, xmin=tx)]
    same(wanted, actual)
    return wanted


def baseline(before, native_before, inputs, seed, setup, label, attempt, server_window, predecessor):
    science = known(seed, label)
    if attempt == 'first':
        assert predecessor is None and setup['removed_copy'] is None
        wanted = migrated(seed, setup, server_window['before_us'])
        wanted = policy(wanted, setup, label, server_window['before_us'])
        same(wanted, before)
        assert native_before == seed['native']
    else:
        assert predecessor is not None and selected(predecessor['mode']) == (label, 'first' if attempt == 'retry' else 'retry')
        verify(**predecessor)
        assert predecessor['seed'] == seed
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        wanted = copy.deepcopy(predecessor['after'])
        assert native_before == predecessor['native_after']
        assert setup['migration'] == predecessor['setup']['migration']
        assert setup['policy'] == predecessor['setup']['policy']
        if attempt == 'repair':
            removed = only(wanted['producer_outcomes'], producer_epoch=2, request_sequence=2)
            assert setup['removed_copy']['row'] == removed
            assert setup['removed_copy']['statement'] == REMOVE_COPY
            answer = setup['removed_copy']['response']
            assert answer['returncode'] == 0 and answer['stderr'] == ''
            assert json.loads(answer['stdout']) == removed
            wanted['producer_outcomes'].remove(removed)
        else:
            assert setup['removed_copy'] is None
        same(wanted, before)
    assert inputs == seed['inputs']
    assert only(before['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'] < server_window['before_us']
    assert only(before['source_streams'], owner_epoch=2)['state'] == 'open'
    assert only(before['producer_epochs'], producer_epoch=2)['reserved_through_request'] == science['issued']
    return science


def backend_checked(backend, current, response, server_window):
    assert set(backend) == {'exact', 'absence'}
    absence = backend['absence']
    database = absence['database']
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    exact = backend['exact']
    expected = dict(pid=current['control_backend_pid'], backend_start=current['control_backend_start'],
        database=database, control_epoch=current['epoch'])
    assert exact['expected'] == expected
    assert exact['returncode'] == 0 and exact['stdout'].strip() == 'f' and exact['stderr'] == ''
    assert exact['present'] is False
    inside_window(instant(expected['backend_start']), server_window)
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['user'] == 'bigbrotr_control'
    assert absence['limit_seconds'] == 5 and math.isfinite(absence['seconds']) and 0 <= absence['seconds'] <= 120
    previous = 0
    for sample in absence['attempts']:
        assert previous <= sample['seconds'] <= 5
        previous = sample['seconds']
        assert type(sample['count']) is int and sample['count'] == len(sample['backends'])
        pids = []
        for row in sample['backends']:
            assert set(row) == {'pid', 'backend_start'} and type(row['pid']) is int and row['pid'] > 0
            assert instant(row['backend_start']) > 0
            pids.append(row['pid'])
        assert pids == sorted(set(pids))
    assert absence['attempts'][-1]['count'] == 0 and previous <= absence['seconds']
    assert response['terminal']['config'] == '/lab/' + database + '.toml'


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, predecessor=None):
    label, attempt = selected(mode)
    window(server_window)
    assert len(before) == len(after) == 72
    science = baseline(before, native_before, inputs_before, seed, setup, label, attempt, server_window, predecessor)
    assert setup['catalog_before'] == setup['catalog_after'] == setup['migration']['catalog_after']
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    for name, raw in input_contents.items():
        value = bytes.fromhex(raw)
        assert dict(bytes=len(value), sha256=sha(value)) == inputs_after[name]
    assert native(native_after) == science['closed']
    assert native_after['sequence'] == science['sequence'] + 1
    assert native(native_before) == (science['opened'] if attempt == 'first' else science['closed'])
    projected, current = control_delta(before, after, 'normal', server_window)
    wanted = copy.deepcopy(before)
    old = only(wanted['producer_epochs'], producer_epoch=2)
    actual = only(projected['producer_epochs'], producer_epoch=2)
    if attempt == 'first':
        assert old['state'] == 'open'
        fresh_xmin(actual['xmin'], before)
        assert actual['xmin'] != current['xmin']
        old.update(state='closing', xmin=actual['xmin'])
    else:
        assert old['state'] == 'closing'
    if attempt == 'repair':
        retained = only(seed['control']['producer_outcomes'], producer_epoch=2, request_sequence=2)
        restored = only(projected['producer_outcomes'], producer_epoch=2, request_sequence=2)
        inside_window(restored['recorded_at_us'], server_window)
        assert current['acquired_at_us'] <= restored['recorded_at_us'] <= current['released_at_us']
        fresh_xmin(restored['xmin'], before)
        assert restored['xmin'] != current['xmin']
        wanted['producer_outcomes'].append(dict(retained, recorder_resource=current['resource_id'],
            recorder_epoch=current['epoch'], recorder_incarnation=current['process_incarnation'],
            recorded_at_us=restored['recorded_at_us'], xmin=restored['xmin']))
    same(wanted, projected)
    intent = science['intent']
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-positive-outcomes-resolved',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2',
        issued_through=str(science['issued']), reserved_through=str(science['issued']),
        resolved_requests=str(science['issued']), source_closure='not-performed',
        native='closed', owner_release='committed', ready=False)
    assert response['outcome'] == 'PROCESS_EXITED' and response['returncode'] == 0 and response['stderr'] == ''
    terminal = response['terminal']
    assert 'output_limit_exceeded' not in response
    assert terminal['exit_code'] == 0 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--activated-successor']
    def unique(pairs):
        assert len(pairs) == len(dict(pairs)), 'duplicate report field'
        return dict(pairs)
    lines = response['stdout'].splitlines()
    assert len(lines) == 1 and encoded(json.loads(lines[0], object_pairs_hook=unique)) == encoded(report)
    backend_checked(backend, current, response, server_window)
    if predecessor:
        assert backend['absence']['database'] == predecessor['backend']['absence']['database']
        previous = predecessor['response']['terminal']
        assert (terminal['pid'], terminal['start_ticks']) != (previous['pid'], previous['start_ticks'])
        old = predecessor['backend']['exact']['expected']
        assert (current['control_backend_pid'], current['control_backend_start']) != (old['pid'], old['backend_start'])
        assert predecessor['server_window']['after_us'] <= instant(current['control_backend_start'])
    return dict(status='PASS', mode=mode, observed_states=2,
        control_tables=72, native_rows=len(science['closed']), native_sequence=native_after['sequence'],
        original_inputs=len(inputs_after), resolved_requests=science['issued'], repaired_outcome=attempt == 'repair',
        new_native_writes=int(attempt == 'first'), source_closed=False, ready=False)


def negative_controls(**context):
    """Mutate complete captures, including row versions; never patch a reducer."""
    verify(**context)
    label, attempt = selected(context['mode'])
    checks = []

    def reject(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        try:
            verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError):
            checks.append(name)
            return
        raise AssertionError('counterexample accepted: ' + name)

    reject('false-native-sequence', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('old-input-bytes', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    reject('lost-old-row', lambda c: c['after']['producer_outcomes'].pop(0))
    reject('source2-closed', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(state='closed'))
    reject('new-reservation', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(reserved_through_request=99))
    reject('activation-mutated', lambda c: only(c['after']['initial_ingest_activations']).update(activated_at_us=1))
    reject('old-recorder-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=1, request_sequence=1).update(recorder_epoch=99))
    reject('barrier-not-durable', lambda c: c['native_after'].update(rows=c['seed']['native']['rows']))
    reject('pg-not-closing', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(state='open'))
    reject('backend-still-present', lambda c: c['backend']['exact'].update(stdout='t\n', present=True))
    reject('unjoined-cli', lambda c: c['response']['terminal'].update(reaped=False))
    reject('wrong-cli-flag', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--retire-empty-attempt'))
    reject('false-report-count', lambda c: c['response'].update(stdout=c['response']['stdout'].replace('"2"', '"99"', 1)))
    reject('catalog-drift', lambda c: c['setup']['catalog_after']['triggers'].pop())
    if label != 'zero':
        def old_policy(c):
            only(c['after']['datasets']).update(config_revision=2, policy_revision=2)
        reject('recovered-under-historical-head', old_policy)
    if attempt == 'repair':
        reject('repair-issuer-instead-of-recorder', lambda c: only(c['after']['producer_requests'], producer_epoch=2, request_sequence=2).update(issuer_epoch=99))
        reject('repair-claims-current-policy', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(policy_epoch=3))
        reject('repair-original-recorder', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=8))
    return dict(status='PASS', rejected=checks, count=len(checks))
