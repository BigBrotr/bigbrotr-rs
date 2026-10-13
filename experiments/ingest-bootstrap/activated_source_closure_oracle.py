"""Independent source2 digest, lifecycle and full retained-state expectations.

The new digest is a fixed-width recipe over previously qualified exact native
and input bytes. No product codec or observed close report provides expectations.
"""
import copy
import hashlib
import json
import math
import re

import activated_recovery_oracle as recovery
from activated_recovery_faults_oracle import failed_control
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import instant


CASES = tuple('installed-source-' + name for name in ('off', 'on', 'zero', 'rejected', 'missing-input'))
MIGRATION = '80b5441cbb2de1f782ce6458e1c2b26a2f3730f51e61619af49f1e8d8adc375a'
GRANT = 'GRANT EXECUTE ON FUNCTION control.require_activated_initial_source(bytea) TO bigbrotr_control;'
SOURCE_FUNCTION = 'control.require_activated_initial_source(bytea)'
DISPATCH = 'control.require_complete_initial_empty_retirement()'
FUNCTIONS = {SOURCE_FUNCTION: '180c250171891fd5385f71973c21d259763bb240d10c1bb0040b0745f8b0f168',
    DISPATCH: '278242f349e9ded7da858e0ae835b6f93c84f5223e60fec68416b90ee72f3028'}
REJECTED = dict(control='2acd5c60df609ffe44740f96351292feecbf0dcecd147cd4827790c4e4addd2e',
    native='f272da9670956ee5f70aa21f154b65a1e22125a287cbf5e2c2ba2758e1a83c1b',
    inputs='5b249160ed1e85714dfe8f486883d435449f160f7d1ca016075ece09f5d84f88',
    contents='e8fa3739de74540a8c47ed26d1a587ae1669acd05e8c022ed0e86dfaf0809655')
DRAIN = """BEGIN;
SET LOCAL session_replication_role=replica;
UPDATE control.owner_epochs SET state='draining' WHERE role='ingest' AND epoch=2 AND state='active'
RETURNING (to_jsonb(owner_epochs)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
"""
MIXED = """BEGIN;
SET LOCAL session_replication_role=replica;
UPDATE control.producer_epochs SET state='open' WHERE producer_epoch=2 AND state='closing'
RETURNING (to_jsonb(producer_epochs)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
"""
REFUSALS = ('closed-missing-copy', 'mixed', 'legacy-closed', 'missing-input')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def selected(mode):
    for base in CASES:
        if mode == base:
            return base.removeprefix('installed-source-'), 'first'
        for suffix in ('retry', 'closed-missing-copy', 'mixed', 'legacy-open', 'legacy-closed'):
            if mode == base + '-' + suffix:
                name = base.removeprefix('installed-source-')
                assert suffix == 'retry' and name != 'missing-input' or (name, suffix) in (
                    ('off', 'closed-missing-copy'), ('on', 'mixed'), ('zero', 'legacy-open'), ('zero', 'legacy-closed'))
                return name, suffix
    raise AssertionError('unselected source closure mode')


def science(seed):
    name = seed['name']
    if name == 'rejected':
        assert set(seed) == {'name', *REJECTED}
        assert all(sha(recovery.encoded(seed[k])) == value for k, value in REJECTED.items())
        issued, cut, sequence = 1, 0, 65
        opened = closed = native(seed['native'])
        intent = only(seed['control']['initial_ingest_successions'])
    else:
        known = recovery.known(seed, name)
        issued, sequence, intent = known['issued'], known['sequence'], known['intent']
        cut = {'off': 5, 'on': 9, 'zero': 0}[name]
        opened, closed = known['opened'], known['closed']
    slot, dataset = (bytes.fromhex(intent[k][2:]) for k in ('producer_slot_id', 'dataset_id'))
    manifest = opened['default', b'\x01\x01']
    handoff = opened['default', b'\x01\x0a' + dataset + bytes.fromhex(intent['owner_resource_id'][2:]) + (1).to_bytes(8, 'big')]
    assert sha(manifest) == intent['target_manifest_sha256'][2:]
    assert handoff.hex() == intent['handoff'][2:] and sha(handoff) == intent['handoff_sha256'][2:]
    assert opened['default', b'\x01\x02' + dataset + bytes(4) + (2).to_bytes(8, 'big')] == b'\x01\x02' + cut.to_bytes(8, 'big')
    digest = hashlib.sha256(b'bigbrotr/activated-initial-source-closure/1\0')
    digest.update(bytes.fromhex(sha(handoff)))
    digest.update(bytes.fromhex(sha(manifest)))
    digest.update(slot)
    digest.update((2).to_bytes(8, 'big'))
    digest.update(issued.to_bytes(8, 'big'))
    for number in range(1, issued + 1):
        name = slot.hex() + '-0000000000000002-' + format(number, '016x') + '.cbor'
        original = bytes.fromhex(seed['contents'][name])
        outcome = opened['receipts', b'\x01\x30' + slot + (2).to_bytes(8, 'big') + number.to_bytes(8, 'big')]
        request = only(seed['control']['producer_requests'], producer_epoch=2, request_sequence=number)
        copied = only(seed['control']['producer_outcomes'], producer_epoch=2, request_sequence=number)
        assert request['request_digest'] == copied['request_digest'] == '\\x' + sha(original)
        assert copied['outcome_bytes'] == '\\x' + outcome.hex() and copied['outcome_sha256'] == '\\x' + sha(outcome)
        assert request['manifest_sha256'] == copied['manifest_sha256'] == '\\x' + sha(manifest)
        digest.update(number.to_bytes(8, 'big'))
        digest.update(bytes.fromhex(sha(original)))
        digest.update(bytes.fromhex(sha(outcome)))
    digest.update(cut.to_bytes(8, 'big'))
    return dict(opened=opened, closed=closed, issued=issued, cut=cut, sequence=sequence,
        final_sequence=sequence + int(opened != closed), intent=intent, digest=digest.hexdigest())


def catalog34(catalog):
    normalized = copy.deepcopy(catalog)
    identities = set()
    for trigger in normalized['triggers']:
        key = trigger['table'], trigger['name']
        assert key not in identities
        identities.add(key)
        if trigger['internal']:
            assert re.fullmatch(r'RI_ConstraintTrigger_[ac]_[1-9][0-9]*', trigger['name'])
            trigger['name'] = trigger['name'].rsplit('_', 1)[0]
    normalized['triggers'].sort(key=recovery.encoded)
    # Qualified ced4c42 catalog; only generated internal FK name suffixes vary.
    assert sha(recovery.encoded(normalized)) == 'bd703a8f9945b2aa228026cd583594b4898900c1faf80cefb953c8f91384341a'


def migrated(seed, setup, clock):
    assert setup['migration_sha256'] == recovery.MIGRATION_SHA256
    assert setup['correction_sha256'] == recovery.CORRECTION_SHA256
    if seed['name'] == 'rejected':
        assert setup['recovery_upgrade'] is None and setup['policy'] is None
        wanted = copy.deepcopy(seed['control'])
    else:
        old = dict(migration=setup['recovery_upgrade'], migration_sha256=setup['migration_sha256'],
            correction_sha256=setup['correction_sha256'], policy=setup['policy'])
        wanted = recovery.migrated(seed, old, clock['before_us'])
        wanted = recovery.policy(wanted, old, seed['name'], clock['before_us'])
    upgrade = setup['source_upgrade']
    same(wanted, upgrade['before'])
    assert setup['source_migration_sha256'] == MIGRATION
    added = only(upgrade['after']['schema_versions'], migration=35)
    fresh_xmin(added['xmin'], wanted)
    assert type(added['applied_at_us']) is int
    assert max(r['applied_at_us'] for r in wanted['schema_versions']) <= added['applied_at_us'] <= clock['before_us']
    assert added == dict(only(wanted['schema_versions'], migration=34), migration=35,
        name='0035_activated_source_closure.sql', checksum='\\x' + MIGRATION,
        applied_at_us=added['applied_at_us'], xmin=added['xmin'])
    wanted['schema_versions'].append(added)
    same(wanted, upgrade['after'])
    assert upgrade['after'] == upgrade['reapplied']
    assert upgrade['physical_before'] == upgrade['physical_after'] and set(upgrade['physical_before']) == set(wanted)
    assert len(upgrade['responses']) == 2
    assert all(r['returncode'] == 0 and r['stderr'] == '' for r in upgrade['responses'])
    catalog34(upgrade['catalog_before'])
    old, new = upgrade['catalog_before'], upgrade['catalog_ungranted']
    assert set(old) == set(new) and all(old[k] == new[k] for k in old if k != 'functions')
    expected = {r['name']: r for r in old['functions']}
    actual = {r['name']: r for r in new['functions']}
    assert len(expected) == len(old['functions']) == 15
    assert len(actual) == len(new['functions']) == 16 and actual.keys() == expected.keys() | {SOURCE_FUNCTION}
    for name, value in actual.items():
        if name in FUNCTIONS:
            assert set(value) == {'name', 'definition', 'execute'}
            assert sha(value['definition'].encode()) == FUNCTIONS[name]
            assert value['execute'] is (name != SOURCE_FUNCTION)
        else:
            assert value == expected[name]
    return wanted


def catalogs(setup, attempt):
    old = setup['source_upgrade']['catalog_ungranted']
    if attempt == 'legacy-open':
        assert setup['grant'] is None
        expected = old
    else:
        grant = setup['grant']
        assert grant['before'] == old and grant['statement'] == GRANT
        answer = grant['response']
        assert answer['returncode'] == 0 and answer['stdout'] == answer['stderr'] == ''
        expected = copy.deepcopy(old)
        only(expected['functions'], name=SOURCE_FUNCTION)['execute'] = True
        assert grant['after'] == expected
    assert setup['catalog_before'] == setup['catalog_after'] == expected


def operator(wanted, raw, contents, setup, name, attempt):
    needed = 'missing-input' if name == 'missing-input' else (
        'missing_copy' if (name, attempt) in (('off', 'first'), ('off', 'closed-missing-copy')) else
        'drain' if (name, attempt) == ('on', 'first') else 'mixed' if attempt == 'mixed' else None)
    mutation = setup['operator']
    if needed is None:
        assert mutation is None
        return wanted, contents
    assert set(mutation) == {'preimage', 'operation', 'after'}
    def census(values):
        return {n: dict(bytes=len(bytes.fromhex(v)), sha256=sha(bytes.fromhex(v))) for n, v in values.items()}
    def exact_state(observed, control, originals):
        assert set(observed) == {'control', 'native', 'inputs', 'contents'}
        same(control, observed['control'])
        assert observed['native'] == raw and observed['contents'] == originals
        assert observed['inputs'] == census(originals)
    exact_state(mutation['preimage'], wanted, contents)
    wanted, contents = copy.deepcopy(wanted), dict(contents)
    op = mutation['operation']
    if needed == 'missing-input':
        slot = only(wanted['initial_ingest_successions'])['producer_slot_id'][2:]
        filename = slot + '-0000000000000002-0000000000000002.cbor'
        original = contents.pop(filename)
        value = bytes.fromhex(original)
        assert op == dict(kind='input', action='remove', name=filename,
            retained=dict(path='/lab/installed-source-missing-input-input-original.cbor',
                bytes=len(value), sha256=sha(value), hex=original, mode=384),
            before_mode=384, after_mode=None, replacement_hex=None)
    else:
        assert op['kind'] == needed and op['authority'] == 'privileged-isolated-fixture'
        assert op['product_transition'] is False
        assert op['statement'] == {'missing_copy': recovery.REMOVE_COPY, 'drain': DRAIN, 'mixed': MIXED}[needed]
        answer = op['response']
        assert answer['returncode'] == 0 and answer['stderr'] == '' and answer['outcome'] == 'PROCESS_EXITED'
        assert json.loads(answer['stdout']) == op['changed']
        if needed == 'missing_copy':
            row = only(wanted['producer_outcomes'], producer_epoch=2, request_sequence=2)
            assert row == op['changed']
            wanted['producer_outcomes'].remove(row)
        else:
            table = 'owner_epochs' if needed == 'drain' else 'producer_epochs'
            row = only(wanted[table], **({'role': 'ingest', 'epoch': 2} if needed == 'drain' else {'producer_epoch': 2}))
            assert row['state'] == ('active' if needed == 'drain' else 'closing')
            fresh_xmin(op['changed']['xmin'], wanted)
            assert op['changed'] == dict(row, state='draining' if needed == 'drain' else 'open', xmin=op['changed']['xmin'])
            wanted[table][wanted[table].index(row)] = op['changed']
    exact_state(mutation['after'], wanted, contents)
    return wanted, contents


def baseline(before, native_before, inputs_before, seed, setup, name, attempt, clock, predecessor):
    known = science(seed)
    assert seed['name'] == ('off' if name == 'missing-input' else name)
    if predecessor is None:
        assert attempt == ('legacy-open' if name == 'zero' else 'first')
        wanted = migrated(seed, setup, clock)
        raw, contents = seed['native'], seed['contents']
    else:
        verify(**predecessor)
        prior_name, prior_attempt = selected(predecessor['mode'])
        assert prior_name == name
        assert prior_attempt == {'first': 'legacy-open', 'retry': 'first',
            'closed-missing-copy': 'retry', 'mixed': 'retry', 'legacy-closed': 'retry'}[attempt]
        assert predecessor['seed'] == seed and predecessor['server_window']['after_us'] <= clock['before_us']
        for key in ('recovery_upgrade', 'source_upgrade', 'migration_sha256', 'correction_sha256',
                'source_migration_sha256', 'policy'):
            assert setup[key] == predecessor['setup'][key]
        if prior_attempt != 'legacy-open':
            assert setup['grant'] == predecessor['setup']['grant']
        wanted, raw, contents = predecessor['after'], predecessor['native_after'], predecessor['input_contents']
    wanted, contents = operator(wanted, raw, contents, setup, name, attempt)
    same(wanted, before)
    assert raw == native_before
    assert inputs_before == {n: dict(bytes=len(bytes.fromhex(v)), sha256=sha(bytes.fromhex(v))) for n, v in contents.items()}
    assert only(before['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'] < clock['before_us']
    return known, contents


def diagnosis(value, current, backend, clock, attempt):
    expected = backend['exact']['expected']
    assert value['backend'] == expected and expected['pid'] == current['control_backend_pid']
    assert value['returncode'] == 0 and value['observed_sqlstate'] in (None, '23514')
    assert instant(value['since']) == clock['before_us'] and instant(value['until']) == clock['after_us']
    message, function = (('activated recovery target registry identity differs', 'require_activated_initial_recovery')
        if attempt == 'legacy-closed' else
        ('activated source closure requires its closing producer and complete outcome copies', 'require_activated_initial_source'))
    error, = value['errors']
    context, = value['contexts']
    prefix = r'\[' + str(expected['pid']) + r'\] '
    assert re.search(prefix + r'ERROR:  (?:23514: )?' + re.escape(message) + '$', error)
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function (?:control\.)?' + function + r'\(bytea\) line [1-9][0-9]* at RAISE', context)
    for line in (error, context):
        recovery.inside_window(instant(line.split()[0].replace('Z', '+00:00')), clock)


def response_checked(response, mode, report, error):
    assert response['outcome'] == 'PROCESS_EXITED'
    assert not any(k in response for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error', 'holder_error'))
    code = 4 if error else 0
    assert type(response['returncode']) is int and response['returncode'] == code
    assert response['stderr'] == ('error: ' + error + '\n' if error else '')
    terminal = response['terminal']
    assert terminal['exit_code'] == code and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    flag = '--activated-successor' if '-legacy-' in mode else '--close-activated-source'
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], flag]
    if error:
        assert response['stdout'] == ''
    else:
        def unique(pairs):
            assert len(pairs) == len(dict(pairs))
            return dict(pairs)
        lines = response['stdout'].splitlines()
        assert len(lines) == 1 and recovery.encoded(json.loads(lines[0], object_pairs_hook=unique)) == recovery.encoded(report)


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, predecessor=None):
    name, attempt = selected(mode)
    window(server_window)
    assert len(before) == len(after) == 72
    known, contents = baseline(before, native_before, inputs_before, seed, setup, name, attempt, server_window, predecessor)
    catalogs(setup, attempt)
    assert inputs_before == inputs_after and input_contents == contents
    refused = name == 'missing-input' or attempt in REFUSALS
    early = attempt in ('closed-missing-copy', 'mixed', 'legacy-closed')
    legacy = attempt == 'legacy-open'
    if refused:
        projected, current = failed_control(before, after, 'close' if early else 'outcome', server_window)
    else:
        projected, current = control_delta(before, after, 'normal', server_window)
    wanted = copy.deepcopy(before)
    expected_native = native(native_before) if early else known['closed']
    assert native(native_after) == expected_native
    writes = int(expected_native != native(native_before))
    assert native_after['sequence'] == native_before['sequence'] + writes
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    if not early and producer['state'] == 'open':
        changed = only(projected['producer_epochs'], producer_epoch=2)
        fresh_xmin(changed['xmin'], before)
        role = only(after['role_instances'], role_instance_id=current['role_instance_id'])
        assert changed['xmin'] not in (current['xmin'], role['xmin'])
        producer.update(state='closing', xmin=changed['xmin'])
    if (name, attempt) == ('off', 'first'):
        original = only(seed['control']['producer_outcomes'], producer_epoch=2, request_sequence=2)
        restored = only(projected['producer_outcomes'], producer_epoch=2, request_sequence=2)
        fresh_xmin(restored['xmin'], before)
        recovery.inside_window(restored['recorded_at_us'], server_window)
        assert current['acquired_at_us'] <= restored['recorded_at_us'] <= current['lease_expires_at_us'] - 20_000_000
        assert restored['xmin'] not in (current['xmin'], producer['xmin'])
        wanted['producer_outcomes'].append(dict(original, recorder_resource=current['resource_id'],
            recorder_epoch=current['epoch'], recorder_incarnation=current['process_incarnation'],
            recorded_at_us=restored['recorded_at_us'], xmin=restored['xmin']))
    source = only(wanted['source_streams'], owner_epoch=2)
    if not refused and not legacy and source['state'] == 'open':
        actual = only(projected['source_streams'], owner_epoch=2)
        recovery.inside_window(actual['closed_at_us'], server_window)
        assert current['lease_expires_at_us'] - 20_000_000 <= actual['closed_at_us'] <= current['released_at_us']
        fresh_xmin(actual['xmin'], before)
        assert actual['xmin'] not in (current['xmin'], producer['xmin'])
        if (name, attempt) == ('off', 'first'):
            assert actual['xmin'] != restored['xmin'] and actual['closed_at_us'] >= restored['recorded_at_us']
        source.update(state='closed', last_complete_sequence=known['cut'], closure_digest='\\x' + known['digest'],
            closed_at_us=actual['closed_at_us'], xmin=actual['xmin'])
    same(wanted, projected)
    if early:
        diagnosis(setup['diagnostic'], current, backend, server_window, attempt)
    else:
        assert setup['diagnostic'] is None
    intent = known['intent']
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-positive-outcomes-resolved' if legacy else 'activated-source-closed',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2',
        issued_through=str(known['issued']), reserved_through=str(known['issued']), resolved_requests=str(known['issued']),
        source_closure='not-performed' if legacy else dict(last_complete_sequence=str(known['cut']), sha256=known['digest']),
        native='closed', owner_release='committed', ready=False)
    error = ('producer operation unavailable: Native(Input(Missing))' if name == 'missing-input' else
        'producer operation unavailable: Control(Backend)' if early else None)
    response_checked(response, mode, report, error)
    recovery.backend_checked(backend, current, response, server_window)
    if predecessor:
        assert backend['absence']['database'] == predecessor['backend']['absence']['database']
        previous = predecessor['response']['terminal']
        terminal = response['terminal']
        assert (terminal['pid'], terminal['start_ticks']) != (previous['pid'], previous['start_ticks'])
        previous = predecessor['backend']['exact']['expected']
        assert (current['control_backend_pid'], current['control_backend_start']) != (previous['pid'], previous['backend_start'])
        assert predecessor['server_window']['after_us'] <= instant(current['control_backend_start'])
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72,
        native_rows=len(expected_native), native_sequence=native_after['sequence'], native_writes=writes,
        retained_inputs=len(contents), issued=known['issued'], source_cut=known['cut'],
        source_digest=known['digest'], source_closed=source['state'] == 'closed',
        outcome_copy_repaired=(name, attempt) == ('off', 'first'), refused=refused, workflow_retired=False)


def negative_controls(**context):
    verify(**context)
    checks = []
    def reject(name, mutation):
        value = copy.deepcopy(context)
        mutation(value)
        try:
            verify(**value)
        except (AssertionError, ValueError, KeyError, TypeError, IndexError):
            checks.append(name)
            return
        raise AssertionError('source closure counterexample accepted: ' + name)
    reject('extra-native-write', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('source1-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=1).update(xmin='99999999'))
    reject('activation-rewritten', lambda c: only(c['after']['initial_ingest_activations']).update(activated_at_us=1))
    reject('handoff-rewritten', lambda c: only(c['after']['initial_ingest_successions']).update(handoff='\\x00'))
    reject('old-recorder-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=1, request_sequence=1).update(recorder_epoch=99))
    reject('original-input-rewritten', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    reject('native-outcome-rewritten', lambda c: next(r for r in c['native_after']['rows'] if r['cf'] == 'receipts' and r['key'].startswith('0130')).update(value='00'))
    reject('producer-retired', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(state='retired'))
    reject('owner-released', lambda c: only(c['after']['owner_epochs'], role='ingest', epoch=2).update(state='released'))
    reject('catalog-drift', lambda c: c['setup']['catalog_after']['triggers'].pop())
    reject('backend-present', lambda c: c['backend']['exact'].update(stdout='t\n', present=True))
    reject('unjoined-cli', lambda c: c['response']['terminal'].update(reaped=False))
    reject('wrong-action', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--retire-initial'))
    source = only(context['after']['source_streams'], owner_epoch=2)
    if source['state'] == 'closed':
        reject('source-cut', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(last_complete_sequence=999))
        reject('source-digest', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(closure_digest='\\x' + '00' * 32))
        if context['response']['returncode'] == 0:
            def coherent_digest(c):
                only(c['after']['source_streams'], owner_epoch=2).update(closure_digest='\\x' + '00' * 32)
                report = json.loads(c['response']['stdout']); report['source_closure']['sha256'] = '00' * 32
                c['response']['stdout'] = json.dumps(report) + '\n'
            reject('coherent-false-closure', coherent_digest)
    else:
        reject('false-source-completion', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(state='closed'))
    name, attempt = selected(context['mode'])
    if attempt == 'closed-missing-copy':
        reject('terminal-copy-repaired', lambda c: c['after']['producer_outcomes'].append(copy.deepcopy(
            only(c['seed']['control']['producer_outcomes'], producer_epoch=2, request_sequence=2))))
    if attempt in ('closed-missing-copy', 'mixed', 'legacy-closed'):
        reject('unrelated-backend-error', lambda c: c['setup']['diagnostic'].update(contexts=[]))
    if (name, attempt) == ('off', 'first'):
        reject('repaired-original-recorder', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=8))
    if attempt == 'retry':
        reject('retry-source-row-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin='99999999'))
    if attempt == 'legacy-open':
        reject('old-command-requires-new-grant', lambda c: only(c['setup']['catalog_before']['functions'], name=SOURCE_FUNCTION).update(execute=True))
    return dict(status='PASS', count=len(checks), rejected=checks)
