"""Retirement workflow discriminators over immutable qualified source histories.

The zero-request claim is genuine. The input binding is explicitly synthetic
retained relational history; it proves refusal, not import execution.
"""
import copy
import json
import math
import re

import activated_retirement_oracle as retirement
from activated_recovery_oracle import encoded, sha
from activated_recovery_faults_oracle import failed_control, backend_checked
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import instant

PREFIX = 'installed-activated-retirement-workflow-'
CASES = tuple(PREFIX + x for x in ('claims', 'checkpoint', 'binding', 'missing-copy',
    'copy-conflict', 'missing-input', 'mixed-terminal'))
SEED_PINS = dict(claims='8045fd5a5d949cf564763b51c7d856d75a3e7b5da33a06a02c7a57164159cb1f',
    terminal='a09193c1cf7c840a504ed0131428ff5fcc54c03b1197f92acb09606da6ba76dc')
PAYLOAD = (b'{"pubkey":"f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9",'
    b'"created_at":1,"kind":1,"tags":[],"content":"import then network",'
    b'"id":"8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849",'
    b'"sig":"ab9e62de93391621735c773f4b1c40f41029d7d49369993a63a226c4ce14605859b112dd261b5fa4f8a2b82bbac0a67323f39c4d007555711547bfd25560b4a6"}\n')
SQL_PINS = {'checkpoint': '073788d20fd02883f11104707a4fdf857883b40bf52a753ea2d975b37638a4f7', 'binding': '20bbe6c97afd757691fde0c015abdc32a0a56593996744127ef458c1b09a5609', 'missing-copy': '964e5d35d3aaa7f90d20b15ad4cfadc966934cb02fcb959226fab790f9541c2e', 'copy-conflict': '964728fce3583399e29e5db1e6a5db0404e33d45f711c56403f3617bfc7fc77d', 'mixed-terminal': 'ced9ee9ace56f1352c2404985f50d31d60c1074d724212f8029430f55330c50a'}
ERRORS = dict(checkpoint='Control(Backend)', binding='Control(Backend)',
    **{'missing-copy': 'Control(Backend)', 'copy-conflict': 'Outcome(Conflict)',
       'missing-input': 'Native(Input(Missing))', 'mixed-terminal': 'Control(Backend)'})


def selected(mode):
    retry = mode == CASES[0] + '-retry'
    base = mode.removesuffix('-retry') if retry else mode
    assert base in CASES
    return base.removeprefix(PREFIX), retry


def seed_checked(seed):
    assert set(seed) == {'name', 'verification', 'control', 'native', 'inputs', 'contents', 'catalog'}
    if seed['name'] == 'off':
        return retirement.seed_checked(seed)
    value = seed['verification']
    assert sha(encoded(value)) == SEED_PINS[seed['name']]
    same(seed['control'], value['after'])
    assert seed['native'] == value['native_after']
    assert seed['inputs'] == value['inputs_after'] and seed['contents'] == value['input_contents']
    if seed['name'] == 'terminal':
        known = retirement.seed_checked(value['seed'])
        assert seed['catalog'] == value['setup']['catalog_after']
        assert only(seed['control']['producer_epochs'], producer_epoch=2)['state'] == 'retired'
        return known
    assert seed['name'] == 'claims'
    assert retirement.baseline_catalog(seed['catalog']) == retirement.baseline_catalog(dict(
        value['setup']['catalog_after'], retirement={}))
    extra = seed['catalog']['retirement']
    row, = extra['functions']
    assert row['name'] == 'control.guard_producer_epoch()' and row['execute'] is True
    assert sha(row['definition'].encode()) == retirement.OLD_PRODUCER_GUARD
    assert extra['privileges'] == {name: dict(select=True, update=True, delete=False, truncate=False)
        for name in ('producer_epochs', 'source_streams', 'allocation_domains', 'owner_epochs', 'role_instances')}
    pg = seed['control']
    source = only(pg['source_streams'], owner_epoch=2)
    assert source['state'] == 'closed' and source['last_complete_sequence'] == 0
    assert only(pg['producer_epochs'], producer_epoch=2)['reserved_through_request'] == 1
    activation = only(pg['initial_ingest_activations'])
    for key in ('a6', 'a7'):
        job = only(pg['jobs'], request_key='\\x' + key * 16)
        gap = only(pg['acquisition_gaps'], job_id=job['job_id'])
        assert job['state'] == 'cancelled' and job['job_revision'] == 4
        assert gap['producer_epoch'] == 2 and gap['issuer_epoch'] == 9
        assert (gap['previous_owner_resource'], gap['previous_owner_epoch'], gap['previous_owner_incarnation']) == (
            activation['activated_by_resource'], 8, activation['activated_by_incarnation'])
        if key == 'a7':
            assert not any(r['acquisition_job_id'] == job['job_id'] for r in pg['producer_requests'])
    accepted = only(pg['jobs'], request_key='\\x' + 'a8' * 16)
    assert accepted['state'] == 'accepted' and accepted['owner_epoch'] is None
    assert len(native(seed['native'])) == 53 and seed['native']['sequence'] == 65 and len(seed['inputs']) == 5
    return dict(issued=1, cut=0, digest=source['closure_digest'][2:], source=source)


def migrated(seed, setup, clock):
    if seed['name'] != 'terminal':
        return retirement.migrated(seed, setup, clock)
    assert setup['migration_sha256'] == retirement.MIGRATION
    value = setup['migration']
    assert set(value) == {'before', 'after', 'reapplied', 'responses', 'physical_before', 'physical_after',
        'catalog_before', 'catalog_ungranted', 'catalog_after', 'grant_statements', 'grant_responses'}
    for key in ('before', 'after', 'reapplied'):
        same(value[key], seed['control'])
    assert value['physical_before'] == value['physical_after'] and set(value['physical_before']) == set(seed['control'])
    assert value['catalog_before'] == value['catalog_ungranted'] == value['catalog_after'] == seed['catalog']
    assert value['grant_statements'] == retirement.GRANTS and value['grant_responses'] == [''] * 3
    assert len(value['responses']) == 2 and all(r['returncode'] == 0 and r['stderr'] == '' for r in value['responses'])
    assert setup['catalog_before'] == setup['catalog_after'] == value['catalog_after']
    return seed['control']


def addition_rows(before, kind):
    """Literal independent relational expectation; no product classifier called."""
    producer = only(before['producer_epochs'], producer_epoch=2)
    source = only(before['source_streams'], owner_epoch=2)
    if kind == 'checkpoint':
        return [('producer_checkpoints', dict(producer_slot_id=producer['producer_slot_id'], producer_epoch=2,
            dataset_id=producer['dataset_id'], checkpoint_revision=1, complete_through_request=0,
            checkpoint_digest='\\x' + 'c1' * 32, committed_at_us=source['closed_at_us']))]
    assert kind == 'binding'
    original = only(before['jobs'], job_id=only(before['acquisition_gaps'], producer_epoch=2)['job_id'])
    job = dict(original, job_id='\\x' + 'b1' * 16, request_key='\\x' + 'b2' * 16,
        request_digest='\\x' + 'b3' * 32, kind='import', state='accepted', job_revision=1,
        checkpoint_revision=0, reason_code=None, result_manifest_digest=None, completed_at_us=None,
        acquisition_job_protocol=None, export_job_protocol=None, input_topology_generation=None,
        input_complete=True, owner_resource_id=None, owner_epoch=None, owner_incarnation=None)
    del job['xmin']
    request = only(before['producer_requests'], producer_epoch=2, request_sequence=2)
    binding = {k: request[k] for k in ('dataset_id', 'producer_slot_id', 'producer_epoch', 'request_sequence',
        'request_digest', 'input_bytes', 'input_profile', 'manifest_sha256', 'issuer_resource',
        'issuer_epoch', 'issuer_incarnation')}
    binding.update(job_id=job['job_id'], shard_id=0,
        source_namespace='\\x' + sha(b'retirement/import-fixture/1\0' + PAYLOAD), source_bytes=len(PAYLOAD),
        source_sha256='\\x' + sha(PAYLOAD), parser_profile=1, batch_first_record=1, batch_records=1,
        obligation_index=0, obligation_kind=1, logical_record=1, byte_start=0, byte_end=len(PAYLOAD),
        record_sha256='\\x' + sha(PAYLOAD))
    return [('jobs', job), ('job_admissions', dict(job_id=job['job_id'],
        admission_ordinal=max(r['admission_ordinal'] for r in before['job_admissions']) + 1)),
        ('producer_input_bindings', binding)]


def mutation_checked(value, expected, seed, kind):
    assert set(value) == {'before', 'after', 'operation'}
    for state in (value['before'], value['after']):
        assert set(state) == {'control', 'native', 'inputs', 'contents'}
    same(value['before']['control'], expected)
    for key in ('native', 'inputs', 'contents'):
        assert value['before'][key] == seed[key]
    wanted = copy.deepcopy(value['before'])
    operation = value['operation']
    if kind == 'claims':
        assert operation is None
    elif kind == 'missing-input':
        name, = [n for n in seed['inputs'] if n.endswith('-0000000000000002-0000000000000002.cbor')]
        raw = seed['contents'][name]
        assert operation == dict(kind='input', action='remove', name=name, before_mode=384, after_mode=None,
            retained=dict(path='/lab/' + PREFIX + kind + '-input-original.cbor', mode=384, hex=raw, **seed['inputs'][name]))
        del wanted['inputs'][name]; del wanted['contents'][name]
    else:
        assert operation['kind'] == 'sql' and operation['actor'] == 'privileged-isolated-fixture'
        assert sha(operation['statement'].encode()) == SQL_PINS[kind]
        answer = operation['response']
        assert answer['outcome'] == 'PROCESS_EXITED' and answer['returncode'] == 0 and answer['stderr'] == ''
        records = [json.loads(line) for line in answer['stdout'].splitlines()]
        header, *returned, tail = records
        assert tail == dict(actor='postgres', replication_role='origin')
        assert set(header) == {'actor', 'replication_role', 'transaction'}
        assert header['actor'] == 'postgres' and header['replication_role'] == ('origin' if kind == 'checkpoint' else 'replica')
        xid8 = header['transaction']; assert re.fullmatch('[1-9][0-9]*', xid8) and int(xid8) < 2**64
        tx = str(int(xid8) % 2**32); fresh_xmin(tx, expected)
        if kind in ('checkpoint', 'binding'):
            additions = addition_rows(expected, kind)
            assert len(returned) == len(additions)
            for (table, row), observed in zip(additions, returned, strict=True):
                assert observed == dict(row, xmin=tx)
                wanted['control'][table].append(observed)
        elif kind in ('missing-copy', 'copy-conflict'):
            old = only(wanted['control']['producer_outcomes'], producer_epoch=2, request_sequence=2)
            if kind == 'missing-copy':
                assert returned == [old]; wanted['control']['producer_outcomes'].remove(old)
            else:
                raw = bytearray.fromhex(old['outcome_bytes'][2:]); raw[-1] ^= 1
                old.update(outcome_bytes='\\x' + raw.hex(), outcome_sha256='\\x' + sha(raw), xmin=tx)
                assert returned == [old]
        else:
            assert kind == 'mixed-terminal'
            owner = only(wanted['control']['owner_epochs'], role='ingest', epoch=2)
            role = only(wanted['control']['role_instances'], role_instance_id=owner['role_instance_id'])
            assert role['retired_at_us'] is not None
            role.update(retired_at_us=None, xmin=tx)
            assert returned == [role]
    same(wanted['control'], value['after']['control'])
    for key in ('native', 'inputs', 'contents'):
        assert wanted[key] == value['after'][key]
    return wanted


def diagnosis(value, current, backend, clock, kind):
    if kind not in ('checkpoint', 'binding', 'missing-copy', 'mixed-terminal'):
        assert value is None
        return
    message, function = ('activated retirement requires exactly ready or complete terminal metadata',
        'require_activated_initial_retirement') if kind == 'mixed-terminal' else (
        'activated retirement requires exact copies and supported workflow obligations', 'lock_activated_initial_retirement')
    assert value['backend'] == backend['exact']['expected'] and value['returncode'] == 0
    assert value['observed_sqlstate'] in (None, '23514')
    assert instant(value['since']) == clock['before_us'] and instant(value['until']) == clock['after_us']
    error, = value['errors']; context, = value['contexts']
    prefix = r'\[' + str(current['control_backend_pid']) + r'\] '
    assert re.search(prefix + r'ERROR:  (?:23514: )?' + re.escape(message) + '$', error)
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function control\.' + function + r'\(bytea\) line [1-9][0-9]* at RAISE', context)
    for line in (error, context):
        assert clock['before_us'] <= instant(line.split()[0].replace('Z', '+00:00')) <= clock['after_us']


def response_failed(value, mode, kind):
    assert value['outcome'] == 'PROCESS_EXITED' and type(value['returncode']) is int and value['returncode'] == 4
    assert value['stdout'] == '' and value['stderr'] == 'error: producer operation unavailable: ' + ERRORS[kind] + '\n'
    assert not any(k in value for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error', 'holder_error'))
    assert math.isfinite(value['seconds']) and 0 <= value['seconds'] <= 120
    t = value['terminal']
    assert t['exit_code'] == 4 and t['forced'] is False and t['reaped'] is True
    assert type(t['pid']) is int and t['pid'] > 0 and type(t['start_ticks']) is int and t['start_ticks'] > 0
    assert math.isfinite(t['elapsed_seconds']) and 0 <= t['elapsed_seconds'] <= 50
    assert t['prefix'] == '/lab/' + mode
    assert t['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', t['config'], '--retire-activated-initial']


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, mutation, predecessor=None):
    kind, retry = selected(mode)
    assert seed['name'] == ('claims' if kind == 'claims' else 'terminal' if kind == 'mixed-terminal' else 'off')
    window(server_window)
    known = seed_checked(seed)
    expected = migrated(seed, setup, server_window)
    prepared = mutation_checked(mutation, expected, seed, kind)
    if retry:
        assert predecessor is not None and predecessor['mode'] == CASES[0]
        verify(**predecessor)
        assert predecessor['seed'] == seed and predecessor['mutation'] == mutation
        assert predecessor['setup']['migration'] == setup['migration']
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        prepared['control'] = predecessor['after']
    else:
        assert predecessor is None
    same(before, prepared['control'])
    assert len(before) == len(after) == 72
    assert native_before == native_after == prepared['native']
    assert inputs_before == inputs_after == prepared['inputs'] and input_contents == prepared['contents']
    for name, raw in input_contents.items():
        assert inputs_after[name] == dict(bytes=len(bytes.fromhex(raw)), sha256=sha(bytes.fromhex(raw)))
    assert only(before['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'] < server_window['before_us']
    if kind == 'claims':
        projected, current = control_delta(before, after, 'normal', server_window)
        wanted = copy.deepcopy(before)
        if not retry:
            retirement.retire_targets(wanted, after, known, current, server_window, released=True)
        same(wanted, projected)
        intent = only(before['initial_ingest_successions'])
        report = dict(format='bigbrotr-ingest-recovery/1', state='activated-initial-retired',
            dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
            producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2', issued_through='1',
            reserved_through='1', resolved_requests='1', source_closure=dict(last_complete_sequence='0', sha256=known['digest']),
            ingest_retirement=dict(allocation_domains='9', acquisition_gaps='2', owner_release='committed',
                native_acknowledged='0', native_retired='0'), native='closed', owner_release='committed', ready=False)
        retirement.response_checked(response, mode, report)
    else:
        projected, current = failed_control(before, after,
            'replay' if kind in ('copy-conflict', 'missing-input') else 'close', server_window)
        same(before, projected)
        response_failed(response, mode, kind)
    database = backend_checked(backend, current, server_window)
    assert response['terminal']['config'] == '/lab/' + database + '.toml'
    diagnosis(setup['diagnostic'], current, backend, server_window, kind)
    if retry:
        assert database == predecessor['backend']['absence']['database']
        old = predecessor['response']['terminal']; new = response['terminal']
        assert (old['pid'], old['start_ticks']) != (new['pid'], new['start_ticks'])
        old = predecessor['backend']['exact']['expected']
        assert (old['pid'], old['backend_start']) != (current['control_backend_pid'], current['control_backend_start'])
        assert predecessor['server_window']['after_us'] <= instant(current['control_backend_start'])
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72,
        native_rows=len(native(native_after)), native_sequence=native_after['sequence'], inputs=len(inputs_after),
        new_native_writes=0, target_rows_changed=12 if kind == 'claims' and not retry else 0,
        retirement=kind == 'claims', error=ERRORS.get(kind), ready=False)


def negative_controls(**context):
    verify(**context)
    controls = []
    def reject(name, mutate):
        c = copy.deepcopy(context); mutate(c)
        try: verify(**c)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError): controls.append(name); return
        raise AssertionError('accepted counterexample: ' + name)
    reject('original-outcome-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=1, request_sequence=1).update(xmin='9999999'))
    reject('old-input-corrupted', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    reject('native-write', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('historical-gap-issuer-reassigned', lambda c: next(r for r in c['after']['acquisition_gaps'] if r['producer_epoch'] == 2).update(issuer_epoch=3))
    reject('source-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin='9999999'))
    reject('unreaped-cli', lambda c: c['response']['terminal'].update(reaped=False))
    reject('unknown-cli', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('lost-clock', lambda c: c['response'].update(clock_error='TimeoutExpired'))
    reject('wrong-config-database', lambda c: (c['response']['terminal'].update(config='/lab/foreign.toml'), c['response']['terminal']['command'].__setitem__(4, '/lab/foreign.toml')))
    reject('backend-still-live', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    reject('guard-disabled', lambda c: only(c['setup']['catalog_after']['triggers'], table='role_instances', name='activated_retirement_role_guard').update(enabled='D'))
    reject('preimage-rewritten', lambda c: only(c['mutation']['before']['control']['source_streams'], owner_epoch=2).update(xmin='9999999'))
    def coherent_operator_row(c, table, predicate, fields):
        for rows in (c['mutation']['after']['control'], c['before'], c['after']):
            only(rows[table], **predicate).update(fields)
        answer = c['mutation']['operation']['response']
        records = [json.loads(line) for line in answer['stdout'].splitlines()]
        returned, = [r for r in records if all(r.get(k) == v for k, v in predicate.items())]
        returned.update(fields)
        answer['stdout'] = ''.join(json.dumps(row) + '\n' for row in records)
    kind, retry = selected(context['mode'])
    if kind == 'claims':
        reject('zero-request-gap-lost', lambda c: c['after']['acquisition_gaps'].remove(only(c['after']['acquisition_gaps'],
            job_id=only(c['after']['jobs'], request_key='\\x' + 'a7' * 16)['job_id'])))
        reject('unclaimed-intent-cancelled', lambda c: only(c['after']['jobs'], request_key='\\x' + 'a8' * 16).update(state='cancelled'))
        reject('partial-retirement', lambda c: retirement.target_rows(c['after'])[1].update(retired_at_us=None))
        reject('source-time-adopted-as-retirement', lambda c: retirement.target_rows(c['after'])[1].update(retired_at_us=only(c['before']['source_streams'], owner_epoch=2)['closed_at_us']))
        if retry:
            reject('retry-rewrites-terminal-xmin', lambda c: retirement.target_rows(c['after'])[2].update(xmin='9999999'))
    else:
        reject('wrong-error', lambda c: c['response'].update(stderr='error: producer operation unavailable: Control(Unavailable)\n'))
        reject('missing-native-repair-invented', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(retired_at_us=1))
        if setup_diagnostic := context['setup']['diagnostic']:
            reject('generic-backend-diagnostic', lambda c: c['setup']['diagnostic'].update(errors=[setup_diagnostic['errors'][0].replace('activated retirement', 'unrelated operation')]))
        if kind == 'checkpoint':
            reject('coherent-checkpoint-nonzero', lambda c: coherent_operator_row(c, 'producer_checkpoints',
                dict(checkpoint_revision=1), dict(complete_through_request=1)))
        elif kind == 'binding':
            reject('coherent-binding-foreign-issuer', lambda c: coherent_operator_row(c, 'producer_input_bindings',
                dict(logical_record=1), dict(issuer_epoch=3)))
        elif kind == 'missing-copy':
            reject('forbidden-copy-repair', lambda c: c['after']['producer_outcomes'].append(copy.deepcopy(
                only(c['seed']['control']['producer_outcomes'], producer_epoch=2, request_sequence=2))))
        elif kind == 'copy-conflict':
            reject('conflict-silently-replaced', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2,
                request_sequence=2).update(only(c['seed']['control']['producer_outcomes'], producer_epoch=2, request_sequence=2)))
        elif kind == 'missing-input':
            reject('missing-input-invented', lambda c: c['inputs_after'].update(c['seed']['inputs']))
        else:
            reject('mixed-target-silently-fixed', lambda c: retirement.target_rows(c['after'])[1].update(
                retired_at_us=retirement.target_rows(c['after'])[2]['retired_at_us']))
        if kind != 'missing-input':
            reject('unreviewed-operator-statement', lambda c: c['mutation']['operation'].update(statement=c['mutation']['operation']['statement'] + '-- changed\n'))
    return dict(status='PASS', count=len(controls), rejected=controls)
