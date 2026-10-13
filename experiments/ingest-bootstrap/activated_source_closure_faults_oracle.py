"""Exact source-publication COMMIT loss, independent of client exit or ACK.

The fixed off seed has two originals. Its missing copy is repaired at COMMIT6;
refresh7 precedes final source publication8. Native closing remains one write.
"""
import copy
import json
import re

import activated_source_closure_oracle as source
from activated_recovery_faults_oracle import (PROXY_SHA256, COUNTERS, backend_checked,
    bounded_number, config_checked, failed_control)
from activated_recovery_oracle import inside_window, sha
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import instant

CASES = ('installed-source-close-before', 'installed-source-close-after')


def selected(mode):
    recovered = mode.endswith('-recover')
    base = mode.removesuffix('-recover')
    assert base in CASES
    return base, base.rsplit('-', 1)[1], recovered


def wire_checked(fault, side):
    number = 8
    mode = 'before-forward' if side == 'before' else 'after-complete'
    assert fault['point'] == 'source' and fault['mode'] == mode and fault['commit_number'] == number
    wire = fault['wire']
    assert wire['status'] == 'CAPTURED'
    assert type(wire['exit_code']) is int and wire['exit_code'] == 0
    assert type(wire['wrapper_exit']) is int and wire['wrapper_exit'] == 0
    assert wire['forced_wrapper_stop'] is False and wire['stderr_nonempty'] is False
    assert wire['output_overflow'] is False and wire['proxy_sha256'] == PROXY_SHA256
    events = wire['events']
    assert events == [wire['ready'], wire['fault'], wire['receipt']]
    assert [e['event'] for e in events] == ['ready', 'fault', 'receipt']
    previous = number - 1
    expected = [number, previous, previous, previous, 1] if side == 'before' else [number, number, number, previous, 1]
    elapsed = 0
    for index, event in enumerate(events):
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1'
        assert event['mode'] == mode and event['commit_number'] == number
        assert event['held'] is False and event['hold_action'] is None
        assert type(event['elapsed_ns']) is int and elapsed <= event['elapsed_ns'] <= 35_000_000_000
        elapsed = event['elapsed_ns']
        assert all(type(event[key]) is int for key in COUNTERS)
        assert [event[k] for k in COUNTERS] == ([0] * 5 if index == 0 else expected)
        if index:
            assert event['reason'] == mode
    ready, _, receipt = events
    assert ready['proxy_sha256'] == PROXY_SHA256 and ready['listen_host'] == '127.0.0.1'
    assert type(ready['listen_port']) is int and 1 <= ready['listen_port'] <= 65535
    assert ready['upstream_port'] == 5432 and ready['listen_port'] != 5432
    assert type(ready['pid']) is int and ready['pid'] > 0
    assert ready['deadline_seconds'] == 30 and ready['max_frame_bytes'] == 1024 ** 2
    assert ready['max_wire_bytes'] == 64 * 1024 ** 2 and ready['max_messages'] == 20000
    assert set(receipt['frames']) == set(receipt['tags']) == {'frontend', 'backend'}
    for direction, tags in receipt['tags'].items():
        assert all(re.fullmatch('[0-9a-f]{2}', tag) and type(n) is int and n > 0 for tag, n in tags.items())
        assert receipt['frames'][direction] == sum(tags.values()) > 0
    assert sum(receipt['frames'].values()) <= ready['max_messages']
    assert receipt['tags']['frontend']['51'] >= number
    assert receipt['tags']['backend']['43'] >= receipt['server_commit_complete']
    assert type(receipt['declared_wire_bytes']) is int and 0 < receipt['declared_wire_bytes'] <= ready['max_wire_bytes']
    # The proxy emits sorted JSON and a newline, without query or secret bytes.
    raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in events).encode()
    assert wire['stdout'].encode() == raw
    assert wire['stdout_sha256'] == sha(raw) and wire['stdout_bytes'] == len(raw)
    return ready['listen_port']



def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, fault, predecessor=None):
    base, side, recovered = selected(mode)
    window(server_window)
    assert seed['name'] == 'off'
    known = source.science(seed)
    assert len(before) == len(after) == 72
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    for name, raw in input_contents.items():
        value = bytes.fromhex(raw)
        assert inputs_after[name] == dict(bytes=len(value), sha256=sha(value))
    source.catalogs(setup, 'first')
    assert setup['diagnostic'] is None
    if recovered:
        assert predecessor is not None and predecessor['mode'] == base
        verify(**predecessor)
        assert predecessor['seed'] == seed and predecessor['fault'] == fault
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        assert setup['operator'] is None
        assert {k:v for k,v in setup.items() if k != 'operator'} == {
            k:v for k,v in predecessor['setup'].items() if k != 'operator'}
        same(predecessor['after'], before)
        assert native_before == predecessor['native_after']
        projected, current = control_delta(before, after, 'fresh-completion', server_window)
    else:
        assert predecessor is None
        wanted = source.migrated(seed, setup, server_window)
        wanted, contents = source.operator(wanted, seed['native'], seed['contents'], setup, 'off', 'first')
        assert contents == seed['contents']
        same(wanted, before)
        assert native_before == seed['native']
        projected, current = failed_control(before, after, 'outcome', server_window)
    assert only(before['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'] < server_window['before_us']
    assert native(native_after) == known['closed'] and native_after['sequence'] == 85
    assert native_before['sequence'] == (85 if recovered else 84)
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    if not recovered:
        assert producer['state'] == 'open'
        actual = only(projected['producer_epochs'], producer_epoch=2)
        fresh_xmin(actual['xmin'], before)
        role = only(after['role_instances'], role_instance_id=current['role_instance_id'])
        assert actual['xmin'] not in (current['xmin'], role['xmin'])
        producer.update(state='closing', xmin=actual['xmin'])
        assert not any(r['producer_epoch'] == 2 and r['request_sequence'] == 2 for r in wanted['producer_outcomes'])
        original = only(seed['control']['producer_outcomes'], producer_epoch=2, request_sequence=2)
        copied = only(projected['producer_outcomes'], producer_epoch=2, request_sequence=2)
        fresh_xmin(copied['xmin'], before)
        assert copied['xmin'] not in (producer['xmin'], current['xmin'], role['xmin'])
        inside_window(copied['recorded_at_us'], server_window)
        # The final refresh7 occurs after the committed copy6, before publication8.
        assert current['acquired_at_us'] <= copied['recorded_at_us'] <= current['lease_expires_at_us'] - 20_000_000
        wanted['producer_outcomes'].append(dict(original, recorder_resource=current['resource_id'],
            recorder_epoch=current['epoch'], recorder_incarnation=current['process_incarnation'],
            recorded_at_us=copied['recorded_at_us'], xmin=copied['xmin']))
    assert producer['state'] == 'closing'
    stream = only(wanted['source_streams'], owner_epoch=2)
    publishes = recovered and side == 'before' or not recovered and side == 'after'
    if publishes:
        assert stream['state'] == 'open'
        actual = only(projected['source_streams'], owner_epoch=2)
        copied = only(wanted['producer_outcomes'], producer_epoch=2, request_sequence=2)
        fresh_xmin(actual['xmin'], before)
        assert actual['xmin'] not in (current['xmin'], producer['xmin'], copied['xmin'])
        inside_window(actual['closed_at_us'], server_window)
        assert current['lease_expires_at_us'] - 20_000_000 <= actual['closed_at_us']
        assert copied['recorded_at_us'] <= actual['closed_at_us']
        if recovered:
            assert actual['closed_at_us'] <= current['released_at_us']
        stream.update(state='closed', last_complete_sequence=known['cut'], closure_digest='\\x' + known['digest'],
            closed_at_us=actual['closed_at_us'], xmin=actual['xmin'])
    assert stream['state'] == ('closed' if recovered or side == 'after' else 'open')
    # On after-complete retry this also pins the original closure timestamp/xmin
    # and missing-copy recorder9: no terminal repair or reattribution is allowed.
    same(wanted, projected)
    database = backend_checked(backend, current, server_window)
    port = wire_checked(fault, side)
    config_checked(fault, base, database, seed, port)
    terminal = response['terminal']
    assert terminal['config'] == fault['config']['direct_path' if recovered else 'proxy_path']
    bounded_number(response['seconds'], 120)
    intent = known['intent']
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-source-closed',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2', issued_through='2',
        reserved_through='2', resolved_requests='2', source_closure=dict(
            last_complete_sequence=str(known['cut']), sha256=known['digest']),
        native='closed', owner_release='committed', ready=False)
    source.response_checked(response, mode, report,
        None if recovered else 'producer operation unavailable: CommitUnknown')
    if recovered:
        old = predecessor['response']['terminal']
        assert (terminal['pid'], terminal['start_ticks']) != (old['pid'], old['start_ticks'])
        old = predecessor['backend']['exact']['expected']
        assert database == old['database']
        assert (current['control_backend_pid'], current['control_backend_start']) != (old['pid'], old['backend_start'])
        assert instant(current['control_backend_start']) >= predecessor['server_window']['after_us']
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72,
        native_rows=70, native_sequence=85, native_writes=int(not recovered), retained_inputs=len(input_contents),
        source_cut=known['cut'], source_digest=known['digest'], source_closed=stream['state'] == 'closed',
        outcome_copy_repaired=not recovered, recovered=recovered, commit_number=8, workflow_retired=False)


def negative_controls(**context):
    verify(**context)
    _, side, recovered = selected(context['mode'])
    checks = []
    def reject(name, mutation):
        value = copy.deepcopy(context)
        mutation(value)
        try:
            verify(**value)
        except (AssertionError, ValueError, KeyError, TypeError, IndexError):
            checks.append(name)
            return
        raise AssertionError('source COMMIT counterexample accepted: ' + name)
    reject('extra-native-write', lambda c: c['native_after'].update(sequence=86))
    reject('native-barrier-absent', lambda c: c['native_after'].update(rows=c['seed']['native']['rows']))
    def corrupt_native(c):
        row = next(r for r in c['native_after']['rows'] if r['cf'] == 'facts')
        value = bytearray.fromhex(row['value']); value[-1] ^= 1
        row['value'] = value.hex()
    reject('original-native-fact-corrupt', corrupt_native)
    reject('old-input-corrupt', lambda c: c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    reject('source1-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=1).update(xmin='999999999'))
    reject('activation-reattributed', lambda c: only(c['after']['initial_ingest_activations']).update(activated_by_epoch=99))
    reject('issuer-changed', lambda c: only(c['after']['producer_requests'], producer_epoch=2, request_sequence=2).update(issuer_epoch=99))
    reject('policy-regressed', lambda c: only(c['after']['datasets']).update(config_revision=2, policy_revision=2))
    reject('copy-missing', lambda c: c['after']['producer_outcomes'].remove(only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2)))
    reject('copy-recorder-changed', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=99))
    reject('copy-original-policy-changed', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(policy_epoch=3))
    reject('old-copy-xmin-changed', lambda c: only(c['after']['producer_outcomes'], producer_epoch=1, request_sequence=1).update(xmin='999999999'))
    reject('producer-reopened', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(state='open'))
    reject('wrong-source-commit-side', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(state='open' if side == 'after' or recovered else 'closed'))
    reject('backend-still-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    reject('CLI-not-joined', lambda c: c['response']['terminal'].update(reaped=False))
    reject('forced-proxy-cleanup', lambda c: c['fault']['wire'].update(forced_wrapper_stop=True))
    def wrong_wire(c):
        wire = c['fault']['wire']
        for event in wire['events'][1:]: event['client_commit_complete'] += 1
        wire['fault'], wire['receipt'] = wire['events'][1:]
        raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in wire['events']).encode()
        wire.update(stdout=raw.decode(), stdout_sha256=sha(raw), stdout_bytes=len(raw))
        if c['predecessor']: c['predecessor']['fault'] = copy.deepcopy(c['fault'])
    reject('coherent-delivered-COMMIT-response', wrong_wire)
    if recovered or side == 'after':
        reject('source-digest-corrupt', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(closure_digest='\\x'+'00'*32))
        reject('source-cut-corrupt', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(last_complete_sequence=0))
    if recovered:
        def reattribute(c):
            current = max((r for r in c['after']['owner_epochs'] if r['role']=='control'), key=lambda r:r['epoch'])
            only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(
                recorder_resource=current['resource_id'], recorder_epoch=current['epoch'],
                recorder_incarnation=current['process_incarnation'], recorded_at_us=current['acquired_at_us'])
        reject('retry-adopts-repaired-copy', reattribute)
        reject('retry-rewrites-copy-xmin', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(xmin='999999999'))
        def unfence(c):
            old=max((r for r in c['before']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            only(c['after']['owner_epochs'], role='control', epoch=old['epoch']).update(old)
        reject('prior-Control-not-fenced', unfence)
        if side == 'after':
            reject('terminal-closure-xmin-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin='999999999'))
            reject('terminal-closure-time-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(closed_at_us=c['server_window']['before_us']))
    else:
        reject('wrong-CLI-error-class', lambda c: c['response'].update(stderr='error: producer operation unavailable: Control(Backend)\n'))
    return dict(status='PASS', count=len(checks), rejected=checks)
