"""Independent installed closing/outcome COMMIT-loss and fresh-process recovery.

Wire completion, client exit and retained SQL/native state are distinct evidence.
No installed observation is invented for session poisoning or driver failure.
"""
import copy
import json
import math
import re
import tomllib

from activated_recovery_oracle import REMOVE_COPY, encoded, known, migrated, policy, sha
from installed_empty_oracle import control_delta, fresh_identity, fresh_xmin, native, window
from installed_live_oracle import instant
from initial_history_oracle import inside_window
from initial_succession_oracle import only, same


CASES = ('installed-activated-close-before', 'installed-activated-close-after',
    'installed-activated-outcome-before', 'installed-activated-outcome-after')
PROXY_SHA256 = '6e737de8223818d0b05b47cb25f16038233d43181b12aec97bd13e3b44e3f829'
COUNTERS = ('commits_seen', 'commits_forwarded', 'server_commit_complete',
    'client_commit_complete', 'faults')


def selected(mode):
    recovery = mode.endswith('-recover')
    base = mode.removesuffix('-recover')
    assert base in CASES, 'unselected installed COMMIT-loss case'
    point, side = base.removeprefix('installed-activated-').split('-')
    return base, point, side, recovery


def bounded_number(value, maximum):
    assert type(value) in (int, float) and math.isfinite(value) and 0 <= value <= maximum


def wire_checked(fault, point, side):
    number = 2 if point == 'close' else 6
    mode = 'before-forward' if side == 'before' else 'after-complete'
    assert fault['point'] == point and fault['mode'] == mode and fault['commit_number'] == number
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


def config_checked(fault, base, database, seed, port):
    config = fault['config']
    assert config['direct_path'] == '/lab/' + database + '.toml'
    assert config['proxy_path'] == '/lab/' + base + '-proxy.toml'
    assert config['endpoint'] == '127.0.0.1:' + str(port)
    for kind in ('direct', 'proxy'):
        assert config[kind + '_sha256'] == sha(config[kind + '_text'].encode())
    direct, proxy = (tomllib.loads(config[k + '_text']) for k in ('direct', 'proxy'))
    expected = copy.deepcopy(direct)
    expected['postgres']['control']['endpoint'] = config['endpoint']
    assert proxy == expected, 'proxy must change only the Control endpoint'
    # Also retain the exact text delta, not just equivalent parsed configuration.
    pattern = r'(?ms)(^\[postgres\.control\]\n)(.*?)(?=^\[|\Z)'
    section, = re.finditer(pattern, config['direct_text'])
    replacement, count = re.subn(r'(?m)^endpoint = "127\.0\.0\.1:5432"$',
        'endpoint = ' + json.dumps(config['endpoint']), section[2])
    assert count == 1
    assert config['proxy_text'] == (config['direct_text'][:section.start()] + section[1]
        + replacement + config['direct_text'][section.end():])
    pg = direct['postgres']['control']
    assert pg['database'] == database and pg['endpoint'] == '127.0.0.1:5432'
    assert pg['user'] == 'bigbrotr_control' and pg['password_ref'] == 'file:/run/secrets/application'
    original = only(seed['control']['datasets'])
    assert direct['topology']['dataset_id'] == original['dataset_id'][2:]
    assert direct['topology']['instance_id'] == original['instance_id'][2:]
    assert direct['storage']['ingest_root'] == '/lab/' + base + '/store'
    assert direct['runtime']['lease_ttl'] == '20s' and direct['runtime']['renewal_timeout'] == '1s'
    assert direct['runtime']['authorization_ttl'] == '5s' and direct['runtime']['graceful_shutdown_timeout'] == '2s'


def failed_control(before, after, point, server_window):
    """Check the unreleased actual claim; closing has no intervening refresh."""
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    assert previous['state'] == 'released'
    current = only(after['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
    role = only(after['role_instances'], role_instance_id=current['role_instance_id'])
    for field in ('role_instance_id', 'process_incarnation'):
        fresh_identity(current[field])
    assert not any(r['role_instance_id'] == current['role_instance_id'] for r in before['role_instances'])
    assert not any(r['process_incarnation'] == current['process_incarnation'] for r in before['owner_epochs'])
    assert type(current['control_backend_pid']) is int and current['control_backend_pid'] > 0
    inside_window(instant(current['control_backend_start']), server_window)
    for value in (current['acquired_at_us'], role['created_at_us'], current['lease_expires_at_us'] - 20_000_000):
        inside_window(value, server_window)
    assert instant(current['control_backend_start']) <= role['created_at_us'] <= current['acquired_at_us']
    assert previous['released_at_us'] <= role['created_at_us']
    renewed = current['lease_expires_at_us'] - 20_000_000
    assert renewed >= current['acquired_at_us']
    fresh_xmin(current['xmin'], before)
    fresh_xmin(role['xmin'], before)
    if point == 'close':
        assert current['xmin'] == role['xmin'] and renewed == current['acquired_at_us']
    else:
        assert current['xmin'] != role['xmin']
    assert current == dict(previous, epoch=previous['epoch'] + 1,
        role_instance_id=current['role_instance_id'], process_incarnation=current['process_incarnation'],
        acquired_at_us=current['acquired_at_us'], lease_expires_at_us=current['lease_expires_at_us'],
        state='active', released_at_us=None, exclusion_method=None, exclusion_digest=None,
        archive_frontier_protocol=None, control_fence_version=1, control_backend_pid=current['control_backend_pid'],
        control_backend_start=current['control_backend_start'], xmin=current['xmin'])
    assert role == dict(role_instance_id=current['role_instance_id'], dataset_id=previous['dataset_id'],
        role='control', created_at_us=role['created_at_us'], retired_at_us=None, xmin=role['xmin'])
    projected = copy.deepcopy(after)
    projected['owner_epochs'].remove(current)
    projected['role_instances'].remove(role)
    return projected, current


def backend_checked(value, current, server_window):
    assert set(value) == {'exact', 'absence'}
    exact, absence = value['exact'], value['absence']
    database = absence['database']
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    assert exact['expected'] == dict(pid=current['control_backend_pid'],
        backend_start=current['control_backend_start'], database=database, control_epoch=current['epoch'])
    assert exact['returncode'] == 0 and exact['stdout'].strip() == 'f' and exact['stderr'] == ''
    assert exact['present'] is False
    inside_window(instant(current['control_backend_start']), server_window)
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['user'] == 'bigbrotr_control'
    assert absence['limit_seconds'] == 5
    bounded_number(absence['seconds'], 120)
    previous = 0
    for sample in absence['attempts']:
        bounded_number(sample['seconds'], 5)
        assert sample['seconds'] >= previous
        previous = sample['seconds']
        assert type(sample['count']) is int and sample['count'] == len(sample['backends'])
        pids = []
        for row in sample['backends']:
            assert set(row) == {'pid', 'backend_start'} and type(row['pid']) is int and row['pid'] > 0
            assert instant(row['backend_start']) > 0
            pids.append(row['pid'])
        assert pids == sorted(set(pids))
    assert absence['attempts'][-1]['count'] == 0 and previous <= absence['seconds']
    return database


def response_checked(response, mode, config, intent, recovery):
    code = 0 if recovery else 4
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert response['returncode'] == code
    assert not any(key in response for key in ('orchestration_error', 'terminal_error',
        'clock_error', 'output_limit_exceeded'))
    bounded_number(response['seconds'], 120)
    terminal = response['terminal']
    assert terminal['exit_code'] == code and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    bounded_number(terminal['elapsed_seconds'], 50)
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['config'] == config['direct_path' if recovery else 'proxy_path']
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--activated-successor']
    if not recovery:
        assert response['stdout'] == '' and response['stderr'] == 'error: producer operation unavailable: CommitUnknown\n'
        return
    expected = dict(format='bigbrotr-ingest-recovery/1', state='activated-positive-outcomes-resolved',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2', issued_through='2',
        reserved_through='2', resolved_requests='2', source_closure='not-performed', native='closed',
        owner_release='committed', ready=False)
    def unique(pairs):
        assert len(pairs) == len(dict(pairs)), 'duplicate report field'
        return dict(pairs)
    lines = response['stdout'].splitlines()
    assert response['stderr'] == '' and len(lines) == 1
    assert encoded(json.loads(lines[0], object_pairs_hook=unique)) == encoded(expected)


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, fault, predecessor=None):
    base, point, side, recovery = selected(mode)
    window(server_window)
    science = known(seed, 'off')
    assert len(before) == len(after) == 72
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    for name, raw in input_contents.items():
        value = bytes.fromhex(raw)
        assert inputs_after[name] == dict(bytes=len(value), sha256=sha(value))
    assert native(native_after) == science['closed'] and native_after['sequence'] == 85
    assert setup['catalog_before'] == setup['catalog_after'] == setup['migration']['catalog_after']
    if recovery:
        assert predecessor is not None and predecessor['mode'] == base
        verify(**predecessor)
        assert predecessor['seed'] == seed and predecessor['fault'] == fault
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        assert setup == predecessor['setup']
        same(predecessor['after'], before)
        assert native_before == predecessor['native_after']
        projected, current = control_delta(before, after, 'fresh-completion', server_window)
    else:
        assert predecessor is None
        wanted = policy(migrated(seed, setup, server_window['before_us']), setup, 'off', server_window['before_us'])
        removed = setup['removed_copy']
        if point == 'outcome':
            original = only(wanted['producer_outcomes'], producer_epoch=2, request_sequence=2)
            assert removed['row'] == original and removed['statement'] == REMOVE_COPY
            answer = removed['response']
            assert answer['returncode'] == 0 and answer['stderr'] == '' and json.loads(answer['stdout']) == original
            wanted['producer_outcomes'].remove(original)
        else:
            assert removed is None
        same(wanted, before)
        assert native_before == seed['native']
        projected, current = failed_control(before, after, point, server_window)
    assert only(before['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'] < server_window['before_us']
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    actual = only(projected['producer_epochs'], producer_epoch=2)
    closes_sql = recovery or point == 'outcome' or side == 'after'
    if closes_sql and producer['state'] == 'open':
        fresh_xmin(actual['xmin'], before)
        assert actual['xmin'] != current['xmin']
        role = only(after['role_instances'], role_instance_id=current['role_instance_id'])
        assert actual['xmin'] != role['xmin']
        producer.update(state='closing', xmin=actual['xmin'])
    assert producer['state'] == ('closing' if closes_sql else 'open')
    inserts_copy = point == 'outcome' and ((not recovery and side == 'after') or (recovery and side == 'before'))
    if inserts_copy:
        assert not any(r['producer_epoch'] == 2 and r['request_sequence'] == 2 for r in wanted['producer_outcomes'])
        copied = only(projected['producer_outcomes'], producer_epoch=2, request_sequence=2)
        retained = only(seed['control']['producer_outcomes'], producer_epoch=2, request_sequence=2)
        fresh_xmin(copied['xmin'], before)
        assert copied['xmin'] not in (current['xmin'], producer['xmin'])
        inside_window(copied['recorded_at_us'], server_window)
        assert current['lease_expires_at_us'] - 20_000_000 <= copied['recorded_at_us']
        if recovery:
            assert copied['recorded_at_us'] <= current['released_at_us']
        wanted['producer_outcomes'].append(dict(retained, recorder_resource=current['resource_id'],
            recorder_epoch=current['epoch'], recorder_incarnation=current['process_incarnation'],
            recorded_at_us=copied['recorded_at_us'], xmin=copied['xmin']))
    same(wanted, projected)
    database = backend_checked(backend, current, server_window)
    port = wire_checked(fault, point, side)
    config_checked(fault, base, database, seed, port)
    response_checked(response, mode, fault['config'], science['intent'], recovery)
    if recovery:
        previous = predecessor['response']['terminal']
        terminal = response['terminal']
        assert (terminal['pid'], terminal['start_ticks']) != (previous['pid'], previous['start_ticks'])
        old = predecessor['backend']['exact']['expected']
        assert database == old['database']
        assert (current['control_backend_pid'], current['control_backend_start']) != (old['pid'], old['backend_start'])
        assert instant(current['control_backend_start']) >= predecessor['server_window']['after_us']
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72, native_rows=70,
        native_sequence=85, new_native_writes=int(not recovery), source_closed=False,
        sql_closing=closes_sql, inserted_outcome=inserts_copy, recovered=recovery,
        commit_number=fault['commit_number'], ready=False)


def negative_controls(**context):
    verify(**context)
    _, point, side, recovery = selected(context['mode'])
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
    reject('native-extra-write', lambda c: c['native_after'].update(sequence=86))
    reject('native-barrier-absent', lambda c: c['native_after'].update(rows=c['seed']['native']['rows']))
    def corrupt_native(c):
        row = next(r for r in c['native_after']['rows'] if r['cf'] == 'facts')
        value = bytearray.fromhex(row['value'])
        value[-1] ^= 1
        row['value'] = value.hex()
    reject('native-original-fact-corrupted', corrupt_native)
    reject('old-input-corrupted', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    reject('activation-attribution-changed', lambda c: only(c['after']['initial_ingest_activations']).update(activated_by_epoch=99))
    reject('old-request-issuer-changed', lambda c: only(c['after']['producer_requests'], producer_epoch=1, request_sequence=4).update(issuer_epoch=99))
    reject('current-policy-regressed', lambda c: only(c['after']['datasets']).update(config_revision=2, policy_revision=2))
    reject('source-closure-invented', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(state='closed'))
    reject('original-outcome-row-version-changed', lambda c: only(c['after']['producer_outcomes'], producer_epoch=1, request_sequence=1).update(xmin='999999999'))
    reject('wrong-sql-commit-boundary', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(state='open' if only(c['after']['producer_epochs'], producer_epoch=2)['state'] == 'closing' else 'closing'))
    reject('backend-still-live', lambda c: c['backend']['exact'].update(stdout='t\n', present=True))
    reject('process-not-reaped', lambda c: c['response']['terminal'].update(reaped=False))
    reject('forced-proxy-cleanup', lambda c: c['fault']['wire'].update(forced_wrapper_stop=True))
    def wrong_wire(c):
        wire = c['fault']['wire']
        for event in wire['events'][1:]:
            event['client_commit_complete'] += 1
        wire['fault'], wire['receipt'] = wire['events'][1:]
        raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in wire['events']).encode()
        wire.update(stdout=raw.decode(), stdout_sha256=sha(raw), stdout_bytes=len(raw))
        if c['predecessor']:
            c['predecessor']['fault'] = copy.deepcopy(c['fault'])
    reject('coherent-client-commit-delivery', wrong_wire)
    if point == 'outcome' and (recovery or side == 'after'):
        reject('wrong-recovered-recorder', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=8))
        reject('rewritten-original-policy', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(policy_epoch=3))
    if point == 'outcome' and not recovery and side == 'before':
        reject('rollback-invented-outcome-copy', lambda c: c['after']['producer_outcomes'].append(
            copy.deepcopy(only(c['seed']['control']['producer_outcomes'], producer_epoch=2, request_sequence=2))))
    if point == 'outcome' and recovery and side == 'after':
        def replace_recorder(c):
            current = max((r for r in c['after']['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
            only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(
                recorder_resource=current['resource_id'], recorder_epoch=current['epoch'],
                recorder_incarnation=current['process_incarnation'], recorded_at_us=current['acquired_at_us'])
        reject('durable-unknown-recorder-adopted-by-recovery', replace_recorder)
    if recovery:
        def no_fence(c):
            prior = max((r for r in c['before']['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
            row = only(c['after']['owner_epochs'], role='control', epoch=prior['epoch'])
            row.update(prior)
        reject('prior-active-control-not-excluded', no_fence)
        reject('recovery-reuses-proxy-config', lambda c: c['response']['terminal'].update(config=c['fault']['config']['proxy_path']))
    else:
        reject('wrong-client-error-class', lambda c: c['response'].update(stderr='error: producer operation unavailable: Control(Backend)\n'))
    return dict(status='PASS', rejected=checks, count=len(checks))
