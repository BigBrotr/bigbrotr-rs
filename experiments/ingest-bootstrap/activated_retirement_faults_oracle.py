"""Independent final twelve-row retirement COMMIT ambiguity on the abandoned off source.

The complete frozen source context fixes source closure, completed gaps and outcome copies.
Wire receipt, installed exit and observed durable effects are separate evidence.
"""
import copy
import json
import re

import activated_retirement_oracle as retirement
from activated_recovery_faults_oracle import (PROXY_SHA256, COUNTERS, backend_checked,
    bounded_number, config_checked, failed_control)
from activated_recovery_oracle import encoded, sha
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, native, window
from installed_live_oracle import instant

CASES = ('installed-activated-retirement-before', 'installed-activated-retirement-after')


def selected(mode):
    recovered = mode.endswith('-recover')
    base = mode.removesuffix('-recover')
    assert base in CASES
    return base, base.rsplit('-', 1)[1], recovered


def wire_checked(fault, side):
    number = 7
    mode = 'before-forward' if side == 'before' else 'after-complete'
    assert fault['point'] == 'retirement' and fault['mode'] == mode and fault['commit_number'] == number
    wire = fault['wire']
    assert wire['status'] == 'CAPTURED'
    assert type(wire['exit_code']) is int and wire['exit_code'] == 0
    assert type(wire['wrapper_exit']) is int and wire['wrapper_exit'] == 0
    assert wire['forced_wrapper_stop'] is False and wire['stderr_nonempty'] is False
    assert wire['output_overflow'] is False and wire['proxy_sha256'] == PROXY_SHA256
    events = wire['events']
    assert events == [wire['ready'], wire['fault'], wire['receipt']]
    assert [e['event'] for e in events] == ['ready', 'fault', 'receipt']
    expected = [7, 6, 6, 6, 1] if side == 'before' else [7, 7, 7, 6, 1]
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
    # The selected proxy counts COMMIT; other simple queries are not classified
    # as independently observed STARTs or attributed to a particular statement.
    raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in events).encode()
    assert wire['stdout'].encode() == raw
    assert wire['stdout_sha256'] == sha(raw) and wire['stdout_bytes'] == len(raw)
    return ready['listen_port']


def response_checked(response, mode, report, recovered):
    if recovered:
        retirement.response_checked(response, mode, report)
        return
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert response['returncode'] == 4 and response['stdout'] == ''
    assert response['stderr'] == 'error: producer operation unavailable: CommitUnknown\n'
    assert not any(k in response for k in ('orchestration_error', 'terminal_error', 'clock_error',
        'output_limit_exceeded', 'holder_error'))
    bounded_number(response['seconds'], 120)
    terminal = response['terminal']
    assert terminal['exit_code'] == 4 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    bounded_number(terminal['elapsed_seconds'], 50)
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'],
        '--retire-activated-initial']


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, fault, predecessor=None):
    base, side, recovered = selected(mode)
    window(server_window)
    assert seed['name'] == 'off'
    known = retirement.seed_checked(seed)
    expected = retirement.migrated(seed, setup, server_window)
    if recovered:
        assert predecessor is not None and predecessor['mode'] == base
        verify(**predecessor)
        assert predecessor['seed'] == seed and predecessor['setup'] == setup and predecessor['fault'] == fault
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        expected = predecessor['after']
        projected, current = control_delta(before, after, 'fresh-completion', server_window)
    else:
        assert predecessor is None
        projected, current = failed_control(before, after, 'retirement', server_window)
    same(before, expected)
    assert len(before) == len(after) == 72
    assert native_before == native_after == seed['native']
    assert native_after['sequence'] == 85 and len(native(native_after)) == 70
    assert inputs_before == inputs_after == seed['inputs'] and input_contents == seed['contents']
    assert current['epoch'] == (14 if recovered else 13)
    wanted = copy.deepcopy(before)
    writes = (recovered and side == 'before') or (not recovered and side == 'after')
    if writes:
        retirement.retire_targets(wanted, after, known, current, server_window, released=recovered)
    same(wanted, projected)
    database = backend_checked(backend, current, server_window)
    port = wire_checked(fault, side)
    config_checked(fault, base, database, seed, port)
    terminal = response['terminal']
    assert terminal['config'] == fault['config']['direct_path' if recovered else 'proxy_path']
    intent = only(before['initial_ingest_successions'])
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-initial-retired',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2',
        issued_through='2', reserved_through='2', resolved_requests='2',
        source_closure=dict(last_complete_sequence='5', sha256=known['digest']),
        ingest_retirement=dict(allocation_domains='9', acquisition_gaps='1', owner_release='committed',
            native_acknowledged='0', native_retired='0'), native='closed', owner_release='committed', ready=False)
    response_checked(response, mode, report, recovered)
    if recovered:
        old = predecessor['response']['terminal']
        assert (terminal['pid'], terminal['start_ticks']) != (old['pid'], old['start_ticks'])
        old = predecessor['backend']['exact']['expected']
        assert database == old['database']
        assert (current['control_backend_pid'], current['control_backend_start']) != (old['pid'], old['backend_start'])
        assert instant(current['control_backend_start']) >= predecessor['server_window']['after_us']
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72, native_rows=70,
        native_sequence=85, retained_inputs=6, native_writes=0, target_rows_changed=12 if writes else 0,
        producer_retired=recovered or side == 'after', recovered=recovered, commit_number=7,
        source_unchanged=True, gaps_unchanged=True, jobs_unchanged=True, sessions_unchanged=True, ready=False)


def negative_controls(**context):
    verify(**context)
    _, side, recovered = selected(context['mode'])
    checks = []
    def reject(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        try:
            verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            checks.append(name)
            return
        raise AssertionError('retirement COMMIT counterexample accepted: ' + name)
    def targets(c):
        return retirement.target_rows(c['after'])
    def gap(c):
        return only(c['after']['acquisition_gaps'], producer_epoch=2)
    reject('native-write', lambda c: c['native_after'].update(sequence=86))
    reject('input-original-mutated', lambda c: c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    reject('source1-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=1).update(xmin='999999'))
    reject('source2-rewritten', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(xmin='999999'))
    reject('copy-reattributed', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=13))
    reject('old-gap-rewritten', lambda c: only(c['after']['acquisition_gaps'], producer_epoch=1).update(xmin='999999'))
    reject('target-gap-reattributed', lambda c: gap(c).update(issuer_epoch=13))
    reject('job-restarted', lambda c: only(c['after']['jobs'], job_id=gap(c)['job_id']).update(state='running'))
    reject('session-revoked', lambda c: c['after']['browser_sessions'][0].update(revoked_at_us=1))
    reject('role-identity-rewritten', lambda c: targets(c)[1].update(created_at_us=1))
    reject('catalog-trigger-disabled', lambda c: c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    reject('unexpected-grant', lambda c: c['setup']['migration']['grant_statements'].append('GRANT ALL'))
    reject('wrong-command', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--abandon-activated-acquisition'))
    reject('CLI-not-joined', lambda c: c['response']['terminal'].update(reaped=False))
    reject('unknown-custody', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('backend-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    reject('backend-database-mismatch', lambda c: c['backend']['exact']['expected'].update(database='other'))
    reject('clock-capture-failed', lambda c: c['response'].update(clock_error='TimeoutExpired'))
    reject('forced-proxy-stop', lambda c: c['fault']['wire'].update(forced_wrapper_stop=True))
    def delivered(c):
        wire = c['fault']['wire']
        for event in wire['events'][1:]:
            event['client_commit_complete'] = 7
        wire['fault'], wire['receipt'] = wire['events'][1:]
        raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in wire['events']).encode()
        wire.update(stdout=raw.decode(), stdout_sha256=sha(raw), stdout_bytes=len(raw))
        if c['predecessor']:
            c['predecessor']['fault'] = copy.deepcopy(c['fault'])
    reject('coherently-rehashed-delivered-reply', delivered)
    def retime(c, at, xid):
        owner, role, producer, domains = targets(c)
        owner.update(released_at_us=at, xmin=xid)
        role.update(retired_at_us=at, xmin=xid)
        producer.update(retired_at_us=at, xmin=xid)
        for row in domains:
            row.update(closed_at_us=at, xmin=xid)
    if recovered or side == 'after':
        reject('producer-only-retirement', lambda c: targets(c)[3][0].update(state='active', closed_at_us=None, closure_digest=None))
        reject('split-terminal-transaction', lambda c: targets(c)[1].update(xmin='999999999'))
        reject('wrong-retirement-digest', lambda c: targets(c)[0].update(exclusion_digest='\\x'+'00'*32))
        reject('coherent-retirement-before-authority', lambda c: retime(c, c['server_window']['before_us']-1, targets(c)[2]['xmin']))
        reject('coherent-old-transaction', lambda c: retime(c, targets(c)[2]['retired_at_us'], only(c['before']['source_streams'], owner_epoch=2)['xmin']))
        def claim_xmin(c):
            current = only(c['after']['owner_epochs'], role='control', epoch=14 if recovered else 13)
            role = only(c['after']['role_instances'], role_instance_id=current['role_instance_id'])
            retime(c, targets(c)[2]['retired_at_us'], role['xmin'])
        reject('coherent-Control-claim-transaction', claim_xmin)
    else:
        reject('before-loss-partial-retirement', lambda c: targets(c)[2].update(state='retired', retired_at_us=c['server_window']['before_us']))
    if recovered:
        def unfence(c):
            old = only(c['before']['owner_epochs'], role='control', epoch=13)
            only(c['after']['owner_epochs'], role='control', epoch=13).update(old)
        reject('failed-Control-not-fenced', unfence)
        reject('same-process-recovery', lambda c: c['response']['terminal'].update(
            pid=c['predecessor']['response']['terminal']['pid'],
            start_ticks=c['predecessor']['response']['terminal']['start_ticks']))
        if side == 'after':
            reject('retry-rewrites-terminal-rows', lambda c: retime(c, c['server_window']['before_us'], '999999999'))
    else:
        reject('wrong-error-class', lambda c: c['response'].update(stderr='error: producer operation unavailable: Control(Backend)\n'))
    return dict(status='PASS', count=len(checks), rejected=checks)
