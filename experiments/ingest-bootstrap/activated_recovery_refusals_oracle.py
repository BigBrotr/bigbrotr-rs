"""Independent installed refusal boundaries over one frozen activated off seed.

Fixture damage is explicit and reversible from a retained operator preimage.
A rejected replay may have durably closed the native and PostgreSQL target
producer; that result is distinct from inspection refusal or no Control claim.
"""
import copy
from datetime import datetime, timezone
import json
import math
import re

from activated_recovery_oracle import known, migrated, policy, sha, backend_checked
from activated_recovery_faults_oracle import failed_control
from installed_empty_oracle import fresh_xmin, native, window
from installed_live_oracle import instant
from installed_sync_oracle import decode
from initial_succession_oracle import only, same


PREFIX = 'installed-activated-'
CASES = tuple(PREFIX + name for name in ('missing-input', 'corrupt-input',
    'missing-old-dependency', 'corrupt-old-dependency', 'wrong-activation',
    'wrong-handoff', 'extra-producer', 'foreign-stream', 'held-native'))
REPLAY = ('missing-input', 'corrupt-input', 'missing-old-dependency', 'corrupt-old-dependency')
NATIVE = ('missing-old-dependency', 'corrupt-old-dependency', 'wrong-handoff',
    'extra-producer', 'foreign-stream')
OLD_FACT_SHA256 = '48f14a320986410db233082bad2b65e7815086a210221781516fe09e6ed5ce38'
SQL_ERROR = 'activated recovery requires exact committed preparation and activation'
ERRORS = {
    'missing-input': 'producer operation unavailable: Native(Input(Missing))',
    'corrupt-input': 'producer operation unavailable: Native(Input(Codec(DigestMismatch)))',
    'missing-old-dependency': 'producer operation unavailable: Native(Native(StateMismatch))',
    'corrupt-old-dependency': 'producer operation unavailable: Native(Native(StateMismatch))',
    'wrong-activation': 'producer operation unavailable: Control(Backend)',
    'wrong-handoff': 'producer operation unavailable: Native(StateMismatch)',
    'extra-producer': 'producer operation unavailable: Native(StateMismatch)',
    'foreign-stream': 'producer operation unavailable: Native(StateMismatch)',
    'held-native': 'native store inspection or exclusive open failed',
}
# Reviewed privileged clone-only damage; product checks remain enabled for recovery.
WRONG_ACTIVATION = """BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='5s';
SET LOCAL session_replication_role=replica;
UPDATE control.initial_ingest_activations SET target_manifest_sha256=
    set_byte(target_manifest_sha256,0,get_byte(target_manifest_sha256,0) # 1)
WHERE target_epoch=2
RETURNING (to_jsonb(initial_ingest_activations)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
"""


def selected(mode):
    assert mode in CASES, 'unselected installed refusal'
    return mode.removeprefix(PREFIX)


def flipped(raw, offset=-1):
    value = bytearray(raw)
    value[offset] ^= 1
    return bytes(value)


def dump(rows, sequence):
    return dict(event='dump-state', sequence=sequence,
        rows=[dict(cf=cf, key=key.hex(), value=value.hex())
            for (cf, key), value in sorted(rows.items())])


def input_census(contents):
    return {name: dict(bytes=len(bytes.fromhex(value)), sha256=sha(bytes.fromhex(value)))
        for name, value in contents.items()}


def mutation_expected(seed, science, label):
    """Known literal keys and one exact mutation; no expected byte from the helper."""
    intent = science['intent']
    dataset = bytes.fromhex(intent['dataset_id'][2:])
    slot = bytes.fromhex(intent['producer_slot_id'][2:])
    resource = bytes.fromhex(intent['owner_resource_id'][2:])
    be = lambda n, size=8: n.to_bytes(size, 'big')
    oldkey = ('facts', b'\x01\x20' + dataset + be(0, 4) + be(1) + be(3))
    old = science['opened'][oldkey]
    assert sha(old) == OLD_FACT_SHA256
    # These golden receipts explicitly retain private VisibilityScope fact(1,3).
    # This distinguishes a required old dependency from an unrelated old row.
    fact = decode(old[34 + len(b'bigbrotr/fact/1\0'):])
    assert old[:2] == b'\x01\x20' and old[34:].startswith(b'bigbrotr/fact/1\0')
    assert fact[0] == [dataset, 0, 1, 3]
    assert fact[1] == 'dictionary' and fact[5][-1] == {'class': 'private'}
    dependency = [[dataset, 0, 1, 3], old[2:34]]
    for sequence in (1, 2):
        receipt = science['opened']['receipts', b'\x01\x30' + slot + be(2) + be(sequence)]
        assert receipt.startswith(b'bigbrotr/receipt/1\0')
        decoded = decode(receipt[len(b'bigbrotr/receipt/1\0'):])
        assert decoded[1] == [slot, 2, sequence] and decoded[-1].count(dependency) == 1
    rows = dict(science['opened'])
    key, value = None, None
    if label in ('missing-old-dependency', 'corrupt-old-dependency'):
        key = oldkey
        value = None if label.startswith('missing') else flipped(old)
    elif label == 'wrong-handoff':
        key = ('default', b'\x01\x0a' + dataset + resource + be(1))
        assert rows[key] == bytes.fromhex(intent['handoff'][2:])
        value = flipped(rows[key], 45)
    elif label == 'extra-producer':
        key = ('receipts', b'\x01\x31' + slot + be(3))
        assert key not in rows
        value = b'\x01\x31' + bytes(24)
    elif label == 'foreign-stream':
        key = ('default', b'\x01\x02' + dataset + be(0, 4) + be(3))
        assert key not in rows
        value = b'\x01\x02' + bytes(8)
    if key is not None:
        if value is None:
            del rows[key]
        else:
            rows[key] = value
    contents = dict(seed['contents'])
    filename = slot.hex() + '-0000000000000002-0000000000000002.cbor'
    if label == 'missing-input':
        del contents[filename]
    elif label == 'corrupt-input':
        contents[filename] = flipped(bytes.fromhex(contents[filename])).hex()
    return rows, contents, key, value, filename


def setup_checked(before, native_before, inputs, seed, setup, mutation, mode, server_window):
    label = selected(mode)
    science = known(seed, 'off')
    wanted = migrated(seed, setup, server_window['before_us'])
    wanted = policy(wanted, setup, 'off', server_window['before_us'])
    assert setup['removed_copy'] is None
    assert setup['catalog_before'] == setup['catalog_after'] == setup['migration']['catalog_after']
    assert set(mutation) == {'preimage', 'operation', 'after'}
    assert set(mutation['preimage']) == set(mutation['after']) == {'control', 'native', 'inputs', 'contents'}
    pre = mutation['preimage']
    same(wanted, pre['control'])
    assert pre['native'] == seed['native'] and pre['inputs'] == seed['inputs'] and pre['contents'] == seed['contents']
    rows, contents, key, value, filename = mutation_expected(seed, science, label)
    operation = mutation['operation']
    if label in NATIVE:
        assert set(operation) == {'kind', 'variant', 'command', 'response', 'events'}
        assert operation['kind'] == 'native' and operation['variant'] == label
        assert operation['command'] == ['/build/ingest_bootstrap_probe', 'activated-corrupt',
            '/lab/' + mode + '/store', label]
        expected_native = dump(rows, 85)
        expected_events = []
        for phase, state in [('before', seed['native']), ('after', expected_native)]:
            expected_events.append(dict(event='activated-recovery-corruption', phase=phase,
                mutation=label, cf=key[0], key=key[1].hex(),
                value=(science['opened'].get(key) if phase == 'before' else value),
                sequence=state['sequence'], rows=state['rows']))
            v = expected_events[-1]['value']
            expected_events[-1]['value'] = None if v is None else v.hex()
        assert operation['events'] == expected_events
        answer = operation['response']
        assert set(answer) == {'returncode', 'stdout', 'stderr'}
        assert answer['returncode'] == 0 and answer['stderr'] == ''
        assert [json.loads(line) for line in answer['stdout'].splitlines()] == expected_events
    else:
        expected_native = seed['native']
    if label in ('missing-input', 'corrupt-input'):
        assert set(operation) == {'kind', 'action', 'name', 'retained', 'before_mode', 'after_mode', 'replacement_hex'}
        assert operation['kind'] == 'input' and operation['name'] == filename
        assert operation['action'] == ('remove' if label == 'missing-input' else 'replace')
        assert operation['retained'] == dict(path='/lab/' + mode + '-input-original.cbor',
            **seed['inputs'][filename], hex=seed['contents'][filename], mode=0o600)
        assert operation['before_mode'] == 0o600
        assert operation['after_mode'] == (None if label == 'missing-input' else 0o600)
        assert operation['replacement_hex'] == contents.get(filename)
    elif label == 'wrong-activation':
        assert set(operation) == {'kind', 'statement', 'original', 'response', 'changed'}
        assert operation['kind'] == 'sql' and operation['statement'] == WRONG_ACTIVATION
        original = only(wanted['initial_ingest_activations'])
        assert operation['original'] == original
        changed = operation['changed']
        fresh_xmin(changed['xmin'], wanted)
        digest = bytes.fromhex(original['target_manifest_sha256'][2:])
        assert changed == dict(original, target_manifest_sha256='\\x' + flipped(digest, 0).hex(), xmin=changed['xmin'])
        wanted['initial_ingest_activations'] = [changed]
        answer = operation['response']
        assert answer['returncode'] == 0 and answer['stderr'] == ''
        assert json.loads(answer['stdout']) == changed
    elif label == 'held-native':
        assert operation['kind'] == 'native-held'
    actual = mutation['after']
    same(wanted, actual['control'])
    assert actual['native'] == expected_native and actual['inputs'] == input_census(contents) and actual['contents'] == contents
    same(wanted, before)
    assert native_before == expected_native and inputs == actual['inputs']
    assert only(before['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'] < server_window['before_us']
    return science, contents


def no_claim_backend(backend, response):
    assert set(backend) == {'exact', 'absence'} and backend['exact'] is None
    absence = backend['absence']
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}', absence['database'])
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['user'] == 'bigbrotr_control'
    assert absence['limit_seconds'] == 5 and math.isfinite(absence['seconds']) and 0 <= absence['seconds'] <= 120
    prior = 0
    for sample in absence['attempts']:
        assert prior <= sample['seconds'] <= 5
        prior = sample['seconds']
        assert type(sample['count']) is int and sample['count'] == len(sample['backends'])
        pids = []
        for row in sample['backends']:
            assert set(row) == {'pid', 'backend_start'} and type(row['pid']) is int and row['pid'] > 0
            assert instant(row['backend_start']) > 0
            pids.append(row['pid'])
        assert pids == sorted(set(pids))
    assert absence['attempts'][-1]['count'] == 0 and prior <= absence['seconds']
    assert response['terminal']['config'] == '/lab/' + absence['database'] + '.toml'


def held_checked(operation, mode, before, backend):
    assert set(operation) == {'kind', 'command', 'ready', 'alive_during_cli', 'stdout', 'stderr',
        'exit_code', 'joined', 'forced', 'cleanup_errors'}
    command = operation['command']
    assert len(command) == 11 and command[:3] == ['docker', 'exec', '-i']
    assert isinstance(command[3], str) and command[3]
    assert command[4:8] == ['/build/ingest_bootstrap_probe', 'history-hold',
        backend['absence']['database'], '/run/secrets/application']
    assert command[8] == '/lab/' + mode + '/store'
    dataset = only(before['datasets'])
    assert command[9:] == [dataset['dataset_id'][2:], dataset['instance_id'][2:]]
    assert operation['ready'] == {'event': 'native-held'}
    assert json.loads(operation['stdout']) == operation['ready'] and operation['stderr'] == ''
    assert operation['alive_during_cli'] is True and operation['exit_code'] == 0
    assert operation['joined'] is True and operation['forced'] is False and operation['cleanup_errors'] == []


def diagnostic_checked(value, current, backend, server_window):
    assert set(value) == {'backend', 'since', 'until', 'returncode', 'observed_sqlstate', 'errors', 'contexts'}
    assert value['backend'] == backend['exact']['expected'] and value['returncode'] == 0
    for key, boundary in [('since', 'before_us'), ('until', 'after_us')]:
        assert value[key] == datetime.fromtimestamp(server_window[boundary] / 1_000_000, timezone.utc).isoformat()
    error, = value['errors']
    context, = value['contexts']
    prefix = r'\[' + str(current['control_backend_pid']) + r'\] '
    match = re.search(prefix + r'ERROR:  (?:(?P<state>[0-9A-Z]{5}): )?' + re.escape(SQL_ERROR) + '$', error)
    assert match and match['state'] in (None, '23514') and value['observed_sqlstate'] == match['state']
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function (?:control\.)?require_activated_initial_recovery\(bytea\) line [1-9][0-9]* at RAISE', context)
    for line in (error, context):
        at = instant(line.split()[0].replace('Z', '+00:00'))
        assert server_window['before_us'] <= at <= server_window['after_us']


def verify(before, after, native_before, native_after, inputs_before, inputs_after,
        input_contents, response, mode, *, seed, setup, server_window, backend, mutation):
    label = selected(mode)
    window(server_window)
    assert len(before) == len(after) == 72
    science, contents = setup_checked(before, native_before, inputs_before, seed, setup, mutation, mode, server_window)
    assert inputs_before == inputs_after == input_census(contents) and input_contents == contents
    expected_native = dict(native(native_before))
    closing = label in REPLAY
    if closing:
        slot = bytes.fromhex(science['intent']['producer_slot_id'][2:])
        key = ('receipts', b'\x01\x31' + slot + (2).to_bytes(8, 'big'))
        expected_native[key] = science['closed'][key]
    assert native(native_after) == expected_native
    assert native_after['sequence'] == native_before['sequence'] + int(closing)
    if label == 'held-native':
        same(before, after)
        no_claim_backend(backend, response)
        held_checked(mutation['operation'], mode, before, backend)
        current = None
    else:
        # Every refusal retains an unreleased actual Control. Only the replay
        # paths refreshed it after the durable closing transaction.
        projected, current = failed_control(before, after, 'outcome' if closing else 'close', server_window)
        assert current['epoch'] == 9
        wanted = copy.deepcopy(before)
        if closing:
            old = only(wanted['producer_epochs'], producer_epoch=2)
            actual = only(projected['producer_epochs'], producer_epoch=2)
            fresh_xmin(actual['xmin'], before)
            role = only(after['role_instances'], role_instance_id=current['role_instance_id'])
            assert actual['xmin'] not in (role['xmin'], current['xmin'])
            old.update(state='closing', xmin=actual['xmin'])
        same(wanted, projected)
        backend_checked(backend, current, response, server_window)
    if label == 'wrong-activation':
        diagnostic_checked(setup['diagnostic'], current, backend, server_window)
    else:
        assert setup['diagnostic'] is None
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int and response['returncode'] == 4
    assert response['stdout'] == '' and response['stderr'] == 'error: ' + ERRORS[label] + '\n'
    assert not any(k in response for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error', 'holder_error'))
    terminal = response['terminal']
    assert terminal['exit_code'] == 4 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0 and type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--activated-successor']
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72,
        source_derived_refusal_stage='native-open' if label == 'held-native' else 'replay' if closing else 'inspection',
        control_claimed=current is not None, native_rows=len(expected_native), native_sequence=native_after['sequence'],
        fixture_native_writes=int(label in NATIVE), product_native_writes=int(closing),
        retained_inputs=len(contents), original_preimage_retained=True, source_closed=False, ready=False)


def negative_controls(**context):
    verify(**context)
    label = selected(context['mode'])
    rejected = []
    def reject(name, mutate):
        c = copy.deepcopy(context)
        mutate(c)
        try:
            verify(**c)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError):
            rejected.append(name)
            return
        raise AssertionError('counterexample accepted: ' + name)
    reject('lost-original-preimage', lambda c: c['mutation']['preimage']['contents'].pop(next(iter(c['seed']['contents']))))
    reject('wrong-refusal', lambda c: c['response'].update(stderr='error: producer operation unavailable: CommitUnknown\n'))
    reject('fictitious-success-report', lambda c: c['response'].update(stdout='{}\n'))
    reject('native-sequence-drift', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('changed-original-input', lambda c: c['input_contents'].update({next(iter(c['seed']['contents'])):'00'}))
    reject('lost-original-outcome', lambda c: c['after']['producer_outcomes'].pop(0))
    reject('changed-old-issuer', lambda c: only(c['after']['producer_requests'], producer_epoch=1, request_sequence=1).update(issuer_epoch=99))
    reject('source2-closed', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(state='closed'))
    reject('policy-head-reinterpreted', lambda c: only(c['after']['datasets']).update(policy_revision=2))
    reject('unjoined-process', lambda c: c['response']['terminal'].update(reaped=False))
    reject('catalog-drift', lambda c: c['setup']['catalog_after']['triggers'].pop())
    def extra_native(c):
        for target in (c['native_before'], c['native_after'], c['mutation']['after']['native']):
            row = next(r for r in target['rows'] if r['cf']=='facts' and r['key'].endswith('0000000000000001'))
            row['value'] = flipped(bytes.fromhex(row['value'])).hex()
    reject('coherent-unannounced-native-damage', extra_native)
    if label == 'held-native':
        reject('holder-not-alive', lambda c: c['mutation']['operation'].update(alive_during_cli=False))
        reject('holder-not-joined', lambda c: c['mutation']['operation'].update(joined=False))
    else:
        reject('backend-still-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
        reject('false-control-release', lambda c: only(c['after']['owner_epochs'], role='control', epoch=9).update(state='released'))
    if label in REPLAY:
        reject('closing-erased', lambda c: c['native_after'].update(rows=c['native_before']['rows']))
        reject('sql-still-open', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(state='open'))
        reject('old-recorder-adopted', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=1).update(recorder_epoch=9))
    elif label != 'held-native':
        reject('inspection-closed-sql', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(state='closing'))
    if label == 'wrong-activation':
        reject('wrong-diagnostic-backend', lambda c: c['setup']['diagnostic']['backend'].update(pid=1))
        reject('unnamed-sql-refusal', lambda c: c['setup']['diagnostic'].update(errors=['ERROR: arbitrary SQL error']))
    if label in NATIVE:
        reject('mutation-not-synced', lambda c: c['mutation']['operation']['events'][1].update(sequence=84))
    if label in ('missing-input', 'corrupt-input'):
        reject('preimage-copy-corrupt', lambda c: c['mutation']['operation']['retained'].update(hex='00'))
    return dict(status='PASS', count=len(rejected), rejected=rejected)
