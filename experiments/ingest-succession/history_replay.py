#!/usr/bin/env python3
"""Independent terminal2 history observations; no database/native/process access.

The golden origins are previously qualified immutable captures, not output from
the reader being qualified. Portable receipt/closure bytes are constructed here
without product codecs. A passed transcript is not a writer or portable proof.
"""
import argparse
import copy
import datetime
import hashlib
import json
import math
from pathlib import Path
import re


CASES = ('activated-terminal2', 'empty-terminal2', 'activated-metadata-short')
NEW_TABLES = ('ingest_successions', 'ingest_activations', 'ingest_terminal_transactions')
PINS = {
    'activated': dict(
        run='ingest-activated-retirement-engine-9bdc86a',
        database='bb_scope_bind_9bdc86a_installed_activated_retirement_off',
        snapshot='installed-activated-retirement-off-retry-after-sql.json',
        snapshot_sha256='7ac690057de370a00144ffd415bb1f504b05ff4b5599d6458b728820a98acbad',
        result_sha256='48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09',
        status='PASS_INSTALLED_ACTIVATED_RETIREMENT_FIRST_WAVE',
        native_file='installed-activated-retirement-off-retry-after-native.json',
        native_sha256='cd49f8bfc44c5c08ae307f845f1aaf1da7f66079bf14f8ee54f9c4c1a3819238',
        inputs_file='installed-activated-retirement-off-retry-after-inputs.json',
        inputs_sha256='f59224bde2f441efa1b74bbdb64c65ae5a30bdd2f283f9d31b5dd5c3adb82589',
        store='installed-activated-retirement-off/store', floor=37,
        tables='0766444dfa5c4c7bf05af20fb2b120b47d595cab73b14409415d1acbac915041',
        ledger='ccfeb0dce9cb9ad127066e3c5e81e42ebae5dc830635cba3056638fee46d006d',
        native='e3c348f35faa881d29c4450744cb2b511d088f5d43ddec9c554b6aebef3149b4',
        inputs='28bb113a743f8327830392c13fff29edfdcea38b66d82b332020d20c63a0618b'),
    'empty': dict(
        run='ingest-empty-installed-engine-788f971',
        database='bb_scope_bind_788f971_installed_empty_normal',
        snapshot='installed-empty-normal-terminal-after.json',
        snapshot_sha256='5cc9803df1c4f2093f56694ca08857bbdf62066c51f260a35637ce2208aa7579',
        result_sha256='ff1d2e111dc42ff4026c7e42985a7814dbf058eb0bd1d7ba92c5e269e7642d34',
        status='PASS_INSTALLED_EMPTY_RETIREMENT',
        native_file='installed-empty-normal-terminal-after-native.json',
        native_sha256='53ca6c812d7ed8de07a0843af25b438794e5aa7b4f925f296c1c0de80c14097b',
        inputs_file='installed-empty-normal-terminal-after-inputs.json',
        inputs_sha256='c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
        store='installed-empty-normal/store', floor=31,
        tables='1730fd0455a24394dec946e1b121936e46a2819765a968bd34fbad8fab6c6d0a',
        ledger='68a0276d2dd589206b81969d26ec6e792c7c295953e507671a0210838909a3ad',
        native='72d99edcd999a4c0db3358ee9622d02a3cb9e9aacb5d3dc762dd0586248156f4',
        inputs='5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea'),
}
MIGRATIONS = {
    32: ('initial_activation', '1af4c64b3c1a311cac185d7ec8f015974b339ab60138a7454a3047d2d50b072f'),
    33: ('activated_initial_recovery', '4f22eb019e831126bf3f5c712d0ddf1e8213dfdfcdf9afaf5b38c80418cb5804'),
    34: ('activated_control_retirement', 'd3ccd19efd29a5c5db849c8bd21db45e4699076122d0bb6cd659fdd736d0b87a'),
    35: ('activated_source_closure', '80b5441cbb2de1f782ce6458e1c2b26a2f3730f51e61619af49f1e8d8adc375a'),
    36: ('activated_acquisition_abandonment', '19e2390b46d9eba7e4eac765eafc30fc7b2cac03e03c0918df64ed0b87642e54'),
    37: ('activated_initial_retirement', 'ea882fd887d0800ab1880646026ac85eeb6485f5fa4fd3323eee09b4bf96eac8'),
    38: ('repeatable_ingest_succession', 'bc5c90d25e5ae533e67dd328fbe1da32af3c19e8c03615241fa2359572aac087'),
}
CLOSURES = ('9e1a5b86a108f064a43c9f69313a39c9c3353fb45dc52dae230d190785d21501',
            '05d4cca350996266cfbdfd651a1e4bc5194c5b95a27650447d542ac66c87d5bb',
            'a986d2d60810f0249801dcddd4730e062c30399ec6748c330f9327627412016c')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def normalized(tables):
    return {name: sorted(rows, key=encoded) for name, rows in tables.items()}


def same(wanted, observed):
    assert encoded(normalized(wanted)) == encoded(normalized(observed)), 'complete SQL state differs'


def only(rows, **fields):
    row, = [r for r in rows if all(r.get(k) == v for k, v in fields.items())]
    return row


def binary(value):
    assert isinstance(value, str) and value.startswith('\\x')
    result = bytes.fromhex(value[2:])
    assert value == '\\x' + result.hex()
    return result


def be(value, width=8):
    assert type(value) is int and 0 <= value < 1 << (8 * width)
    return value.to_bytes(width, 'big')


def cbor(value):
    def head(major, n):
        if n < 24:
            return bytes([32 * major + n])
        width = next(w for w in (1, 2, 4, 8) if n < 1 << (8 * w))
        return bytes([32 * major + {1: 24, 2: 25, 4: 26, 8: 27}[width]]) + be(n, width)
    if type(value) is int:
        return head(0, value)
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    assert isinstance(value, list)
    return head(4, len(value)) + b''.join(map(cbor, value))


def instant(value):
    time = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    assert time.tzinfo is not None
    delta = time - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def origin_for(case):
    assert case in CASES
    return 'empty' if case == 'empty-terminal2' else 'activated'


def baseline(before, native, inputs, origin, window):
    pin = PINS[origin]
    assert len(before) == 75
    old = copy.deepcopy(before)
    for name in NEW_TABLES + (('initial_ingest_activations',) if origin == 'empty' else ()):
        assert old.pop(name) == [], 'new schema relation is not empty'
    ledger = old.pop('schema_versions')
    original = [r for r in ledger if r['migration'] <= pin['floor'] or r['migration'] > 38]
    added = [r for r in ledger if pin['floor'] < r['migration'] <= 38]
    assert sha(encoded(normalized(old))) == pin['tables'], 'original rows/xmin differ'
    assert sha(encoded(sorted(original, key=encoded))) == pin['ledger'], 'old migration ledger differs'
    assert len(added) == 38 - pin['floor']
    sample = only(original, migration=pin['floor'])
    old_xids = {r['xmin'] for table in [*old.values(), original] for r in table}
    migration_xid = added[0]['xmin']
    assert migration_xid.isdecimal() and migration_xid not in old_xids
    for version in range(pin['floor'] + 1, 39):
        row = only(added, migration=version)
        name, digest = MIGRATIONS[version]
        assert type(row['applied_at_us']) is int
        assert max(r['applied_at_us'] for r in original) <= row['applied_at_us'] <= window['before_us']
        assert row == dict(sample, migration=version, name=f'{version:04d}_{name}.sql',
                           checksum='\\x' + digest, applied_at_us=row['applied_at_us'], xmin=migration_xid)
    assert sha(encoded(native)) == pin['native'], 'native seed differs'
    assert sha(encoded(inputs)) == pin['inputs'], 'original input set differs'
    assert set(native) == {'event', 'rows', 'sequence'} and native['event'] == 'dump-state'
    return old


def science(pg, native, inputs, origin):
    """Literal request batches/dependencies, independent of product observations."""
    rows = {(r['cf'], bytes.fromhex(r['key'])): bytes.fromhex(r['value']) for r in native['rows']}
    assert len(rows) == len(native['rows']) and list(rows) == sorted(rows)
    intent = only(pg['initial_ingest_successions'])
    ds, resource, slot = (binary(intent[k]) for k in ('dataset_id', 'owner_resource_id', 'producer_slot_id'))
    manifests = [binary(intent[k]) for k in ('previous_manifest', 'target_manifest')]
    handoff = binary(intent['handoff'])
    assert rows['default', b'\x01\x0a' + ds + resource + be(1)] == handoff
    assert handoff == b'\x01\x0a' + slot + be(1) + be(4) + be(12) + bytes.fromhex(CLOSURES[0]) \
        + b'\x01\x0a' + manifests[0] + b'\x01\x0a' + manifests[1]
    assert rows['default', b'\x01\x01'] == manifests[1]
    counts = (4, 0 if origin == 'empty' else 2)
    cuts = (12, 0 if origin == 'empty' else 5)
    summaries, entries = [], []

    def fact(epoch, sequence):
        source = [ds, 0, epoch, sequence]
        value = rows['facts', b'\x01\x20' + ds + be(0, 4) + be(epoch) + be(sequence)]
        assert value[:2] == b'\x01\x20' and sha(value[34:]) == value[2:34].hex()
        return [source, value[2:34]]

    for epoch, (manifest, count, cut) in enumerate(zip(manifests, counts, cuts), 1):
        assert len(manifest) == 266 and manifest[:10] == b'\x01\x01' + be(epoch, 2) + be(1, 2) * 3
        assert manifest[10:26] == ds and manifest[26:34] == be(epoch - 1)
        assert manifest[34:62] == resource + be(0, 4) + be(epoch)
        assert manifest[102:104] == b'\x01\x09'
        assert rows['default', b'\x01\x02' + ds + be(0, 4) + be(epoch)] == b'\x01\x02' + be(cut)
        assert rows['receipts', b'\x01\x31' + slot + be(epoch)] == b'\x02\x31' + be(count) + bytes(16) + b'\x01'
        domain = 'initial-source' if epoch == 1 else ('initial-empty-attempt' if origin == 'empty' else 'activated-initial-source')
        prefix = b'bigbrotr/' + domain.encode() + b'-closure/1\0'
        incoming = sha(handoff) if epoch == 2 else '00' * 32
        if epoch == 2:
            prefix += bytes.fromhex(incoming)
        prefix += bytes.fromhex(sha(manifest)) + slot + be(epoch) + be(count)
        for sequence in range(1, count + 1):
            request = only(pg['producer_requests'], producer_epoch=epoch, request_sequence=sequence)
            outcome = only(pg['producer_outcomes'], producer_epoch=epoch, request_sequence=sequence)
            name = slot.hex() + f'-{epoch:016x}-{sequence:016x}.cbor'
            assert inputs[name] == dict(bytes=request['input_bytes'], sha256=request['request_digest'][2:])
            if epoch == 1:
                first, last = ((1, 2), (3, 3), (4, 7), (8, 12))[sequence - 1]
                batch = [(1, n) for n in range(first, last + 1)]
                deps = [(1, n) for n in ((1, 2), (3,), range(3, 8), range(3, 13))[sequence - 1]]
            else:
                batch = [(2, 1)] if sequence == 1 else [(2, n) for n in range(2, 6)]
                deps = [(1, 3)] + [(2, n) for n in range(1, 2 if sequence == 1 else 6)]
            policy = int.from_bytes(manifest[94:102], 'big')
            receipt = b'bigbrotr/receipt/1\0' + cbor([1, [slot, epoch, sequence],
                binary(request['request_digest']), [ds, epoch - 1, 1, manifest[78:94], policy],
                [fact(*v) for v in batch], [fact(*v) for v in deps]])
            assert rows['receipts', b'\x01\x30' + slot + be(epoch) + be(sequence)] == receipt
            assert binary(outcome['outcome_bytes']) == receipt and outcome['outcome_sha256'] == '\\x' + sha(receipt)
            assert outcome['policy_epoch'] == policy and outcome['outcome_kind'] == 1
            assert outcome['request_digest'] == request['request_digest']
            assert outcome['manifest_sha256'] == request['manifest_sha256'] == '\\x' + sha(manifest)
            prefix += be(sequence) + binary(request['request_digest']) + bytes.fromhex(sha(receipt))
        closure = sha(prefix + be(cut))
        assert closure == CLOSURES[0 if epoch == 1 else (2 if origin == 'empty' else 1)]
        assert only(pg['source_streams'], owner_epoch=epoch)['closure_digest'] == '\\x' + closure
        kind = 'initial' if epoch == 1 else ('unactivated-empty' if origin == 'empty' else 'activated')
        summaries.append(dict(epoch=epoch, terminal_kind=kind, resolved_requests=count,
                              last_complete_sequence=cut, digest=closure,
                              event_request_mask=8 if epoch == 1 else (0 if origin == 'empty' else 2)))
        entries.append(dict(manifest=manifest.hex(), issued_through=count, source_through=cut,
                            closing=True, incoming_sha256=incoming,
                            terminal_kind='initial' if epoch == 1 else None,
                            terminal_digest=closure if epoch == 1 else None))
    return dict(slot=slot.hex(), summaries=summaries, entries=entries, counts=counts)


def verify(events, before, native, inputs, case, server_window):
    """All finite phases and whole SQL/native/input states, not process authority."""
    origin = origin_for(case)
    success = case != 'activated-metadata-short'
    assert set(server_window) == {'before_us', 'after_us'}
    assert all(type(v) is int and v > 0 for v in server_window.values())
    assert 0 <= server_window['after_us'] - server_window['before_us'] <= 120_000_000
    baseline(before, native, inputs, origin, server_window)
    known = science(before, native, inputs, origin)
    booleans = {'closing', 'present', 'failed', 'joined', 'success', 'admission', 'repair',
                'matches_history', 'existing_copy_checked'}
    numbers = {'epoch', 'sequence', 'pid', 'control_epoch', 'config_revision', 'policy_revision',
               'native_epoch', 'registered_epoch', 'replay_through', 'metadata_bytes', 'memberships',
               'issued_through', 'source_through', 'resolved_requests', 'last_complete_sequence',
               'event_request_mask', 'verified_through'}
    def types(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in booleans:
                    assert type(child) is bool
                if key in numbers and child is not None:
                    assert type(child) is int
                types(child)
        elif isinstance(value, list):
            for child in value:
                types(child)
    types(events)
    position = snapshots = refreshes = 0
    current = before
    used_xids = {r['xmin'] for table in before.values() for r in table}

    def at(value):
        assert type(value) is int and server_window['before_us'] <= value <= server_window['after_us'], 'time outside observed window'

    def fresh(value):
        assert isinstance(value, str) and value.isdecimal() and int(value) > 0 and value not in used_xids, 'transaction identity reused'
        used_xids.add(value)

    def take(event):
        nonlocal position
        assert position < len(events), ('missing event', event)
        value = events[position]
        position += 1
        assert value['event'] == 'lineage-history-' + event, ('unexpected event', value)
        return value

    def snapshot(stage):
        nonlocal snapshots
        event = take('state')
        assert set(event) == {'event', 'stage', 'state', 'rows'} and event['stage'] == stage
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence', 'control_sha256', 'control_tables', 'originals'}
        assert event['rows'] == native['rows'] and state['sequence'] == native['sequence'], 'native mutation'
        assert state['native_rows'] == len(native['rows']) and state['native_sha256'] == sha(encoded(event['rows']))
        tables = state['control_tables']
        assert len(tables) == 75 and [name for name, _ in tables] == sorted(before)
        assert state['control_sha256'] == sha(encoded(tables))
        assert state['originals'] == [[name, inputs[name]] for name in sorted(inputs)], 'input mutation'
        snapshots += 1
        return {name: loads(value) for name, value in tables}

    resource = take('resources')
    assert set(resource) == {'event', 'metadata_bytes', 'metadata_scratch_bytes', 'metadata_entry_bytes',
        'input_bytes', 'outcome_bytes', 'effect_bytes', 'native_bytes', 'transport_bytes',
        'observation_bytes', 'operation_timeout_ms', 'frame_bytes'}
    assert resource == dict(resource, metadata_bytes=132608 if success else 132095,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2,
        operation_timeout_ms=10000, frame_bytes=16 * 1024**2)
    # These three observed grant sizes include Rust struct sizes. Check their
    # separate bounded presence without inventing a portable sizeof/RSS result.
    for name in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resource[name]) is int and 0 < resource[name] <= 256 * 1024**2
    assert sum(resource[n] for n in ('metadata_bytes', 'input_bytes', 'outcome_bytes',
                                    'effect_bytes', 'transport_bytes', 'observation_bytes')) <= 1024**3
    planned = take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    for field in ('role', 'incarnation'):
        assert re.fullmatch('[0-9a-f]{32}', planned[field]) and int(planned[field], 16)
    same(current, snapshot('before-claim'))
    claimed = take('control')
    acquired_policy = take('policy')
    backend = take('backend')
    observed = snapshot('after-claim')
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    assert previous['state'] == 'released'
    control = only(observed['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
    role = only(observed['role_instances'], role_instance_id='\\x' + planned['role'])
    assert not any(r['role_instance_id'] == role['role_instance_id'] for r in before['role_instances'])
    assert not any(r['process_incarnation'] == '\\x' + planned['incarnation'] for r in before['owner_epochs'])
    at(role['created_at_us'])
    at(control['acquired_at_us'])
    assert previous['released_at_us'] <= role['created_at_us'] <= control['acquired_at_us']
    fresh(control['xmin'])
    assert type(control['control_backend_pid']) is int and control['control_backend_pid'] > 0
    assert server_window['before_us'] <= instant(control['control_backend_start']) <= control['acquired_at_us']
    wanted = copy.deepcopy(before)
    wanted['role_instances'].append(dict(role_instance_id='\\x' + planned['role'],
        dataset_id=previous['dataset_id'], role='control', created_at_us=role['created_at_us'],
        retired_at_us=None, xmin=control['xmin']))
    wanted['owner_epochs'].append(dict(previous, epoch=previous['epoch'] + 1,
        role_instance_id='\\x' + planned['role'], process_incarnation='\\x' + planned['incarnation'],
        state='active', acquired_at_us=control['acquired_at_us'],
        lease_expires_at_us=control['acquired_at_us'] + 20_000_000, released_at_us=None,
        exclusion_method=None, exclusion_digest=None, control_backend_pid=control['control_backend_pid'],
        control_backend_start=control['control_backend_start'], xmin=control['xmin']))
    same(wanted, observed)
    assert claimed == dict(event='lineage-history-control', phase='claimed', resource=control['resource_id'][2:],
        epoch=control['epoch'], role=planned['role'], incarnation=planned['incarnation'])
    assert re.fullmatch('[a-z0-9_]{1,63}', backend['database'])
    assert backend == dict(event='lineage-history-backend', pid=control['control_backend_pid'],
        backend_start=control['control_backend_start'], database=backend['database'],
        control_epoch=control['epoch'], present=True)
    current = observed

    def policy(event, phase, epoch, sequence):
        dataset = only(before['datasets'])
        config = only(before['config_revisions'], revision=dataset['config_revision'])
        accepted = only(before['policy_revisions'], revision=dataset['policy_revision'])
        assert event == dict(event='lineage-history-policy', phase=phase, epoch=epoch, sequence=sequence,
            control_epoch=control['epoch'], config_revision=dataset['config_revision'],
            policy_revision=dataset['policy_revision'], config_digest=config['document_digest'][2:],
            policy_digest=accepted['document_digest'][2:])

    def refresh(epoch, sequence):
        nonlocal current, control, refreshes
        policy(take('policy'), 'refreshed', epoch, sequence)
        observed = snapshot(f"refreshed-{epoch if epoch is not None else 'final'}-{sequence if sequence is not None else 'epoch'}")
        updated = only(observed['owner_epochs'], role='control', epoch=control['epoch'])
        sample = updated['lease_expires_at_us'] - 20_000_000
        at(sample)
        assert updated['lease_expires_at_us'] >= control['lease_expires_at_us']
        fresh(updated['xmin'])
        wanted = copy.deepcopy(current)
        wanted['owner_epochs'][wanted['owner_epochs'].index(control)] = dict(control,
            lease_expires_at_us=updated['lease_expires_at_us'], xmin=updated['xmin'])
        same(wanted, observed)
        current, control = observed, updated
        refreshes += 1

    policy(acquired_policy, 'acquired', None, None)
    same(current, snapshot('before-read'))
    if success:
        assert take('loaded') == dict(event='lineage-history-loaded', state='Terminal', native_epoch=2,
            registered_epoch=2, replay_through=2, producer_slot=known['slot'], pending_target=None,
            metadata_bytes=132096, memberships=3, entries=known['entries'])
        same(current, snapshot('loaded'))
        for epoch, count in enumerate(known['counts'], 1):
            for sequence in range(1, count + 1):
                refresh(epoch, sequence)
                assert take('request') == dict(event='lineage-history-request', producer_slot=known['slot'],
                    epoch=epoch, sequence=sequence, existing_copy_checked=True)
                same(current, snapshot(f'resolved-{epoch}-{sequence}'))
            refresh(epoch, None)
            assert take('epoch') == dict(event='lineage-history-epoch', summary=known['summaries'][epoch - 1])
            same(current, snapshot(f'epoch-{epoch}'))
        refresh(None, None)
        assert take('proof') == dict(event='lineage-history-proof', verified_through=2,
            last_epoch=known['summaries'][-1], matches_history=True, admission=False)
        same(current, snapshot('proof'))
    assert take('result') == dict(event='lineage-history-result', stage='complete' if success else 'inspect',
        epoch=None, sequence=None, error=None if success else 'Native(Native(InsufficientReservation))')
    same(current, snapshot('after-read'))
    assert take('native') == dict(event='lineage-history-native', state='closed')
    same(current, snapshot('before-control-end'))
    assert take('driver') == dict(event='lineage-history-driver', driver='control', failed=False, joined=True)
    assert take('backend') == dict(backend, present=False)
    phase = 'released-and-joined' if success else 'dropped-and-joined'
    assert take('control') == dict(event='lineage-history-control', phase=phase,
        resource=control['resource_id'][2:], epoch=control['epoch'])
    observed = snapshot('after-control-end')
    wanted = copy.deepcopy(current)
    if success:
        released = only(observed['owner_epochs'], role='control', epoch=control['epoch'])
        retired = only(observed['role_instances'], role_instance_id=control['role_instance_id'])
        at(released['released_at_us'])
        at(retired['retired_at_us'])
        assert control['lease_expires_at_us'] - 20_000_000 <= released['released_at_us'] <= retired['retired_at_us']
        assert released['released_at_us'] < control['lease_expires_at_us']
        fresh(released['xmin'])
        exclusion = sha(b'bigbrotr/control-exclusion/1\0' + binary(control['dataset_id'])
            + binary(control['resource_id']) + be(control['epoch']) + binary(control['process_incarnation']) * 2)
        wanted['owner_epochs'][wanted['owner_epochs'].index(control)] = dict(control, state='released',
            released_at_us=released['released_at_us'], exclusion_method='storage_fenced',
            exclusion_digest='\\x' + exclusion, xmin=released['xmin'])
        wanted['role_instances'][wanted['role_instances'].index(role)] = dict(role,
            retired_at_us=retired['retired_at_us'], xmin=released['xmin'])
    same(wanted, observed)
    same(observed, snapshot('final'))
    assert take('driver') == dict(event='lineage-history-driver', driver='observer', failed=False, joined=True)
    assert take('complete') == dict(event='lineage-history-complete', native='closed', control=phase,
        observer='joined', success=success, admission=False, repair=False)
    assert position == len(events), 'unclassified trailing event'
    assert snapshots == ((26 if origin == 'activated' else 22) if success else 7)
    return dict(status='PASS', case=case, snapshots=snapshots, tables=75,
        native_rows=len(native['rows']), sequence=native['sequence'], inputs=len(inputs),
        control_epoch=control['epoch'], refreshes=refreshes, database=backend['database'],
        summaries=known['summaries'] if success else [], proof=success,
        error=None if success else 'Native(Native(InsufficientReservation))',
        preserved='All original rows/xmin, native keys/sequence and input digests',
        scope='Finite connected history observation; no admission, repair, installed supervision or RSS claim')


def loads(text):
    def unique(pairs):
        result = dict(pairs)
        assert len(result) == len(pairs), 'duplicate JSON field'
        return result
    return json.loads(text, object_pairs_hook=unique, parse_constant=lambda s: (_ for _ in ()).throw(ValueError(s)))


def negative_controls(events, before, native, inputs, case, server_window):
    """Small coherent capture perturbations; never product/runtime evidence."""
    verify(events, before, native, inputs, case, server_window)
    rejected = []

    def reject(name, change):
        changed = copy.deepcopy(events)
        change(changed)
        try:
            verify(changed, before, native, inputs, case, server_window)
        except (AssertionError, KeyError, ValueError, TypeError, OverflowError):
            rejected.append(name)
        else:
            raise AssertionError('accepted altered evidence: ' + name)

    def event(values, name, **fields):
        return only(values, event='lineage-history-' + name, **fields)

    def sql(values, mutate):
        for value in values:
            if value['event'] != 'lineage-history-state':
                continue
            state = value['state']
            pg = {name: loads(rows) for name, rows in state['control_tables']}
            mutate(pg)
            state['control_tables'] = [[name, encoded(rows).decode()] for name, rows in sorted(pg.items())]
            state['control_sha256'] = sha(encoded(state['control_tables']))

    def original_changed(values):
        sql(values, lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='4000000000'))

    def native_changed(values):
        for value in values:
            if value['event'] == 'lineage-history-state':
                row = only(value['rows'], cf='default', key='0101')
                raw = bytearray.fromhex(row['value'])
                raw[-1] ^= 1
                row['value'] = raw.hex()
                value['state']['native_sha256'] = sha(encoded(value['rows']))

    def wrong_backend(values):
        for value in values:
            if value['event'] == 'lineage-history-backend':
                value['pid'] += 1

    reject('historical-xmin-rehashed-throughout', original_changed)
    reject('native-bytes-rehashed-throughout', native_changed)
    reject('both-backend-events-use-foreign-pid', wrong_backend)
    reject('native-sequence', lambda v: event(v, 'state', stage='final')['state'].update(sequence=native['sequence'] + 1))
    reject('original-input-digest', lambda v: event(v, 'state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('wrong-metadata-grant', lambda v: event(v, 'resources').update(metadata_bytes=132096))
    reject('control-driver-not-joined', lambda v: event(v, 'driver', driver='control').update(joined=False))
    reject('observer-driver-failed', lambda v: event(v, 'driver', driver='observer').update(failed=True))
    reject('terminal-repair-claim', lambda v: event(v, 'complete').update(repair=True))
    if case == 'activated-metadata-short':
        reject('unrelated-failure-taxonomy', lambda v: event(v, 'result').update(error='DeliveryUnknown'))
        reject('refusal-claimed-positive', lambda v: event(v, 'complete').update(success=True))
    else:
        def summary_changed(values, field, replacement):
            for value in values:
                if value['event'] == 'lineage-history-epoch' and value['summary']['epoch'] == 2:
                    value['summary'][field] = replacement
                if value['event'] == 'lineage-history-proof':
                    value['last_epoch'][field] = replacement

        reject('coherent-last-closure', lambda v: summary_changed(v, 'digest', '00' * 32))
        reject('coherent-event-mask', lambda v: summary_changed(v, 'event_request_mask', 16))
        reject('wrong-terminal-kind', lambda v: summary_changed(v, 'terminal_kind', 'initial'))
        reject('claiming-pending-epoch3', lambda v: event(v, 'loaded').update(pending_target=3))
        reject('skip-original-request', lambda v: v.pop(next(i for i, e in enumerate(v) if e['event'] == 'lineage-history-request')))
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def replay(root):
    """Bind immutable origins, migration/transport files and external final rows."""
    root = Path(root).resolve()
    folder = root / 'history'
    bound = {}

    def read(path):
        path = Path(path)
        raw = path.read_bytes()
        bound[str(path)] = sha(raw)
        return loads(raw)

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_HISTORY_FIRST_WAVE'
    assert [case['name'] for case in report['cases']] == list(CASES)
    assert len({case['database'] for case in report['cases']}) == 3, 'cases must use distinct database copies'
    for field in ('cleanup_errors', 'missing_owned', 'unrelated_changed'):
        assert report[field] == []
    originals = {}
    for origin, pin in PINS.items():
        declaration = report['origins'][origin]
        for field in ('run', 'database', 'snapshot', 'snapshot_sha256', 'result_sha256', 'status'):
            assert declaration[field] == pin[field], ('origin locator differs', origin, field)
        basis = root.parent / pin['run'] / 'bindings'
        result = read(basis / 'result.json')
        assert bound[str(basis / 'result.json')] == pin['result_sha256']
        assert result['status'] == pin['status']
        original = read(basis / pin['snapshot'])
        assert bound[str(basis / pin['snapshot'])] == pin['snapshot_sha256']
        native = read(basis / pin['native_file'])
        assert bound[str(basis / pin['native_file'])] == pin['native_sha256']
        raw_inputs = read(basis / pin['inputs_file'])
        assert bound[str(basis / pin['inputs_file'])] == pin['inputs_sha256']
        inputs = raw_inputs.get('inputs', raw_inputs)
        same(original, read(folder / (origin + '-original.json')))
        original_input_dir = basis / pin['store'] / 'producer-inputs-v1'
        assert sorted(p.name for p in original_input_dir.iterdir()) == sorted(inputs)
        for name, identity in inputs.items():
            path = original_input_dir / name
            raw = path.read_bytes()
            assert dict(bytes=len(raw), sha256=sha(raw)) == identity
            bound[str(path)] = sha(raw)
            if 'contents' in raw_inputs:
                assert raw.hex() == raw_inputs['contents'][name]
        preservation = declaration['preserved']
        assert preservation == dict(rows_unchanged=True, catalog_unchanged=True,
            native_unchanged=True, inputs_unchanged=True, physical_unchanged=True)
        assert all(value is True for value in preservation.values())
        originals[origin] = dict(control=original, native=native, inputs=inputs)

    results = []
    for declared in report['cases']:
        case = declared['name']
        origin = origin_for(case)
        seed = originals[origin]
        assert declared['origin'] == origin and declared['status'] == 'PASS'
        assert declared['metadata_bytes'] == (132095 if case == CASES[2] else 132608)
        assert type(declared['returncode']) is int and declared['returncode'] == 0
        assert declared['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        migrations = declared['migrations']
        assert [r['action'] for r in migrations] == ['plan', 'apply', 'apply']
        steps = [dict(name=f'{v:04d}_{MIGRATIONS[v][0]}.sql', sha256=MIGRATIONS[v][1], version=str(v))
                 for v in range(PINS[origin]['floor'] + 1, 39)]
        for index, answer in enumerate(migrations):
            assert set(answer) == {'action', 'exit_code', 'stdout', 'stderr'}
            assert answer['exit_code'] == 0 and answer['stderr'] == ''
            assert loads(answer['stdout']) == dict(format='bigbrotr-control-migration/1',
                scope='postgres-control', target_version='38',
                source_version=str(PINS[origin]['floor']) if index < 2 else '38',
                current_version=str(PINS[origin]['floor']) if index == 0 else '38',
                status=('planned', 'applied', 'current')[index],
                applied_count=str(len(steps)) if index == 1 else '0',
                steps=steps if index < 2 else [], dataset_initialization='not-performed',
                release_upgrade_qualification='not-performed')
        case_read = lambda suffix: read(folder / (case + '-' + suffix + '.json'))
        same(seed['control'], case_read('copy-before'))
        before = case_read('before')
        same(before, case_read('migrated'))
        native = case_read('before-native')
        inputs = case_read('inputs')
        assert native == seed['native'] and inputs == seed['inputs']
        response = case_read('response')
        assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
        assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds'])
        assert 0 <= response['seconds'] <= 120
        assert not any(k in response for k in ('error', 'orchestration_error', 'cleanup_error'))
        events = [loads(line) for line in response['stdout'].splitlines()]
        window = case_read('clock')
        checked = verify(events, before, native, inputs, case, window)
        assert checked['database'] == declared['database']
        final = only(events, event='lineage-history-state', stage='final')
        same({name: loads(rows) for name, rows in final['state']['control_tables']}, case_read('after'))
        assert case_read('after-native') == native and case_read('after-inputs') == inputs
        assert case_read('backends') == []
        checked['negative_controls'] = negative_controls(events, before, native, inputs, case, window)
        results.append(checked)
    return dict(status='PASS_CONNECTED_HISTORY_CAPTURE_REPLAY', source=report['source'], cases=results,
        snapshots=sum(r['snapshots'] for r in results), control_tables=75, bound_files=bound,
        original_preservation='Pinned source bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Three finite terminal2 observations, not installed recovery, new-family gaps, writer authority or RSS')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = replay(args.root)
    output = json.dumps(result, indent=2) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(output)
    else:
        print(output, end='')
