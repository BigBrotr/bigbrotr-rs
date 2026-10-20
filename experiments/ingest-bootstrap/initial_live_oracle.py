"""Independent finite live-initial activation and import-admission observations.

No SQL, product codecs, process control or inferred writer authority. Candidate
identities are parameters checked for freshness; scientific fields and all byte
recipes are fixed independently. Import without observation does not qualify
network provenance-off behavior or an installed import executor.
"""
import copy
import json

from bootstrap_oracle import cbor
from initial_succession_oracle import (
    be, binary, bytea, claim, digest, encoded, ended, expected, native_rows,
    only, prepared, renewed, same,
)


MODES = ('successor-live-zero', 'successor-live-normal', 'successor-live-off',
         'successor-live-commit-before', 'successor-live-commit-after')
ACTIVATIONS = 'initial_ingest_activations'
SEED_CONTROL = '87ff8c09c1eec90dbbed8b333761ef9816828273ac6bd53beae2028e02b6c21a'
SEED_NATIVE = '91b64595e263c5d297e0d3a67b82fdf834777ade7f3e93524f0cc972eeff3b1d'
SEED_INPUTS = '5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea'
MIGRATIONS = {
    30: ('0030_initial_succession.sql', '55049328c3f0be13a22af199f1599af157ce02e5a74760a296909469693be1f4'),
    31: ('0031_initial_empty_retirement.sql', '52819215086abb15c06b25ebb536b16955256e4707aaf0f1a89060c66fc96b8a'),
    32: ('0032_initial_activation.sql', '1af4c64b3c1a311cac185d7ec8f015974b339ab60138a7454a3047d2d50b072f'),
}
CORPUS_DIGEST = bytes.fromhex('4c931ab34cb7adeb707fc2866f4a50b3ec8b694cbb2d4fb5e335f8f37a2d8875')
EVENT_ID = bytes.fromhex('a669974f5d6f76e4668b0be874435f14be799c04f0e70e5ec3d0967f1babf6bb')
BODY = [EVENT_ID, bytes.fromhex('79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798'),
        1, 1, [['x'], ['x', ''], ['t', 'é'], ['t', 'é']], 'same event\nwith ordered tags']
PUBLIC_SIG = bytes.fromhex('9444d1fab8095f14d43137f841a400f0648cd614515f53bb5545749e373056381ce2963d43fdb5d10d9294846da6fc895d1fdfaf3417399a5c2699414877c180')
ALTERNATE_SIG = bytes.fromhex('f455e992a8359238a5583f91843be3c4fd0847574162222afdd8dc0aee67f5fab742f6eaf378cae2d66ce268f7b86698b4912579c534d568c6f53adf4cc90c85')


def sha(value):
    return bytes.fromhex(digest(value))


def clock(value, window):
    assert type(value) is int
    assert window['before_us'] <= value <= window['after_us'], 'time outside server window'


def check_window(window):
    assert set(window) == {'before_us', 'after_us'}
    assert all(type(v) is int and v > 0 for v in window.values())
    assert 0 <= window['after_us'] - window['before_us'] <= 120_000_000


def fresh_xmin(value, previous):
    assert isinstance(value, str) and value.isascii() and value.isdecimal() and int(value) > 0
    assert all(value != row['xmin'] for rows in previous.values() for row in rows), 'reused transaction'


def migrated(seed, original, native, inputs, window):
    assert set(seed) == {'control', 'native', 'inputs', 'sequence'}
    assert seed['sequence'] == 58 and len(seed['control']) == 70
    assert digest(encoded({k: sorted(v, key=encoded) for k, v in seed['control'].items()})) == SEED_CONTROL
    assert digest(encoded(seed['native'])) == SEED_NATIVE
    assert digest(encoded(seed['inputs'])) == SEED_INPUTS
    assert native == seed['native'] and inputs == seed['inputs']
    wanted = copy.deepcopy(seed['control'])
    wanted['initial_ingest_successions'] = []
    wanted[ACTIVATIONS] = []
    migration_tx = only(original['schema_versions'], migration=30)['xmin']
    fresh_xmin(migration_tx, wanted)
    for number, (name, checksum) in MIGRATIONS.items():
        row = only(original['schema_versions'], migration=number)
        assert row['xmin'] == migration_tx, 'pending migrations must share one transaction'
        assert type(row['applied_at_us']) is int
        assert max(r['applied_at_us'] for r in wanted['schema_versions']) <= row['applied_at_us'] <= window['before_us']
        assert row == dict(name=name, checksum='\\x' + checksum, migration=number,
            engine='postgres-control', applied_by='bigbrotr_migrator', reader_min=1,
            reader_max=1, writer_floor=1, applied_at_us=row['applied_at_us'], xmin=row['xmin'])
        wanted['schema_versions'].append(row)
    assert len(wanted) == len(original) == 72
    same(wanted, original)


def minted_plan(original, native, inputs, observed):
    plan, old, _ = expected(original, native, inputs, observed['created_at_us'])
    role, incarnation = observed['target_role'], observed['target_incarnation']
    domains = observed['domains']
    assert len(domains) == 9 and [r[0] for r in domains] == list(range(1, 10))
    for value in [role, incarnation, *(r[1] for r in domains)]:
        assert isinstance(value, str) and len(value) == 32
        assert bytes.fromhex(value).hex() == value and int(value, 16) != 0
    assert role != incarnation, 'candidate role and incarnation collide'
    assert not any(r['role_instance_id'] == '\\x' + role for r in original['role_instances'])
    assert not any(r['process_incarnation'] == '\\x' + incarnation for r in original['owner_epochs'])
    old_domains = {r['allocation_domain'][2:] for r in original['allocation_domains']}
    assert len({r[1] for r in domains}) == 9
    assert not old_domains.intersection(r[1] for r in domains), 'candidate domain is not fresh'
    target = bytearray.fromhex(plan['target_manifest'])
    target[62:78] = bytes.fromhex(incarnation)
    for kind, domain in domains:
        offset = 104 + (kind - 1) * 18
        target[offset:offset + 18] = be(kind, 2) + bytes.fromhex(domain)
    target = bytes(target)
    previous = bytes.fromhex(plan['previous_manifest'])
    ds, slot, resource = (bytes.fromhex(plan[k]) for k in ('dataset', 'producer_slot', 'resource'))
    handoff = (b'\x01\x0a' + slot + be(1) + be(4) + be(12)
        + bytes.fromhex(plan['closure_digest']) + be(len(previous), 2) + previous
        + be(len(target), 2) + target)
    plan.update(target_incarnation=incarnation, target_role=role, domains=domains,
        target_manifest=target.hex(), handoff=handoff.hex())
    assert observed == plan, 'plan differs from independent encoding'
    wanted = dict(old)
    wanted['default', b'\x01\x01'] = target
    wanted['default', b'\x01\x0a' + ds + resource + be(1)] = handoff
    wanted['default', b'\x01\x02' + ds + be(0, 4) + be(2)] = b'\x01\x02' + be(0)
    wanted['receipts', b'\x01\x31' + slot + be(2)] = b'\x01\x31' + be(0) * 3
    assert len(wanted) == 52
    # Independently rebuild every retained original receipt, including closure
    # membership. These old bytes are never rewritten for the new owner.
    refs = {}
    for n in range(1, 13):
        value = old['facts', b'\x01\x20' + ds + be(0, 4) + be(1) + be(n)]
        assert value[:2] == b'\x01\x20' and sha(value[34:]) == value[2:34]
        refs[n] = [[ds, 0, 1, n], value[2:34]]
    for n, (first, last), dependencies in zip(range(1, 5),
            [(1, 2), (3, 3), (4, 7), (8, 12)],
            [[1, 2], [3], list(range(3, 8)), list(range(3, 13))]):
        request = only(original['producer_requests'], producer_epoch=1, request_sequence=n)
        outcome = only(original['producer_outcomes'], producer_epoch=1, request_sequence=n)
        receipt = b'bigbrotr/receipt/1\0' + cbor([1, [slot, 1, n], binary(request['request_digest']),
            [ds, 0, 1, previous[78:94], 1], [refs[k] for k in range(first, last + 1)],
            [refs[k] for k in dependencies]])
        assert old['receipts', b'\x01\x30' + slot + be(1) + be(n)] == receipt
        assert outcome['outcome_bytes'] == bytea(receipt) and outcome['outcome_sha256'] == bytea(sha(receipt))
        assert outcome['outcome_kind'] == outcome['policy_epoch'] == 1
        assert outcome['manifest_sha256'] == request['manifest_sha256'] == bytea(sha(previous))
    return plan, old, wanted


class Science:
    """Fixed finite import recipe with complete byte-level native effects."""
    def __init__(self, plan, prepared_rows, inputs):
        self.plan, self.rows, self.inputs = plan, dict(prepared_rows), copy.deepcopy(inputs)
        self.ds, self.slot = (bytes.fromhex(plan[k]) for k in ('dataset', 'producer_slot'))
        self.domains = {k: bytes.fromhex(v) for k, v in plan['domains']}
        old_manifest = bytes.fromhex(plan['previous_manifest'])
        old_visibility = old_manifest[104 + 3 * 18 + 2:104 + 4 * 18]
        self.scope = [self.ds, 4, old_visibility, 3]
        self.lineage = self.ref(9)
        self.sequence = 62
        self.writes = 0
        old = self.rows['facts', b'\x01\x20' + self.ds + be(0, 4) + be(1) + be(3)]
        self.refs = {(1, 3): [[self.ds, 0, 1, 3], old[2:34]]}
        self.requests, self.receipts = {}, {}
        assert ('default', b'\x01\x06' + self.ds + EVENT_ID) not in self.rows

    def ref(self, kind, ordinal=1):
        assert type(ordinal) is int and 1 <= ordinal <= 3
        return [self.ds, kind, self.domains[kind], ordinal]

    @staticmethod
    def ref_key(reference):
        ds, kind, domain, ordinal = reference
        return ds + be(kind, 2) + domain + be(ordinal)

    def src_key(self, n):
        return self.ds + be(0, 4) + be(2) + be(n)

    def put(self, cf, key, value):
        self.rows[cf, key] = value
        self.writes += 1

    def fact(self, n, family, payload, timestamp):
        source = [self.ds, 0, 2, n]
        value = b'bigbrotr/fact/1\0' + cbor([source, family, 1, self.scope, timestamp, payload])
        self.refs[2, n] = [source, sha(value)]
        self.put('facts', b'\x01\x20' + self.src_key(n), b'\x01\x20' + sha(value) + value)

    def index(self, domain, key, n):
        prefix = bytes([1, domain])
        self.put('default', prefix + key, prefix + self.src_key(n) + self.refs[2, n][1])

    def dictionary(self, n, kind, schema, value, timestamp, *, ordinal=1):
        reference = self.ref(kind, ordinal)
        descriptor = [1, schema, 1, value]
        payload = [reference, *descriptor]
        self.fact(n, 'dictionary', payload, timestamp)
        self.index(4, self.ref_key(reference), n)
        canonical = cbor(descriptor)
        self.put('default', b'\x01\x05' + self.ds + be(kind, 2) + sha(canonical) + self.ref_key(reference),
            b'\x01\x05' + canonical)
        self.put('default', b'\x01\x03' + self.ds + be(kind, 2) + self.domains[kind], b'\x01\x03' + be(ordinal))
        return ['dictionary', 1, payload]

    def commit(self, n, original, members, dependencies):
        batch = [self.refs[2, k] for k in members] if members else None
        receipt = b'bigbrotr/receipt/1\0' + cbor([1 if batch else 2, [self.slot, 2, n], sha(original),
            [self.ds, 1, 1, bytes.fromhex(self.plan['recovery_generation']), 1], batch,
            [self.refs[key] for key in dependencies]])
        if batch:
            first, last = members[0], members[-1]
            self.put('default', b'\x01\x02' + self.ds + be(0, 4) + be(2), b'\x01\x02' + be(last))
            pending = sha(b'bigbrotr/admission-batch/1\0' + cbor([self.ds, 0, 2, first, last, batch]))
            self.put('facts', b'\x01\x21' + self.src_key(first), b'\x01\x21' + be(last) + pending)
        self.put('receipts', b'\x01\x30' + self.slot + be(2) + be(n), receipt)
        self.put('receipts', b'\x01\x31' + self.slot + be(2), b'\x01\x31' + be(n) + be(0) * 2)
        name = self.slot.hex() + '-0000000000000002-' + f'{n:016x}.cbor'
        self.inputs[name] = dict(bytes=len(original), sha256=digest(original))
        self.requests[n], self.receipts[n] = original, receipt
        self.sequence += self.writes
        self.writes = 0
        return receipt

    def advance(self, stage):
        if stage == 'dictionary':
            entry = self.dictionary(1, 9, 'dataset-lineage', dict(format='bigbrotr-signed-fixture',
                format_version=1, source_dataset_id=None, source_artifact_digest=CORPUS_DIGEST), 900_000_001)
            original = b'bigbrotr/producer-request/1\0' + cbor([[self.slot, 2, 1], 'admit-evidence', 1,
                [1, self.scope, [entry]]])
            result = self.commit(1, original, [1], [(1, 3), (2, 1)])
            assert (len(self.rows), self.sequence) == (58, 70)
            return result
        if stage == 'same-request':
            return self.receipts[2]
        assert stage in ('new-event', 'same-id-alternate-signature')
        n, signature = (2, PUBLIC_SIG) if stage == 'new-event' else (3, ALTERNATE_SIG)
        original = b'bigbrotr/producer-request/1\0' + cbor([[self.slot, 2, n], 'admit', 1,
            [1, self.scope, [*BODY, signature], 'import', self.lineage, None]])
        if n == 2:
            self.dictionary(2, 8, 'event', dict(event_id=EVENT_ID), 900_000_002)
            self.fact(3, 'event-payload', [*BODY, self.ref(8)], 900_000_002)
            self.index(6, self.ds + EVENT_ID, 3)
            self.fact(4, 'event-eligibility', [EVENT_ID, sha(b'bigbrotr/body/1\0' + cbor(BODY)),
                PUBLIC_SIG, 900_000_002, 'import', self.lineage], 900_000_002)
            self.index(7, self.ds + EVENT_ID + self.ref_key(self.scope), 4)
        result = self.commit(n, original, [2, 3, 4] if n == 2 else [],
            [(1, 3), (2, 1), (2, 2), (2, 3), (2, 4)])
        assert (len(self.rows), self.sequence) == ((68, 82) if n == 2 else (69, 84))
        return result


def activated(before, after, plan, control, window, reported=None):
    assert not before[ACTIVATIONS]
    row = only(after[ACTIVATIONS])
    tx, at = row['xmin'], row['activated_at_us']
    fresh_xmin(tx, before)
    clock(at, window)
    assert plan['created_at_us'] <= at < min(plan['lease_expires_at_us'], control['lease_expires_at_us'])
    assert isinstance(row['activation_transaction'], str) and row['activation_transaction'].isdecimal()
    assert 0 < int(row['activation_transaction']) < 1 << 64
    assert int(row['activation_transaction']) % (1 << 32) == int(tx)
    if reported is not None:
        assert reported == at
    intent = only(before['initial_ingest_successions'])
    fields = ('dataset_id', 'owner_resource_id', 'target_epoch', 'target_incarnation',
        'target_role_instance_id', 'producer_slot_id', 'producer_epoch', 'previous_manifest_sha256',
        'target_manifest_sha256', 'handoff_sha256', 'prepared_by_resource', 'prepared_by_epoch',
        'prepared_by_incarnation', 'config_revision', 'policy_revision', 'config_digest', 'policy_digest')
    value = {field: intent[field] for field in fields}
    value.update(activated_by_resource=control['resource_id'], activated_by_epoch=control['epoch'],
        activated_by_incarnation=control['process_incarnation'], activated_at_us=at,
        activation_transaction=row['activation_transaction'], xmin=tx)
    wanted = copy.deepcopy(before)
    wanted[ACTIVATIONS] = [value]
    same(wanted, after)
    return row


def admitted(before, after, science, n, control, window):
    request = only(after['producer_requests'], producer_epoch=2, request_sequence=n)
    outcome = only(after['producer_outcomes'], producer_epoch=2, request_sequence=n)
    input_bytes, receipt = science.requests[n], science.receipts[n]
    fresh_xmin(request['xmin'], before)
    fresh_xmin(outcome['xmin'], before)
    assert request['xmin'] != outcome['xmin'], 'reservation and outcome must commit separately'
    for value in (request['reserved_at_us'], outcome['recorded_at_us']):
        clock(value, window)
    activation = only(before[ACTIVATIONS])
    assert activation['activated_at_us'] <= request['reserved_at_us'] <= outcome['recorded_at_us']
    assert outcome['recorded_at_us'] < min(control['lease_expires_at_us'], science.plan['lease_expires_at_us'])
    if n > 1:
        previous = only(before['producer_outcomes'], producer_epoch=2, request_sequence=n - 1)
        assert previous['recorded_at_us'] <= request['reserved_at_us']
    manifest = bytea(sha(bytes.fromhex(science.plan['target_manifest'])))
    wanted_request = dict(producer_slot_id=bytea(science.slot), producer_epoch=2, request_sequence=n,
        dataset_id=bytea(science.ds), request_digest=bytea(sha(input_bytes)), input_bytes=len(input_bytes),
        input_profile=1, manifest_sha256=manifest, issuer_resource=control['resource_id'],
        issuer_epoch=control['epoch'], issuer_role='control', issuer_incarnation=control['process_incarnation'],
        reserved_at_us=request['reserved_at_us'], relay_endpoint_id=None, relay_access_scope_id=None,
        relay_enrollment_generation=None, relay_route_id=None, acquisition_scope_id=None,
        acquisition_job_id=None, xmin=request['xmin'])
    wanted_outcome = dict(producer_slot_id=bytea(science.slot), producer_epoch=2, request_sequence=n,
        dataset_id=bytea(science.ds), request_digest=bytea(sha(input_bytes)), manifest_sha256=manifest,
        outcome_kind=2 if n == 3 else 1, policy_epoch=1, outcome_bytes=bytea(receipt),
        outcome_sha256=bytea(sha(receipt)), recorder_resource=control['resource_id'],
        recorder_epoch=control['epoch'], recorder_role='control', recorder_incarnation=control['process_incarnation'],
        recorded_at_us=outcome['recorded_at_us'], xmin=outcome['xmin'])
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    assert producer['reserved_through_request'] == n - 1
    producer.update(reserved_through_request=n, xmin=request['xmin'])
    wanted['producer_requests'].append(wanted_request)
    wanted['producer_outcomes'].append(wanted_outcome)
    same(wanted, after)


class Capture:
    def __init__(self, events, original, native, inputs):
        self.events, self.original = events, original
        self.native, self.inputs, self.sequence = native, inputs, 58
        self.position = self.snapshots = 0

    def take(self, name, stage=None, phase=None):
        assert self.position < len(self.events), (name, stage, 'missing event')
        event = self.events[self.position]
        self.position += 1
        assert (event['event'], event.get('stage'), event.get('phase')) == (name, stage, phase), event
        return event

    def snapshot(self, stage):
        event = self.take('successor-state', stage)
        assert set(event) == {'event', 'stage', 'state', 'rows'}
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence',
            'control_sha256', 'control_tables', 'originals'}
        assert native_rows(event['rows']) == self.native, stage
        assert state['native_rows'] == len(event['rows'])
        assert state['native_sha256'] == digest(encoded(event['rows']))
        assert type(state['sequence']) is int and state['sequence'] == self.sequence, stage
        assert state['originals'] == [[k, self.inputs[k]] for k in sorted(self.inputs)], stage
        assert state['control_sha256'] == digest(encoded(state['control_tables']))
        tables = {name: json.loads(rows) for name, rows in state['control_tables']}
        assert len(tables) == len(state['control_tables']) == 72
        assert list(tables) == sorted(self.original)
        self.snapshots += 1
        return tables


def verify(events, original, native, inputs, mode, *, seed, server_window):
    """Check every raw stage and return finite evidence, never writer authority."""
    assert mode in MODES
    check_window(server_window)
    migrated(seed, original, native, inputs, server_window)
    plan_event = only(events, event='successor-plan')
    clock(plan_event['created_at_us'], server_window)
    plan, old_rows, prepared_rows = minted_plan(original, native, inputs, plan_event)
    capture = Capture(events, original, old_rows, inputs)
    planned = capture.take('successor-control', 'initial', 'claim-planned')
    assert planned == dict(event='successor-control', stage='initial', phase='claim-planned',
        role='d3' * 16, incarnation='dd' * 16, lease_ttl_us='20000000')
    assert plan['target_role'] != planned['role'] and plan['target_incarnation'] != planned['incarnation']
    same(original, capture.snapshot('initial-before-claim'))
    claimed = capture.take('successor-control', 'initial', 'claimed')
    current_pg = capture.snapshot('initial-after-claim')
    control = claim(original, current_pg, 1)
    assert control['epoch'] == 8
    assert claimed == dict(event='successor-control', stage='initial', phase='claimed',
        resource=control['resource_id'][2:], epoch=8, role='d3' * 16, incarnation='dd' * 16)
    clock(control['acquired_at_us'], server_window)
    clock(only(current_pg['role_instances'], role_instance_id=control['role_instance_id'])['created_at_us'], server_window)
    assert capture.take('successor-plan') == plan
    after = capture.snapshot('before-prepare')
    control = renewed(current_pg, after, control, plan['created_at_us'])
    fresh_xmin(control['xmin'], current_pg)
    clock(control['lease_expires_at_us'] - 20_000_000, server_window)
    current_pg = after
    capture.native, capture.sequence = prepared_rows, 62
    after = capture.snapshot('prepared')
    # The old prepared-only oracle stays strict at71 tables. Verify the added
    # activation table is empty on both sides before projecting just that table.
    assert current_pg[ACTIVATIONS] == after[ACTIVATIONS] == []
    prepared({k: v for k, v in current_pg.items() if k != ACTIVATIONS},
        {k: v for k, v in after.items() if k != ACTIVATIONS}, plan, control)
    current_pg = after
    backend = capture.take('successor-backend', 'initial')
    name = backend['database']
    assert isinstance(name, str) and 1 <= len(name) <= 63
    assert name.isascii() and name.replace('_', '').isalnum()
    assert backend == dict(event='successor-backend', stage='initial', database=name,
        pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
        control_epoch=8, present=True)
    assert capture.take('live-activation-start') == dict(event='live-activation-start', mode=mode, control_epoch=8)
    result = capture.take('live-activation-result')
    fault = mode in MODES[3:]
    receipt = None
    if fault:
        assert result == dict(event='live-activation-result', writer=False, error='CommitUnknown')
        assert capture.take('successor-session-poisoned') == dict(event='successor-session-poisoned', control_epoch=8)
        after = capture.snapshot('activation-unknown')
        if mode == 'successor-live-commit-after':
            receipt = activated(current_pg, after, plan, control, server_window)
        else:
            same(current_pg, after)
        current_pg = after
    else:
        assert result == dict(event='live-activation-result', writer=True, error=None,
            activated_at_us=result['activated_at_us'])
        after = capture.snapshot('activated-zero')
        receipt = activated(current_pg, after, plan, control, server_window, result['activated_at_us'])
        current_pg = after
        if mode != MODES[0]:
            science = Science(plan, prepared_rows, inputs)
            for stage, n in [('dictionary', 1), ('new-event', 2), ('same-request', 2),
                             ('same-id-alternate-signature', 3)]:
                wanted_receipt = science.advance(stage)
                report = dict(event='live-receipt', stage=stage, request=n, receipt=wanted_receipt.hex())
                if stage != 'dictionary':
                    report.update(event_id=EVENT_ID.hex(), supplied_signature=(ALTERNATE_SIG if n == 3 else PUBLIC_SIG).hex(),
                        provenance=mode != 'successor-live-off')
                assert capture.take('live-receipt', stage) == report
                capture.native, capture.inputs, capture.sequence = science.rows, science.inputs, science.sequence
                after = capture.snapshot(stage)
                if stage == 'same-request':
                    same(current_pg, after)
                else:
                    admitted(current_pg, after, science, n, control, server_window)
                current_pg = after
    stage = 'unknown' if fault else 'final'
    same(current_pg, capture.snapshot(stage + '-before-control-end'))
    if fault:
        assert capture.take('successor-driver', stage) == dict(event='successor-driver', stage=stage, failed=True, joined=True)
    phase = 'dropped-and-joined' if fault else 'released-and-joined'
    assert capture.take('successor-control', stage, phase) == dict(event='successor-control',
        stage=stage, phase=phase, resource=control['resource_id'][2:], epoch=8)
    after = capture.snapshot(stage + '-after-control-end')
    ended(current_pg, after, control, not fault)
    if not fault:
        released = only(after['owner_epochs'], role='control', epoch=8)
        fresh_xmin(released['xmin'], current_pg)
        clock(released['released_at_us'], server_window)
        clock(only(after['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    current_pg = after
    assert capture.take('successor-backend', 'initial') == dict(backend, present=False)
    same(current_pg, capture.snapshot('final'))
    completion = dict(event='live-complete', mode=mode, native='closed', writer=False,
        admission=not fault and mode != MODES[0])
    if not fault:
        completion['control'] = 'released-and-joined'
    assert capture.take('live-complete') == completion
    assert capture.position == len(events), 'unclassified trailing event'
    return dict(status='PASS', mode=mode, snapshots=capture.snapshots, control_tables=72,
        original_native_rows=49, final_native_rows=len(capture.native), native_sequence_delta=capture.sequence - 58,
        original_inputs=4, final_inputs=len(capture.inputs), activated=receipt is not None,
        activation_delivered=not fault, activated_at_us=receipt['activated_at_us'] if receipt else None,
        activation_transaction=receipt['activation_transaction'] if receipt else None,
        commit_unknown=fault, original_requests_preserved=4, new_requests=0 if fault or mode == MODES[0] else 3,
        target_manifest_sha256=digest(bytes.fromhex(plan['target_manifest'])),
        handoff_sha256=digest(bytes.fromhex(plan['handoff'])), backend_absent=dict(backend, present=False),
        finite_import_only=True, network_provenance_off=False, installed_import=False,
        fresh_process_adoption=False, post_activation_retirement=False)


def negative_controls(events, original, native, inputs, mode, *, seed, server_window):
    """Rehash altered captures: byte and transition checks must still reject."""
    verify(events, original, native, inputs, mode, seed=seed, server_window=server_window)
    rejected = []

    def check(name, mutate):
        changed = copy.deepcopy(events)
        mutate(changed)
        try:
            verify(changed, original, native, inputs, mode, seed=seed, server_window=server_window)
        except (AssertionError, KeyError, ValueError, TypeError):
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted perturbation: ' + name)

    def state(changed, stage='final'):
        return only(changed, event='successor-state', stage=stage)

    def sql(changed, mutate, stage='final'):
        event = state(changed, stage)
        pg = {k: json.loads(v) for k, v in event['state']['control_tables']}
        mutate(pg)
        event['state']['control_tables'] = [[k, encoded(pg[k]).decode()] for k in sorted(pg)]
        event['state']['control_sha256'] = digest(encoded(event['state']['control_tables']))

    def native_change(changed, domain, mutate, stage='final'):
        event = state(changed, stage)
        row = next(r for r in event['rows'] if r['key'].startswith(domain))
        value = bytearray.fromhex(row['value'])
        mutate(value)
        row['value'] = value.hex()
        event['state']['native_sha256'] = digest(encoded(event['rows']))

    check('retained-old-row-xmin', lambda e: sql(e, lambda pg: only(pg['source_streams'], owner_epoch=1).update(xmin='999999999')))
    check('retained-old-input', lambda e: state(e)['state']['originals'][0][1].update(sha256='00' * 32))
    check('retained-old-science', lambda e: native_change(e, '0120', lambda v: v.__setitem__(-1, v[-1] ^ 1)))
    check('sequence', lambda e: state(e)['state'].update(sequence=state(e)['state']['sequence'] + 1))
    check('missing-stage', lambda e: e.pop(next(i for i, v in enumerate(e) if v['event'] == 'successor-state')))
    check('minted-domain-reuse', lambda e: only(e, event='successor-plan')['domains'][0].__setitem__(1,
        only(original['allocation_domains'], dictionary_kind=1)['allocation_domain'][2:]))
    check('minted-role-reuse', lambda e: only(e, event='successor-plan').update(target_role=original['role_instances'][0]['role_instance_id'][2:]))
    check('changed-preparation-attribution', lambda e: sql(e, lambda pg: only(pg['initial_ingest_successions']).update(prepared_by_epoch=7)))
    check('invented-target-source-bound', lambda e: sql(e, lambda pg: only(pg['source_streams'], owner_epoch=2).update(last_complete_sequence=0)))
    check('invented-job-progress', lambda e: sql(e, lambda pg: pg['jobs'][0].update(job_revision=pg['jobs'][0]['job_revision'] + 1)))
    check('wrong-backend-absence-tuple', lambda e: only(e, event='successor-backend', present=False).update(pid=1))
    check('backend-not-absent', lambda e: only(e, event='successor-backend', present=False).update(present=True))
    check('native-not-closed', lambda e: only(e, event='live-complete').update(native='open'))
    has_activation = mode != 'successor-live-commit-before'
    if has_activation:
        activation_stage = 'activation-unknown' if mode == MODES[4] else 'activated-zero'
        check('activation-missing', lambda e: sql(e, lambda pg: pg[ACTIVATIONS].clear(), activation_stage))
        check('activation-preparer-substitution', lambda e: sql(e, lambda pg: only(pg[ACTIVATIONS]).update(activated_by_epoch=7), activation_stage))
        check('activation-xid-mismatch', lambda e: sql(e, lambda pg: only(pg[ACTIVATIONS]).update(activation_transaction='1'), activation_stage))
        check('activation-xid-overflow', lambda e: sql(e, lambda pg: only(pg[ACTIVATIONS]).update(
            activation_transaction=str((1 << 64) + int(only(pg[ACTIVATIONS])['xmin']))), activation_stage))
        check('activation-time-rewritten', lambda e: sql(e, lambda pg: only(pg[ACTIVATIONS]).update(activated_at_us=server_window['after_us'] + 1), activation_stage))
    if mode in MODES[3:]:
        check('writer-delivered-on-unknown', lambda e: only(e, event='live-activation-result').update(writer=True))
        check('unknown-session-reused', lambda e: only(e, event='successor-session-poisoned').update(control_epoch=9))
        check('driver-not-joined', lambda e: only(e, event='successor-driver').update(joined=False))
        check('wrong-driver-outcome', lambda e: only(e, event='successor-driver').update(failed=False))
        check('admission-on-unknown', lambda e: only(e, event='live-complete').update(admission=True))
        if not has_activation:
            check('unknown-before-fabricated-activation', lambda e: sql(e,
                lambda pg: pg[ACTIVATIONS].append({'fabricated': True}), 'activation-unknown'))
    elif mode != MODES[0]:
        check('same-request-receipt-changed', lambda e: only(e, event='live-receipt', stage='same-request').update(receipt='00'))
        check('alternate-signature-replaced', lambda e: only(e, event='live-receipt', stage='same-id-alternate-signature').update(supplied_signature=PUBLIC_SIG.hex()))
        check('provenance-choice-reversed', lambda e: only(e, event='live-receipt', stage='new-event').update(provenance=mode == 'successor-live-off'))
        check('request-old-manifest', lambda e: sql(e, lambda pg: only(pg['producer_requests'], producer_epoch=2, request_sequence=1).update(
            manifest_sha256=only(pg['producer_requests'], producer_epoch=1, request_sequence=1)['manifest_sha256'])))
        check('outcome-old-recorder', lambda e: sql(e, lambda pg: only(pg['producer_outcomes'], producer_epoch=2, request_sequence=2).update(recorder_epoch=3)))
        check('retry-rewrites-outcome-xmin', lambda e: sql(e, lambda pg: only(pg['producer_outcomes'], producer_epoch=2, request_sequence=2).update(xmin='999999999'), 'same-request'))
        check('reservation-outcome-same-transaction', lambda e: sql(e, lambda pg: only(pg['producer_outcomes'], producer_epoch=2, request_sequence=1).update(
            xmin=only(pg['producer_requests'], producer_epoch=2, request_sequence=1)['xmin']), 'dictionary'))
        check('new-input-digest-replaced', lambda e: state(e, 'dictionary')['state']['originals'][-1][1].update(sha256='00' * 32))
        source_prefix = '0120' + only(events, event='successor-plan')['dataset'] + be(0, 4).hex() + be(2).hex()
        check('new-scientific-fact-replaced', lambda e: native_change(e, source_prefix,
            lambda v: v.__setitem__(-1, v[-1] ^ 1), 'new-event'))
        target_marker = '0131' + only(events, event='successor-plan')['producer_slot'] + be(2).hex()
        check('producer-ack-fabricated', lambda e: native_change(e, target_marker, lambda v: v.__setitem__(-1, 1)))
    return dict(status='PASS', rejected=rejected)
