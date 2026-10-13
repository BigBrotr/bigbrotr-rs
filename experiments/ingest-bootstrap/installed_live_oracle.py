"""Independent complete installed successor observations, without SQL or engines.

The retained original and the signed distinct event are frozen. Only minted IDs,
transaction identifiers and bounded clock samples parameterize expected bytes.
The installed transcript is never rewritten as a finite-probe transcript.
"""
import copy
from datetime import datetime
import json
import re
import math
from installed_live_http_oracle import add_http, ACTIONS

from bootstrap_oracle import cbor
from initial_succession_oracle import (be, binary, bytea, digest, encoded, exclusion,
    expected, native_rows, only, same)
from initial_live_oracle import Science, check_window, clock, fresh_xmin, migrated, sha
from installed_sync_oracle import decode

CASES = ('installed-live-off', 'installed-live-on', 'installed-live-epoch2',
    'installed-live-missing-original', 'installed-live-held')
ENDPOINT = 'ws://127.0.0.1:7447/installed-live-fixture'
ACTIVATIONS = 'initial_ingest_activations'
EVENT_ID = bytes.fromhex('32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791')
BODY = [EVENT_ID, bytes.fromhex('79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798'),
    23, 1, [['t', 'allowed']], 'included']
SIGNATURE = bytes.fromhex('a7f054f4f22d05778e3ecabda6daf07f1d37422b2a31c5dbc8836395b7f7616258632a2adc978a353d1181bd89a51953b1f9a13e51577bf8657c9f2c9eef6d3d')


def identity(value, width=16):
    assert isinstance(value, str) and re.fullmatch(r'\\x[0-9a-f]{' + str(2 * width) + '}', value)
    assert int(value[2:], 16) != 0
    return value


def instant(text):
    value = datetime.fromisoformat(text)
    assert value.tzinfo is not None
    epoch = datetime.fromisoformat('1970-01-01T00:00:00+00:00')
    delta = value - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def catalog_identity(catalog):
    # FK trigger names include catalog-assigned OIDs, newly allocated when the
    # same migrations run in a fresh clone. Preserve every other field and the
    # full multiset; raw names must still remain exact within this invocation.
    normalized = copy.deepcopy(catalog)
    identities = set()
    for trigger in normalized['triggers']:
        key = trigger['table'], trigger['name']
        assert key not in identities
        identities.add(key)
        if trigger['internal']:
            assert re.fullmatch(r'RI_ConstraintTrigger_[ac]_[1-9][0-9]*', trigger['name'])
            trigger['name'] = trigger['name'].rsplit('_', 1)[0]
    normalized['triggers'].sort(key=encoded)
    # Derived from the qualified 2c142c6 schema32 catalog, not this run.
    assert digest(encoded(normalized)) == '27758551dcec655548798ecacec07bdb21ffda5ac50080926c059d09a3a3247a'


def policy_setup(seed, setup, original, window, provenance):
    migrated(seed, setup['before'], seed['native'], seed['inputs'], window)
    assert setup['authority'] == 'privileged-isolated-fixture'
    assert setup['product_policy_activation'] is False and setup['status'] == 'APPLIED'
    assert setup['provenance'] is provenance
    assert digest(setup['sql'].encode()) == ({False:
        '89ecf4cc4201aa5e5dc3babbde97cc389bdac35023aaa4e0b2e85bd52d19ccce', True:
        '3968d902983450d64b18b1f318ce87ef7822ba6e73d836c8fbb1299d703e2ed1'}[provenance])
    assert setup['guards_before'] == setup['guards_after']
    guards = setup['guards_before']
    assert guards['session_replication_role'] == 'origin'
    assert guards['triggers'] and all(t['enabled'] in ('O', 'A') for t in guards['triggers'])
    assert guards['constraints'] and all(t['validated'] for t in guards['constraints'])
    check_window(setup['server_window'])
    assert setup['server_window']['after_us'] <= window['before_us']
    answer = setup['response']
    assert answer['returncode'] == 0 and not answer['stderr']
    transaction = answer['stdout'].strip()
    assert transaction.isdecimal() and 0 < int(transaction) < 1 << 64
    wanted = copy.deepcopy(setup['before'])
    tx = only(original['config_revisions'], revision=2)['xmin']
    fresh_xmin(tx, wanted)
    assert int(transaction) % (1 << 32) == int(tx)
    for table in ('config_revisions', 'policy_revisions'):
        old, = wanted[table]
        observed = only(original[table], revision=2)
        document = copy.deepcopy(old['document'])
        document['settings']['ingest.producer_outstanding_requests'] = 2
        document['settings']['sync.provenance'] = 'local_first_witness' if provenance else 'off'
        at = observed['accepted_at_us']
        clock(at, setup['server_window'])
        value = dict(old, revision=2, previous_revision=1, document=document,
            document_digest=bytea(sha(b'bigbrotr/product-policy/2\0' + encoded(document))), accepted_at_us=at, xmin=tx)
        if table == 'policy_revisions':
            value['config_revision'] = 2
        wanted[table].append(value)
    wanted['datasets'] = [dict(only(wanted['datasets']), config_revision=2, policy_revision=2, xmin=tx)]
    same(wanted, setup['after'])
    same(wanted, original)


def native_plan(seed, state, report):
    plan, old, _ = expected(seed['control'], seed['native'], seed['inputs'], int(report['created_at_us']))
    intent = only(state['initial_ingest_successions'])
    role, incarnation = intent['target_role_instance_id'], intent['target_incarnation']
    identity(role); identity(incarnation)
    assert role != incarnation
    assert role not in {r['role_instance_id'] for r in seed['control']['role_instances']}
    assert incarnation not in {r['process_incarnation'] for r in seed['control']['owner_epochs']}
    domains = sorted((r['dictionary_kind'], r['allocation_domain'][2:])
        for r in state['allocation_domains'] if r['owner_epoch'] == 2)
    assert [kind for kind, _ in domains] == list(range(1, 10))
    assert len({domain for _, domain in domains}) == 9
    assert not {r['allocation_domain'][2:] for r in seed['control']['allocation_domains']} & {d for _, d in domains}
    target = bytearray.fromhex(plan['target_manifest'])
    target[62:78] = binary(incarnation)
    target[94:102] = be(2)
    for kind, domain in domains:
        identity('\\x' + domain)
        offset = 104 + (kind - 1) * 18
        target[offset + 2:offset + 18] = bytes.fromhex(domain)
    target = bytes(target)
    previous = bytes.fromhex(plan['previous_manifest'])
    handoff = b'\x01\x0a' + bytes.fromhex(plan['producer_slot']) + be(1) + be(4) + be(12) \
        + bytes.fromhex(plan['closure_digest']) + be(len(previous), 2) + previous + be(len(target), 2) + target
    plan.update(target_role=role[2:], target_incarnation=incarnation[2:], domains=domains,
        target_manifest=target.hex(), handoff=handoff.hex(), config_revision=2, policy_revision=2,
        config_digest=only(state['config_revisions'], revision=2)['document_digest'][2:],
        policy_digest=only(state['policy_revisions'], revision=2)['document_digest'][2:],
        window=2, lease_expires_at_us=int(report['initial_lease_expires_at_us']))
    prepared = dict(old)
    ds, slot, resource = (bytes.fromhex(plan[k]) for k in ('dataset', 'producer_slot', 'resource'))
    prepared['default', b'\x01\x01'] = target
    prepared['default', b'\x01\x0a' + ds + resource + be(1)] = handoff
    prepared['default', b'\x01\x02' + ds + be(0, 4) + be(2)] = b'\x01\x02' + be(0)
    prepared['receipts', b'\x01\x31' + slot + be(2)] = b'\x01\x31' + be(0) * 3
    return plan, prepared


class NetworkScience(Science):
    def commit(self, n, original, members, dependencies):
        batch = [self.refs[2, k] for k in members]
        receipt = b'bigbrotr/receipt/1\0' + cbor([1, [self.slot, 2, n], sha(original),
            [self.ds, 1, 1, bytes.fromhex(self.plan['recovery_generation']), 2], batch,
            [self.refs[key] for key in dependencies]])
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

    def network(self, provenance, dictionary_at, received, recorded, resolution):
        definitions = [(2, 'observer', dict(clock='system-utc', vantage=None, software=None,
            method_id=None, public_key=None, route_class=None, software_version=None))]
        if provenance:
            definitions += [(1, 'endpoint', dict(url=ENDPOINT)),
                (3, 'access-scope', {'class': 'public', 'label': None, 'visibility_scope_id': self.scope}),
                (5, 'event-verification', dict(input='nip01-event/1', algorithm='sha256-bip340',
                    capability='event-id-and-signature', output_unit='verification-result',
                    failure_taxonomy='bigbrotr-nip01-verifier/1', algorithm_version=1,
                    protocol_revision='a2494f4f81d46684e5814a9bf35e2b1df978f955'))]
        entries = [self.dictionary(n, kind, schema, value, dictionary_at)
            for n, (kind, schema, value) in enumerate(definitions, 1)]
        n = len(entries)
        original = b'bigbrotr/producer-request/1\0' + cbor([[self.slot, 2, 1], 'admit-evidence', 1,
            [1, self.scope, entries]])
        self.commit(1, original, list(range(1, n + 1)), [(1, 3)] + [(2, k) for k in range(1, n + 1)])
        self.dictionary(n + 1, 8, 'event', dict(event_id=EVENT_ID), recorded)
        self.fact(n + 2, 'event-payload', [*BODY, self.ref(8)], recorded)
        self.index(6, self.ds + EVENT_ID, n + 2)
        self.fact(n + 3, 'event-eligibility', [EVENT_ID, sha(b'bigbrotr/body/1\0' + cbor(BODY)),
            SIGNATURE, recorded, 'network', None], recorded)
        self.index(7, self.ds + EVENT_ID + self.ref_key(self.scope), n + 3)
        observation_clock = [self.ref(2), resolution, None, 4 if resolution > 1 else 0]
        self.fact(n + 4, 'event-receive', [EVENT_ID, received, self.ref(2), observation_clock, None], recorded)
        self.index(8, self.ds + EVENT_ID + self.ref_key(self.scope), n + 4)
        last = n + 4
        witness = None
        if provenance:
            last += 1
            witness = [self.ref(1), self.ref(3), self.ref(5)]
            self.fact(last, 'provenance', [EVENT_ID, self.ref(1), self.ref(2), self.ref(3), received,
                observation_clock, self.ref(5), None], recorded)
            self.index(9, self.ds + EVENT_ID + self.ref_key(self.ref(1)) + self.ref_key(self.ref(2))
                + self.ref_key(self.ref(3)) + self.ref_key(self.scope), last)
        original = b'bigbrotr/producer-request/1\0' + cbor([[self.slot, 2, 2], 'admit', 1,
            [1, self.scope, [*BODY, SIGNATURE], 'network', None,
                [received, self.ref(2), observation_clock, witness]]])
        self.commit(2, original, list(range(n + 1, last + 1)), [(1, 3)] + [(2, k) for k in range(1, last + 1)])
        assert (len(self.rows), self.sequence) == ((84, 98) if provenance else (70, 84))


def startup(original, observed, report, window):
    """Complete aggregate startup rows; no fictitious intermediate snapshot."""
    wanted = copy.deepcopy(original)
    prior = max((r for r in original['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    control = only(observed['owner_epochs'], role='control', epoch=prior['epoch'] + 1)
    role = only(observed['role_instances'], role_instance_id=control['role_instance_id'])
    identity(control['role_instance_id']); identity(control['process_incarnation'])
    assert control['role_instance_id'] not in {r['role_instance_id'] for r in original['role_instances']}
    assert control['process_incarnation'] not in {r['process_incarnation'] for r in original['owner_epochs']}
    assert control['role_instance_id'] != control['process_incarnation']
    assert type(control['control_backend_pid']) is int and control['control_backend_pid'] > 0
    assert 0 < instant(control['control_backend_start']) <= control['acquired_at_us']
    for at in (role['created_at_us'], control['acquired_at_us'], control['lease_expires_at_us'] - 6_000_000):
        clock(at, window)
    assert role['created_at_us'] <= control['acquired_at_us'] <= control['lease_expires_at_us'] - 6_000_000
    fresh_xmin(role['xmin'], original)
    fresh_xmin(control['xmin'], original)
    wanted['role_instances'].append(dict(role_instance_id=control['role_instance_id'],
        dataset_id=prior['dataset_id'], role='control', created_at_us=role['created_at_us'],
        retired_at_us=None, xmin=role['xmin']))
    wanted['owner_epochs'].append(dict(prior, epoch=prior['epoch'] + 1, role_instance_id=control['role_instance_id'],
        process_incarnation=control['process_incarnation'], acquired_at_us=control['acquired_at_us'],
        lease_expires_at_us=control['lease_expires_at_us'], state='active', released_at_us=None,
        exclusion_method=None, exclusion_digest=None, control_fence_version=1,
        control_backend_pid=control['control_backend_pid'], control_backend_start=control['control_backend_start'],
        archive_frontier_protocol=None, xmin=control['xmin']))
    if prior['state'] != 'released':
        old_role = only(wanted['role_instances'], role_instance_id=prior['role_instance_id'])
        released = only(observed['owner_epochs'], role='control', epoch=prior['epoch'])
        retired = only(observed['role_instances'], role_instance_id=old_role['role_instance_id'])
        assert prior['acquired_at_us'] <= released['released_at_us'] <= retired['retired_at_us'] <= role['created_at_us']
        wanted['owner_epochs'][wanted['owner_epochs'].index(prior)] = dict(prior, state='released',
            released_at_us=released['released_at_us'], exclusion_method='storage_fenced',
            exclusion_digest=exclusion(prior, control['process_incarnation']), xmin=role['xmin'])
        old_role.update(retired_at_us=retired['retired_at_us'], xmin=role['xmin'])
    if report is None:
        same(wanted, observed)
        return control
    # The accepted report and immutable preparation pin all target rows. The
    # live owner may already have renewed its deadline before publication.
    p = report['_plan']
    tx = only(observed['initial_ingest_successions'])['xmin']
    fresh_xmin(tx, original)
    ds, resource, inc, role_id = ('\\x' + p[k] for k in ('dataset', 'resource', 'target_incarnation', 'target_role'))
    target = only(observed['owner_epochs'], role='ingest', epoch=2)
    clock(p['created_at_us'], window)
    assert p['created_at_us'] < p['lease_expires_at_us'] <= p['created_at_us'] + 6_000_000
    clock(target['lease_expires_at_us'] - 6_000_000, window)
    fresh_xmin(target['xmin'], original)
    old_owner = only(original['owner_epochs'], role='ingest')
    wanted['owner_epochs'].append(dict(old_owner, epoch=2, role_instance_id=role_id,
        process_incarnation=inc, acquired_at_us=p['created_at_us'],
        lease_expires_at_us=target['lease_expires_at_us'], state='active', released_at_us=None,
        exclusion_method=None, exclusion_digest=None, xmin=target['xmin']))
    wanted['role_instances'].append(dict(only(original['role_instances'], role_instance_id=old_owner['role_instance_id']),
        role_instance_id=role_id, created_at_us=p['created_at_us'], retired_at_us=None, xmin=tx))
    wanted['producer_epochs'].append(dict(only(original['producer_epochs']), producer_epoch=2,
        producer_role_instance_id=role_id, producer_incarnation=inc, owner_epoch=2, owner_incarnation=inc,
        state='open', reserved_through_request=0, outstanding_request_limit=2,
        opened_at_us=p['created_at_us'], retired_at_us=None, closure_digest=None, xmin=tx))
    wanted['source_streams'].append(dict(only(original['source_streams']), owner_epoch=2, owner_incarnation=inc,
        state='open', opened_at_us=p['created_at_us'], closed_at_us=None, last_complete_sequence=None,
        closure_digest=None, xmin=tx))
    for kind, domain in p['domains']:
        wanted['allocation_domains'].append(dict(only(original['allocation_domains'], dictionary_kind=kind),
            allocation_domain='\\x' + domain, owner_epoch=2, owner_incarnation=inc, state='active',
            created_at_us=p['created_at_us'], closed_at_us=None, closure_digest=None, xmin=tx))
    wanted['topology_generations'].append(dict(only(original['topology_generations']), generation=1,
        previous_generation=0, created_at_us=p['created_at_us'], sealed=True, xmin=tx))
    wanted['topology_streams'] += [dict(only(original['topology_streams']), topology_generation=1,
        owner_epoch=epoch, handoff_last_complete_sequence=12 if epoch == 1 else None, xmin=tx) for epoch in (1, 2)]
    wanted['datasets'] = [dict(only(original['datasets']), topology_generation=1, xmin=tx)]
    intent = dict(dataset_id=ds, owner_resource_id=resource, target_epoch=2, owner_role='ingest',
        target_incarnation=inc, target_role_instance_id=role_id, producer_slot_id='\\x' + p['producer_slot'],
        producer_epoch=2, shard_id=0, previous_topology=0, target_topology=1, issued_through=4,
        last_complete_sequence=12, closure_digest='\\x' + p['closure_digest'],
        previous_manifest='\\x' + p['previous_manifest'], target_manifest='\\x' + p['target_manifest'],
        handoff='\\x' + p['handoff'], previous_manifest_sha256=bytea(sha(bytes.fromhex(p['previous_manifest']))),
        target_manifest_sha256=bytea(sha(bytes.fromhex(p['target_manifest']))), handoff_sha256=bytea(sha(bytes.fromhex(p['handoff']))),
        config_revision=2, policy_revision=2, config_digest='\\x' + p['config_digest'], policy_digest='\\x' + p['policy_digest'],
        outstanding_request_limit=2, created_at_us=p['created_at_us'], lease_expires_at_us=p['lease_expires_at_us'],
        prepared_by_resource=control['resource_id'], prepared_by_epoch=control['epoch'], prepared_by_role='control',
        prepared_by_incarnation=control['process_incarnation'], xmin=tx)
    wanted['initial_ingest_successions'] = [intent]
    activation = only(observed[ACTIVATIONS])
    at, activated_tx = activation['activated_at_us'], activation['xmin']
    fresh_xmin(activated_tx, wanted)
    clock(at, window)
    assert p['created_at_us'] <= at < p['lease_expires_at_us']
    assert at <= control['lease_expires_at_us'] - 6_000_000
    assert at <= target['lease_expires_at_us'] - 6_000_000
    assert p['lease_expires_at_us'] <= target['lease_expires_at_us']
    assert activation['activation_transaction'].isdecimal() and 0 < int(activation['activation_transaction']) < 1 << 64
    assert int(activation['activation_transaction']) % (1 << 32) == int(activated_tx)
    fields = ('dataset_id', 'owner_resource_id', 'target_epoch', 'target_incarnation', 'target_role_instance_id',
        'producer_slot_id', 'producer_epoch', 'previous_manifest_sha256', 'target_manifest_sha256', 'handoff_sha256',
        'prepared_by_resource', 'prepared_by_epoch', 'prepared_by_incarnation',
        'config_revision', 'policy_revision', 'config_digest', 'policy_digest')
    wanted[ACTIVATIONS] = [dict({k: intent[k] for k in fields}, activated_by_resource=control['resource_id'],
        activated_by_epoch=control['epoch'], activated_by_incarnation=control['process_incarnation'],
        activated_at_us=at, activation_transaction=activation['activation_transaction'], xmin=activated_tx)]
    same(wanted, observed)
    return control


def renewed(wanted, observed, previous, window, *, stopped=False):
    """Permit only actual owner lease updates and the final Control release."""
    for role, epoch in [('control', 8), ('ingest', 2)]:
        old = only(previous['owner_epochs'], role=role, epoch=epoch)
        current = only(observed['owner_epochs'], role=role, epoch=epoch)
        row = only(wanted['owner_epochs'], role=role, epoch=epoch)
        deadline = current['lease_expires_at_us']
        assert old['lease_expires_at_us'] <= deadline
        clock(deadline - 6_000_000, window)
        tx = current['xmin']
        if tx != old['xmin']:
            fresh_xmin(tx, previous)
        else:
            assert deadline == old['lease_expires_at_us']
        row.update(lease_expires_at_us=deadline, xmin=tx)
        if stopped and role == 'control':
            at = current['released_at_us']
            clock(at, window)
            assert deadline - 6_000_000 <= at < deadline
            assert tx != old['xmin']
            row.update(state='released', released_at_us=at, exclusion_method='storage_fenced',
                exclusion_digest=exclusion(old, old['process_incarnation']))
            role_row = only(wanted['role_instances'], role_instance_id=row['role_instance_id'])
            ended = only(observed['role_instances'], role_instance_id=row['role_instance_id'])
            clock(ended['retired_at_us'], window)
            assert at <= ended['retired_at_us']
            role_row.update(retired_at_us=ended['retired_at_us'], xmin=tx)


def admission_rows(wanted, observed, science, scope, enrollment, control, window):
    previous_at = only(wanted[ACTIVATIONS])['activated_at_us']
    for n in (1, 2):
        request = only(observed['producer_requests'], producer_epoch=2, request_sequence=n)
        outcome = only(observed['producer_outcomes'], producer_epoch=2, request_sequence=n)
        raw, receipt = science.requests[n], science.receipts[n]
        for row, field in [(request, 'reserved_at_us'), (outcome, 'recorded_at_us')]:
            clock(row[field], window); fresh_xmin(row['xmin'], wanted)
        assert request['xmin'] != outcome['xmin']
        assert previous_at <= request['reserved_at_us'] <= outcome['recorded_at_us']
        previous_at = outcome['recorded_at_us']
        assert outcome['recorded_at_us'] < min(
            only(observed['owner_epochs'], role='control', epoch=8)['lease_expires_at_us'],
            only(observed['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'])
        manifest = bytea(sha(bytes.fromhex(science.plan['target_manifest'])))
        source = dict(relay_endpoint_id=None, relay_access_scope_id=None, relay_enrollment_generation=None,
            relay_route_id=None, acquisition_scope_id=None, acquisition_job_id=None)
        if n == 2:
            job = only(observed['jobs'], request_key='\\x' + 'f4' * 16)
            source = dict(relay_endpoint_id=scope['endpoint_id'], relay_access_scope_id=scope['access_scope_id'],
                relay_enrollment_generation=1, relay_route_id=enrollment['route_id'],
                acquisition_scope_id=scope['scope_id'], acquisition_job_id=job['job_id'])
        wanted['producer_requests'].append(dict(producer_slot_id=bytea(science.slot), producer_epoch=2,
            request_sequence=n, dataset_id=bytea(science.ds), request_digest=bytea(sha(raw)), input_bytes=len(raw),
            input_profile=1, manifest_sha256=manifest, issuer_resource=control['resource_id'], issuer_epoch=8,
            issuer_role='control', issuer_incarnation=control['process_incarnation'],
            reserved_at_us=request['reserved_at_us'], xmin=request['xmin'], **source))
        wanted['producer_outcomes'].append(dict(producer_slot_id=bytea(science.slot), producer_epoch=2,
            request_sequence=n, dataset_id=bytea(science.ds), request_digest=bytea(sha(raw)),
            manifest_sha256=manifest, outcome_kind=1, policy_epoch=2, outcome_bytes=bytea(receipt),
            outcome_sha256=bytea(sha(receipt)), recorder_resource=control['resource_id'], recorder_epoch=8,
            recorder_role='control', recorder_incarnation=control['process_incarnation'],
            recorded_at_us=outcome['recorded_at_us'], xmin=outcome['xmin']))
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    producer.update(reserved_through_request=2,
        xmin=only(observed['producer_requests'], producer_epoch=2, request_sequence=2)['xmin'])


def backend_checked(value, control, database, window):
    assert set(value) == {'exact', 'absence'}
    expected = [] if control is None else [dict(pid=control['control_backend_pid'],
        backend_start=control['control_backend_start'], database=database, control_epoch=8)]
    assert value['exact'] == [dict(e, present=False) for e in expected]
    if control:
        clock(instant(control['control_backend_start']), window)
    absence = value['absence']
    assert set(absence) == {'database', 'user', 'limit_seconds', 'status', 'attempts', 'seconds'}
    assert absence['database'] == database and absence['user'] == 'bigbrotr_control'
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['limit_seconds'] == 5
    samples = absence['attempts']
    assert samples and math.isfinite(absence['seconds']) and 0 <= absence['seconds'] <= 120
    previous = 0
    for sample in samples:
        assert set(sample) == {'seconds', 'count', 'backends'}
        assert previous <= sample['seconds'] <= 5
        previous = sample['seconds']
        assert type(sample['count']) is int and sample['count'] == len(sample['backends'])
        pids = []
        for row in sample['backends']:
            assert set(row) == {'pid', 'backend_start'}
            assert type(row['pid']) is int and row['pid'] > 0 and instant(row['backend_start']) > 0
            pids.append(row['pid'])
        assert pids == sorted(set(pids))
    assert samples[-1]['count'] == 0 and samples[-1]['seconds'] <= absence['seconds']


def wire_checked(journey, fixture, report, initial, window):
    assert journey['terminal'] == dict(exit_code=0, reaped=True, outcome='PROCESS_EXITED')
    assert journey['stdout'] == journey['stderr'] == ''
    result = journey['result']
    assert set(result) == {'status', 'login', 'logout', 'me', 'digests', 'observations',
        'job_status', 'relay_joined', 'unauthenticated'}
    assert result['status'] == 'PASS' and result['relay_joined'] is True and result['unauthenticated'] == 401
    assert result['login'] == dict(format='bigbrotr-cli-login/1', status='session-persisted', authority='observation-only')
    assert result['logout'] == dict(format='bigbrotr-cli-logout/1', status='revocation-acknowledged',
        local_file='retained', drain='unobserved')
    session = only(initial['browser_sessions'])
    assert result['me'] == dict(format='bigbrotr-session/1', principal_id='10' * 16,
        credential_revision='1', grant_revision='4', config_revision='2', policy_revision='2',
        expires_at_us=str(session['expires_at_us']), idle_expires_at_us=str(session['idle_expires_at_us']),
        stored_actions=ACTIONS, authority='observation-only')
    assert len(result['observations']) == 4
    opened, closed = journey['opened'], journey['closed']
    assert 2 <= len(opened) == len(closed) <= 128
    nonces = set()
    for number, (first, last) in enumerate(zip(opened, closed), 1):
        assert first['number'] == last['number'] == number
        assert all(last[k] == v for k, v in first.items())
        assert set(first) == {'number', 'connected_at_us', 'clock_resolution_us', 'request',
            'request_at_us', 'request_monotonic_ns'}
        for field in ('connected_at_us', 'request_at_us', 'closed_at_us'):
            clock(last[field], window)
        assert first['connected_at_us'] <= first['request_at_us'] <= last['closed_at_us']
        assert last['closed_at_us'] - first['request_at_us'] < 3_000_000
        assert type(first['request_monotonic_ns']) is int and first['request_monotonic_ns'] > 0
        assert 0 <= last['closed_monotonic_ns'] - first['request_monotonic_ns'] < 3_000_000_000
        assert type(first['clock_resolution_us']) is int and 0 < first['clock_resolution_us'] <= 1_000_000
        request = first['request']
        assert len(request) == 3 and request[0] == 'REQ' and request[2] == {'kinds': [1], 'limit': 3}
        assert re.fullmatch('[0-9a-f]{48}', request[1]) and int(request[1][:32], 16)
        assert request[1][:32] not in nonces
        nonces.add(request[1][:32])
        if number == 1:
            assert last['frames'] == ['distinct', 'predecessor', 'distinct-resend', 'EOSE']
            assert last['event_ids'] == [EVENT_ID.hex(),
                '8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849', EVENT_ID.hex()]
            assert last['completion'] == 'request-and-websocket-close'
            assert first['request_at_us'] <= last['sent_at_us'] <= last['closed_at_us']
        else:
            assert last['frames'] == [] and last['completion'] == 'quiet-eof'
    quiet = fixture['quiet']
    assert quiet in opened and quiet['number'] > 1
    assert int(report['initial_lease_expires_at_us']) < quiet['request_at_us']
    signal = fixture['signal']
    assert set(signal) == {'kind', 'identity', 'opened', 'signal_at_us', 'signal_monotonic_ns',
        'closed_before_signal', 'signal_returned_at_us', 'signal_returned_monotonic_ns'}
    assert signal['opened'] == quiet and signal['closed_before_signal'] is False
    clock(signal['signal_at_us'], window); clock(signal['signal_returned_at_us'], window)
    assert quiet['request_at_us'] <= signal['signal_at_us'] <= signal['signal_returned_at_us']
    assert 0 <= signal['signal_monotonic_ns'] - quiet['request_monotonic_ns'] < 100_000_000
    assert signal['signal_monotonic_ns'] <= signal['signal_returned_monotonic_ns']
    selected, = [r for r in closed if r['number'] == quiet['number']]
    assert signal['signal_at_us'] <= selected['closed_at_us']
    assert signal['signal_monotonic_ns'] <= selected['closed_monotonic_ns']
    return closed[0]


def verify(mode, original, after, native_before, native_after, inputs_before, inputs_after,
        input_contents, snapshots, response, journey, fixture, server_window, backend,
        catalog_before, catalog_after, seed, setup, predecessor=None):
    assert mode in CASES
    check_window(server_window)
    assert len(original) == len(after) == 72
    assert catalog_before == catalog_after
    catalog_identity(catalog_before)
    assert setup['guards_before'] == {k: v for k, v in catalog_before.items() if k not in ('functions', 'tables')}
    assert fixture['cleanup_errors'] == []
    terminal = response['terminal']
    assert response['outcome'] == 'PROCESS_EXITED' and terminal['reaped'] is True and terminal['forced'] is False
    assert response['returncode'] == terminal['exit_code']
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert 0 <= terminal['elapsed_seconds'] <= 60 and math.isfinite(terminal['elapsed_seconds'])
    database = backend['absence']['database']
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    assert terminal['config'] == '/lab/' + database + '.toml'
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'serve', '--file', terminal['config'],
        '--activate-initial-successor', '--roles', 'control,ingest,sync']
    reports = [json.loads(line) for line in response['stdout'].splitlines()]
    if mode == 'installed-live-epoch2':
        assert predecessor is not None and predecessor['mode'] == 'installed-live-on'
        assert predecessor.get('predecessor') is None
        verify(**predecessor)
        same(original, predecessor['after'])
        assert native_before == predecessor['native_after'] and inputs_before == predecessor['inputs_after']
        assert setup == predecessor['setup'] and seed == predecessor['seed']
        assert server_window['before_us'] >= predecessor['server_window']['after_us']
    else:
        assert predecessor is None
        policy_setup(seed, setup, original, server_window, mode != 'installed-live-off')
        assert native_before['event'] == 'dump-state' and native_before['sequence'] == 58
        assert native_rows(native_before['rows']) == native_rows(seed['native']['rows'])
        expected_inputs = copy.deepcopy(seed['inputs'])
        if mode == 'installed-live-missing-original':
            removed = setup['removed']
            name = removed['name']
            assert name.endswith('-0000000000000001-0000000000000004.cbor')
            assert removed['identity'] == expected_inputs.pop(name)
            content = bytes.fromhex(removed['bytes'])
            assert removed['identity'] == dict(bytes=len(content), sha256=digest(content))
            assert removed['path'] == '/lab/' + mode + '/removed-original.cbor'
        else:
            assert 'removed' not in setup
        assert inputs_before == expected_inputs
    control = None
    positive = mode in CASES[:2]
    if not positive:
        assert reports == [] and response['returncode'] == 4 and snapshots == [] and journey is None
        errors = {'installed-live-epoch2': 'initial successor requires native epoch1; retained later state is not adopted',
            'installed-live-held': 'native store inspection or exclusive open failed',
            'installed-live-missing-original': 'producer operation unavailable: Native(Input(Missing))'}
        assert response['stderr'] == 'error: ' + errors[mode] + '\n'
        if mode == 'installed-live-missing-original':
            control = startup(original, after, None, server_window)
        else:
            same(original, after)
        assert native_after == native_before and inputs_after == inputs_before
        if mode == 'installed-live-held':
            holder = fixture['holder']
            assert holder['joined'] is True and holder['forced'] is False
            assert holder['alive_during_cli'] is True and holder['exit_code'] == 0 and holder['stderr'] == ''
            assert holder['ready']['event'] == 'native-held'
            assert holder['command'][:3] == ['docker', 'exec', '-i']
            assert holder['command'][4:] == ['/build/ingest_bootstrap_probe', 'history-hold', database,
                '/run/secrets/application', '/lab/' + mode + '/store',
                only(original['datasets'])['dataset_id'][2:], only(original['datasets'])['instance_id'][2:]]
            assert [json.loads(line) for line in holder['stdout'].splitlines()] == [{'event': 'native-held'}]
        else:
            assert fixture['holder'] is None
    else:
        assert response['returncode'] == 0 and response['stderr'] == '' and len(reports) == 2
        assert fixture['holder'] is None
        report, ended = reports
        assert ended == dict(format='bigbrotr-initial-successor/1', state='stopped', native='closed',
            owner_release='committed', source_closure='not-performed', producer_epoch='2', ready=False)
        assert [s['stage'] for s in snapshots] == ['activated', 'prepared', 'submitted', 'renewed', 'before-stop']
        previous_clock = server_window['before_us']
        for s in snapshots:
            assert set(s) == {'stage', 'server_us', 'observed_by_us', 'control', 'inputs'}
            assert previous_clock <= s['server_us'] <= s['observed_by_us'] <= server_window['after_us']
            previous_clock = s['observed_by_us']
            assert len(s['control']) == 72
        active = snapshots[0]['control']
        plan, prepared_rows = native_plan(seed, active, report)
        control = startup(original, active, dict(report, _plan=plan),
            dict(before_us=server_window['before_us'], after_us=snapshots[0]['observed_by_us']))
        activation = only(active[ACTIVATIONS])
        assert report == dict(format='bigbrotr-initial-successor/1', state='activated',
            dataset_id=plan['dataset'], owner_resource_id=plan['resource'], producer_slot_id=plan['producer_slot'],
            producer_epoch='2', process_incarnation=plan['target_incarnation'], topology_generation='1',
            config_revision='2', policy_revision='2', config_sha256=plan['config_digest'], policy_sha256=plan['policy_digest'],
            outstanding_request_limit='2', created_at_us=str(plan['created_at_us']),
            initial_lease_expires_at_us=str(plan['lease_expires_at_us']), activated_at_us=str(activation['activated_at_us']),
            predecessor=dict(producer_epoch='1', issued_through='4', reserved_through='4', resolved_requests='4',
                last_complete_sequence='12', sha256=plan['closure_digest']),
            native_profile=1, native_allowance_bytes=134217728, acquisition='finite-initial-successor', ready=False,
            listeners=[dict(kind='private-session-http', host='127.0.0.1', port=18081, profile='initial-source-commands')])
        first = wire_checked(journey, fixture, report, snapshots[1]['control'], server_window)
        science = NetworkScience(plan, prepared_rows, seed['inputs'])
        final_rows = native_rows(native_after['rows'])
        n = 4 if mode == 'installed-live-on' else 1
        def fact_scalar(number):
            value = final_rows['facts', b'\x01\x20' + science.src_key(number)]
            assert value[:2] == b'\x01\x20' and value[34:].startswith(b'bigbrotr/fact/1\0')
            return decode(value[34:].split(b'\0', 1)[1])
        dictionary_at, recorded = fact_scalar(1)[4], fact_scalar(n + 1)[4]
        receive = fact_scalar(n + 4)
        received, resolution = receive[5][1], receive[5][3][1]
        assert type(resolution) is int and 0 < resolution <= 1_000_000
        assert resolution == first['clock_resolution_us']
        assert first['sent_at_us'] - resolution <= received <= first['closed_at_us'] + resolution
        # The peer's EOF timestamp need not precede the product's next task.
        # Receive precedes fact construction; both remain inside the wire/run bounds.
        assert received <= recorded
        for timestamp in (dictionary_at, recorded, received):
            clock(timestamp, server_window)
        assert activation['activated_at_us'] <= dictionary_at <= first['request_at_us'] <= recorded
        science.network(mode == 'installed-live-on', dictionary_at, received, recorded, resolution)
        assert final_rows == science.rows and native_after['event'] == 'dump-state'
        assert native_after['sequence'] == science.sequence
        assert inputs_after == science.inputs
        previous = active
        assert snapshots[0]['inputs'] == seed['inputs']
        for s in snapshots[1:] + [dict(stage='final', control=after, inputs=inputs_after,
                server_us=server_window['after_us'], observed_by_us=server_window['after_us'])]:
            phase = s['stage']
            state = s['control']
            window = dict(before_us=server_window['before_us'], after_us=s['observed_by_us'])
            wanted = copy.deepcopy(active)
            renewed(wanted, state, previous, window, stopped=phase == 'final')
            scope, enrollment = add_http(wanted, state, original, journey['result'], phase, window, control)
            if phase != 'prepared':
                admission_rows(wanted, state, science, scope, enrollment, control, window)
            same(wanted, state)
            if previous is not active:
                mutable = {'owner_epochs', 'role_instances', 'browser_sessions', 'producer_epochs'}
                for table in state:
                    if table not in mutable:
                        assert all(row in state[table] for row in previous[table]), (phase, table, 'changed retained row')
                old_session, new_session = only(previous['browser_sessions']), only(state['browser_sessions'])
                changed_session_fields = {'last_seen_at_us', 'idle_expires_at_us', 'revoked_at_us', 'xmin'}
                assert {k: v for k, v in old_session.items() if k not in changed_session_fields} == {
                    k: v for k, v in new_session.items() if k not in changed_session_fields}
            if phase in ('before-stop', 'final'):
                same({k: v for k, v in previous.items() if k not in ('owner_epochs', 'role_instances')},
                    {k: v for k, v in state.items() if k not in ('owner_epochs', 'role_instances')})
            assert s['inputs'] == (seed['inputs'] if phase == 'prepared' else science.inputs)
            if phase not in ('prepared', 'submitted'):
                previous_session = only(previous['browser_sessions'])
                current_session = only(state['browser_sessions'])
                assert current_session['last_seen_at_us'] >= previous_session['last_seen_at_us']
            previous = state
        renewal = fixture['renewal']
        assert renewal['current'] == snapshots[3]
        target_initial = only(active['owner_epochs'], role='ingest', epoch=2)
        assert renewal['initial_target'] == target_initial
        assert renewal['initial_deadline_us'] == plan['lease_expires_at_us']
        assert plan['lease_expires_at_us'] < renewal['observed_at_us'] <= snapshots[3]['server_us']
        for role, epoch in [('ingest', 2), ('control', 8)]:
            current = only(snapshots[3]['control']['owner_epochs'], role=role, epoch=epoch)
            assert current['state'] == 'active' and current['lease_expires_at_us'] > snapshots[3]['observed_by_us']
        assert fixture['signal']['kind'] == 'SIGTERM'
        assert fixture['signal']['identity'] == {k: terminal[k] for k in ('pid', 'config', 'prefix', 'command', 'start_ticks')}
        assert fixture['client_terminal'] == journey['terminal']
    assert set(input_contents) == set(inputs_after)
    for name, content in input_contents.items():
        raw = bytes.fromhex(content)
        assert inputs_after[name] == dict(bytes=len(raw), sha256=digest(raw))
        if positive and '-0000000000000002-' in name:
            assert raw == science.requests[int(name[-21:-5], 16)]
    backend_checked(backend, control, database, server_window)
    return dict(status='PASS', mode=mode, snapshots=len(snapshots), sql_tables=72,
        native_rows=len(native_after['rows']), native_sequence=native_after['sequence'],
        original_inputs=len(inputs_before), final_inputs=len(inputs_after), actual_cli=True,
        certification_per_resend='not-observed', completion='not-qualified')


def negative_controls(**context):
    verify(**context)
    mutations = []
    def case(name, mutate):
        altered = copy.deepcopy(context)
        mutate(altered)
        try:
            verify(**altered)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError):
            mutations.append(name)
        else:
            raise AssertionError('installed oracle accepted counterexample: ' + name)
    case('unreaped-process', lambda c: c['response']['terminal'].update(reaped=False))
    case('unclosed-server-backend', lambda c: c['backend']['absence']['attempts'][-1].update(
        count=1, backends=[dict(pid=42, backend_start='2026-10-13T00:00:00+00:00')]))
    case('native-sequence', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    case('historical-source-cut', lambda c: c['after']['source_streams'][0].update(last_complete_sequence=13))
    case('missing-historical-row', lambda c: c['after']['producer_outcomes'].pop(0))
    case('changed-original-input', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    case('catalog-drift', lambda c: c['catalog_after'].update(session_replication_role='replica'))
    def catalogs(c):
        return [c['catalog_before'], c['catalog_after'], c['setup']['guards_before'], c['setup']['guards_after']]
    def corrupt_internal(c, action):
        for catalog in catalogs(c):
            trigger = next(t for t in catalog['triggers'] if t['internal'])
            action(catalog, trigger)
    case('coherent-internal-trigger-disabled', lambda c: corrupt_internal(c,
        lambda _, t: t.update(enabled='D')))
    case('coherent-internal-trigger-missing', lambda c: corrupt_internal(c,
        lambda catalog, t: catalog['triggers'].remove(t)))
    case('coherent-internal-trigger-duplicated', lambda c: corrupt_internal(c,
        lambda catalog, t: catalog['triggers'].append(copy.deepcopy(t))))
    case('coherent-invalid-internal-trigger-name', lambda c: corrupt_internal(c,
        lambda _, t: t.update(name='unrecognized_internal_trigger')))
    case('coherent-internal-trigger-wrong-function', lambda c: corrupt_internal(c,
        lambda _, t: t.update(function='control.unrelated()')))
    if context['mode'] in CASES[:2]:
        case('wire-missing-predecessor', lambda c: c['journey']['closed'][0]['event_ids'].__setitem__(1, EVENT_ID.hex()))
        case('historical-window-adoption', lambda c: only(c['after']['producer_epochs'], producer_epoch=2).update(outstanding_request_limit=32))
        case('current-policy-drift', lambda c: only(c['after']['datasets']).update(policy_revision=1))
        case('wrong-network-job', lambda c: only(c['after']['producer_requests'], producer_epoch=2, request_sequence=2).update(acquisition_job_id=None))
        case('forged-outcome', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(outcome_bytes='\\x00'))
        case('unreleased-control', lambda c: only(c['after']['owner_epochs'], role='control', epoch=8).update(state='active', released_at_us=None))
        case('released-control-missing-fence', lambda c: only(c['after']['owner_epochs'], role='control', epoch=8).update(exclusion_method=None))
        case('released-control-wrong-fence-digest', lambda c: only(c['after']['owner_epochs'], role='control', epoch=8).update(exclusion_digest='\\x' + '00' * 32))
        case('old-owner-retirement', lambda c: only(c['after']['owner_epochs'], role='ingest', epoch=1).update(released_at_us=0))
        case('premature-renewal-proof', lambda c: c['fixture']['renewal'].update(observed_at_us=c['fixture']['renewal']['initial_deadline_us']))
        case('job-false-completion', lambda c: only(c['after']['jobs'], request_key='\\x' + 'f4' * 16).update(state='succeeded'))
        case('session-grant-drift', lambda c: only(c['after']['acquisition_jobs'], request_key='\\x' + 'f4' * 16).update(actor_grant_revision=1))
        case('management-digest', lambda c: only(c['after']['management_commands'], request_key='\\x' + 'f1' * 16).update(request_digest='\\x' + '00' * 32))
        case('http-authority-overclaim', lambda c: c['journey']['result']['observations'][1]['body'].update(authority='writer'))
        case('http-wrong-request-body', lambda c: c['journey']['result']['observations'][0]['input'].update(url='ws://127.0.0.1:7447/other'))
        case('job-status-false-completion', lambda c: c['journey']['result']['job_status'].update(state='succeeded'))
        case('quiet-receive-already-closed', lambda c: c['fixture']['signal'].update(closed_before_signal=True))
        def rewrite_revocation(c):
            for state in [c['snapshots'][-1]['control'], c['after']]:
                session = only(state['browser_sessions'])
                session.update(revoked_at_us=session['revoked_at_us'] + 100, xmin='4294967294')
                audit = only(state['management_audit'], action='browser.session.revoke', subject_id=session['session_id'])
                audit.update(created_at_us=audit['created_at_us'] + 100, xmin=session['xmin'])
        case('coherent-revocation-rewrite-after-client-finished', rewrite_revocation)
        def backdate_startup(c):
            state = c['snapshots'][0]['control']
            activation = only(state[ACTIVATIONS])['activated_at_us']
            for role, epoch in [('control', 8), ('ingest', 2)]:
                only(state['owner_epochs'], role=role, epoch=epoch)['lease_expires_at_us'] = activation - 1 + 6_000_000
            c['fixture']['renewal']['initial_target'] = copy.deepcopy(only(state['owner_epochs'], role='ingest', epoch=2))
        case('coherent-startup-renewal-before-activation', backdate_startup)
    return dict(status='PASS', rejected=len(mutations), cases=mutations)
