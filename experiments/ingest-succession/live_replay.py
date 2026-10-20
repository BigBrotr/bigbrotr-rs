#!/usr/bin/env python3
"""Independent finite successor activation/admission capture reader.

The unchanged history/preparation models bind the qualified terminal originals.
New live SQL rows and native bytes below are independently encoded from literal
signed events and the selected wire contracts, never decoded product receipts.
This is a finite import API observation, not network or installed qualification.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import tomllib

import history_replay as history
import transition_replay as transition


CASES = ('activated-live3', 'empty-live3', 'activated-later-prepare3',
         'activated-later-empty3', 'activated-later-live4')
PINS = history.PINS
encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, loads = history.only, history.same, history.loads
native_map, native_rows, bx = transition.native_map, transition.native_rows, transition.bx

OLD_EVENT = dict(
    id='8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849',
    pubkey='f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9',
    created_at=1, kind=1, tags=[], content='import then network',
    sig='ab9e62de93391621735c773f4b1c40f41029d7d49369993a63a226c4ce14605859b112dd261b5fa4f8a2b82bbac0a67323f39c4d007555711547bfd25560b4a6')
NEW_EVENT = dict(
    id='57b20f44c0095dfc789765ed38caa8dd54ae37635dab32237c563146c8865b02',
    pubkey='c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5',
    created_at=15, kind=1, tags=[], content='arrives after its authorized deletion',
    sig='051b8c5ac03828f2e6ab0b4d99de9a57a455e3549e652eda67df061ef39ec1f73c01aee2ccb43a2dad348f51e9aa09ffc63ba189f9ec0449f96606d1f3632092')


def cbor(value):
    """Small deterministic CBOR encoder for this literal fixture vocabulary."""
    def head(major, n):
        assert type(n) is int and 0 <= n < 2**64
        if n < 24:
            return bytes([32 * major + n])
        width = next(w for w in (1, 2, 4, 8) if n < 1 << (8 * w))
        return bytes([32 * major + {1: 24, 2: 25, 4: 26, 8: 27}[width]]) + be(n, width)
    if value is None:
        return b'\xf6'
    if type(value) is int:
        return head(0, value)
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, str):
        raw = value.encode()
        return head(3, len(raw)) + raw
    if isinstance(value, list):
        return head(4, len(value)) + b''.join(map(cbor, value))
    assert isinstance(value, dict)
    pairs = sorted(((cbor(k), cbor(v)) for k, v in value.items()), key=lambda p: (len(p[0]), p[0]))
    return head(5, len(pairs)) + b''.join(k + v for k, v in pairs)


def body(event):
    return [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']),
            event['created_at'], event['kind'], event['tags'], event['content']]


def specification(case):
    name = case['name']
    assert name in CASES
    origin = 'empty' if name == 'empty-live3' else 'activated'
    later = '-later-' in name
    operation = 'prepare' if name == CASES[2] else 'empty' if name == CASES[3] else 'live-new'
    previous = CASES[CASES.index(name) - 1] if name in CASES[3:] else None
    assert case['origin'] == origin and case['operation'] == ('lineage-live-new' if operation == 'live-new' else operation)
    assert case['clone'] == ('activated-later' if later else origin)
    assert case['predecessor'] == previous and case['metadata_bytes'] == 262144
    assert case.get('proxy_port') is None
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    return origin, operation, 4 if name == CASES[4] else 3


def transition_case(case):
    """Equivalent case descriptor for unchanged prepare/empty protocol only."""
    assert case['name'] in CASES[2:4]
    prepare = case['operation'] == 'prepare'
    return dict(case, name='activated-prepare3' if prepare else 'activated-empty3',
                predecessor=None if prepare else 'activated-prepare3')


class Science:
    """Exact native puts, sidecars and receipts; no receipt supplies expectations."""
    def __init__(self, model, plan, corpus):
        self.m, self.plan, self.corpus = model, plan, corpus
        self.values = native_map(model.native)
        self.ds, self.slot = binary(model.ds['dataset_id']), binary(model.slot)
        self.epoch, self.manifest = plan['epoch'], plan['manifest']
        self.domains = {k: binary(d) for k, d in enumerate(plan['domains'], 1)}
        old = binary(model.legacy['previous_manifest'])
        self.scope = [self.ds, 4, old[160:176], 3]
        self.lineage = self.ref(9)
        self.refs, self.inputs, self.receipts, self.deps = {}, {}, {}, {}
        self.writes = 0
        for n in (3, 8, 9, 10):
            raw = bytes.fromhex(self.values['facts', (b'\x01\x20' + self.source(1, n)).hex()])
            assert raw[:2] == b'\x01\x20' and sha(raw[34:]) == raw[2:34].hex()
            self.refs[1, n] = [[self.ds, 0, 1, n], raw[2:34]]
        # Qualified original scope3 is private, and old event mapping/payload/
        # eligibility remain source1 even when the immediate parent is epoch3.
        scope_index = self.values['default', (b'\x01\x04' + self.ref_key(self.scope)).hex()]
        assert scope_index == (b'\x01\x04' + self.source(1, 3) + self.refs[1, 3][1]).hex()
        assert ('default', (b'\x01\x06' + self.ds + bytes.fromhex(NEW_EVENT['id'])).hex()) not in self.values
        assert [json.loads(corpus)[i] for i in range(2)] == [OLD_EVENT, NEW_EVENT]

    def ref(self, kind, ordinal=1):
        assert type(ordinal) is int and 1<=ordinal<=3
        return [self.ds, kind, self.domains[kind], ordinal]

    @staticmethod
    def ref_key(reference):
        ds, kind, domain, ordinal = reference
        return ds + be(kind, 2) + domain + be(ordinal)

    def source(self, epoch, n):
        return self.ds + be(0, 4) + be(epoch) + be(n)

    def put(self, cf, key, value):
        self.values[cf, key.hex()] = value.hex()
        self.writes += 1

    def fact(self, n, family, payload, timestamp):
        source = [self.ds, 0, self.epoch, n]
        raw = b'bigbrotr/fact/1\0' + cbor([source, family, 1, self.scope, timestamp, payload])
        self.refs[self.epoch, n] = [source, bytes.fromhex(sha(raw))]
        self.put('facts', b'\x01\x20' + self.source(self.epoch, n), b'\x01\x20' + bytes.fromhex(sha(raw)) + raw)

    def index(self, domain, key, n):
        prefix = bytes([1, domain])
        self.put('default', prefix + key, prefix + self.source(self.epoch, n) + self.refs[self.epoch, n][1])

    def dictionary(self, n, kind, schema, value, timestamp, *, ordinal=1):
        reference = self.ref(kind, ordinal)
        descriptor = [1, schema, 1, value]
        payload = [reference, *descriptor]
        self.fact(n, 'dictionary', payload, timestamp)
        self.index(4, self.ref_key(reference), n)
        canonical = cbor(descriptor)
        self.put('default', b'\x01\x05' + self.ds + be(kind, 2) + bytes.fromhex(sha(canonical))
                 + self.ref_key(reference), b'\x01\x05' + canonical)
        self.put('default', b'\x01\x03' + self.ds + be(kind, 2) + self.domains[kind], b'\x01\x03' + be(ordinal))
        return ['dictionary', 1, payload]

    def context(self):
        return [self.ds, self.plan['topology'], 1, self.manifest[78:94], self.m.ds['policy_revision']]

    def intent(self, event):
        return [1, self.scope, [*body(event), bytes.fromhex(event['sig'])], 'import', self.lineage, None]

    def request(self, n):
        if n == 1:
            descriptor = [1, 'dataset-lineage', 1, dict(format='bigbrotr-finite-successor-events',
                format_version=1, source_dataset_id=None, source_artifact_digest=bytes.fromhex(sha(self.corpus)))]
            intent = [1, self.scope, [['dictionary', 1, [self.lineage, *descriptor]]]]
            operation = 'admit-evidence'
        else:
            intent, operation = self.intent(OLD_EVENT if n == 2 else NEW_EVENT), 'admit'
        return b'bigbrotr/producer-request/1\0' + cbor([[self.slot, self.epoch, n], operation, 1, intent])

    def advance(self, n):
        assert n == len(self.receipts) + 1 and n in (1, 2, 3)
        raw = self.request(n)
        if n == 1:
            self.dictionary(1, 9, 'dataset-lineage', dict(format='bigbrotr-finite-successor-events',
                format_version=1, source_dataset_id=None, source_artifact_digest=bytes.fromhex(sha(self.corpus))), 900_000_001)
            members, deps = [1], [(1, 3), (self.epoch, 1)]
        elif n == 2:
            members, deps = [], [(1, k) for k in (3, 8, 9, 10)] + [(self.epoch, 1)]
        else:
            at, event = 900_000_005, NEW_EVENT
            event_id = bytes.fromhex(event['id'])
            self.dictionary(2, 8, 'event', dict(event_id=event_id), at)
            self.fact(3, 'event-payload', [*body(event), self.ref(8)], at)
            self.index(6, self.ds + event_id, 3)
            self.fact(4, 'event-eligibility', [event_id, bytes.fromhex(sha(b'bigbrotr/body/1\0' + cbor(body(event)))),
                bytes.fromhex(event['sig']), at, 'import', self.lineage], at)
            self.index(7, self.ds + event_id + self.ref_key(self.scope), 4)
            members, deps = [2, 3, 4], [(1, 3)] + [(self.epoch, k) for k in range(1, 5)]
        batch = [self.refs[self.epoch, k] for k in members] if members else None
        dependencies = [self.refs[pair] for pair in deps]
        receipt = b'bigbrotr/receipt/1\0' + cbor([1 if members else 2,
            [self.slot, self.epoch, n], bytes.fromhex(sha(raw)), self.context(), batch, dependencies])
        if members:
            first, last = members[0], members[-1]
            self.put('default', b'\x01\x02' + self.ds + be(0, 4) + be(self.epoch), b'\x01\x02' + be(last))
            pending = sha(b'bigbrotr/admission-batch/1\0' + cbor([self.ds, 0, self.epoch, first, last, batch]))
            self.put('facts', b'\x01\x21' + self.source(self.epoch, first), b'\x01\x21' + be(last) + bytes.fromhex(pending))
        self.put('receipts', b'\x01\x30' + self.slot + be(self.epoch) + be(n), receipt)
        self.put('receipts', b'\x01\x31' + self.slot + be(self.epoch), b'\x01\x31' + be(n) + bytes(16))
        self.inputs[n], self.receipts[n], self.deps[n] = raw, receipt, dependencies
        name = self.slot.hex() + f'-{self.epoch:016x}-{n:016x}.cbor'
        assert name not in self.m.inputs
        self.m.inputs[name] = dict(bytes=len(raw), sha256=sha(raw))
        assert self.writes == {1: 8, 2: 2, 3: 12}[n]
        self.m.native.update(rows=native_rows(self.values), sequence=self.m.native['sequence'] + self.writes)
        self.writes = 0
        return receipt

    def certification(self, n):
        intent = self.intent(OLD_EVENT if n == 2 else NEW_EVENT)
        digest = sha(b'bigbrotr/producer-request/1\0' + cbor([[bytes(16), 0, 0], 'certify-existing', 1, intent]))
        context = self.context()
        proof = sha(b'bigbrotr/existing-proof/1\0' + cbor([*context[:2], 'local-synced',
            *context[3:], bytes.fromhex(digest), self.deps[n]]))
        return digest, proof


class Model(transition.Model):
    def activate(self, observed, plan):
        intent = only(self.pg['ingest_successions'], target_epoch=plan['epoch'])
        activation = only(observed['ingest_activations'], target_epoch=plan['epoch'])
        xid = self.fresh(activation['xmin'], activation['activation_transaction'])
        at = self.at(activation['activated_at_us'], plan['created'])
        assert at < min(plan['deadline'], self.control['lease_expires_at_us'])
        row = {k: intent[k] for k in ('dataset_id', 'owner_resource_id', 'target_epoch',
            'target_incarnation', 'target_role_instance_id', 'producer_slot_id', 'producer_epoch',
            'target_manifest_sha256', 'handoff_sha256')}
        row.update(activated_by_resource=self.control['resource_id'], activated_by_epoch=self.control['epoch'],
            activated_by_incarnation=self.control['process_incarnation'], activated_at_us=at,
            activation_transaction=activation['activation_transaction'], xmin=xid)
        self.pg['ingest_activations'].append(row)
        same(self.pg, observed)

    def renew_ingest(self, observed, epoch):
        row = only(observed['owner_epochs'], resource_id=self.resource, epoch=epoch)
        wanted = only(self.pg['owner_epochs'], resource_id=self.resource, epoch=epoch)
        assert self.clock < min(wanted['lease_expires_at_us'], self.control['lease_expires_at_us'])
        wanted.update(lease_expires_at_us=self.control['lease_expires_at_us'], xmin=self.fresh(row['xmin']))
        same(self.pg, observed)

    def admit(self, observed, science, n):
        epoch, c = science.epoch, self.control
        request = only(observed['producer_requests'], producer_epoch=epoch, request_sequence=n)
        outcome = only(observed['producer_outcomes'], producer_epoch=epoch, request_sequence=n)
        activation = only(self.pg['ingest_activations'], target_epoch=epoch)
        reserved = self.at(request['reserved_at_us'], activation['activated_at_us'])
        rx = self.fresh(request['xmin'])
        at, ox = self.at(outcome['recorded_at_us']), self.fresh(outcome['xmin'])
        target = only(self.pg['owner_epochs'], resource_id=self.resource, epoch=epoch)
        assert at < min(c['lease_expires_at_us'], target['lease_expires_at_us'])
        raw, receipt = science.inputs[n], science.receipts[n]
        manifest = '\\x' + sha(science.manifest)
        base = dict(producer_slot_id=self.slot, producer_epoch=epoch, request_sequence=n,
            dataset_id=self.ds['dataset_id'], request_digest='\\x' + sha(raw), manifest_sha256=manifest)
        self.pg['producer_requests'].append(dict(base, input_bytes=len(raw), input_profile=1,
            issuer_resource=c['resource_id'], issuer_epoch=c['epoch'], issuer_role='control',
            issuer_incarnation=c['process_incarnation'], reserved_at_us=reserved,
            relay_endpoint_id=None, relay_access_scope_id=None, relay_enrollment_generation=None,
            relay_route_id=None, acquisition_scope_id=None, acquisition_job_id=None, xmin=rx))
        self.pg['producer_outcomes'].append(dict(base, outcome_kind=2 if n == 2 else 1,
            policy_epoch=self.ds['policy_revision'], outcome_bytes=bx(receipt), outcome_sha256='\\x' + sha(receipt),
            recorder_resource=c['resource_id'], recorder_epoch=c['epoch'], recorder_role='control',
            recorder_incarnation=c['process_incarnation'], recorded_at_us=at, xmin=ox))
        producer = only(self.pg['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch)
        assert producer['reserved_through_request'] == n - 1
        producer.update(reserved_through_request=n, xmin=rx)
        same(self.pg, observed)


def exact(observed, expected):
    assert encoded(observed) == encoded(expected), ('event differs', observed, expected)


def ref_json(reference):
    ds, kind, domain, ordinal = reference
    return dict(dataset_id=ds.hex(), dictionary_kind=kind, allocation_domain=domain.hex(), ordinal=str(ordinal))


def request_json(science, n):
    return dict(producer_slot_id=science.slot.hex(), producer_epoch=str(science.epoch), request_sequence=str(n))


def dependencies_json(dependencies):
    return [dict(source_record_id=dict(dataset_id=src[0].hex(), shard_id=src[1],
        owner_epoch=str(src[2]), sequence=str(src[3])), payload_digest=digest.hex())
        for src, digest in dependencies]


class Transcript(transition.Transcript):
    def __init__(self, events, model):
        # Preserve the history protocol's scalar checks. New model metadata uses
        # decimal strings (including FactRef.sequence), checked by exact events.
        super().__init__([e for e in events if e['event'].startswith('lineage-history-')], model)
        self.events = events

    def live(self, name):
        assert self.position < len(self.events), ('missing live event', name)
        event = self.events[self.position]
        self.position += 1
        assert event['event'] == 'lineage-live-' + name, event
        return event


def verify(events, before, native, inputs, case, server_window):
    """Verify one finite process; replay() binds every previous process state."""
    origin, operation, target = specification(case)
    if operation in ('prepare', 'empty'):
        result = transition.verify(events, before, native, inputs, transition_case(case), server_window)
        return dict(result, case=case['name'])
    if target == 3:
        history.baseline(before, native, inputs, origin, server_window)
        history.science(before, native, inputs, origin)
    base_count, base_sequence, input_count = (70, 85, 6) if origin == 'activated' else (52, 63, 4)
    assert len(native['rows']) == base_count + (3 if target == 4 else 0)
    assert native['sequence'] == base_sequence + (5 if target == 4 else 0)
    assert len(inputs) == input_count and sha(encoded(inputs)) == PINS[origin]['inputs']
    known = transition.lineage(before, native, origin)
    assert known['head'] == target - 1 and known['state'] == 'Terminal'
    return verify_live(events, before, native, inputs, case, server_window, known)


def verify_live(events, before, native, inputs, case, server_window, known):
    """Unchanged finite live protocol after an independently checked terminal head.

    Callers own the original pins and literal complete-history reconstruction.
    This shared body permits an activated predecessor without inventing another
    preparation/admission oracle or weakening the original case specification.
    """
    target, operation = known['head'] + 1, 'live-new'
    assert known['state'] == 'Terminal' and known['through'] == target - 1
    m = Model(before, native, inputs, server_window)
    t = Transcript(events, m)
    candidate = t.take('candidate', True)
    assert set(candidate) == {'event', 'role', 'incarnation', 'namespaces'}
    exact([r['kind'] for r in candidate['namespaces']], list(range(1, 10)))
    assert all(set(r) == {'kind', 'id'} for r in candidate['namespaces'])
    ids = [candidate['role'], candidate['incarnation']] + [r['id'] for r in candidate['namespaces']]
    assert len(set(ids)) == 11
    assert all(re.fullmatch('[0-9a-f]{32}', v) and int(v, 16) and v not in m.identities for v in ids)
    live_resource = t.live('resources')
    exact(live_resource, dict(event='lineage-live-resources', prepared_input_bytes=1024**2,
        operation_data_bytes=live_resource['operation_data_bytes'], operation_native_bytes=live_resource['operation_native_bytes'],
        max_input_bytes=8192, max_event_bytes=256 * 1024, max_dependencies=256,
        outstanding_request_limit=32, provenance=False))
    for name in ('operation_data_bytes', 'operation_native_bytes'):
        assert type(live_resource[name]) is int and 0 < live_resource[name] <= 256 * 1024**2
    resource = t.take('resources')
    exact(resource, dict(event='lineage-history-resources', metadata_bytes=262144,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2, operation_timeout_ms=10000,
        frame_bytes=16 * 1024**2, **{k: resource[k] for k in ('input_bytes', 'outcome_bytes', 'effect_bytes')}))
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resource[field]) is int and 0 < resource[field] <= 256 * 1024**2
    assert sum(resource[k] for k in ('metadata_bytes', 'input_bytes', 'outcome_bytes', 'effect_bytes',
        'transport_bytes', 'observation_bytes')) + 8192 + live_resource['prepared_input_bytes'] + live_resource['operation_data_bytes'] <= 1024**3
    exact(t.take('resources', True), dict(event='lineage-transition-resources', operation=operation,
        mutation_data_bytes=8192, mutation_native_bytes=8192, lease_ttl_us='20000000', outstanding_request_limit=32))
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    assert not ({planned['role'], planned['incarnation']} & set(ids))
    t.snapshot('before-claim')
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    exact(claimed, dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation']))
    exact(backend, dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'],
        control_epoch=m.control['epoch'], present=True))
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-read')
    t.history(known)
    event = t.take('plan', True)
    created, deadline = event['created_at_us'], event['lease_expires_at_us']
    assert re.fullmatch('[1-9][0-9]*', created) and re.fullmatch('[1-9][0-9]*', deadline)
    plan = m.plan(candidate['role'], candidate['incarnation'], [r['id'] for r in candidate['namespaces']],
                  target, int(created), int(deadline))
    record = transition.record_value(plan['record'], target)
    exact(event, dict(event='lineage-transition-plan', operation='prepare', record=record,
        role=candidate['role'], created_at_us=created, lease_expires_at_us=deadline,
        outstanding_request_limit=32, policy=dict(config_revision=m.ds['config_revision'],
            policy_revision=m.ds['policy_revision'],
            config_digest=only(m.pg['config_revisions'], revision=m.ds['config_revision'])['document_digest'][2:],
            policy_digest=only(m.pg['policy_revisions'], revision=m.ds['policy_revision'])['document_digest'][2:])))
    t.snapshot('planned')
    t.snapshot('transition-returned', lambda pg: m.prepare(pg, plan))
    exact(t.take('observed', True), dict(event='lineage-transition-observed', operation='prepare',
        prepared=True, record=record, activation=False))
    exact(t.live('activation-start'), dict(event='lineage-live-activation-start', target_epoch=target,
        control_epoch=m.control['epoch']))
    activation = t.live('activation-result')
    t.snapshot('activated-zero', lambda pg: m.activate(pg, plan))
    exact(activation, dict(event='lineage-live-activation-result', writer=True, target_epoch=target,
        activated_at_us=str(only(m.pg['ingest_activations'], target_epoch=target)['activated_at_us']),
        manifest=plan['manifest'].hex(), role=candidate['role'], producer_slot=m.slot[2:], error=None))
    t.policy(t.take('policy'), 'live-refreshed', target, None)
    t.snapshot('live-control-refreshed', m.refresh)
    exact(t.live('renewal'), dict(event='lineage-live-renewal', commit_started=True, error=None))
    t.snapshot('live-renew-returned', lambda pg: m.renew_ingest(pg, target))
    fixture = t.live('fixture')
    # Struct field order is explicit; unlike arbitrary observed JSON, these
    # bytes independently bind the exact two signed events in the finite import.
    corpus = json.dumps([OLD_EVENT, NEW_EVENT], separators=(',', ':'), ensure_ascii=False).encode()
    science = Science(m, plan, corpus)
    old_name = m.slot[2:] + '-0000000000000001-0000000000000004.cbor'
    original = bytes.fromhex(fixture['original_bytes'])
    assert dict(bytes=len(original), sha256=sha(original)) == inputs[old_name]
    assert sha(original) == '19820e2cea792c8780de0d8542baa76dabbdb611ab96237eed2230a228d82977'
    exact(fixture, dict(event='lineage-live-fixture', original_name=old_name,
        original_bytes=original.hex(), original_sha256=sha(original), scope=ref_json(science.scope),
        lineage=ref_json(science.lineage), artifact_bytes=corpus.hex(), artifact_sha256=sha(corpus),
        old_event=OLD_EVENT, new_event=NEW_EVENT, provenance=False, installed_import=False))
    for n, label in ((1, 'dictionary'), (2, 'already-durable'), (3, 'new-event')):
        for retry in (False, True):
            stage = label + ('-retry' if retry else '')
            raw = science.request(n)
            exact(t.live('input'), dict(event='lineage-live-input', stage=stage, request=request_json(science, n),
                preimage=raw.hex(), request_digest=sha(raw), recorded_at_us=str(900_000_001 + 2 * (n - 1) + retry),
                signed_event=None if n == 1 else OLD_EVENT if n == 2 else NEW_EVENT, provenance=False))
            t.snapshot('before-' + stage)
            if not retry:
                science.advance(n)
            t.snapshot('after-' + stage, None if retry else lambda pg: m.admit(pg, science, n))
            receipt = science.receipts[n]
            exact(t.live('receipt'), dict(event='lineage-live-receipt', stage=stage,
                request=request_json(science, n), receipt=receipt.hex(), sha256=sha(receipt)))
    t.snapshot('before-visibility')
    t.snapshot('after-visibility')
    visible = b'bigbrotr/dictionary/1\0' + cbor([science.scope, 1, 'visibility', 1, {'class': 'private'}])
    exact(t.live('visibility'), dict(event='lineage-live-visibility', reference=ref_json(science.scope),
        **{'class': 'private'}, bytes=visible.hex(), sha256=sha(visible)))
    for n, kind in ((2, 'historical'), (3, 'current')):
        t.snapshot('before-certify-' + kind)
        t.snapshot('after-certify-' + kind)
        digest, proof = science.certification(n)
        context = science.context()
        exact(t.live('certification'), dict(event='lineage-live-certification', kind=kind,
            event_id=(OLD_EVENT if n == 2 else NEW_EVENT)['id'], intent_digest=digest, proof_digest=proof,
            context=dict(dataset_id=science.ds.hex(), topology_generation=str(context[1]), protection_class='local-synced',
                recovery_generation=context[3].hex(), policy_epoch=str(context[4])),
            dependencies=dependencies_json(science.deps[n])))
    exact(t.live('result'), dict(event='lineage-live-result', operation=operation, stage='complete',
        epoch=None, sequence=None, error=None, observation=dict(target_epoch=target, requests=3,
            writer_delivered=True, new_event=True, read_only_checks=3)))
    t.snapshot('after-transition')
    exact(t.take('native'), dict(event='lineage-history-native', state='closed'))
    t.snapshot('before-control-end')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='control', failed=False, joined=True))
    exact(t.take('backend'), dict(backend, present=False))
    exact(t.take('control'), dict(event='lineage-history-control', phase='released-and-joined',
        resource=m.control['resource_id'][2:], epoch=m.control['epoch']))
    t.snapshot('after-control-end', m.release)
    t.snapshot('final')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='observer', failed=False, joined=True))
    exact(t.live('complete'), dict(event='lineage-live-complete', operation=operation, native='closed',
        control='released-and-joined', observer='joined', success=True, writer=False, activation=True, admission=True))
    assert t.position == len(events), 'unclassified trailing events'
    assert t.snapshots == 33 + 2 * (sum(known['counts']) + len(known['counts']))
    assert m.refreshes == sum(known['counts']) + len(known['counts']) + 2
    assert len(m.native['rows']) == len(native['rows']) + 20
    assert m.native['sequence'] == native['sequence'] + 26 and len(m.inputs) == len(inputs) + 3
    return dict(status='PASS', case=case['name'], database=case['database'], snapshots=t.snapshots, tables=75,
        native_rows=len(m.native['rows']), sequence=m.native['sequence'], inputs=len(m.inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, target_epoch=target, requests=3,
        scope='Finite same-process activation/import admission and reads; no network/installed acquisition or retirement claim')


def negative_controls(events, before, native, inputs, case, server_window):
    verify(events, before, native, inputs, case, server_window)
    if case['name'] in CASES[2:4]:
        return transition.negative_controls(events, before, native, inputs, transition_case(case), server_window)
    rejected = []

    def reject(name, mutate):
        changed = copy.deepcopy(events)
        mutate(changed)
        try:
            verify(changed, before, native, inputs, case, server_window)
        except (AssertionError, KeyError, ValueError, TypeError, OverflowError):
            rejected.append(name)
        else:
            raise AssertionError('accepted altered evidence: ' + name)

    def live(values, event_kind, **fields):
        return only(values, event='lineage-live-' + event_kind, **fields)

    def state(values, stage):
        return only(values, event='lineage-history-state', stage=stage)

    def sql(values, stages, change):
        for item in values:
            if item['event'] == 'lineage-history-state' and (stages is None or item['stage'] in stages):
                view = item['state']
                tables = {name: loads(raw) for name, raw in view['control_tables']}
                change(tables)
                view['control_tables'] = [[name, encoded(rows).decode()] for name, rows in sorted(tables.items())]
                view['control_sha256'] = sha(encoded(view['control_tables']))

    def prior_xmin(values):
        sql(values, None, lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='4000000000'))
    reject('retained-original-xmin-rehashed', prior_xmin)
    reject('activation-without-writer', lambda v: live(v, 'activation-result').update(writer=False))
    reject('activation-role-foreign', lambda v: live(v, 'activation-result').update(role='ff' * 16))
    reject('renewal-commit-not-started', lambda v: live(v, 'renewal').update(commit_started=False))
    reject('same-id-other-signature-in-input', lambda v: live(v, 'input', stage='already-durable')['signed_event'].update(sig='00' * 64))
    reject('provenance-enabled', lambda v: live(v, 'fixture').update(provenance=True))
    reject('claimed-installed-import', lambda v: live(v, 'fixture').update(installed_import=True))
    reject('historic-copy-receipt-rehashed', lambda v: live(v, 'receipt', stage='already-durable').update(receipt='00', sha256=sha(b'\0')))
    reject('newfacts-retry-receipt-rehashed', lambda v: live(v, 'receipt', stage='new-event-retry').update(receipt='00', sha256=sha(b'\0')))
    reject('historical-certification-parent-epoch', lambda v: live(v, 'certification', kind='historical')['dependencies'][0]['source_record_id'].update(owner_epoch='2'))
    reject('current-certification-context-relabeled', lambda v: live(v, 'certification', kind='current')['context'].update(policy_epoch='0'))
    reject('visibility-scope-class-rewritten', lambda v: live(v, 'visibility').update(**{'class': 'public'}))
    reject('input-digest-changed', lambda v: state(v, 'final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('control-driver-not-joined', lambda v: only(v, event='lineage-history-driver', driver='control').update(joined=False))
    reject('observer-driver-failed', lambda v: only(v, event='lineage-history-driver', driver='observer').update(failed=True))
    reject('writer-retained-at-process-end', lambda v: live(v, 'complete').update(writer=True))
    reject('boolean-as-integer', lambda v: live(v, 'complete').update(activation=1))

    target = specification(case)[2]
    def first_request_activation_xid(values):
        def mutation(pg):
            activation = only(pg['ingest_activations'], target_epoch=target)
            for row in pg['producer_requests']:
                if row['producer_epoch'] == target and row['request_sequence'] == 1:
                    row['xmin'] = activation['xmin']
            producer = only(pg['producer_epochs'], producer_epoch=target)
            if producer['reserved_through_request'] == 1:
                producer['xmin'] = activation['xmin']
        stages = {e['stage'] for e in values if e['event'] == 'lineage-history-state'}
        stages -= {'before-claim', 'after-claim', 'before-read', 'loaded', 'proof', 'planned', 'transition-returned',
                   'activated-zero', 'live-control-refreshed', 'live-renew-returned', 'before-dictionary'}
        stages = {s for s in stages if not s.startswith(('refreshed-', 'resolved-', 'epoch-'))}
        sql(values, stages, mutation)
    reject('first-reservation-same-activation-transaction', first_request_activation_xid)

    def retry_xmin(values):
        stage = 'after-dictionary-retry'
        tail = [e['stage'] for e in values[values.index(state(values, stage)):] if e['event'] == 'lineage-history-state']
        sql(values, tail, lambda pg: only(pg['producer_outcomes'], producer_epoch=target, request_sequence=1).update(xmin='4000000000'))
    reject('retry-outcome-version-rewritten-in-entire-tail', retry_xmin)

    def native_tail(values):
        beginning = values.index(state(values, 'after-already-durable'))
        for item in values[beginning:]:
            if item['event'] == 'lineage-history-state':
                item['state']['sequence'] += 1
    reject('no-fact-request-stray-native-put-through-final', native_tail)
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def replay(root):
    """Bind actual stdout, original pins and every successive external baseline."""
    root = Path(root).resolve()
    folder, bound = root / 'live', {}

    def raw(path):
        path = Path(path)
        value = path.read_bytes()
        bound[str(path)] = sha(value)
        return value

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_LIVE_FIRST_WAVE'
    assert [c['name'] for c in report['cases']] == list(CASES)
    assert [c['name'] for c in report['clones']] == ['activated', 'empty', 'activated-later']
    assert len({c['database'] for c in report['clones']}) == 3
    for field in ('cleanup_errors', 'missing_owned', 'unrelated_changed', 'unexpected_containers'):
        assert report[field] == []
    origins = {}
    for origin, pin in PINS.items():
        declaration = report['origins'][origin]
        for field in ('run', 'database', 'snapshot', 'snapshot_sha256', 'result_sha256', 'status'):
            assert declaration[field] == pin[field]
        basis = root.parent / pin['run'] / 'bindings'
        result = read(basis / 'result.json')
        assert bound[str(basis / 'result.json')] == pin['result_sha256'] and result['status'] == pin['status']
        rows = read(basis / pin['snapshot'])
        assert bound[str(basis / pin['snapshot'])] == pin['snapshot_sha256']
        native = read(basis / pin['native_file'])
        assert bound[str(basis / pin['native_file'])] == pin['native_sha256']
        original_inputs = read(basis / pin['inputs_file'])
        assert bound[str(basis / pin['inputs_file'])] == pin['inputs_sha256']
        inputs = original_inputs.get('inputs', original_inputs)
        same(rows, read(folder / (origin + '-original.json')))
        input_root = basis / pin['store'] / 'producer-inputs-v1'
        assert sorted(p.name for p in input_root.iterdir()) == sorted(inputs)
        for name, identity in inputs.items():
            content = raw(input_root / name)
            assert dict(bytes=len(content), sha256=sha(content)) == identity
            if 'contents' in original_inputs:
                assert content.hex() == original_inputs['contents'][name]
        preserved = dict(rows_unchanged=True, catalog_unchanged=True, native_unchanged=True,
            inputs_unchanged=True, physical_unchanged=True)
        assert declaration['preserved'] == preserved == read(folder / (origin + '-preserved.json'))
        assert all(v is True for v in declaration['preserved'].values())
        origins[origin] = rows, native, inputs

    chains, windows, clones = {}, {}, {}
    for clone in report['clones']:
        name, origin = clone['name'], clone['origin']
        assert origin == ('empty' if name == 'empty' else 'activated')
        assert clone['status'] == 'PASS' and clone['directory'] == name
        assert clone['config'] == '/lab/' + clone['database'] + '.toml'
        config_path = folder / Path(clone['config']).name
        config = tomllib.loads(raw(config_path).decode())
        ds = only(origins[origin][0]['datasets'])
        assert config['postgres']['control']['database'] == clone['database']
        assert config['storage']['ingest_root'] == '/lab/' + name + '/store'
        assert config['topology']['dataset_id'] == ds['dataset_id'][2:]
        assert config['topology']['instance_id'] == ds['instance_id'][2:]
        assert config['runtime']['lease_ttl'] == '20s'
        validation = read(folder / (name + '-config-validation.json'))
        assert validation['returncode'] == 0 and validation['stderr'] == ''
        migrations = clone['migrations']
        assert [r['action'] for r in migrations] == ['plan', 'apply', 'apply']
        steps = [dict(name=f'{v:04d}_{history.MIGRATIONS[v][0]}.sql', sha256=history.MIGRATIONS[v][1], version=str(v))
                 for v in range(PINS[origin]['floor'] + 1, 39)]
        for index, answer in enumerate(migrations):
            assert set(answer) == {'action', 'exit_code', 'stdout', 'stderr'}
            assert answer['exit_code'] == 0 and answer['stderr'] == ''
            assert loads(answer['stdout']) == dict(format='bigbrotr-control-migration/1',
                scope='postgres-control', target_version='38',
                source_version=str(PINS[origin]['floor']) if index < 2 else '38',
                current_version=str(PINS[origin]['floor']) if index == 0 else '38',
                status=('planned', 'applied', 'current')[index], applied_count=str(len(steps)) if index == 1 else '0',
                steps=steps if index < 2 else [], dataset_initialization='not-performed',
                release_upgrade_qualification='not-performed')
        grants = clone['grants']
        assert len(grants) == 2 and all(set(g) == {'statement', 'stdout'} for g in grants)
        assert sha(grants[0]['statement'].encode()) == '36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
        assert grants[1]['statement'] == 'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;'
        assert all(not g['stdout'].strip() for g in grants)
        assert isinstance(clone['catalog_before'], str) and clone['catalog_before']
        assert clone['catalog_after'] == clone['final_catalog'] == clone['catalog_before']
        assert clone['final_backends'] == []
        same(origins[origin][0], read(folder / (name + '-copy-before.json')))
        migrated = read(folder / (name + '-migrated.json'))
        chains[name] = migrated, origins[origin][1], origins[origin][2]
        clones[name] = clone

    checked = []
    for case in report['cases']:
        origin, _, _ = specification(case)
        name, clone_name = case['name'], case['clone']
        expected_clone = 'activated-later' if name in CASES[2:] else origin
        assert clone_name == expected_clone
        clone = clones[clone_name]
        assert case['database'] == clone['database'] and case['config'] == clone['config']
        assert case['status'] == 'PASS' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert type(case['returncode']) is int and case['returncode'] == 0
        assert case['catalog_after'] == clone['catalog_before']
        case_read = lambda suffix: read(folder / (name + '-' + suffix + '.json'))
        before, native, inputs = case_read('before'), case_read('before-native'), case_read('inputs')
        previous = chains[clone_name]
        same(previous[0], before)
        assert native == previous[1] and inputs == previous[2], 'process boundary state changed'
        assert case_read('capture-errors') == []
        response = case_read('response')
        assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
        assert not any(k in response for k in ('error', 'orchestration_error', 'cleanup_error'))
        assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds'])
        assert 0 <= response['seconds'] <= 120
        assert case['response_bytes'] == len(response['stdout'].encode())
        events = [loads(line) for line in response['stdout'].splitlines()]
        window = case_read('clock')
        assert window == case['server_window']
        assert window['before_us'] >= windows.get(clone_name, 0), 'process windows overlap'
        result = verify(events, before, native, inputs, case, window)
        final = only(events, event='lineage-history-state', stage='final')
        after, after_native, after_inputs = case_read('after'), case_read('after-native'), case_read('after-inputs')
        same({n: loads(v) for n, v in final['state']['control_tables']}, after)
        assert after_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
        assert after_inputs == dict(final['state']['originals']) and case_read('backends') == []
        for input_name, identity in after_inputs.items():
            content = raw(folder / clone_name / 'store' / 'producer-inputs-v1' / input_name)
            assert dict(bytes=len(content), sha256=sha(content)) == identity
        assert case_read('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED', database=case['database'],
            backend_absent=True, errors=[])
        result['negative_controls'] = negative_controls(events, before, native, inputs, case, window)
        checked.append(result)
        chains[clone_name], windows[clone_name] = (after, after_native, after_inputs), window['after_us']
    for name, clone in clones.items():
        assert clone['final_case'] == (CASES[0] if name == 'activated' else CASES[1] if name == 'empty' else CASES[-1])
        same(chains[name][0], read(folder / (name + '-final-sql.json')))
        spool = folder / name / 'store' / 'producer-inputs-v1'
        assert sorted(p.name for p in spool.iterdir()) == sorted(chains[name][2]), 'unclassified sidecar'
    return dict(status='PASS_CONNECTED_LIVE_CAPTURE_REPLAY', source=report['source'], cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        bound_files=bound, original_preservation='Pinned bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Five finite processes, three continuous writers, three original-input/new-fact/retry/read journeys; no network/installed acquisition, activated retirement, failure injection or RSS qualification')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = replay(args.root)
    output = __import__('json').dumps(result, indent=2) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(output)
    else:
        print(output, end='')
