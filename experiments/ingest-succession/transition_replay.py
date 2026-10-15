#!/usr/bin/env python3
"""Independent full-state reader for finite preparation and empty retirement.

Golden original science is checked by the independent history reader. New wire
bytes and every relational/native delta are constructed below using Python byte
operations, without a product codec or the probe's receipt as expected output.
This does not qualify activation, admission, installed supervision or native gaps.
"""
import argparse
import copy
import math
from pathlib import Path
import re
import tomllib

import history_replay as history


CASES = ('activated-prepare3', 'activated-empty3', 'activated-empty-retry3',
         'activated-prepare4', 'empty-prepare3', 'empty-empty3',
         'empty-empty-retry3', 'empty-prepare4', 'activated-metadata-short')
PINS = history.PINS
encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, loads, instant = history.only, history.same, history.loads, history.instant


def specification(case):
    name = case['name']
    assert name in CASES
    index = CASES.index(name)
    origin = 'empty' if 4 <= index < 8 else 'activated'
    stage = index % 4 if index < 8 else 0
    operation = ('prepare', 'empty', 'empty-retry', 'prepare')[stage]
    previous = CASES[index - 1] if stage else None
    assert case['origin'] == origin and case['operation'] == operation
    assert case['predecessor'] == previous
    assert case['metadata_bytes'] == (132607 if index == 8 else 262144)
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    return origin, operation, (4 if stage == 3 else 3), index == 8


def native_map(native):
    assert set(native) == {'event', 'rows', 'sequence'} and native['event'] == 'dump-state'
    assert type(native['sequence']) is int and native['sequence'] >= 0
    result = {(r['cf'], r['key']): r['value'] for r in native['rows']}
    assert len(result) == len(native['rows']) and list(result) == sorted(result)
    for (cf, key), value in result.items():
        assert isinstance(cf, str) and bytes.fromhex(key).hex() == key
        assert bytes.fromhex(value).hex() == value
    return result


def native_rows(values):
    return [dict(cf=cf, key=key, value=value) for (cf, key), value in sorted(values.items())]


def bx(value):
    return '\\x' + value.hex()


def empty_digest(record, manifest, slot, epoch):
    return sha(b'bigbrotr/ingest-successor-empty-closure/1\0'
        + bytes.fromhex(sha(record)) + bytes.fromhex(sha(manifest))
        + slot + be(epoch) + be(0) + be(0))


class Model:
    """Only fresh identity/time/xid coordinates are taken from observations."""
    def __init__(self, before, native, inputs, window):
        assert len(before) == 75
        assert set(window) == {'before_us', 'after_us'}
        assert all(type(v) is int and v > 0 for v in window.values())
        assert 0 <= window['after_us'] - window['before_us'] <= 120_000_000
        self.pg, self.native, self.inputs = copy.deepcopy((before, native, inputs))
        self.window, self.clock = window, window['before_us']
        self.used = {row['xmin'] for rows in before.values() for row in rows}
        self.previous_xid = None
        self.control = self.control_role = None
        self.refreshes = 0
        self.ds = only(self.pg['datasets'])
        self.legacy = only(self.pg['initial_ingest_successions'])
        self.resource, self.slot = (self.legacy[k] for k in ('owner_resource_id', 'producer_slot_id'))
        self.identities = set(re.findall(r'\\\\x([0-9a-f]{32})"', encoded(before).decode()))
        native_map(native)

    def at(self, value, lower=None):
        assert type(value) is int and max(self.clock, lower or self.clock) <= value <= self.window['after_us'], 'phase UTC differs'
        self.clock = value
        return value

    def fresh(self, xid, top=None):
        assert isinstance(xid, str) and re.fullmatch('[1-9][0-9]*', xid)
        number = int(xid)
        assert number < 2**32 and xid not in self.used, 'transaction identity reused'
        if self.previous_xid is not None:
            assert 0 < (number - self.previous_xid) % 2**32 < 2**31, 'transaction order differs'
        if top is not None:
            assert isinstance(top, str) and re.fullmatch('[1-9][0-9]*', top)
            assert int(top) < 2**64 and int(top) % 2**32 == number, 'top-level transaction differs'
        self.previous_xid = number
        self.used.add(xid)
        return xid

    def identity(self, value):
        assert re.fullmatch('[0-9a-f]{32}', value) and int(value, 16)
        assert value not in self.identities, 'identity reused'
        self.identities.add(value)
        return '\\x' + value

    def claim(self, observed, planned):
        previous = max((r for r in self.pg['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
        assert previous['state'] == 'released'
        row = only(observed['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
        role = only(observed['role_instances'], role_instance_id=row['role_instance_id'])
        xid = self.fresh(row['xmin'])
        role_id, incarnation = self.identity(planned['role']), self.identity(planned['incarnation'])
        created = self.at(role['created_at_us'], previous['released_at_us'])
        acquired = self.at(row['acquired_at_us'])
        assert type(row['control_backend_pid']) is int and row['control_backend_pid'] > 0
        assert self.window['before_us'] <= instant(row['control_backend_start']) <= acquired
        self.control_role = dict(role_instance_id=role_id, dataset_id=self.ds['dataset_id'],
            role='control', created_at_us=created, retired_at_us=None, xmin=xid)
        self.control = dict(previous, epoch=previous['epoch'] + 1, role_instance_id=role_id,
            process_incarnation=incarnation, acquired_at_us=acquired,
            lease_expires_at_us=acquired + 20_000_000, state='active', released_at_us=None,
            exclusion_method=None, exclusion_digest=None, control_backend_pid=row['control_backend_pid'],
            control_backend_start=row['control_backend_start'], xmin=xid)
        self.pg['role_instances'].append(self.control_role)
        self.pg['owner_epochs'].append(self.control)
        same(self.pg, observed)

    def refresh(self, observed):
        row = only(observed['owner_epochs'], role='control', epoch=self.control['epoch'])
        self.at(row['lease_expires_at_us'] - 20_000_000)
        self.control.update(lease_expires_at_us=row['lease_expires_at_us'], xmin=self.fresh(row['xmin']))
        same(self.pg, observed)
        self.refreshes += 1

    def release(self, observed):
        c = self.control
        row = only(observed['owner_epochs'], role='control', epoch=c['epoch'])
        role = only(observed['role_instances'], role_instance_id=c['role_instance_id'])
        at = self.at(row['released_at_us'])
        assert at < c['lease_expires_at_us']
        retired = self.at(role['retired_at_us'])
        xid = self.fresh(row['xmin'])
        digest = sha(b'bigbrotr/control-exclusion/1\0' + binary(c['dataset_id'])
            + binary(c['resource_id']) + be(c['epoch']) + binary(c['process_incarnation']) * 2)
        c.update(state='released', released_at_us=at, exclusion_method='storage_fenced',
            exclusion_digest='\\x' + digest, xmin=xid)
        self.control_role.update(retired_at_us=retired, xmin=xid)
        same(self.pg, observed)

    def plan(self, role_hex, incarnation_hex, domains, epoch, created, deadline):
        """Build the exact portable record and complete intended SQL rowset."""
        role, incarnation = self.identity(role_hex), self.identity(incarnation_hex)
        at = self.at(created)
        assert deadline == at + 20_000_000 and at < self.control['lease_expires_at_us']
        previous = self.legacy if epoch == 3 else only(self.pg['ingest_successions'], target_epoch=epoch - 1)
        old_manifest, parent = binary(previous['target_manifest']), binary(previous['handoff'])
        source = only(self.pg['source_streams'], owner_resource_id=self.resource, owner_epoch=epoch - 1)
        producer = only(self.pg['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch - 1)
        assert source['state'] == 'closed' and producer['state'] == 'retired'
        assert at >= producer['retired_at_us']
        kind = 1 if (bool(self.pg['initial_ingest_activations']) if epoch == 3 else
                     any(r['target_epoch'] == epoch - 1 for r in self.pg['ingest_activations'])) else 2
        topology = self.ds['topology_generation'] + 1
        manifest = (bytes.fromhex('01010003000100010001') + binary(self.ds['dataset_id']) + be(topology)
            + binary(self.resource) + be(self.legacy['shard_id'], 4) + be(epoch) + binary(incarnation)
            + old_manifest[78:94] + be(self.ds['policy_revision']) + b'\x01\x09')
        assert len(domains) == 9
        expected_domains = []
        for kind_number, domain in enumerate(domains, 1):
            identity = self.identity(domain)
            expected_domains.append(identity)
            manifest += be(kind_number, 2) + binary(identity)
        record = (b'\x02\x0a' + bytes([kind]) + binary(self.slot) + be(epoch - 1)
            + be(producer['reserved_through_request']) + be(source['last_complete_sequence'])
            + binary(source['closure_digest']) + bytes.fromhex(sha(parent))
            + be(len(old_manifest), 2) + old_manifest + be(len(manifest), 2) + manifest)
        assert len(manifest) == 266 and len(record) == 643
        return dict(epoch=epoch, created=at, deadline=deadline, role=role, incarnation=incarnation,
            domains=expected_domains, previous=previous, kind=kind, topology=topology,
            manifest=manifest, record=record, source=copy.deepcopy(source), producer=copy.deepcopy(producer))

    def prepare(self, observed, plan):
        epoch, at = plan['epoch'], plan['created']
        receipt = only(observed['ingest_successions'], target_epoch=epoch)
        xid = self.fresh(receipt['xmin'], receipt['preparation_transaction'])
        assert self.clock < min(plan['deadline'], self.control['lease_expires_at_us'])
        ds, resource, slot = self.ds['dataset_id'], self.resource, self.slot
        role, incarnation, topology = (plan[k] for k in ('role', 'incarnation', 'topology'))
        self.pg['role_instances'].append(dict(role_instance_id=role, dataset_id=ds, role='ingest',
            created_at_us=at, retired_at_us=None, xmin=xid))
        self.pg['owner_epochs'].append(dict(resource_id=resource, epoch=epoch, dataset_id=ds,
            role='ingest', role_instance_id=role, process_incarnation=incarnation, acquired_at_us=at,
            lease_expires_at_us=plan['deadline'], state='active', released_at_us=None,
            exclusion_method=None, exclusion_digest=None, control_fence_version=None,
            control_backend_pid=None, control_backend_start=None, archive_frontier_protocol=None, xmin=xid))
        self.pg['source_streams'].append(dict(dataset_id=ds, shard_id=self.legacy['shard_id'], owner_epoch=epoch,
            owner_resource_id=resource, owner_role='ingest', owner_incarnation=incarnation, opened_at_us=at,
            state='open', closed_at_us=None, last_complete_sequence=None, closure_digest=None, xmin=xid))
        for number, domain in enumerate(plan['domains'], 1):
            self.pg['allocation_domains'].append(dict(dataset_id=ds, dictionary_kind=number,
                allocation_domain=domain, owner_resource_id=resource, owner_epoch=epoch, owner_role='ingest',
                owner_incarnation=incarnation, created_at_us=at, state='active', closed_at_us=None,
                closure_digest=None, xmin=xid))
        self.pg['producer_epochs'].append(dict(producer_slot_id=slot, producer_epoch=epoch, dataset_id=ds,
            producer_role_instance_id=role, producer_role='ingest', producer_incarnation=incarnation,
            owner_resource_id=resource, owner_epoch=epoch, owner_role='ingest', owner_incarnation=incarnation,
            outstanding_request_limit=32, opened_at_us=at, state='open', retired_at_us=None,
            closure_digest=None, reserved_through_request=0, xmin=xid))
        self.pg['topology_generations'].append(dict(dataset_id=ds, generation=topology,
            previous_generation=self.ds['topology_generation'], created_at_us=at, sealed=True, xmin=xid))
        for e in range(1, epoch + 1):
            source = only(self.pg['source_streams'], owner_resource_id=resource, owner_epoch=e)
            self.pg['topology_streams'].append(dict(dataset_id=ds, topology_generation=topology,
                shard_id=self.legacy['shard_id'], owner_epoch=e,
                handoff_last_complete_sequence=source['last_complete_sequence'], xmin=xid))
        old_topology = self.ds['topology_generation']
        self.ds.update(topology_generation=topology, xmin=xid)
        previous = plan['previous']
        self.pg['ingest_successions'].append(dict(dataset_id=ds, owner_resource_id=resource, owner_role='ingest',
            target_epoch=epoch, previous_epoch=epoch - 1, terminal_kind=plan['kind'], target_incarnation=incarnation,
            target_role_instance_id=role, producer_slot_id=slot, producer_epoch=epoch, shard_id=self.legacy['shard_id'],
            previous_topology=old_topology, target_topology=topology,
            issued_through=plan['producer']['reserved_through_request'],
            last_complete_sequence=plan['source']['last_complete_sequence'], closure_digest=plan['source']['closure_digest'],
            previous_manifest=previous['target_manifest'], target_manifest=bx(plan['manifest']),
            previous_manifest_sha256='\\x' + sha(binary(previous['target_manifest'])),
            target_manifest_sha256='\\x' + sha(plan['manifest']), handoff=bx(plan['record']),
            handoff_sha256='\\x' + sha(plan['record']), parent_handoff_sha256='\\x' + sha(binary(previous['handoff'])),
            config_revision=self.ds['config_revision'], policy_revision=self.ds['policy_revision'],
            config_digest=only(self.pg['config_revisions'], revision=self.ds['config_revision'])['document_digest'],
            policy_digest=only(self.pg['policy_revisions'], revision=self.ds['policy_revision'])['document_digest'],
            outstanding_request_limit=32, created_at_us=at, lease_expires_at_us=plan['deadline'],
            prepared_by_resource=self.control['resource_id'], prepared_by_epoch=self.control['epoch'],
            prepared_by_role='control', prepared_by_incarnation=self.control['process_incarnation'],
            preparation_transaction=receipt['preparation_transaction'], xmin=xid))
        values = native_map(self.native)
        for cf, key, value in [
            ('default', b'\x01\x01', plan['manifest']),
            ('default', b'\x01\x0a' + binary(ds) + binary(resource) + be(epoch - 1), plan['record']),
            ('default', b'\x01\x02' + binary(ds) + be(self.legacy['shard_id'], 4) + be(epoch), b'\x01\x02' + be(0)),
            ('receipts', b'\x01\x31' + binary(slot) + be(epoch), b'\x01\x31' + bytes(24)),
        ]:
            assert (cf, key.hex()) not in values or key == b'\x01\x01'
            values[cf, key.hex()] = value.hex()
        self.native.update(rows=native_rows(values), sequence=self.native['sequence'] + 4)
        same(self.pg, observed)

    def retire(self, observed, epoch):
        receipt = only(observed['ingest_terminal_transactions'], target_epoch=epoch)
        producer = only(observed['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch)
        xid = self.fresh(producer['xmin'], receipt['terminal_transaction'])
        at = self.at(producer['retired_at_us'])
        assert at < self.control['lease_expires_at_us']
        intent = only(self.pg['ingest_successions'], target_epoch=epoch)
        assert at >= intent['created_at_us']
        digest = empty_digest(binary(intent['handoff']), binary(intent['target_manifest']), binary(self.slot), epoch)
        only(self.pg['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch).update(
            state='retired', retired_at_us=at, closure_digest='\\x' + digest, xmin=xid)
        only(self.pg['source_streams'], owner_resource_id=self.resource, owner_epoch=epoch).update(
            state='closed', closed_at_us=at, last_complete_sequence=0, closure_digest='\\x' + digest, xmin=xid)
        only(self.pg['owner_epochs'], resource_id=self.resource, epoch=epoch).update(
            state='released', released_at_us=at, exclusion_method='storage_fenced', exclusion_digest='\\x' + digest, xmin=xid)
        only(self.pg['role_instances'], role_instance_id=intent['target_role_instance_id']).update(retired_at_us=at, xmin=xid)
        domains = [r for r in self.pg['allocation_domains'] if r['owner_epoch'] == epoch]
        assert len(domains) == 9
        for row in domains:
            row.update(state='closed', closed_at_us=at, closure_digest='\\x' + digest, xmin=xid)
        self.pg['ingest_terminal_transactions'].append(dict(owner_resource_id=self.resource, target_epoch=epoch,
            terminal_transaction=receipt['terminal_transaction'], control_resource=self.control['resource_id'],
            control_epoch=self.control['epoch'], control_incarnation=self.control['process_incarnation'], xmin=xid))
        values = native_map(self.native)
        key = ('receipts', (b'\x01\x31' + binary(self.slot) + be(epoch)).hex())
        assert values[key] == (b'\x01\x31' + bytes(24)).hex()
        values[key] = (b'\x02\x31' + bytes(24) + b'\x01').hex()
        self.native.update(rows=native_rows(values), sequence=self.native['sequence'] + 1)
        same(self.pg, observed)
        return at, digest


def lineage(pg, native, origin):
    """Expected ordered replay of the pinned originals and newly empty epochs.

    Later invocation baselines are bound to the prior independently checked
    external state by replay(); they are never substituted for the original pin.
    """
    intent = only(pg['initial_ingest_successions'])
    slot = binary(intent['producer_slot_id'])
    values = native_map(native)
    current_manifest = bytes.fromhex(values['default', '0101'])
    head = int.from_bytes(current_manifest[54:62], 'big')
    assert head in (2, 3)
    ds, resource = binary(intent['dataset_id']), binary(intent['owner_resource_id'])
    entries, summaries, counts = [], [], []
    for epoch in range(1, head + 1):
        if epoch == 1:
            manifest, incoming = binary(intent['previous_manifest']), bytes(32)
            count, cut, kind, digest, mask = 4, 12, 'initial', history.CLOSURES[0], 8
        elif epoch == 2:
            manifest, incoming = binary(intent['target_manifest']), bytes.fromhex(sha(binary(intent['handoff'])))
            count, cut = (2, 5) if origin == 'activated' else (0, 0)
            kind = 'activated' if origin == 'activated' else 'unactivated-empty'
            digest, mask = history.CLOSURES[1 if origin == 'activated' else 2], 2 if origin == 'activated' else 0
        else:
            s = only(pg['ingest_successions'], target_epoch=epoch)
            manifest, incoming = binary(s['target_manifest']), bytes.fromhex(sha(binary(s['handoff'])))
            count, cut, kind, mask = 0, 0, 'unactivated-empty', 0
            digest = empty_digest(binary(s['handoff']), manifest, slot, epoch)
        marker = bytes.fromhex(values['receipts', (b'\x01\x31' + slot + be(epoch)).hex()])
        closed = marker == b'\x02\x31' + be(count) + bytes(16) + b'\x01'
        assert closed or marker == b'\x01\x31' + be(count) + bytes(16)
        assert values['default', (b'\x01\x02' + ds + be(0, 4) + be(epoch)).hex()] == (b'\x01\x02' + be(cut)).hex()
        assert manifest == (current_manifest if epoch == head else manifest)
        ancestor = epoch < head
        entries.append(dict(manifest=manifest.hex(), issued_through=count, source_through=cut,
            closing=closed, incoming_sha256=incoming.hex(), terminal_kind=kind if ancestor else None,
            terminal_digest=digest if ancestor else None))
        summaries.append(dict(epoch=epoch, terminal_kind=kind, resolved_requests=count,
            last_complete_sequence=cut, digest=digest, event_request_mask=mask))
        counts.append(count)
        if ancestor or closed:
            assert only(pg['source_streams'], owner_epoch=epoch)['closure_digest'] == '\\x' + digest
        if epoch >= 2:
            stored = values['default', (b'\x01\x0a' + ds + resource + be(epoch - 1)).hex()]
            expected = binary(intent['handoff']) if epoch == 2 else binary(s['handoff'])
            assert stored == expected.hex() and sha(expected) == incoming.hex()
    state = 'PreparedOpen' if not entries[-1]['closing'] else 'Terminal'
    through = head - 1 if state == 'PreparedOpen' else head
    return dict(slot=slot.hex(), entries=entries, summaries=summaries[:through],
        counts=counts[:through], head=head, through=through, state=state)


class Transcript:
    def __init__(self, events, model):
        self.events, self.model, self.position, self.snapshots = events, model, 0, 0
        # JSON bool must not impersonate an integer, including in raw SQL.
        booleans = {'closing', 'present', 'failed', 'joined', 'success', 'admission',
            'repair', 'matches_history', 'existing_copy_checked', 'prepared', 'activation', 'read_only'}
        numbers = {'epoch', 'sequence', 'pid', 'control_epoch', 'config_revision', 'policy_revision',
            'native_epoch', 'registered_epoch', 'replay_through', 'metadata_bytes', 'memberships',
            'issued_through', 'source_through', 'resolved_requests', 'last_complete_sequence',
            'event_request_mask', 'verified_through', 'target_epoch', 'predecessor_epoch',
            'outstanding_request_limit', 'mutation_data_bytes', 'mutation_native_bytes'}
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

    def take(self, name, transition=False):
        assert self.position < len(self.events), ('missing event', name)
        event = self.events[self.position]
        self.position += 1
        assert event['event'] == ('lineage-transition-' if transition else 'lineage-history-') + name, ('unexpected event', event)
        return event

    def snapshot(self, stage, mutation=None):
        event = self.take('state')
        assert set(event) == {'event', 'stage', 'state', 'rows'} and event['stage'] == stage
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence', 'control_sha256', 'control_tables', 'originals'}
        tables = state['control_tables']
        assert len(tables) == 75 and [name for name, _ in tables] == sorted(self.model.pg)
        assert state['control_sha256'] == sha(encoded(tables))
        pg = {name: loads(value) for name, value in tables}
        if mutation:
            mutation(pg)
        same(self.model.pg, pg)
        assert event['rows'] == self.model.native['rows'] and state['sequence'] == self.model.native['sequence'], 'native bytes/sequence differ'
        assert state['native_rows'] == len(event['rows']) and state['native_sha256'] == sha(encoded(event['rows']))
        assert state['originals'] == [[name, self.model.inputs[name]] for name in sorted(self.model.inputs)], 'original inputs differ'
        self.snapshots += 1
        return pg

    def policy(self, event, phase, epoch, sequence):
        m = self.model
        config = only(m.pg['config_revisions'], revision=m.ds['config_revision'])
        policy = only(m.pg['policy_revisions'], revision=m.ds['policy_revision'])
        assert event == dict(event='lineage-history-policy', phase=phase, epoch=epoch, sequence=sequence,
            control_epoch=m.control['epoch'], config_revision=m.ds['config_revision'],
            policy_revision=m.ds['policy_revision'], config_digest=config['document_digest'][2:],
            policy_digest=policy['document_digest'][2:])

    def refresh(self, epoch, sequence):
        self.policy(self.take('policy'), 'refreshed', epoch, sequence)
        self.snapshot(f"refreshed-{epoch if epoch is not None else 'final'}-{sequence if sequence is not None else 'epoch'}", self.model.refresh)

    def history(self, known):
        assert self.take('loaded') == dict(event='lineage-history-loaded', state=known['state'],
            native_epoch=known['head'], registered_epoch=known['head'], replay_through=known['through'],
            producer_slot=known['slot'], pending_target=None, metadata_bytes=131072 + 512 * known['head'],
            memberships=known['head'] * (known['head'] + 1) // 2, entries=known['entries'])
        self.snapshot('loaded')
        for epoch, count in enumerate(known['counts'], 1):
            for sequence in range(1, count + 1):
                self.refresh(epoch, sequence)
                assert self.take('request') == dict(event='lineage-history-request', producer_slot=known['slot'],
                    epoch=epoch, sequence=sequence, existing_copy_checked=True)
                self.snapshot(f'resolved-{epoch}-{sequence}')
            self.refresh(epoch, None)
            assert self.take('epoch') == dict(event='lineage-history-epoch', summary=known['summaries'][epoch - 1])
            self.snapshot(f'epoch-{epoch}')
        self.refresh(None, None)
        assert self.take('proof') == dict(event='lineage-history-proof', verified_through=known['through'],
            last_epoch=known['summaries'][-1], matches_history=True, admission=False)
        self.snapshot('proof')


def record_value(record, target):
    return dict(bytes=record.hex(), sha256=sha(record), predecessor_epoch=target - 1, target_epoch=target)


def verify(events, before, native, inputs, case, server_window):
    """Check one invocation. Later baselines require replay's exact prior link."""
    origin, operation, target, short = specification(case)
    if case['predecessor'] is None:
        history.baseline(before, native, inputs, origin, server_window)
        history.science(before, native, inputs, origin)
    base_count, base_sequence, input_count = (70, 85, 6) if origin == 'activated' else (52, 63, 4)
    stage = CASES.index(case['name']) % 4 if not short else 0
    assert len(native['rows']) == base_count + (0 if stage == 0 else 3)
    assert native['sequence'] == base_sequence + (0 if stage == 0 else (4 if stage == 1 else 5))
    assert len(inputs) == input_count
    assert sha(encoded(inputs)) == PINS[origin]['inputs']
    m = Model(before, native, inputs, server_window)
    known = lineage(before, native, origin)
    assert known['head'] == (2 if stage == 0 else 3)
    assert known['state'] == ('PreparedOpen' if operation == 'empty' else 'Terminal')
    t = Transcript(events, m)
    candidate = None
    if operation == 'prepare':
        candidate = t.take('candidate', True)
        assert set(candidate) == {'event', 'role', 'incarnation', 'namespaces'}
        assert [r['kind'] for r in candidate['namespaces']] == list(range(1, 10))
        assert all(set(r) == {'kind', 'id'} and type(r['kind']) is int for r in candidate['namespaces'])
        ids = [candidate['role'], candidate['incarnation']] + [r['id'] for r in candidate['namespaces']]
        assert len(set(ids)) == 11
        assert all(re.fullmatch('[0-9a-f]{32}', v) and int(v, 16) and v not in m.identities for v in ids)
    resource = t.take('resources')
    assert set(resource) == {'event', 'metadata_bytes', 'metadata_scratch_bytes', 'metadata_entry_bytes',
        'input_bytes', 'outcome_bytes', 'effect_bytes', 'native_bytes', 'transport_bytes',
        'observation_bytes', 'operation_timeout_ms', 'frame_bytes'}
    assert resource == dict(resource, metadata_bytes=case['metadata_bytes'], metadata_scratch_bytes=131072,
        metadata_entry_bytes=512, native_bytes=128 * 1024**2, transport_bytes=4 * 1024**2,
        observation_bytes=32 * 1024**2, operation_timeout_ms=10000, frame_bytes=16 * 1024**2)
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resource[field]) is int and 0 < resource[field] <= 256 * 1024**2
    mutation_bytes = {'prepare': 8192, 'empty': 4096, 'empty-retry': 0}[operation]
    assert sum(resource[k] for k in ('metadata_bytes', 'input_bytes', 'outcome_bytes',
        'effect_bytes', 'transport_bytes', 'observation_bytes')) + mutation_bytes <= 1024**3
    assert t.take('resources', True) == dict(event='lineage-transition-resources', operation=operation,
        mutation_data_bytes=mutation_bytes, mutation_native_bytes=mutation_bytes,
        lease_ttl_us='20000000', outstanding_request_limit=32)
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    if candidate:
        assert not ({planned['role'], planned['incarnation']} & set(ids))
    t.snapshot('before-claim')
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    assert claimed == dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation'])
    assert backend == dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'],
        control_epoch=m.control['epoch'], present=True)
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-read')
    t.history(known)
    observation = None
    if not short:
        event = t.take('plan', True)
        if operation == 'prepare':
            created, deadline = event['created_at_us'], event['lease_expires_at_us']
            assert re.fullmatch('[1-9][0-9]*', created) and re.fullmatch('[1-9][0-9]*', deadline)
            plan = m.plan(candidate['role'], candidate['incarnation'],
                [r['id'] for r in candidate['namespaces']], target, int(created), int(deadline))
            expected_record = record_value(plan['record'], target)
            assert event == dict(event='lineage-transition-plan', operation=operation, record=expected_record,
                role=candidate['role'], created_at_us=created, lease_expires_at_us=deadline,
                outstanding_request_limit=32, policy=dict(config_revision=m.ds['config_revision'],
                    policy_revision=m.ds['policy_revision'],
                    config_digest=only(m.pg['config_revisions'], revision=m.ds['config_revision'])['document_digest'][2:],
                    policy_digest=only(m.pg['policy_revisions'], revision=m.ds['policy_revision'])['document_digest'][2:]))
            t.snapshot('planned')
            t.snapshot('transition-returned', lambda pg: m.prepare(pg, plan))
            assert t.take('observed', True) == dict(event='lineage-transition-observed', operation=operation,
                prepared=True, record=expected_record, activation=False)
            observation = dict(prepared=True, target_epoch=target, retired_at_us=None)
        else:
            intent = only(m.pg['ingest_successions'], target_epoch=target)
            digest = empty_digest(binary(intent['handoff']), binary(intent['target_manifest']), binary(m.slot), target)
            if operation == 'empty':
                assert event == dict(event='lineage-transition-plan', operation=operation,
                    record=record_value(binary(intent['handoff']), target), closure_digest=digest)
                t.snapshot('planned')
                t.snapshot('transition-returned', lambda pg: m.retire(pg, target))
            else:
                assert event == dict(event='lineage-transition-plan', operation=operation,
                    target_epoch=target, closure_digest=digest, read_only=True)
                t.snapshot('planned')
                t.snapshot('transition-returned')
            retired = str(only(m.pg['producer_epochs'], producer_slot_id=m.slot, producer_epoch=target)['retired_at_us'])
            expected = dict(event='lineage-transition-observed', operation=operation,
                target_epoch=target, retired_at_us=retired, closure_digest=digest)
            if operation == 'empty-retry':
                expected['read_only'] = True
            assert t.take('observed', True) == expected
            observation = dict(prepared=False, target_epoch=target, retired_at_us=retired)
    assert t.take('result', True) == dict(event='lineage-transition-result', operation=operation,
        stage='plan' if short else 'complete', epoch=None, sequence=None,
        error='Native(Native(InsufficientReservation))' if short else None, observation=observation)
    t.snapshot('after-transition')
    assert t.take('native') == dict(event='lineage-history-native', state='closed')
    t.snapshot('before-control-end')
    assert t.take('driver') == dict(event='lineage-history-driver', driver='control', failed=False, joined=True)
    assert t.take('backend') == dict(backend, present=False)
    phase = 'dropped-and-joined' if short else 'released-and-joined'
    assert t.take('control') == dict(event='lineage-history-control', phase=phase,
        resource=m.control['resource_id'][2:], epoch=m.control['epoch'])
    t.snapshot('after-control-end', None if short else m.release)
    t.snapshot('final')
    assert t.take('driver') == dict(event='lineage-history-driver', driver='observer', failed=False, joined=True)
    assert t.take('complete', True) == dict(event='lineage-transition-complete', operation=operation,
        native='closed', control=phase, observer='joined', success=not short, admission=False, activation=False)
    assert t.position == len(events), 'unclassified trailing event'
    expected_snapshots = (26 if short else ((28 if origin == 'activated' else 24) + (2 if stage >= 2 else 0)))
    assert t.snapshots == expected_snapshots
    assert m.refreshes == sum(known['counts']) + len(known['counts']) + 1
    return dict(status='PASS', case=case['name'], snapshots=t.snapshots, tables=75,
        native_rows=len(m.native['rows']), sequence=m.native['sequence'], inputs=len(inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, database=case['database'],
        observation=observation, refusal=short,
        scope='Finite preparation/empty retirement; no activation, adoption, admission, installed or gap claim')


def negative_controls(events, before, native, inputs, case, server_window):
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

    def event(values, name, transition=False, **fields):
        return only(values, event=('lineage-transition-' if transition else 'lineage-history-') + name, **fields)

    def sql(values, mutation):
        for item in values:
            if item['event'] == 'lineage-history-state':
                state = item['state']
                pg = {name: loads(rows) for name, rows in state['control_tables']}
                mutation(pg)
                state['control_tables'] = [[name, encoded(rows).decode()] for name, rows in sorted(pg.items())]
                state['control_sha256'] = sha(encoded(state['control_tables']))

    def original(values):
        sql(values, lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='4000000000'))

    def sequence(values):
        for item in values:
            if item['event'] == 'lineage-history-state':
                item['state']['sequence'] += 1

    def backend(values):
        for item in values:
            if item['event'] == 'lineage-history-backend':
                item['pid'] += 1

    def closure(values):
        for item in values:
            if item['event'] == 'lineage-history-epoch' and item['summary']['epoch'] == 2:
                item['summary']['digest'] = '00' * 32
            if item['event'] == 'lineage-history-proof' and item['last_epoch']['epoch'] == 2:
                item['last_epoch']['digest'] = '00' * 32

    reject('retained-original-xmin-rehashed', original)
    reject('all-native-sequences-shifted', sequence)
    reject('both-backend-events-foreign-pid', backend)
    reject('coherent-old-closure-claim', closure)
    reject('input-digest-changed', lambda v: event(v, 'state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('metadata-grant-changed', lambda v: event(v, 'resources').update(metadata_bytes=132096))
    reject('control-driver-not-joined', lambda v: event(v, 'driver', driver='control').update(joined=False))
    reject('observer-driver-failed', lambda v: event(v, 'driver', driver='observer').update(failed=True))
    reject('claimed-writer-activation', lambda v: event(v, 'complete', True).update(activation=True))
    reject('boolean-as-integer', lambda v: event(v, 'proof').update(matches_history=1))
    reject('missing-request-observation', lambda v: v.pop(next(i for i, e in enumerate(v) if e['event'] == 'lineage-history-request')))
    if case['name'] == 'activated-metadata-short':
        reject('capacity-failure-in-wrong-stage', lambda v: event(v, 'result', True).update(stage='inspect'))
        reject('unrelated-failure-taxonomy', lambda v: event(v, 'result', True).update(error='DeliveryUnknown'))
    elif case['operation'] == 'prepare':
        def parent(values):
            for name in ('plan', 'observed'):
                record = event(values, name, True)['record']
                raw = bytearray.fromhex(record['bytes'])
                raw[75] ^= 1
                record.update(bytes=raw.hex(), sha256=sha(raw))
        reject('rehashed-parent-hash-in-both-observations', parent)
        reject('prepared-without-candidate-binding', lambda v: event(v, 'plan', True).update(role='ff' * 16))
        reject('wrong-request-window', lambda v: event(v, 'plan', True).update(outstanding_request_limit=31))
    else:
        def retired(values):
            item = event(values, 'observed', True)
            item['retired_at_us'] = str(int(item['retired_at_us']) + 1)
            event(values, 'result', True)['observation']['retired_at_us'] = item['retired_at_us']
        reject('coherent-retirement-time-change', retired)
        def digest(values):
            for name in ('plan', 'observed'):
                event(values, name, True)['closure_digest'] = '00' * 32
        reject('coherent-empty-closure-change', digest)
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def replay(root):
    """Bind actual stdout, original pins and every successive external baseline."""
    root = Path(root).resolve()
    folder, bound = root / 'transition', {}

    def raw(path):
        path = Path(path)
        value = path.read_bytes()
        bound[str(path)] = sha(value)
        return value

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_TRANSITION_FIRST_WAVE'
    assert [c['name'] for c in report['cases']] == list(CASES)
    assert [c['name'] for c in report['clones']] == ['activated', 'empty', 'activated-short']
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
        origin, _, _, _ = specification(case)
        name, clone_name = case['name'], case['clone']
        expected_clone = 'activated-short' if name == CASES[-1] else origin
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
        assert after_inputs == inputs and case_read('backends') == []
        assert case_read('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED', database=case['database'],
            backend_absent=True, errors=[])
        result['negative_controls'] = negative_controls(events, before, native, inputs, case, window)
        checked.append(result)
        chains[clone_name], windows[clone_name] = (after, after_native, after_inputs), window['after_us']
    for name, clone in clones.items():
        assert clone['final_case'] == (CASES[3] if name == 'activated' else CASES[7] if name == 'empty' else CASES[-1])
        same(chains[name][0], read(folder / (name + '-final-sql.json')))
    return dict(status='PASS_CONNECTED_TRANSITION_CAPTURE_REPLAY', source=report['source'], cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        bound_files=bound, original_preservation='Pinned bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Nine finite processes; gap, activation, writer/admission, installed and RSS qualification remain NOT_RUN')


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
