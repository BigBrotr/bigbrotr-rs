#!/usr/bin/env python3
"""Independent retained-state reader for interrupted successor preparation.

The caller's CommitUnknown is not a rollback receipt. Wire observations and
complete SQL/native states distinguish the two selected loss boundaries. A new
process acquires its own Control and reconstructs history without a candidate.
The qualified nine-case reader is reused as data reducers, never as a projected
or rewritten transcript. No product codec supplies expected bytes.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import tomllib

import transition_replay as base

history = base.history
encoded, sha, binary, be = base.encoded, base.sha, base.binary, base.be
only, same, loads, instant = base.only, base.same, base.loads, base.instant
CASES = ('activated-prepare-after', 'activated-gap', 'activated-empty-before',
         'activated-empty-complete', 'activated-empty-retry')
OPERATIONS = ('prepare', 'gap', 'empty', 'empty', 'empty-retry')
PROXY_SHA256 = '6e737de8223818d0b05b47cb25f16038233d43181b12aec97bd13e3b44e3f829'
COUNTERS = ('commits_seen', 'commits_forwarded', 'server_commit_complete',
            'client_commit_complete', 'faults')


def specification(case):
    index = CASES.index(case['name'])
    assert case['origin'] == case['clone'] == 'activated'
    assert case['operation'] == OPERATIONS[index]
    assert case['predecessor'] == (CASES[index - 1] if index else None)
    assert case['metadata_bytes'] == 262144
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return index, OPERATIONS[index], index in (0, 2)


def wire_checked(fault, index, port):
    if index not in (0, 2):
        assert fault is None and port is None
        return
    point, mode = ('prepare', 'after-complete') if index == 0 else ('empty', 'before-forward')
    assert set(fault) == {'point', 'mode', 'commit_number', 'proxy_port', 'wire'}
    assert fault['point'] == point and fault['mode'] == mode and fault['commit_number'] == 11
    assert type(port) is int and 1 <= port <= 65535 and port != 5432
    assert fault['proxy_port'] == port
    wire = fault['wire']
    assert wire['status'] == 'CAPTURED'
    assert type(wire['exit_code']) is int and wire['exit_code'] == 0
    assert type(wire['wrapper_exit']) is int and wire['wrapper_exit'] == 0
    assert wire['forced_wrapper_stop'] is False and wire['stderr_nonempty'] is False
    assert wire['output_overflow'] is False and wire['proxy_sha256'] == PROXY_SHA256
    assert not any(k in wire for k in ('error', 'cleanup_error', 'metadata_error'))
    events = wire['events']
    assert events == [wire['ready'], wire['fault'], wire['receipt']]
    assert [e['event'] for e in events] == ['ready', 'fault', 'receipt']
    vector = [11, 11, 11, 10, 1] if index == 0 else [11, 10, 10, 10, 1]
    elapsed = 0
    for number, event in enumerate(events):
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1'
        assert event['mode'] == mode and event['commit_number'] == 11
        assert event['held'] is False and event['hold_action'] is None
        assert type(event['elapsed_ns']) is int and elapsed <= event['elapsed_ns'] <= 35_000_000_000
        elapsed = event['elapsed_ns']
        assert all(type(event[k]) is int for k in COUNTERS)
        assert [event[k] for k in COUNTERS] == (vector if number else [0] * 5)
        if number:
            assert event['reason'] == mode
    ready, _, receipt = events
    assert ready['proxy_sha256'] == PROXY_SHA256 and ready['listen_host'] == '127.0.0.1'
    assert ready['listen_port'] == port and ready['upstream_port'] == 5432
    assert type(ready['pid']) is int and ready['pid'] > 0
    assert ready['deadline_seconds'] == 30 and ready['max_frame_bytes'] == 1024**2
    assert ready['max_wire_bytes'] == 64 * 1024**2 and ready['max_messages'] == 20000
    assert set(receipt['frames']) == set(receipt['tags']) == {'frontend', 'backend'}
    for direction, tags in receipt['tags'].items():
        assert all(re.fullmatch('[0-9a-f]{2}', key) and type(n) is int and n > 0 for key, n in tags.items())
        assert type(receipt['frames'][direction]) is int
        assert receipt['frames'][direction] == sum(tags.values()) > 0
    assert sum(receipt['frames'].values()) <= ready['max_messages']
    assert receipt['tags']['frontend']['51'] >= 11
    assert receipt['tags']['backend']['43'] >= receipt['server_commit_complete']
    assert type(receipt['declared_wire_bytes']) is int and 0 < receipt['declared_wire_bytes'] <= ready['max_wire_bytes']
    raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in events).encode()
    assert wire['stdout'].encode() == raw
    assert wire['stdout_sha256'] == sha(raw) and wire['stdout_bytes'] == len(raw)


class Model(base.Model):
    def claim(self, observed, planned):
        previous = max((r for r in self.pg['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
        if previous['state'] == 'released':
            return super().claim(observed, planned)
        # Exclusion and the fresh claim are one transaction. Its freshness is
        # checked against the original before, not an artificially fenced basis.
        assert previous['state'] == 'active' and previous['released_at_us'] is None
        old_role = only(self.pg['role_instances'], role_instance_id=previous['role_instance_id'])
        assert old_role['retired_at_us'] is None
        fenced = only(observed['owner_epochs'], role='control', epoch=previous['epoch'])
        retired = only(observed['role_instances'], role_instance_id=previous['role_instance_id'])
        current = only(observed['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
        role = only(observed['role_instances'], role_instance_id=current['role_instance_id'])
        xid = self.fresh(current['xmin'])
        role_id, incarnation = self.identity(planned['role']), self.identity(planned['incarnation'])
        released = self.at(fenced['released_at_us'])
        retirement = self.at(retired['retired_at_us'])
        created, acquired = self.at(role['created_at_us']), self.at(current['acquired_at_us'])
        assert type(current['control_backend_pid']) is int and current['control_backend_pid'] > 0
        start = instant(current['control_backend_start'])
        assert self.window['before_us'] <= start <= created
        assert (current['control_backend_pid'], start) != (previous['control_backend_pid'], instant(previous['control_backend_start']))
        digest = sha(b'bigbrotr/control-exclusion/1\0' + binary(previous['dataset_id'])
            + binary(previous['resource_id']) + be(previous['epoch'])
            + binary(previous['process_incarnation']) + binary(incarnation))
        old = copy.deepcopy(previous)
        previous.update(state='released', released_at_us=released, exclusion_method='storage_fenced',
            exclusion_digest='\\x' + digest, xmin=xid)
        old_role.update(retired_at_us=retirement, xmin=xid)
        self.control_role = dict(role_instance_id=role_id, dataset_id=self.ds['dataset_id'], role='control',
            created_at_us=created, retired_at_us=None, xmin=xid)
        self.control = dict(old, epoch=old['epoch'] + 1, role_instance_id=role_id,
            process_incarnation=incarnation, acquired_at_us=acquired, lease_expires_at_us=acquired + 20_000_000,
            state='active', released_at_us=None, exclusion_method=None, exclusion_digest=None,
            control_backend_pid=current['control_backend_pid'], control_backend_start=current['control_backend_start'], xmin=xid)
        self.pg['role_instances'].append(self.control_role)
        self.pg['owner_epochs'].append(self.control)
        same(self.pg, observed)

    def prepare_sql(self, observed, plan):
        # Reuse the exact independent SQL row constructor. This boundary lost
        # the response before native dispatch; its expected native map stays old.
        retained = copy.deepcopy(self.native)
        super().prepare(observed, plan)
        self.native = retained

    def complete_gap(self, observed):
        same(self.pg, observed)
        intent = only(self.pg['ingest_successions'], target_epoch=3)
        values = base.native_map(self.native)
        changes = (
            ('default', b'\x01\x01', binary(intent['target_manifest'])),
            ('default', b'\x01\x0a' + binary(intent['dataset_id']) + binary(self.resource) + be(2), binary(intent['handoff'])),
            ('default', b'\x01\x02' + binary(intent['dataset_id']) + be(intent['shard_id'], 4) + be(3), b'\x01\x02' + be(0)),
            ('receipts', b'\x01\x31' + binary(self.slot) + be(3), b'\x01\x31' + bytes(24)),
        )
        for cf, key, value in changes:
            assert (cf, key.hex()) not in values or key == b'\x01\x01'
            values[cf, key.hex()] = value.hex()
        self.native.update(rows=base.native_rows(values), sequence=self.native['sequence'] + 4)

    def native_close(self, observed):
        same(self.pg, observed)
        values = base.native_map(self.native)
        key = 'receipts', (b'\x01\x31' + binary(self.slot) + be(3)).hex()
        assert values[key] == (b'\x01\x31' + bytes(24)).hex()
        values[key] = (b'\x02\x31' + bytes(24) + b'\x01').hex()
        self.native.update(rows=base.native_rows(values), sequence=self.native['sequence'] + 1)

    def retire_sql(self, observed):
        intent = only(self.pg['ingest_successions'], target_epoch=3)
        receipt = only(observed['ingest_terminal_transactions'], target_epoch=3)
        producer = only(observed['producer_epochs'], producer_slot_id=self.slot, producer_epoch=3)
        xid = self.fresh(producer['xmin'], receipt['terminal_transaction'])
        at = self.at(producer['retired_at_us'])
        assert intent['created_at_us'] <= at < self.control['lease_expires_at_us']
        digest = base.empty_digest(binary(intent['handoff']), binary(intent['target_manifest']), binary(self.slot), 3)
        only(self.pg['producer_epochs'], producer_slot_id=self.slot, producer_epoch=3).update(
            state='retired', retired_at_us=at, closure_digest='\\x' + digest, xmin=xid)
        only(self.pg['source_streams'], owner_resource_id=self.resource, owner_epoch=3).update(
            state='closed', closed_at_us=at, last_complete_sequence=0, closure_digest='\\x' + digest, xmin=xid)
        only(self.pg['owner_epochs'], resource_id=self.resource, epoch=3).update(
            state='released', released_at_us=at, exclusion_method='storage_fenced', exclusion_digest='\\x' + digest, xmin=xid)
        only(self.pg['role_instances'], role_instance_id=intent['target_role_instance_id']).update(retired_at_us=at, xmin=xid)
        domains = [r for r in self.pg['allocation_domains'] if r['owner_epoch'] == 3]
        assert len(domains) == 9
        for row in domains:
            row.update(state='closed', closed_at_us=at, closure_digest='\\x' + digest, xmin=xid)
        self.pg['ingest_terminal_transactions'].append(dict(owner_resource_id=self.resource, target_epoch=3,
            terminal_transaction=receipt['terminal_transaction'], control_resource=self.control['resource_id'],
            control_epoch=self.control['epoch'], control_incarnation=self.control['process_incarnation'], xmin=xid))
        same(self.pg, observed)
        # The preceding failed process already wrote CLOSING. This commit must
        # preserve the complete native map and sequence, not write it again.
        key = 'receipts', (b'\x01\x31' + binary(self.slot) + be(3)).hex()
        assert base.native_map(self.native)[key] == (b'\x02\x31' + bytes(24) + b'\x01').hex()


def lineage(before, native, index):
    if index in (0, 1, 2, 4):
        known = base.lineage(before, native, 'activated')
    else:
        # SQL remains prepared while the actual native marker is CLOSING. Build
        # that head directly; do not relabel it as a terminal proof through3.
        intent = only(before['initial_ingest_successions'])
        incoming = only(before['ingest_successions'], target_epoch=3)
        known = dict(slot=intent['producer_slot_id'][2:], head=3, through=2,
            state='PreparedClosing', counts=[4, 2], summaries=[
                dict(epoch=1, terminal_kind='initial', resolved_requests=4, last_complete_sequence=12,
                    digest=history.CLOSURES[0], event_request_mask=8),
                dict(epoch=2, terminal_kind='activated', resolved_requests=2, last_complete_sequence=5,
                    digest=history.CLOSURES[1], event_request_mask=2)], entries=[])
        for epoch, manifest, digest, count, cut, kind, closure in (
            (1, intent['previous_manifest'], '00' * 32, 4, 12, 'initial', history.CLOSURES[0]),
            (2, intent['target_manifest'], sha(binary(intent['handoff'])), 2, 5, 'activated', history.CLOSURES[1]),
            (3, incoming['target_manifest'], sha(binary(incoming['handoff'])), 0, 0, None, None),
        ):
            known['entries'].append(dict(manifest=manifest[2:], issued_through=count, source_through=cut,
                closing=True, incoming_sha256=digest, terminal_kind=kind, terminal_digest=closure))
        values = base.native_map(native)
        assert values['default', '0101'] == incoming['target_manifest'][2:]
        assert values['receipts', (b'\x01\x31' + binary(intent['producer_slot_id']) + be(3)).hex()] == (b'\x02\x31' + bytes(24) + b'\x01').hex()
    known.update(registered=3 if index else 2, pending=3 if index == 1 else None)
    if index == 1:
        known['state'] = 'PendingNative'
    assert known['state'] == ('Terminal', 'PendingNative', 'PreparedOpen', 'PreparedClosing', 'Terminal')[index]
    assert known['head'] == (2 if index < 2 else 3)
    assert known['through'] == (3 if index == 4 else 2)
    return known


class Transcript(base.Transcript):
    def history(self, known):
        assert self.take('loaded') == dict(event='lineage-history-loaded', state=known['state'],
            native_epoch=known['head'], registered_epoch=known['registered'], replay_through=known['through'],
            producer_slot=known['slot'], pending_target=known['pending'],
            metadata_bytes=131072 + 512 * known['registered'],
            memberships=known['registered'] * (known['registered'] + 1) // 2, entries=known['entries'])
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


def verify(events, before, native, inputs, case, server_window, *, fault=None):
    """Later baselines are accepted only with replay's complete previous link."""
    index, operation, failed = specification(case)
    wire_checked(fault, index, case['proxy_port'])
    if index == 0:
        history.baseline(before, native, inputs, 'activated', server_window)
        history.science(before, native, inputs, 'activated')
    assert len(native['rows']) == (70 if index < 2 else 73)
    assert native['sequence'] == (85, 85, 89, 90, 90)[index]
    assert len(inputs) == 6 and sha(encoded(inputs)) == history.PINS['activated']['inputs']
    m = Model(before, native, inputs, server_window)
    known = lineage(before, native, index)
    t = Transcript(events, m)
    candidate = None
    if operation == 'prepare':
        candidate = t.take('candidate', True)
        assert set(candidate) == {'event', 'role', 'incarnation', 'namespaces'}
        assert [r['kind'] for r in candidate['namespaces']] == list(range(1, 10))
        assert all(set(r) == {'kind', 'id'} and type(r['kind']) is int for r in candidate['namespaces'])
        ids = [candidate['role'], candidate['incarnation']] + [r['id'] for r in candidate['namespaces']]
        assert len(set(ids)) == 11
        assert all(re.fullmatch('[0-9a-f]{32}', value) and int(value, 16) and value not in m.identities for value in ids)
    resource = t.take('resources')
    assert set(resource) == {'event', 'metadata_bytes', 'metadata_scratch_bytes', 'metadata_entry_bytes',
        'input_bytes', 'outcome_bytes', 'effect_bytes', 'native_bytes', 'transport_bytes',
        'observation_bytes', 'operation_timeout_ms', 'frame_bytes'}
    assert resource == dict(resource, metadata_bytes=262144, metadata_scratch_bytes=131072,
        metadata_entry_bytes=512, native_bytes=128 * 1024**2, transport_bytes=4 * 1024**2,
        observation_bytes=32 * 1024**2, operation_timeout_ms=10000, frame_bytes=16 * 1024**2)
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resource[field]) is int and 0 < resource[field] <= 256 * 1024**2
    mutation = {'prepare': 8192, 'gap': 8192, 'empty': 4096, 'empty-retry': 0}[operation]
    assert sum(resource[k] for k in ('metadata_bytes', 'input_bytes', 'outcome_bytes', 'effect_bytes',
        'transport_bytes', 'observation_bytes')) + mutation <= 1024**3
    assert t.take('resources', True) == dict(event='lineage-transition-resources', operation=operation,
        mutation_data_bytes=mutation, mutation_native_bytes=mutation, lease_ttl_us='20000000', outstanding_request_limit=32)
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    if candidate:
        assert not ({planned['role'], planned['incarnation']} & set(ids))
    t.snapshot('before-claim')
    if failed:
        transport = t.take('transport', True)
        assert transport['proxied'] is True
        assert transport == dict(event='lineage-transition-transport',
            control_port=case['proxy_port'], observer_port=5432, proxied=True)
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    assert claimed == dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation'])
    assert backend == dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'], control_epoch=m.control['epoch'], present=True)
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-read')
    t.history(known)
    event = t.take('plan', True)
    observation = None
    if index == 0:
        created, deadline = event['created_at_us'], event['lease_expires_at_us']
        assert re.fullmatch('[1-9][0-9]*', created) and re.fullmatch('[1-9][0-9]*', deadline)
        plan = m.plan(candidate['role'], candidate['incarnation'], [r['id'] for r in candidate['namespaces']],
            3, int(created), int(deadline))
        assert event == dict(event='lineage-transition-plan', operation=operation, record=base.record_value(plan['record'], 3),
            role=candidate['role'], created_at_us=created, lease_expires_at_us=deadline, outstanding_request_limit=32,
            policy=dict(config_revision=m.ds['config_revision'], policy_revision=m.ds['policy_revision'],
                config_digest=only(m.pg['config_revisions'], revision=m.ds['config_revision'])['document_digest'][2:],
                policy_digest=only(m.pg['policy_revisions'], revision=m.ds['policy_revision'])['document_digest'][2:]))
        t.snapshot('planned')
        t.snapshot('transition-returned', lambda pg: m.prepare_sql(pg, plan))
    else:
        intent = only(m.pg['ingest_successions'], target_epoch=3)
        record = base.record_value(binary(intent['handoff']), 3)
        digest = base.empty_digest(binary(intent['handoff']), binary(intent['target_manifest']), binary(m.slot), 3)
        if index == 1:
            assert event['candidate'] is False
            assert event == dict(event='lineage-transition-plan', operation='gap', record=record, candidate=False)
            t.snapshot('planned')
            t.snapshot('transition-returned', m.complete_gap)
            observed = t.take('observed', True)
            assert observed['candidate'] is False
            assert observed == dict(event='lineage-transition-observed', operation='gap',
                record=record, candidate=False, activation=False)
            observation = dict(prepared=False, target_epoch=3, retired_at_us=None)
        elif index in (2, 3):
            assert event == dict(event='lineage-transition-plan', operation='empty', record=record, closure_digest=digest)
            t.snapshot('planned')
            t.snapshot('transition-returned', m.native_close if index == 2 else m.retire_sql)
            if index == 3:
                at = str(only(m.pg['producer_epochs'], producer_epoch=3)['retired_at_us'])
                assert t.take('observed', True) == dict(event='lineage-transition-observed', operation='empty',
                    target_epoch=3, retired_at_us=at, closure_digest=digest)
                observation = dict(prepared=False, target_epoch=3, retired_at_us=at)
        else:
            assert event == dict(event='lineage-transition-plan', operation='empty-retry', target_epoch=3,
                closure_digest=digest, read_only=True)
            t.snapshot('planned')
            t.snapshot('transition-returned')
            at = str(only(m.pg['producer_epochs'], producer_epoch=3)['retired_at_us'])
            assert t.take('observed', True) == dict(event='lineage-transition-observed', operation='empty-retry',
                target_epoch=3, retired_at_us=at, closure_digest=digest, read_only=True)
            observation = dict(prepared=False, target_epoch=3, retired_at_us=at)
    assert t.take('result', True) == dict(event='lineage-transition-result', operation=operation,
        stage=operation if failed else 'complete', epoch=None, sequence=None,
        error='CommitUnknown' if failed else None, observation=observation)
    t.snapshot('after-transition')
    assert t.take('native') == dict(event='lineage-history-native', state='closed')
    t.snapshot('before-control-end')
    assert t.take('driver') == dict(event='lineage-history-driver', driver='control', failed=failed, joined=True)
    assert t.take('backend') == dict(backend, present=False)
    phase = 'dropped-and-joined' if failed else 'released-and-joined'
    assert t.take('control') == dict(event='lineage-history-control', phase=phase,
        resource=m.control['resource_id'][2:], epoch=m.control['epoch'])
    t.snapshot('after-control-end', None if failed else m.release)
    t.snapshot('final')
    assert t.take('driver') == dict(event='lineage-history-driver', driver='observer', failed=False, joined=True)
    assert t.take('complete', True) == dict(event='lineage-transition-complete', operation=operation,
        native='closed', control=phase, observer='joined', success=not failed, admission=False, activation=False)
    assert t.position == len(events)
    assert t.snapshots == (30 if index == 4 else 28)
    assert m.refreshes == (10 if index == 4 else 9)
    return dict(status='PASS', case=case['name'], database=case['database'], snapshots=t.snapshots, tables=75,
        native_rows=len(m.native['rows']), sequence=m.native['sequence'], inputs=len(inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, caller_unknown=failed,
        persistent_outcome=('prepared-native-old', 'prepared-native-complete', 'native-closing-SQL-open',
            'empty-terminal', 'unchanged-empty-terminal')[index],
        scope='Finite interrupted preparation/gap/empty recovery; no candidate adoption, activation, admission, installed or RSS claim')


def negative_controls(events, before, native, inputs, case, server_window, *, fault=None):
    verify(events, before, native, inputs, case, server_window, fault=fault)
    rejected = []
    index, _, failed = specification(case)

    def reject(name, change):
        context = copy.deepcopy(dict(events=events, before=before, native=native,
            inputs=inputs, case=case, server_window=server_window, fault=fault))
        change(context)
        try:
            verify(**context)
        except (AssertionError, KeyError, ValueError, TypeError, OverflowError):
            rejected.append(name)
        else:
            raise AssertionError('accepted altered evidence: ' + name)

    def event(c, name, transition=False, **fields):
        return only(c['events'], event=('lineage-transition-' if transition else 'lineage-history-') + name, **fields)

    def sql(c, change):
        for e in c['events']:
            if e['event'] == 'lineage-history-state':
                pg = {name: loads(rows) for name, rows in e['state']['control_tables']}
                change(pg, e['stage'])
                tables = [[name, encoded(rows).decode()] for name, rows in sorted(pg.items())]
                e['state'].update(control_tables=tables, control_sha256=sha(encoded(tables)))

    def native_sequence(c):
        for e in c['events']:
            if e['event'] == 'lineage-history-state':
                e['state']['sequence'] += 1

    def old_digest(c):
        for e in c['events']:
            if e['event'] == 'lineage-history-epoch' and e['summary']['epoch'] == 2:
                e['summary']['digest'] = '00' * 32
            if e['event'] == 'lineage-history-proof' and e['last_epoch']['epoch'] == 2:
                e['last_epoch']['digest'] = '00' * 32

    reject('retained-row-xmin-rehashed', lambda c: sql(c, lambda pg, _: only(pg['producer_epochs'], producer_epoch=1).update(xmin='4000000000')))
    reject('coherent-old-closure-claim', old_digest)
    reject('all-native-sequences-shifted', native_sequence)
    reject('missing-request-observation', lambda c: c['events'].pop(next(i for i, e in enumerate(c['events']) if e['event'] == 'lineage-history-request')))
    reject('original-input-digest', lambda c: event(c, 'state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    reject('metadata-grant', lambda c: event(c, 'resources').update(metadata_bytes=132096))
    reject('control-join-missing', lambda c: event(c, 'driver', driver='control').update(joined=False))
    reject('observer-join-missing', lambda c: event(c, 'driver', driver='observer').update(joined=False))
    reject('driver-result-fabricated', lambda c: event(c, 'driver', driver='control').update(failed=not failed))
    reject('writer-activation-claim', lambda c: event(c, 'complete', True).update(activation=True))
    reject('boolean-integer', lambda c: event(c, 'proof').update(matches_history=1))
    reject('historical-topology-head', lambda c: sql(c, lambda pg, _: only(pg['topology_generations'], generation=1).update(sealed=False)))
    if failed:
        reject('caller-unknown-relabelled', lambda c: event(c, 'result', True).update(error='DeliveryUnknown'))
        reject('fault-port-unbound', lambda c: event(c, 'transport', True).update(control_port=5432))
        reject('proxy-forced-stop', lambda c: c['fault']['wire'].update(forced_wrapper_stop=True))
        reject('proxy-stdout-unbound', lambda c: c['fault']['wire'].update(stdout=c['fault']['wire']['stdout'] + '\n'))
        def counters(c):
            wire = c['fault']['wire']
            for e in wire['events'][1:]:
                e['server_commit_complete'] -= 1
            wire['fault'], wire['receipt'] = wire['events'][1:]
            raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in wire['events']).encode()
            wire.update(stdout=raw.decode(), stdout_sha256=sha(raw), stdout_bytes=len(raw))
        reject('rehash-wrong-server-commit-boundary', counters)
    if index == 0:
        def parent(c):
            record = event(c, 'plan', True)['record']
            raw = bytearray.fromhex(record['bytes'])
            raw[75] ^= 1
            record.update(bytes=raw.hex(), sha256=sha(raw))
        reject('rehashed-parent-receipt', parent)
        reject('native-dispatch-after-unknown', lambda c: event(c, 'state', stage='transition-returned')['state'].update(sequence=89))
    if index in (1, 3):
        previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
        def split(c):
            def mutate(pg, stage):
                if stage != 'before-claim':
                    only(pg['owner_epochs'], role='control', epoch=previous['epoch']).update(xmin='3999999999')
                    only(pg['role_instances'], role_instance_id=previous['role_instance_id']).update(xmin='3999999999')
            sql(c, mutate)
        reject('fence-and-claim-split-transaction', split)
        def alias(c):
            def mutate(pg, stage):
                if stage != 'before-claim':
                    only(pg['owner_epochs'], role='control', epoch=previous['epoch'] + 1).update(
                        control_backend_pid=previous['control_backend_pid'], control_backend_start=previous['control_backend_start'])
            sql(c, mutate)
            for e in c['events']:
                if e['event'] == 'lineage-history-backend':
                    e.update(pid=previous['control_backend_pid'], backend_start=previous['control_backend_start'])
        reject('coherent-old-physical-backend-reuse', alias)
    if index == 1:
        reject('gap-candidate-adoption', lambda c: event(c, 'observed', True).update(candidate=True))
        reject('gap-capacity-omits-pending', lambda c: event(c, 'loaded').update(metadata_bytes=132096, memberships=3))
    if index == 3:
        reject('closing-is-terminal-relabel', lambda c: event(c, 'loaded').update(state='Terminal', replay_through=3))
        reject('repeated-native-closing-write', lambda c: event(c, 'state', stage='transition-returned')['state'].update(sequence=91))
    if index == 4:
        def time(c):
            item = event(c, 'observed', True)
            item['retired_at_us'] = str(int(item['retired_at_us']) + 1)
            event(c, 'result', True)['observation']['retired_at_us'] = item['retired_at_us']
        reject('coherent-retry-retirement-time', time)
        reject('retry-skips-head-proof', lambda c: event(c, 'proof').update(verified_through=2))
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def replay(root):
    root = Path(root).resolve()
    folder, bound = root / 'transition-faults', {}

    def raw(path):
        path = Path(path)
        value = path.read_bytes()
        bound[str(path)] = sha(value)
        return value

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_TRANSITION_COMMIT_LOSS'
    assert [c['name'] for c in report['cases']] == list(CASES)
    assert [c['name'] for c in report['clones']] == ['activated']
    for field in ('cleanup_errors', 'missing_owned', 'unrelated_changed', 'unexpected_containers'):
        assert report[field] == []
    assert sha(raw(folder / 'successor-transition-fault-proxy.py')) == PROXY_SHA256
    origins = {}
    for origin, pin in history.PINS.items():
        declaration = report['origins'][origin]
        for field in ('run', 'database', 'snapshot', 'snapshot_sha256', 'result_sha256', 'status'):
            assert declaration[field] == pin[field]
        basis = root.parent / pin['run'] / 'bindings'
        retained = read(basis / 'result.json')
        assert bound[str(basis / 'result.json')] == pin['result_sha256'] and retained['status'] == pin['status']
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
    clone, = report['clones']
    assert clone['origin'] == clone['name'] == clone['directory'] == 'activated'
    assert clone['status'] == 'PASS' and clone['final_case'] == CASES[-1]
    assert clone['config'] == '/lab/' + clone['database'] + '.toml'
    assert report['databases'] == [clone['database']]
    config = tomllib.loads(raw(folder / Path(clone['config']).name).decode())
    ds = only(origins['activated'][0]['datasets'])
    assert config['postgres']['control']['database'] == clone['database']
    assert config['postgres']['control']['endpoint'] == '127.0.0.1:5432'
    assert config['postgres']['control']['user'] == 'bigbrotr_control'
    assert config['postgres']['control']['password_ref'] == 'file:/run/secrets/application'
    assert config['storage']['ingest_root'] == '/lab/activated/store'
    assert config['topology']['dataset_id'] == ds['dataset_id'][2:]
    assert config['topology']['instance_id'] == ds['instance_id'][2:]
    assert config['runtime']['lease_ttl'] == '20s'
    validation = read(folder / 'activated-config-validation.json')
    assert validation['returncode'] == 0 and validation['stderr'] == ''
    assert [a['action'] for a in clone['migrations']] == ['plan', 'apply', 'apply']
    step = dict(name='0038_' + history.MIGRATIONS[38][0] + '.sql', sha256=history.MIGRATIONS[38][1], version='38')
    for index, answer in enumerate(clone['migrations']):
        assert set(answer) == {'action', 'exit_code', 'stdout', 'stderr'}
        assert type(answer['exit_code']) is int and answer['exit_code'] == 0 and answer['stderr'] == ''
        assert loads(answer['stdout']) == dict(format='bigbrotr-control-migration/1', scope='postgres-control',
            target_version='38', source_version='37' if index < 2 else '38',
            current_version='37' if index == 0 else '38', status=('planned', 'applied', 'current')[index],
            applied_count='1' if index == 1 else '0', steps=[step] if index < 2 else [],
            dataset_initialization='not-performed', release_upgrade_qualification='not-performed')
    grants = clone['grants']
    assert len(grants) == 2 and all(set(g) == {'statement', 'stdout'} for g in grants)
    assert sha(grants[0]['statement'].encode()) == '36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
    assert grants[1]['statement'] == 'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;'
    assert all(not g['stdout'].strip() for g in grants)
    assert isinstance(clone['catalog_before'], str) and clone['catalog_before']
    assert clone['catalog_after'] == clone['final_catalog'] == clone['catalog_before']
    assert clone['final_backends'] == []
    same(origins['activated'][0], read(folder / 'activated-copy-before.json'))
    expected = (read(folder / 'activated-migrated.json'), *origins['activated'][1:])
    last_window = 0
    checked = []
    for case in report['cases']:
        index, _, _ = specification(case)
        assert case['database'] == clone['database'] and case['config'] == clone['config']
        assert case['status'] == 'PASS' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert type(case['returncode']) is int and case['returncode'] == 0
        assert case['catalog_after'] == clone['catalog_before']
        def case_read(suffix):
            return read(folder / (case['name'] + '-' + suffix + '.json'))
        before, native, inputs = case_read('before'), case_read('before-native'), case_read('inputs')
        same(expected[0], before)
        assert native == expected[1] and inputs == expected[2], 'process boundary state changed'
        response = case_read('response')
        assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
        assert not any(k in response for k in ('error', 'orchestration_error', 'cleanup_error', 'clock_error'))
        assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds'])
        assert 0 <= response['seconds'] <= 120
        assert case['response_bytes'] == len(response['stdout'].encode())
        events = [loads(line) for line in response['stdout'].splitlines()]
        window, fault = case_read('clock'), case_read('fault')
        assert window == case['server_window'] and window['before_us'] >= last_window
        assert fault == case['fault']
        if index in (0, 2):
            assert case_read('wire') == fault['wire']
        result = verify(events, before, native, inputs, case, window, fault=fault)
        final = only(events, event='lineage-history-state', stage='final')
        after, after_native, after_inputs = case_read('after'), case_read('after-native'), case_read('after-inputs')
        same({name: loads(value) for name, value in final['state']['control_tables']}, after)
        assert after_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
        assert after_inputs == inputs and case_read('backends') == []
        assert case_read('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED', database=case['database'], backend_absent=True, errors=[])
        result['negative_controls'] = negative_controls(events, before, native, inputs, case, window, fault=fault)
        checked.append(result)
        expected, last_window = (after, after_native, after_inputs), window['after_us']
    same(expected[0], read(folder / 'activated-final-sql.json'))
    return dict(status='PASS_SUCCESSOR_TRANSITION_COMMIT_LOSS_REPLAY', source=report['source'], cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        bound_files=bound, original_preservation='Pinned bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Five finite processes, two actual wire boundaries; no installed, activation, candidate adoption, admission, cancellation or RSS claim')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = replay(args.root)
    rendered = json.dumps(result, indent=2) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(rendered)
    else:
        print(rendered, end='')
