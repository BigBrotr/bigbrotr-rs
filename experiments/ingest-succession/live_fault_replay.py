#!/usr/bin/env python3
"""Independent complete-state reader for activation and outcome COMMIT loss.

A missing COMMIT reply is caller uncertainty. The actual wire boundary and all
75 SQL tables distinguish persistence; the native bytes distinguish dispatch.
The fresh process retires only the unactivated empty head. Activated history
currently refuses, preserving every receipt and original: this is not repair.
Literal science and history reducers are reused without transcript projection.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import tomllib

import live_replay as live
import transition_fault_replay as interrupted

base, history = live.transition, live.history
encoded, sha, binary, be = live.encoded, live.sha, live.binary, live.be
only, same, loads, exact = live.only, live.same, live.loads, live.exact
CLONES = ('activation-before', 'activation-after', 'outcome-before', 'outcome-after')
CASES = tuple(clone + '-' + step for clone in CLONES for step in ('loss', 'fresh'))
PROXY_SHA256, COUNTERS = interrupted.PROXY_SHA256, interrupted.COUNTERS


def specification(case):
    index = CASES.index(case['name'])
    clone = CLONES[index // 2]
    loss = index % 2 == 0
    operation = 'lineage-live-new' if loss else 'empty' if index == 1 else 'lineage-history'
    assert case['clone'] == clone and case['origin'] == 'activated'
    assert case['operation'] == operation and case['metadata_bytes'] == 262144
    assert case['predecessor'] == (None if loss else CASES[index - 1])
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return index, loss, operation, clone.startswith('outcome'), clone.endswith('after')


def wire_checked(fault, index, port):
    if index % 2:
        assert fault is None and port is None
        return
    point = 'activation' if index < 4 else 'outcome'
    count = 12 if point == 'activation' else 16
    mode = 'after-complete' if index in (2, 6) else 'before-forward'
    assert set(fault) == {'point', 'mode', 'commit_number', 'proxy_port', 'wire'}
    assert fault['point'] == point and fault['mode'] == mode and type(fault['commit_number']) is int and fault['commit_number'] == count
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
    vector = [count, count, count, count - 1, 1] if mode == 'after-complete' else [count, count - 1, count - 1, count - 1, 1]
    elapsed = 0
    for number, event in enumerate(events):
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1'
        assert event['mode'] == mode and type(event['commit_number']) is int and event['commit_number'] == count
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
    assert receipt['tags']['frontend']['51'] >= count
    assert receipt['tags']['backend']['43'] >= receipt['server_commit_complete']
    assert type(receipt['declared_wire_bytes']) is int and 0 < receipt['declared_wire_bytes'] <= ready['max_wire_bytes']
    raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in events).encode()
    assert wire['stdout'].encode() == raw
    assert wire['stdout_sha256'] == sha(raw) and wire['stdout_bytes'] == len(raw)


class Model(interrupted.Model, live.Model):
    """Reuse exact acquisition/fencing, preparation and activation constructors."""
    def reserve_only(self, observed, science):
        c, epoch = self.control, science.epoch
        request = only(observed['producer_requests'], producer_epoch=epoch, request_sequence=1)
        activation = only(self.pg['ingest_activations'], target_epoch=epoch)
        reserved = self.at(request['reserved_at_us'], activation['activated_at_us'])
        xid = self.fresh(request['xmin'])
        target = only(self.pg['owner_epochs'], resource_id=self.resource, epoch=epoch)
        assert reserved < min(c['lease_expires_at_us'], target['lease_expires_at_us'])
        raw = science.inputs[1]
        self.pg['producer_requests'].append(dict(producer_slot_id=self.slot, producer_epoch=epoch,
            request_sequence=1, dataset_id=self.ds['dataset_id'], request_digest='\\x' + sha(raw),
            manifest_sha256='\\x' + sha(science.manifest), input_bytes=len(raw), input_profile=1,
            issuer_resource=c['resource_id'], issuer_epoch=c['epoch'], issuer_role='control',
            issuer_incarnation=c['process_incarnation'], reserved_at_us=reserved,
            relay_endpoint_id=None, relay_access_scope_id=None, relay_enrollment_generation=None,
            relay_route_id=None, acquisition_scope_id=None, acquisition_job_id=None, xmin=xid))
        producer = only(self.pg['producer_epochs'], producer_slot_id=self.slot, producer_epoch=epoch)
        assert producer['reserved_through_request'] == 0
        producer.update(reserved_through_request=1, xmin=xid)
        same(self.pg, observed)


def resources(t, loss, operation):
    extra = 0
    if loss:
        value = t.live('resources')
        exact(value, dict(event='lineage-live-resources', prepared_input_bytes=1024**2,
            operation_data_bytes=value['operation_data_bytes'], operation_native_bytes=value['operation_native_bytes'],
            max_input_bytes=8192, max_event_bytes=256 * 1024, max_dependencies=256,
            outstanding_request_limit=32, provenance=False))
        for field in ('operation_data_bytes', 'operation_native_bytes'):
            assert type(value[field]) is int and 0 < value[field] <= 256 * 1024**2
        extra = value['prepared_input_bytes'] + value['operation_data_bytes']
    value = t.take('resources')
    exact(value, dict(event='lineage-history-resources', metadata_bytes=262144,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2, operation_timeout_ms=10000,
        frame_bytes=16 * 1024**2, **{k: value[k] for k in ('input_bytes', 'outcome_bytes', 'effect_bytes')}))
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(value[field]) is int and 0 < value[field] <= 256 * 1024**2
    mutation = 8192 if loss else 4096 if operation == 'empty' else 0
    assert sum(value[k] for k in ('metadata_bytes', 'input_bytes', 'outcome_bytes', 'effect_bytes',
        'transport_bytes', 'observation_bytes')) + mutation + extra <= 1024**3
    if operation != 'lineage-history':
        exact(t.take('resources', True), dict(event='lineage-transition-resources',
            operation='live-new' if loss else operation, mutation_data_bytes=mutation,
            mutation_native_bytes=mutation, lease_ttl_us='20000000', outstanding_request_limit=32))


def preparation(t, m, candidate):
    event = t.take('plan', True)
    created, deadline = event['created_at_us'], event['lease_expires_at_us']
    assert re.fullmatch('[1-9][0-9]*', created) and re.fullmatch('[1-9][0-9]*', deadline)
    plan = m.plan(candidate['role'], candidate['incarnation'], [r['id'] for r in candidate['namespaces']],
                  3, int(created), int(deadline))
    record = base.record_value(plan['record'], 3)
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
    return plan


def dictionary(t, m, plan, inputs, after):
    fixture = t.live('fixture')
    corpus = json.dumps([live.OLD_EVENT, live.NEW_EVENT], separators=(',', ':'), ensure_ascii=False).encode()
    science = live.Science(m, plan, corpus)
    old_name = m.slot[2:] + '-0000000000000001-0000000000000004.cbor'
    original = bytes.fromhex(fixture['original_bytes'])
    assert dict(bytes=len(original), sha256=sha(original)) == inputs[old_name]
    assert sha(original) == '19820e2cea792c8780de0d8542baa76dabbdb611ab96237eed2230a228d82977'
    exact(fixture, dict(event='lineage-live-fixture', original_name=old_name,
        original_bytes=original.hex(), original_sha256=sha(original), scope=live.ref_json(science.scope),
        lineage=live.ref_json(science.lineage), artifact_bytes=corpus.hex(), artifact_sha256=sha(corpus),
        old_event=live.OLD_EVENT, new_event=live.NEW_EVENT, provenance=False, installed_import=False))
    raw = science.request(1)
    exact(t.live('input'), dict(event='lineage-live-input', stage='dictionary', request=live.request_json(science, 1),
        preimage=raw.hex(), request_digest=sha(raw), recorded_at_us='900000001', signed_event=None, provenance=False))
    t.snapshot('before-dictionary')
    # Native durability precedes the lost SQL outcome COMMIT in both cases.
    # Neither the probe's receipt nor an observed SQL copy supplies these bytes.
    science.advance(1)
    t.snapshot('after-dictionary', lambda pg: m.admit(pg, science, 1) if after else m.reserve_only(pg, science))


def verify(events, before, native, inputs, case, server_window, *, fault=None):
    """Fresh baselines require replay's exact complete link to the loss process."""
    index, loss, operation, outcome, after = specification(case)
    wire_checked(fault, index, case['proxy_port'])
    if loss:
        history.baseline(before, native, inputs, 'activated', server_window)
        history.science(before, native, inputs, 'activated')
    expected_native = (70, 85, 6) if loss else (79, 97, 7) if outcome else (73, 89, 6)
    assert (len(native['rows']), native['sequence'], len(inputs)) == expected_native
    m = Model(before, native, inputs, server_window)
    t = live.Transcript(events, m)
    candidate = None
    if loss:
        candidate = t.take('candidate', True)
        assert set(candidate) == {'event', 'role', 'incarnation', 'namespaces'}
        exact([r['kind'] for r in candidate['namespaces']], list(range(1, 10)))
        assert all(set(r) == {'kind', 'id'} for r in candidate['namespaces'])
        ids = [candidate['role'], candidate['incarnation']] + [r['id'] for r in candidate['namespaces']]
        assert len(set(ids)) == 11
        assert all(re.fullmatch('[0-9a-f]{32}', v) and int(v, 16) and v not in m.identities for v in ids)
    resources(t, loss, operation)
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    if candidate:
        assert not ({planned['role'], planned['incarnation']} & set(ids))
    t.snapshot('before-claim')
    if loss:
        exact(t.take('transport', True), dict(event='lineage-transition-transport',
            control_port=case['proxy_port'], observer_port=5432, proxied=True))
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    exact(claimed, dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation']))
    exact(backend, dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'],
        control_epoch=m.control['epoch'], present=True))
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-read')
    if operation == 'lineage-history':
        assert only(m.pg['ingest_activations'], target_epoch=3)
        exact(t.take('result'), dict(event='lineage-history-result', stage='inspect',
            epoch=None, sequence=None, error='Control(InvalidMetadata)'))
        t.snapshot('after-read')
    else:
        known = base.lineage(before, native, 'activated')
        assert known['head'] == (2 if loss else 3) and known['through'] == 2
        assert known['state'] == ('Terminal' if loss else 'PreparedOpen')
        t.history(known)
        if loss:
            plan = preparation(t, m, candidate)
            exact(t.live('activation-start'), dict(event='lineage-live-activation-start', target_epoch=3,
                control_epoch=m.control['epoch']))
            activation = t.live('activation-result')
            if not outcome:
                exact(activation, dict(event='lineage-live-activation-result', writer=False,
                    target_epoch=3, error='CommitUnknown'))
                t.snapshot('activation-returned', (lambda pg: m.activate(pg, plan)) if after else None)
            else:
                t.snapshot('activated-zero', lambda pg: m.activate(pg, plan))
                exact(activation, dict(event='lineage-live-activation-result', writer=True, target_epoch=3,
                    activated_at_us=str(only(m.pg['ingest_activations'], target_epoch=3)['activated_at_us']),
                    manifest=plan['manifest'].hex(), role=candidate['role'], producer_slot=m.slot[2:], error=None))
                t.policy(t.take('policy'), 'live-refreshed', 3, None)
                t.snapshot('live-control-refreshed', m.refresh)
                exact(t.live('renewal'), dict(event='lineage-live-renewal', commit_started=True, error=None))
                t.snapshot('live-renew-returned', lambda pg: m.renew_ingest(pg, 3))
                dictionary(t, m, plan, inputs, after)
            exact(t.live('result'), dict(event='lineage-live-result', operation='live-new',
                stage='dictionary' if outcome else 'activate', epoch=3, sequence=1 if outcome else None,
                error='Outcome(CommitUnknown)' if outcome else 'CommitUnknown', observation=None))
        else:
            intent = only(m.pg['ingest_successions'], target_epoch=3)
            digest = base.empty_digest(binary(intent['handoff']), binary(intent['target_manifest']), binary(m.slot), 3)
            exact(t.take('plan', True), dict(event='lineage-transition-plan', operation='empty',
                record=base.record_value(binary(intent['handoff']), 3), closure_digest=digest))
            t.snapshot('planned')
            t.snapshot('transition-returned', lambda pg: m.retire(pg, 3))
            at = str(only(m.pg['producer_epochs'], producer_epoch=3)['retired_at_us'])
            exact(t.take('observed', True), dict(event='lineage-transition-observed', operation='empty',
                target_epoch=3, retired_at_us=at, closure_digest=digest))
            exact(t.take('result', True), dict(event='lineage-transition-result', operation='empty',
                stage='complete', epoch=None, sequence=None, error=None,
                observation=dict(prepared=False, target_epoch=3, retired_at_us=at)))
        t.snapshot('after-transition')
    exact(t.take('native'), dict(event='lineage-history-native', state='closed'))
    t.snapshot('before-control-end')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='control', failed=loss, joined=True))
    exact(t.take('backend'), dict(backend, present=False))
    phase = 'released-and-joined' if operation == 'empty' else 'dropped-and-joined'
    exact(t.take('control'), dict(event='lineage-history-control', phase=phase,
        resource=m.control['resource_id'][2:], epoch=m.control['epoch']))
    t.snapshot('after-control-end', m.release if operation == 'empty' else None)
    t.snapshot('final')
    exact(t.take('driver'), dict(event='lineage-history-driver', driver='observer', failed=False, joined=True))
    if loss:
        exact(t.live('complete'), dict(event='lineage-live-complete', operation='live-new', native='closed',
            control=phase, observer='joined', success=False, writer=False, activation=outcome, admission=None))
    elif operation == 'empty':
        exact(t.take('complete', True), dict(event='lineage-transition-complete', operation='empty', native='closed',
            control=phase, observer='joined', success=True, admission=False, activation=False))
    else:
        exact(t.take('complete'), dict(event='lineage-history-complete', native='closed', control=phase,
            observer='joined', success=False, admission=False, repair=False))
    assert t.position == len(events), 'unclassified trailing events'
    assert t.snapshots == ((33 if outcome else 29) if loss else (28 if index == 1 else 7))
    assert m.refreshes == ((10 if outcome else 9) if loss else (9 if index == 1 else 0))
    result_shape = (79, 97, 7) if outcome else (73, 90 if index == 1 else 89, 6)
    assert (len(m.native['rows']), m.native['sequence'], len(m.inputs)) == result_shape
    return dict(status='PASS', case=case['name'], database=case['database'], snapshots=t.snapshots, tables=75,
        native_rows=len(m.native['rows']), sequence=m.native['sequence'], inputs=len(m.inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, caller_unknown=loss,
        sql_activation=index >= 2, local_writer_delivered=loss and outcome,
        sql_outcome=index >= 6, repair=False, empty_retired=index == 1,
        scope='Finite COMMIT loss and fresh empty retirement or explicit activated-history refusal; no positive activated recovery, installed or RSS claim')


def negative_controls(events, before, native, inputs, case, server_window, *, fault=None):
    verify(events, before, native, inputs, case, server_window, fault=fault)
    index, loss, operation, outcome, after = specification(case)
    rejected = []

    def reject(name, change):
        context = copy.deepcopy(dict(events=events, before=before, native=native, inputs=inputs,
            case=case, server_window=server_window, fault=fault))
        change(context)
        try:
            verify(**context)
        except (AssertionError, KeyError, ValueError, TypeError, OverflowError):
            rejected.append(name)
        else:
            raise AssertionError('accepted altered evidence: ' + name)

    def event(c, kind, **fields):
        return only(c['events'], event=kind, **fields)

    def sql(c, change, start=None):
        enabled = start is None
        for e in c['events']:
            if e['event'] == 'lineage-history-state':
                enabled |= e['stage'] == start
                if enabled:
                    pg = {name: loads(rows) for name, rows in e['state']['control_tables']}
                    change(pg)
                    tables = [[name, encoded(rows).decode()] for name, rows in sorted(pg.items())]
                    e['state'].update(control_tables=tables, control_sha256=sha(encoded(tables)))

    reject('historical-row-xmin-rehashed', lambda c: sql(c,
        lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='4000000000')))
    reject('missing-control-driver-join', lambda c: event(c, 'lineage-history-driver', driver='control').update(joined=False))
    reject('observer-driver-failure', lambda c: event(c, 'lineage-history-driver', driver='observer').update(failed=True))
    reject('wrong-control-driver-result', lambda c: event(c, 'lineage-history-driver', driver='control').update(failed=not loss))
    reject('backend-still-present', lambda c: event(c, 'lineage-history-backend', present=False).update(present=True))
    reject('physical-database-relabel', lambda c: c['case'].update(database='other_db', config='/lab/other_db.toml'))
    reject('input-digest-rewritten', lambda c: event(c, 'lineage-history-state', stage='final')['state']['originals'][0][1].update(sha256='00' * 32))
    def sequence(c):
        for e in c['events']:
            if e['event'] == 'lineage-history-state':
                e['state']['sequence'] += 1
    reject('coherent-native-sequence-extra-put', sequence)
    reject('unexpected-event', lambda c: c['events'].append(dict(event='unclassified')))
    if loss:
        reject('caller-unknown-relabelled', lambda c: event(c, 'lineage-live-result').update(error='DeliveryUnknown'))
        reject('local-delivery-confused-with-durability', lambda c: event(c, 'lineage-live-complete').update(activation=not outcome))
        reject('writer-not-closed', lambda c: event(c, 'lineage-live-complete').update(writer=True))
        reject('boolean-as-integer', lambda c: event(c, 'lineage-live-complete').update(activation=int(outcome)))
        reject('proxy-port-unbound', lambda c: event(c, 'lineage-transition-transport').update(control_port=5432))
        reject('proxy-forced-stop', lambda c: c['fault']['wire'].update(forced_wrapper_stop=True))
        reject('proxy-stdout-unbound', lambda c: c['fault']['wire'].update(stdout=c['fault']['wire']['stdout'] + '\n'))
        def wrong_boundary(c):
            wire = c['fault']['wire']
            for e in wire['events'][1:]:
                e['server_commit_complete'] -= 1
            wire['fault'], wire['receipt'] = wire['events'][1:]
            raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in wire['events']).encode()
            wire.update(stdout=raw.decode(), stdout_sha256=sha(raw), stdout_bytes=len(raw))
        reject('coherent-wrong-COMMIT-vector', wrong_boundary)
        def parent(c):
            record = event(c, 'lineage-transition-plan')['record']
            raw = bytearray.fromhex(record['bytes'])
            raw[75] ^= 1
            record.update(bytes=raw.hex(), sha256=sha(raw))
        reject('rehashed-parent-receipt', parent)
        if outcome or after:
            stage = 'activated-zero' if outcome else 'activation-returned'
            reject('activation-missing-in-complete-tail', lambda c: sql(c, lambda pg: pg['ingest_activations'].clear(), stage))
            def activation_xid(pg):
                row = only(pg['ingest_activations'], target_epoch=3)
                intent = only(pg['ingest_successions'], target_epoch=3)
                row.update(xmin=intent['xmin'], activation_transaction=intent['preparation_transaction'])
            reject('activation-reuses-preparation-transaction', lambda c: sql(c, activation_xid, stage))
        if outcome:
            reject('dictionary-input-changed', lambda c: event(c, 'lineage-live-input').update(preimage='00', request_digest=sha(b'\0')))
            def reservation_xid(pg):
                activation = only(pg['ingest_activations'], target_epoch=3)
                only(pg['producer_requests'], producer_epoch=3, request_sequence=1)['xmin'] = activation['xmin']
                only(pg['producer_epochs'], producer_epoch=3)['xmin'] = activation['xmin']
            reject('reservation-reuses-activation-xid', lambda c: sql(c, reservation_xid, 'after-dictionary'))
            reject('reservation-lost-after-native-dispatch', lambda c: sql(c,
                lambda pg: pg['producer_requests'].__setitem__(slice(None), [r for r in pg['producer_requests'] if r['producer_epoch'] != 3]), 'after-dictionary'))
            if after:
                def bad_copy(pg):
                    raw = b'wrong literal receipt'
                    only(pg['producer_outcomes'], producer_epoch=3, request_sequence=1).update(
                        outcome_bytes=live.bx(raw), outcome_sha256='\\x' + sha(raw))
                reject('coherent-outcome-copy-byte-conflict', lambda c: sql(c, bad_copy, 'after-dictionary'))
        elif not after:
            def invented(pg):
                row = dict(only(pg['ingest_successions'], target_epoch=3))
                pg['ingest_activations'].append(row)
            reject('activation-invented-before-forward', lambda c: sql(c, invented, 'activation-returned'))
    else:
        old = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
        def split(pg):
            only(pg['owner_epochs'], role='control', epoch=old['epoch'])['xmin'] = '3999999999'
            only(pg['role_instances'], role_instance_id=old['role_instance_id'])['xmin'] = '3999999999'
        reject('fence-and-new-claim-split-transaction', lambda c: sql(c, split, 'after-claim'))
        def backend_alias(c):
            sql(c, lambda pg: only(pg['owner_epochs'], role='control', epoch=old['epoch'] + 1).update(
                control_backend_pid=old['control_backend_pid'], control_backend_start=old['control_backend_start']), 'after-claim')
            for e in c['events']:
                if e['event'] == 'lineage-history-backend':
                    e.update(pid=old['control_backend_pid'], backend_start=old['control_backend_start'])
        reject('coherent-reuse-of-excluded-physical-backend', backend_alias)
        if operation == 'lineage-history':
            reject('inspect-refusal-relabelled-success', lambda c: event(c, 'lineage-history-result').update(error=None))
            reject('unsupported-repair-claim', lambda c: event(c, 'lineage-history-complete').update(repair=True))
            reject('false-clean-release-after-refusal', lambda c: event(c, 'lineage-history-control', phase='dropped-and-joined').update(phase='released-and-joined'))
            reject('false-native-closing-after-inspect-refusal', lambda c: event(c, 'lineage-history-state', stage='after-read')['state'].update(sequence=native['sequence'] + 1))
        else:
            reject('empty-retirement-native-put-omitted', lambda c: event(c, 'lineage-history-state', stage='transition-returned')['state'].update(sequence=native['sequence']))
            reject('empty-retirement-activation-claim', lambda c: event(c, 'lineage-transition-complete').update(activation=True))
    return dict(status='PASS', count=len(rejected), rejected=rejected)


def replay(root):
    """Bind actual stdout, original pins and every successive external baseline."""
    root = Path(root).resolve()
    folder, bound = root / 'live-faults', {}

    def raw(path):
        path = Path(path)
        value = path.read_bytes()
        bound[str(path)] = sha(value)
        return value

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_LIVE_COMMIT_LOSS'
    assert [c['name'] for c in report['cases']] == list(CASES)
    assert [c['name'] for c in report['clones']] == list(CLONES)
    assert len({c['database'] for c in report['clones']}) == 4
    assert report['databases'] == [c['database'] for c in report['clones']]
    for field in ('cleanup_errors', 'missing_owned', 'unrelated_changed', 'unexpected_containers'):
        assert report[field] == []
    assert sha(raw(folder / 'successor-live-fault-proxy.py')) == PROXY_SHA256
    origins = {}
    for origin, pin in history.PINS.items():
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
        assert origin == 'activated' and name in CLONES
        assert clone['status'] == 'PASS' and clone['directory'] == name
        assert clone['config'] == '/lab/' + clone['database'] + '.toml'
        config_path = folder / Path(clone['config']).name
        config = tomllib.loads(raw(config_path).decode())
        ds = only(origins[origin][0]['datasets'])
        assert {k: config['postgres']['control'][k] for k in ('database', 'endpoint', 'user', 'password_ref')} == dict(
            database=clone['database'], endpoint='127.0.0.1:5432', user='bigbrotr_control', password_ref='file:/run/secrets/application')
        assert config['storage']['ingest_root'] == '/lab/' + name + '/store'
        assert config['topology']['dataset_id'] == ds['dataset_id'][2:]
        assert config['topology']['instance_id'] == ds['instance_id'][2:]
        assert config['runtime']['lease_ttl'] == '20s'
        validation = read(folder / (name + '-config-validation.json'))
        assert validation['returncode'] == 0 and validation['stderr'] == ''
        migrations = clone['migrations']
        assert [r['action'] for r in migrations] == ['plan', 'apply', 'apply']
        steps = [dict(name=f'{v:04d}_{history.MIGRATIONS[v][0]}.sql', sha256=history.MIGRATIONS[v][1], version=str(v))
                 for v in range(history.PINS[origin]['floor'] + 1, 39)]
        for index, answer in enumerate(migrations):
            assert set(answer) == {'action', 'exit_code', 'stdout', 'stderr'}
            assert answer['exit_code'] == 0 and answer['stderr'] == ''
            assert loads(answer['stdout']) == dict(format='bigbrotr-control-migration/1',
                scope='postgres-control', target_version='38',
                source_version=str(history.PINS[origin]['floor']) if index < 2 else '38',
                current_version=str(history.PINS[origin]['floor']) if index == 0 else '38',
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
        index, loss, operation, outcome, after_reply = specification(case)
        name, clone_name = case['name'], case['clone']
        expected_clone = CLONES[index // 2]
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
        assert not any(k in response for k in ('error', 'orchestration_error', 'cleanup_error', 'clock_error', 'response_error'))
        assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds'])
        assert 0 <= response['seconds'] <= 120
        assert case['response_bytes'] == len(response['stdout'].encode())
        events = [loads(line) for line in response['stdout'].splitlines()]
        window = case_read('clock')
        assert window == case['server_window']
        assert window['before_us'] >= windows.get(clone_name, 0), 'process windows overlap'
        fault = case_read('fault')
        assert fault == case['fault']
        if loss:
            assert case_read('wire') == fault['wire']
        result = verify(events, before, native, inputs, case, window, fault=fault)
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
        result['negative_controls'] = negative_controls(events, before, native, inputs, case, window, fault=fault)
        checked.append(result)
        chains[clone_name], windows[clone_name] = (after, after_native, after_inputs), window['after_us']
    for name, clone in clones.items():
        assert clone['final_case'] == name + '-fresh'
        same(chains[name][0], read(folder / (name + '-final-sql.json')))
        spool = folder / name / 'store' / 'producer-inputs-v1'
        assert sorted(p.name for p in spool.iterdir()) == sorted(chains[name][2]), 'unclassified sidecar'
    return dict(status='PASS_SUCCESSOR_LIVE_COMMIT_LOSS_REPLAY', source=report['source'], cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        bound_files=bound, original_preservation='Pinned bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Eight finite processes, four actual wire boundaries, empty retirement and three explicit history refusals; no positive activated recovery/repair, installed or RSS qualification')


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
