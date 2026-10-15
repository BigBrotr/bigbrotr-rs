"""Independent 75-table and native-byte model for three installed lifetimes.

The qualified origins are immutable. New effects are encoded from literal
commands/events and bounded identity/time/xid coordinates, not a projection of
an earlier finite transcript. Raw-file custody is checked by a separate reader.
"""
import copy
import hashlib
import json
import math
import re

import history_replay as history
import transition_replay as transition
from live_replay import OLD_EVENT, NEW_EVENT
from installed_family_science import NetworkScience, decode

encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, instant = history.only, history.same, history.instant
native_map, native_rows, bx = transition.native_map, transition.native_rows, transition.bx
CASES = ('activated-live3', 'empty-live3', 'later-live4')
ENDPOINT = 'ws://127.0.0.1:7447/installed-successor-fixture'


def digest_bytes(value):
    return hashlib.sha256(value).digest()


def clock(value, window):
    assert type(value) is int and window['before_us'] <= value <= window['after_us']


def fresh_xmin(value, state):
    assert isinstance(value, str) and re.fullmatch('[1-9][0-9]*', value) and int(value) < 2**32
    assert value not in {r['xmin'] for rows in state.values() for r in rows}


def exclusion(owner):
    return '\\x' + sha(b'bigbrotr/control-exclusion/1\0' + binary(owner['dataset_id'])
        + binary(owner['resource_id']) + be(owner['epoch']) + binary(owner['process_incarnation']) * 2)


def baseline(context):
    origin = context['case']['origin']
    before, native, inputs, window = (context[k] for k in ('before', 'native_before', 'inputs_before', 'server_window'))
    if origin in ('activated', 'empty'):
        history.baseline(before, native, inputs, origin, window)
        history.science(before, native, inputs, origin)
    else:
        assert origin == 'later'
        assert sha(encoded(history.normalized(before))) == LATER['tables']
        assert sha(encoded(native)) == LATER['native'] and sha(encoded(inputs)) == LATER['inputs']
        import retirement_replay
        retirement_replay.science(before, native, inputs, 'genuine-gap', True)
    assert len(before) == 75
    old_keys = {r['request_key'] for r in before['management_commands']}
    assert not {'\\x' + f'e{n}' * 16 for n in (1, 2, 3, 4)} & old_keys
    assert not any(binary(r['url_bytes']) == ENDPOINT.encode() for r in before['relay_endpoints'])
    if origin == 'later':
        job = only(before['jobs'], request_key='\\x' + 'a7' * 16)
        assert job['state'] == 'accepted' and job['job_revision'] == 1
        assert job['owner_resource_id'] is job['owner_epoch'] is job['owner_incarnation'] is None
        assert only(before['acquisition_jobs'], job_id=job['job_id'])['deadline_at_us'] < window['before_us']


class Model:
    def __init__(self, before, native, inputs, window):
        self.pg, self.native, self.inputs = copy.deepcopy((before, native, inputs))
        self.ds = only(self.pg['datasets'])
        self.legacy = only(self.pg['initial_ingest_successions'])
        self.resource, self.slot = (self.legacy[k] for k in ('owner_resource_id', 'producer_slot_id'))
        self.window = window
        self.ids = set(re.findall(r'\\\\x([0-9a-f]{32})"', encoded(before).decode()))

    def identity(self, value):
        assert re.fullmatch('[0-9a-f]{32}', value) and int(value, 16)
        assert value not in self.ids
        self.ids.add(value)
        return '\\x' + value

    def startup(self, observed, report):
        before, ds = copy.deepcopy(self.pg), self.ds['dataset_id']
        previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
        assert previous['state'] == 'released'
        actual = only(observed['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
        role = only(observed['role_instances'], role_instance_id=actual['role_instance_id'])
        rid, incarnation = self.identity(role['role_instance_id'][2:]), self.identity(actual['process_incarnation'][2:])
        for at in (role['created_at_us'], actual['acquired_at_us'], actual['lease_expires_at_us'] - 6_000_000):
            clock(at, self.window)
        assert role['created_at_us'] <= actual['acquired_at_us'] <= actual['lease_expires_at_us'] - 6_000_000
        fresh_xmin(role['xmin'], before); fresh_xmin(actual['xmin'], before)
        assert type(actual['control_backend_pid']) is int and actual['control_backend_pid'] > 0
        assert self.window['before_us'] <= instant(actual['control_backend_start']) <= actual['acquired_at_us']
        self.pg['role_instances'].append(dict(role_instance_id=rid, dataset_id=ds, role='control',
            created_at_us=role['created_at_us'], retired_at_us=None, xmin=role['xmin']))
        self.control = dict(previous, epoch=previous['epoch'] + 1, role_instance_id=rid,
            process_incarnation=incarnation, acquired_at_us=actual['acquired_at_us'],
            lease_expires_at_us=actual['lease_expires_at_us'], state='active', released_at_us=None,
            exclusion_method=None, exclusion_digest=None, control_backend_pid=actual['control_backend_pid'],
            control_backend_start=actual['control_backend_start'], xmin=actual['xmin'])
        self.pg['owner_epochs'].append(self.control)
        epoch = max(r['producer_epoch'] for r in before['producer_epochs']) + 1
        assert epoch in (3, 4)
        intent = only(observed['ingest_successions'], target_epoch=epoch)
        at, deadline, tx = intent['created_at_us'], intent['lease_expires_at_us'], intent['xmin']
        clock(at, self.window)
        assert actual['acquired_at_us'] <= at < deadline <= at + 5_000_000
        fresh_xmin(tx, self.pg)
        assert re.fullmatch('[1-9][0-9]*', intent['preparation_transaction'])
        assert int(intent['preparation_transaction']) < 2**64 and int(intent['preparation_transaction']) % 2**32 == int(tx)
        role_id, inc = self.identity(report['role_instance_id']), self.identity(report['process_incarnation'])
        domains = [self.identity(only(observed['allocation_domains'], owner_epoch=epoch,
            dictionary_kind=k)['allocation_domain'][2:]) for k in range(1, 10)]
        previous_intent = self.legacy if epoch == 3 else only(before['ingest_successions'], target_epoch=epoch - 1)
        previous_manifest, parent = binary(previous_intent['target_manifest']), binary(previous_intent['handoff'])
        source = only(before['source_streams'], owner_epoch=epoch - 1)
        producer = only(before['producer_epochs'], producer_epoch=epoch - 1)
        assert source['state'] == 'closed' and producer['state'] == 'retired'
        assert at >= producer['retired_at_us']
        kind = 1 if (bool(before['initial_ingest_activations']) if epoch == 3 else
            any(r['target_epoch'] == epoch - 1 for r in before['ingest_activations'])) else 2
        topology = self.ds['topology_generation'] + 1
        manifest = (bytes.fromhex('01010003000100010001') + binary(ds) + be(topology)
            + binary(self.resource) + be(0, 4) + be(epoch) + binary(inc) + previous_manifest[78:94]
            + be(self.ds['policy_revision']) + b'\x01\x09'
            + b''.join(be(k, 2) + binary(domain) for k, domain in enumerate(domains, 1)))
        record = (b'\x02\x0a' + bytes([kind]) + binary(self.slot) + be(epoch - 1)
            + be(producer['reserved_through_request']) + be(source['last_complete_sequence'])
            + binary(source['closure_digest']) + digest_bytes(parent)
            + be(len(previous_manifest), 2) + previous_manifest + be(len(manifest), 2) + manifest)
        assert len(manifest) == 266 and len(record) == 643
        policy = only(before['policy_revisions'], revision=self.ds['policy_revision'])
        limit = policy['document']['settings']['ingest.producer_outstanding_requests']
        assert type(limit) is int and limit in (2, 32)
        config = only(before['config_revisions'], revision=self.ds['config_revision'])
        self.plan = dict(epoch=epoch, created=at, deadline=deadline, role=role_id, incarnation=inc,
            domains=domains, topology=topology, manifest=manifest, record=record, limit=limit)
        self.pg['role_instances'].append(dict(role_instance_id=role_id, dataset_id=ds, role='ingest',
            created_at_us=at, retired_at_us=None, xmin=tx))
        target = only(observed['owner_epochs'], role='ingest', epoch=epoch)
        target_deadline = target['lease_expires_at_us']
        assert deadline <= target_deadline
        if target['xmin'] == tx:
            assert target_deadline == deadline
        else:
            fresh_xmin(target['xmin'], self.pg); clock(target_deadline - 6_000_000, self.window)
        self.pg['owner_epochs'].append(dict(resource_id=self.resource, epoch=epoch, dataset_id=ds,
            role='ingest', role_instance_id=role_id, process_incarnation=inc, acquired_at_us=at,
            lease_expires_at_us=target_deadline, state='active', released_at_us=None, exclusion_method=None,
            exclusion_digest=None, control_fence_version=None, control_backend_pid=None,
            control_backend_start=None, archive_frontier_protocol=None, xmin=target['xmin']))
        self.pg['source_streams'].append(dict(dataset_id=ds, shard_id=0, owner_epoch=epoch,
            owner_resource_id=self.resource, owner_role='ingest', owner_incarnation=inc, opened_at_us=at,
            state='open', closed_at_us=None, last_complete_sequence=None, closure_digest=None, xmin=tx))
        for number, domain in enumerate(domains, 1):
            self.pg['allocation_domains'].append(dict(dataset_id=ds, dictionary_kind=number,
                allocation_domain=domain, owner_resource_id=self.resource, owner_epoch=epoch, owner_role='ingest',
                owner_incarnation=inc, created_at_us=at, state='active', closed_at_us=None, closure_digest=None, xmin=tx))
        self.pg['producer_epochs'].append(dict(producer_slot_id=self.slot, producer_epoch=epoch, dataset_id=ds,
            producer_role_instance_id=role_id, producer_role='ingest', producer_incarnation=inc,
            owner_resource_id=self.resource, owner_epoch=epoch, owner_role='ingest', owner_incarnation=inc,
            outstanding_request_limit=limit, opened_at_us=at, state='open', retired_at_us=None,
            closure_digest=None, reserved_through_request=0, xmin=tx))
        self.pg['topology_generations'].append(dict(dataset_id=ds, generation=topology,
            previous_generation=self.ds['topology_generation'], created_at_us=at, sealed=True, xmin=tx))
        for e in range(1, epoch + 1):
            stream = only(self.pg['source_streams'], owner_epoch=e)
            self.pg['topology_streams'].append(dict(dataset_id=ds, topology_generation=topology, shard_id=0,
                owner_epoch=e, handoff_last_complete_sequence=stream['last_complete_sequence'], xmin=tx))
        old_topology = self.ds['topology_generation']
        self.ds.update(topology_generation=topology, xmin=tx)
        expected_intent = dict(dataset_id=ds, owner_resource_id=self.resource, owner_role='ingest',
            target_epoch=epoch, previous_epoch=epoch - 1, terminal_kind=kind, target_incarnation=inc,
            target_role_instance_id=role_id, producer_slot_id=self.slot, producer_epoch=epoch, shard_id=0,
            previous_topology=old_topology, target_topology=topology,
            issued_through=producer['reserved_through_request'], last_complete_sequence=source['last_complete_sequence'],
            closure_digest=source['closure_digest'], previous_manifest=previous_intent['target_manifest'],
            target_manifest=bx(manifest), previous_manifest_sha256=bx(digest_bytes(previous_manifest)),
            target_manifest_sha256=bx(digest_bytes(manifest)), handoff=bx(record), handoff_sha256=bx(digest_bytes(record)),
            parent_handoff_sha256=bx(digest_bytes(parent)), config_revision=self.ds['config_revision'],
            policy_revision=self.ds['policy_revision'], config_digest=config['document_digest'],
            policy_digest=policy['document_digest'], outstanding_request_limit=limit, created_at_us=at,
            lease_expires_at_us=deadline, prepared_by_resource=self.control['resource_id'],
            prepared_by_epoch=self.control['epoch'], prepared_by_role='control',
            prepared_by_incarnation=self.control['process_incarnation'],
            preparation_transaction=intent['preparation_transaction'], xmin=tx)
        self.pg['ingest_successions'].append(expected_intent)
        activation = only(observed['ingest_activations'], target_epoch=epoch)
        ax, activated = activation['xmin'], activation['activated_at_us']
        fresh_xmin(ax, self.pg); clock(activated, self.window)
        assert at <= activated < deadline
        assert re.fullmatch('[1-9][0-9]*', activation['activation_transaction'])
        assert int(activation['activation_transaction']) < 2**64 and int(activation['activation_transaction']) % 2**32 == int(ax)
        assert int(activation['activation_transaction']) > int(intent['preparation_transaction'])
        self.pg['ingest_activations'].append(dict({k: expected_intent[k] for k in ('dataset_id',
            'owner_resource_id', 'target_epoch', 'target_incarnation', 'target_role_instance_id',
            'producer_slot_id', 'producer_epoch', 'target_manifest_sha256', 'handoff_sha256')},
            activated_by_resource=self.control['resource_id'], activated_by_epoch=self.control['epoch'],
            activated_by_incarnation=self.control['process_incarnation'], activated_at_us=activated,
            activation_transaction=activation['activation_transaction'], xmin=ax))
        assert report == dict(format='bigbrotr-ingest-successor/1', state='activated', dataset_id=ds[2:],
            owner_resource_id=self.resource[2:], producer_slot_id=self.slot[2:], producer_epoch=str(epoch),
            process_incarnation=inc[2:], role_instance_id=role_id[2:], activated_at_us=str(activated),
            prepared_at_us=str(at), lease_expires_at_us=str(deadline), config_revision=str(self.ds['config_revision']),
            policy_revision=str(self.ds['policy_revision']), outstanding_request_limit=str(limit),
            predecessor=dict(producer_epoch=str(epoch - 1), resolved_requests=str(producer['reserved_through_request']),
                last_complete_sequence=str(source['last_complete_sequence']), sha256=source['closure_digest'][2:]),
            acquisition='finite-successor', ready=False, listeners=[dict(kind='private-session-http',
                host='127.0.0.1', port=18081, profile='initial-source-commands')])
        values = native_map(self.native)
        for cf, key, value in [('default', b'\x01\x01', manifest),
            ('default', b'\x01\x0a' + binary(ds) + binary(self.resource) + be(epoch - 1), record),
            ('default', b'\x01\x02' + binary(ds) + be(0, 4) + be(epoch), b'\x01\x02' + be(0)),
            ('receipts', b'\x01\x31' + binary(self.slot) + be(epoch), b'\x01\x31' + bytes(24))]:
            assert key == b'\x01\x01' or (cf, key.hex()) not in values
            values[cf, key.hex()] = value.hex()
        self.native.update(rows=native_rows(values), sequence=self.native['sequence'] + 4)
        same(self.pg, observed)


def leases(wanted, observed, previous, window, control_epoch, epoch, *, stopped=False):
    for role, current_epoch in (('control', control_epoch), ('ingest', epoch)):
        old = only(previous['owner_epochs'], role=role, epoch=current_epoch)
        current = only(observed['owner_epochs'], role=role, epoch=current_epoch)
        row = only(wanted['owner_epochs'], role=role, epoch=current_epoch)
        deadline, tx = current['lease_expires_at_us'], current['xmin']
        assert old['lease_expires_at_us'] <= deadline
        if tx == old['xmin']:
            assert deadline == old['lease_expires_at_us']
        else:
            fresh_xmin(tx, previous)
            clock(deadline - 6_000_000, window)
        row.update(lease_expires_at_us=deadline, xmin=tx)
        if stopped and role == 'control':
            at = current['released_at_us']
            clock(at, window)
            assert deadline - 6_000_000 <= at < deadline and tx != old['xmin']
            row.update(state='released', released_at_us=at, exclusion_method='storage_fenced',
                exclusion_digest=exclusion(old))
            ended = only(observed['role_instances'], role_instance_id=row['role_instance_id'])
            clock(ended['retired_at_us'], window)
            assert at <= ended['retired_at_us']
            only(wanted['role_instances'], role_instance_id=row['role_instance_id']).update(
                retired_at_us=ended['retired_at_us'], xmin=tx)


def admissions(wanted, observed, science, scope, enrollment, control, window):
    epoch = science.epoch
    previous_at = only(wanted['ingest_activations'], target_epoch=epoch)['activated_at_us']
    prior_tx = only(wanted['ingest_activations'], target_epoch=epoch)['xmin']
    for n in range(1, len(science.inputs) + 1):
        request = only(observed['producer_requests'], producer_epoch=epoch, request_sequence=n)
        outcome = only(observed['producer_outcomes'], producer_epoch=epoch, request_sequence=n)
        raw, receipt = science.inputs[n], science.receipts[n]
        for row, field in ((request, 'reserved_at_us'), (outcome, 'recorded_at_us')):
            clock(row[field], window); fresh_xmin(row['xmin'], wanted)
            assert 0 < (int(row['xmin']) - int(prior_tx)) % 2**32 < 2**31
            prior_tx = row['xmin']
        assert request['xmin'] != outcome['xmin']
        assert previous_at <= request['reserved_at_us'] <= outcome['recorded_at_us']
        previous_at = outcome['recorded_at_us']
        assert previous_at < min(only(observed['owner_epochs'], role='control', epoch=control['epoch'])['lease_expires_at_us'],
            only(observed['owner_epochs'], role='ingest', epoch=epoch)['lease_expires_at_us'])
        source = dict(relay_endpoint_id=None, relay_access_scope_id=None, relay_enrollment_generation=None,
            relay_route_id=None, acquisition_scope_id=None, acquisition_job_id=None)
        if n > 1:
            job = only(observed['jobs'], request_key='\\x' + 'e4' * 16)
            source = dict(relay_endpoint_id=scope['endpoint_id'], relay_access_scope_id=scope['access_scope_id'],
                relay_enrollment_generation=1, relay_route_id=enrollment['route_id'],
                acquisition_scope_id=scope['scope_id'], acquisition_job_id=job['job_id'])
        common = dict(producer_slot_id=bx(science.slot), producer_epoch=epoch, request_sequence=n,
            dataset_id=bx(science.ds), request_digest=bx(digest_bytes(raw)), manifest_sha256=bx(digest_bytes(science.manifest)))
        wanted['producer_requests'].append(dict(common, input_bytes=len(raw), input_profile=1,
            issuer_resource=control['resource_id'], issuer_epoch=control['epoch'], issuer_role='control',
            issuer_incarnation=control['process_incarnation'], reserved_at_us=request['reserved_at_us'], xmin=request['xmin'], **source))
        wanted['producer_outcomes'].append(dict(common, outcome_kind=1, policy_epoch=science.m.ds['policy_revision'],
            outcome_bytes=bx(receipt), outcome_sha256=bx(digest_bytes(receipt)), recorder_resource=control['resource_id'],
            recorder_epoch=control['epoch'], recorder_role='control', recorder_incarnation=control['process_incarnation'],
            recorded_at_us=outcome['recorded_at_us'], xmin=outcome['xmin']))
    only(wanted['producer_epochs'], producer_epoch=epoch).update(reserved_through_request=len(science.inputs),
        xmin=only(observed['producer_requests'], producer_epoch=epoch, request_sequence=len(science.inputs))['xmin'])


def backend_checked(value, control, database, window):
    assert set(value) == {'exact', 'absence', 'all'} and value['all'] == []
    assert value['exact'] == [dict(pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
        database=database, control_epoch=control['epoch'], present=False)]
    clock(instant(control['control_backend_start']), window)
    absence = value['absence']
    assert set(absence) == {'database', 'user', 'limit_seconds', 'status', 'attempts', 'seconds'}
    assert absence['database'] == database and absence['user'] == 'bigbrotr_control'
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['limit_seconds'] == 5
    assert math.isfinite(absence['seconds']) and 0 <= absence['seconds'] <= 120
    assert absence['attempts']
    last = 0
    for sample in absence['attempts']:
        assert set(sample) == {'seconds', 'count', 'backends'}
        assert math.isfinite(sample['seconds']) and last <= sample['seconds'] <= absence['seconds']
        last = sample['seconds']
        assert type(sample['count']) is int and sample['count'] == len(sample['backends'])
        assert sample['count'] in (0, 1)
        for row in sample['backends']:
            assert row == dict(pid=control['control_backend_pid'], backend_start=control['control_backend_start'])
    assert absence['attempts'][-1]['backends'] == []


def wire_checked(journey, fixture, report, before, initial, window):
    from installed_family_http import ACTIONS
    assert journey['terminal'] == dict(exit_code=0, reaped=True, outcome='PROCESS_EXITED')
    assert journey['stdout'] == journey['stderr'] == ''
    result = journey['result']
    assert set(result) == {'status', 'login', 'logout', 'me', 'digests', 'observations',
        'job_status', 'relay_joined', 'unauthenticated'}
    assert result['status'] == 'PASS' and result['relay_joined'] is True and result['unauthenticated'] == 401
    assert result['login'] == dict(format='bigbrotr-cli-login/1', status='session-persisted', authority='observation-only')
    assert result['logout'] == dict(format='bigbrotr-cli-logout/1', status='revocation-acknowledged',
        local_file='retained', drain='unobserved')
    old_sessions = {r['session_id'] for r in before['browser_sessions']}
    session, = [r for r in initial['browser_sessions'] if r['session_id'] not in old_sessions]
    assert result['me'] == dict(format='bigbrotr-session/1', principal_id='10' * 16,
        credential_revision='1', grant_revision='4', config_revision=report['config_revision'], policy_revision=report['policy_revision'],
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
            assert last['event_ids'] == [NEW_EVENT['id'],
                '8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849', NEW_EVENT['id']]
            assert last['completion'] == 'request-and-websocket-close'
            assert first['request_at_us'] <= last['sent_at_us'] <= last['closed_at_us']
        else:
            assert last['frames'] == [] and last['completion'] == 'quiet-eof'
    quiet = fixture['quiet']
    assert quiet in opened and quiet['number'] > 1
    assert int(report['lease_expires_at_us']) < quiet['request_at_us']
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



def verify(context):
    from installed_family_http import add_http
    assert set(context) == {'case', 'before', 'after', 'native_before', 'native_after', 'inputs_before',
        'inputs_after', 'input_contents', 'snapshots', 'response', 'journey', 'fixture', 'server_window',
        'backend', 'catalog_before', 'catalog_after', 'fixture_input'}
    case, window, response = (context[k] for k in ('case', 'server_window', 'response'))
    assert set(case) == {'name', 'clone', 'origin', 'operation', 'predecessor', 'database', 'config', 'status', 'custody'}
    assert case['name'] in CASES
    index = CASES.index(case['name']); origin = ('activated', 'empty', 'later')[index]
    assert case['clone'] == case['origin'] == origin and case['operation'] == 'installed-live'
    assert case['predecessor'] is None and case['status'] == 'RUNNING' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    assert set(window) == {'before_us', 'after_us'}
    assert all(type(v) is int and v > 0 for v in window.values())
    assert 0 < window['after_us'] - window['before_us'] <= 120_000_000
    baseline(context)
    before, after, snapshots = (context[k] for k in ('before', 'after', 'snapshots'))
    assert len(before) == len(after) == 75
    assert context['catalog_before'] == context['catalog_after']
    assert isinstance(context['catalog_before'], str) and context['catalog_before']
    assert set(response) == {'returncode', 'stdout', 'stderr', 'outcome', 'terminal'}
    assert response['outcome'] == 'PROCESS_EXITED' and response['returncode'] == 0 and not response['stderr']
    terminal = response['terminal']
    assert set(terminal) == {'pid', 'config', 'prefix', 'command', 'start_ticks', 'exit_code', 'reaped', 'forced', 'elapsed_seconds'}
    assert terminal['reaped'] is True and terminal['forced'] is False and terminal['exit_code'] == 0
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 6 < terminal['elapsed_seconds'] <= 60
    assert terminal['config'] == case['config'] and terminal['prefix'] == '/lab/' + case['name']
    assert terminal['command'] == ['/build/bigbrotr', 'serve', '--file', case['config'],
        '--activate-successor', '--roles', 'control,ingest,sync']
    report, stopped = [json.loads(line) for line in response['stdout'].splitlines()]
    assert stopped == {k: report[k] for k in ('format', 'dataset_id', 'owner_resource_id', 'producer_slot_id', 'producer_epoch')} | dict(
        state='stopped', native='closed', owner_release='committed', source_closure='not-performed', ready=False)
    assert [s['stage'] for s in snapshots] == ['activated', 'prepared', 'submitted', 'renewed', 'before-stop']
    previous_clock = window['before_us']
    for snap in snapshots:
        assert set(snap) == {'stage', 'server_us', 'observed_by_us', 'control', 'inputs'}
        assert previous_clock <= snap['server_us'] <= snap['observed_by_us'] <= window['after_us']
        previous_clock = snap['observed_by_us']
        assert len(snap['control']) == 75
    model = Model(before, context['native_before'], context['inputs_before'],
        dict(before_us=window['before_us'], after_us=snapshots[0]['observed_by_us']))
    active = snapshots[0]['control']
    model.startup(active, report)
    epoch = model.plan['epoch']
    assert epoch == (4 if origin == 'later' else 3)
    assert model.ds['policy_revision'] == model.ds['config_revision'] == (3 if origin == 'activated' else 1)
    assert model.plan['limit'] == (2 if origin == 'activated' else 32)
    provenance = origin != 'activated'
    policy = only(before['policy_revisions'], revision=model.ds['policy_revision'])
    assert policy['document']['settings']['sync.provenance'] == ('local_first_witness' if provenance else 'off')
    oldscope = only(before['allocation_domains'], owner_epoch=1, dictionary_kind=4)
    assert context['fixture_input'] == dict(config_revision=str(model.ds['config_revision']),
        policy_revision=str(model.ds['policy_revision']), visibility=dict(dataset_id=model.ds['dataset_id'][2:],
            dictionary_kind=4, allocation_domain=oldscope['allocation_domain'][2:], ordinal='3'))
    fixture = context['fixture']
    assert set(fixture) == {'cleanup_errors', 'client_terminal', 'renewal', 'signal', 'quiet'}
    assert fixture['cleanup_errors'] == []
    assert fixture['client_terminal'] == context['journey']['terminal']
    first = wire_checked(context['journey'], fixture, report, before, snapshots[1]['control'], window)
    science = NetworkScience(model, model.plan)
    observed_native = native_map(context['native_after'])
    n = 4 if provenance else 1
    def fact_scalar(number):
        raw = bytes.fromhex(observed_native['facts', (b'\x01\x20' + science.source(epoch, number)).hex()])
        assert raw[:2] == b'\x01\x20' and raw[34:].startswith(b'bigbrotr/fact/1\0')
        return decode(raw[34:].split(b'\0', 1)[1])
    dictionary_at, recorded = fact_scalar(1)[4], fact_scalar(n + 1)[4]
    received, resolution = fact_scalar(n + 4)[5][1], fact_scalar(n + 4)[5][3][1]
    assert type(resolution) is int and 0 < resolution <= 1_000_000 and resolution == first['clock_resolution_us']
    assert first['sent_at_us'] - resolution <= received <= first['closed_at_us'] + resolution
    assert received <= recorded
    for at in (dictionary_at, recorded, received):
        clock(at, window)
    activation = only(active['ingest_activations'], target_epoch=epoch)
    assert activation['activated_at_us'] <= dictionary_at <= first['request_at_us'] <= recorded
    historical = None
    if provenance:
        old = fact_scalar(10)
        old_received, old_recorded = old[5][4], old[4]
        clock(old_received, window); clock(old_recorded, window)
        assert received <= old_received <= old_recorded
        assert first['sent_at_us'] - resolution <= old_received <= first['closed_at_us'] + resolution
        original_receive = bytes.fromhex(native_map(context['native_before'])['facts',
            (b'\x01\x20' + science.source(1, 11)).hex()])
        assert decode(original_receive[34:].split(b'\0', 1)[1])[5][1] < old_received
        historical = old_received, old_recorded
    science.network(provenance, dictionary_at, received, recorded, resolution, historical)
    assert context['native_after'] == model.native, 'complete native bytes or sequence differ'
    assert context['inputs_after'] == model.inputs, 'complete input census differs'
    assert snapshots[0]['inputs'] == context['inputs_before']
    previous = active
    for snap in snapshots[1:] + [dict(stage='final', control=after, inputs=context['inputs_after'],
            server_us=window['after_us'], observed_by_us=window['after_us'])]:
        phase, observed = snap['stage'], snap['control']
        bounded = dict(before_us=window['before_us'], after_us=snap['observed_by_us'])
        wanted = copy.deepcopy(active)
        leases(wanted, observed, previous, bounded, model.control['epoch'], epoch, stopped=phase == 'final')
        scope, enrollment = add_http(wanted, observed, before, context['journey']['result'], phase, bounded, model.control)
        if phase != 'prepared':
            admissions(wanted, observed, science, scope, enrollment, model.control, bounded)
        same(wanted, observed)
        if previous is not active:
            mutable = {'owner_epochs', 'role_instances', 'browser_sessions', 'producer_epochs'}
            for table in observed:
                if table not in mutable:
                    assert all(row in observed[table] for row in previous[table]), (phase, table, 'retained row changed')
            oldids = {r['session_id'] for r in before['browser_sessions']}
            oldsession, = [r for r in previous['browser_sessions'] if r['session_id'] not in oldids]
            session, = [r for r in observed['browser_sessions'] if r['session_id'] not in oldids]
            assert session['last_seen_at_us'] >= oldsession['last_seen_at_us']
        if phase in ('before-stop', 'final'):
            same({k: v for k, v in previous.items() if k not in ('owner_epochs', 'role_instances')},
                 {k: v for k, v in observed.items() if k not in ('owner_epochs', 'role_instances')})
        assert snap['inputs'] == (context['inputs_before'] if phase == 'prepared' else model.inputs)
        previous = observed
    renewal = fixture['renewal']
    assert renewal == dict(initial_deadline_us=model.plan['deadline'], observed_at_us=renewal['observed_at_us'],
        initial_target=only(active['owner_epochs'], role='ingest', epoch=epoch), current=snapshots[3])
    assert max(model.plan['deadline'], model.plan['created'] + 6_000_000) < renewal['observed_at_us'] <= snapshots[3]['server_us']
    for role, number in (('control', model.control['epoch']), ('ingest', epoch)):
        current = only(snapshots[3]['control']['owner_epochs'], role=role, epoch=number)
        assert current['state'] == 'active' and current['lease_expires_at_us'] > snapshots[3]['observed_by_us']
    assert fixture['signal']['kind'] == 'SIGTERM'
    assert fixture['signal']['identity'] == {k: terminal[k] for k in ('pid', 'config', 'prefix', 'command', 'start_ticks')}
    assert set(context['input_contents']) == set(model.inputs)
    for name, value in context['input_contents'].items():
        raw = bytes.fromhex(value)
        assert model.inputs[name] == dict(bytes=len(raw), sha256=sha(raw))
        if f'-{epoch:016x}-' in name:
            assert raw == science.inputs[int(name[-21:-5], 16)]
    backend_checked(context['backend'], model.control, case['database'], window)
    return dict(status='PASS', name=case['name'], snapshots=5, observed_states=7, sql_tables=75,
        target_epoch=epoch, requests=len(science.inputs), source_cut=10 if provenance else 5,
        native_rows=len(model.native['rows']), native_sequence=model.native['sequence'],
        original_inputs=len(context['inputs_before']), final_inputs=len(model.inputs), actual_cli=True,
        certification_per_frame='not-observed', source_closure='not-performed', workflow_completion='not-qualified')


def negative_controls(context):
    """Bounded state/custody discriminators, not engine fault observations."""
    checks = []
    def reject(name, mutate):
        other = copy.deepcopy(context)
        mutate(other)
        try:
            verify(other)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError):
            checks.append(name)
            return
        raise AssertionError('negative control accepted: ' + name)
    reject('unknown-process', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    reject('forced-cli', lambda c: c['response']['terminal'].update(forced=True))
    reject('wrong-native-sequence', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('missing-old-native', lambda c: c['native_after']['rows'].pop(0))
    reject('missing-original-input', lambda c: c['inputs_after'].pop(next(iter(c['inputs_before']))))
    reject('missing-current-input', lambda c: c['input_contents'].pop(next(n for n in c['input_contents']
        if f'-{int(json.loads(c["response"]["stdout"].splitlines()[0])["producer_epoch"]):016x}-' in n)))
    reject('changed-original-policy', lambda c: c['after']['policy_revisions'][0].update(xmin='1'))
    reject('changed-original-job', lambda c: c['after']['jobs'][0].update(job_revision=999))
    reject('unjoined-relay', lambda c: c['journey']['result'].update(relay_joined=False))
    reject('missing-new-event-retry', lambda c: c['journey']['closed'][0]['event_ids'].pop())
    reject('known-present-backend', lambda c: c['backend']['exact'][0].update(present=True))
    reject('unknown-native-custody', lambda c: c['case'].update(custody='OUTCOME_UNKNOWN'))
    reject('unexpected-relational-table', lambda c: c['after'].update(unexpected=[]))
    reject('rewritten-catalog', lambda c: c.update(catalog_after=c['catalog_after'] + '\nchanged'))
    reject('expired-renewal', lambda c: only(c['snapshots'][3]['control']['owner_epochs'], role='control',
        epoch=max(r['epoch'] for r in c['before']['owner_epochs'] if r['role'] == 'control') + 1).update(lease_expires_at_us=1))
    reject('rewritten-session-auth', lambda c: c['after']['browser_sessions'][0].update(authentication_revision=999))
    reject('expired-session-not-pruned', lambda c: c['after']['browser_sessions'].append(copy.deepcopy(c['before']['browser_sessions'][0])))
    reject('fabricated-workflow-completion', lambda c: only(c['after']['jobs'], request_key='\\x' + 'e4' * 16).update(state='succeeded'))
    reject('changed-request-issuer', lambda c: only(c['after']['producer_requests'],
        producer_epoch=int(json.loads(c['response']['stdout'].splitlines()[0])['producer_epoch']),
        request_sequence=1).update(issuer_epoch=1))
    reject('changed-outcome-recorder', lambda c: only(c['after']['producer_outcomes'],
        producer_epoch=int(json.loads(c['response']['stdout'].splitlines()[0])['producer_epoch']),
        request_sequence=1).update(recorder_epoch=1))
    return dict(status='PASS', controls=len(checks), rejected=checks,
        scope='artificial perturbations of retained observations; no engine fault injection')


# Frozen final retirement-fault58b endpoint, not the earlier intermediate head.
LATER = {'tables': 'd69f12b2939444424d453843da4c4d15d64542038d32e1ebe650ed94b9038a57', 'native': '88feef2fa0e9e62dc304a71ebc93a07f6af7c56c04346ee7ce7902938081595d', 'inputs': '5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea'}
