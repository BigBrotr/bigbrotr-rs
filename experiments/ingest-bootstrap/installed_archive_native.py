"""Full installed signed native oracle, including original producer attribution.

The signed-only path preserves the qualified Sync oracle without changing its
case dispatch. The caller first checks the complete historical owner union.
"""
import hashlib
import json
import lab
from installed_sync_oracle import SIGNED_SHA256, decode, expected
from installed_archive_oracle import expected as archive_expected


def native(client, output, name, state, initialized, transcript, completed_us):
    directory = output/name
    dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/'+name+'/store')
    (output/(name+'-native.json')).write_text(dumped.stdout)
    actual = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    originals = sorted((directory/'store/producer-inputs-v1').glob('*.cbor'))
    assert len(originals) == 4
    actual_inputs = [p.read_bytes() for p in originals]
    facts = {int.from_bytes(k[-8:], 'big'): v for cf, k, v in actual if cf == 'facts' and k[:2] == b'\x01\x20'}
    # Select known sequence keys, extract ONLY time scalars. Full keys/payloads,
    # exact sequence inventory and timestamp equality are independently checked below.
    fact_prefix = b'bigbrotr/fact/1\0'
    def recorded(sequence):
        data = facts[sequence][34:]
        assert data.startswith(fact_prefix)
        value = decode(data[len(fact_prefix):])[4]
        assert type(value) is int
        return value
    dictionary_time = recorded(4)
    scope, = state['acquisition_scopes']
    source, = [r for r in state['relay_enrollments'] if
        (r['endpoint_id'], r['access_scope_id']) == (scope['endpoint_id'], scope['access_scope_id'])]
    job, = [r for r in state['jobs'] if r['state'] == 'queued' and any(
        detail['job_id'] == r['job_id'] and detail['scope_id'] == scope['scope_id'] for detail in state['acquisition_jobs'])]
    assert job['created_at_us'] <= dictionary_time <= transcript[0]['connected_at_us']
    prefix = b'bigbrotr/producer-request/1\0'
    assert actual_inputs[3].startswith(prefix)
    received = decode(actual_inputs[3][len(prefix):])[3][5][0]
    recorded_at = recorded(8)
    assert type(received) is int
    assert transcript[0]['sent_at_us'] <= received <= transcript[0]['closed_at_us']
    assert received <= recorded_at <= completed_us
    artifact = (directory/'event.jsonl').read_bytes()
    assert hashlib.sha256(artifact).hexdigest() == SIGNED_SHA256
    wanted, inputs, receipts = expected(state, initialized, json.loads(artifact), dictionary_time,
        received, recorded_at, transcript[0]['clock_resolution_us'])
    assert actual == wanted, 'complete independently encoded native rows'
    assert actual_inputs == inputs, 'complete original producer inputs'
    requests = sorted(state['producer_requests'], key=lambda r: r['request_sequence'])
    outcomes = sorted(state['producer_outcomes'], key=lambda r: r['request_sequence'])
    assert len(requests) == len(outcomes) == len(inputs)
    assert [r['outcome_bytes'] for r in outcomes] == ['\\x'+v.hex() for v in receipts]
    for sequence, (row, original, outcome, receipt) in enumerate(zip(requests, inputs, outcomes, receipts), 1):
        assert row['request_sequence'] == sequence and row['input_bytes'] == len(original)
        assert row['request_digest'] == '\\x'+hashlib.sha256(original).hexdigest()
        assert outcome['outcome_sha256'] == '\\x'+hashlib.sha256(receipt).hexdigest()
        assert row['producer_slot_id'] == '\\x'+initialized['producer_slot_id'] and row['producer_epoch'] == 1
        columns = ('relay_endpoint_id', 'relay_access_scope_id', 'relay_enrollment_generation',
            'relay_route_id', 'acquisition_scope_id', 'acquisition_job_id')
        if sequence < 4:
            assert all(row[k] is None for k in columns)
        else:
            assert tuple(row[k] for k in columns) == (source['endpoint_id'], source['access_scope_id'],
                source['generation'], source['route_id'], scope['scope_id'], job['job_id'])
    assert state['producer_epochs'][0]['reserved_through_request'] == len(inputs)
    assert state['producer_epochs'][0]['outstanding_request_limit'] == 32
    assert state['source_streams'][0]['state'] == 'open'
    control, = [r for r in state['owner_epochs'] if r['role'] == 'control']
    manifest = next(v for cf, k, v in wanted if cf == 'default' and k == b'\x01\x01')
    common = dict(dataset_id=state['datasets'][0]['dataset_id'],
        producer_slot_id='\\x'+initialized['producer_slot_id'], producer_epoch=1,
        manifest_sha256='\\x'+hashlib.sha256(manifest).hexdigest())
    for sequence, (row, outcome, original, receipt) in enumerate(zip(requests, outcomes, inputs, receipts), 1):
        attribution = {k: row[k] for k in ('relay_endpoint_id', 'relay_access_scope_id',
            'relay_enrollment_generation', 'relay_route_id', 'acquisition_scope_id', 'acquisition_job_id')}
        request = dict(common, request_sequence=sequence, request_digest='\\x'+hashlib.sha256(original).hexdigest(),
            input_bytes=len(original), input_profile=1, issuer_role='control', issuer_resource=control['resource_id'],
            issuer_epoch=control['epoch'], issuer_incarnation=control['process_incarnation'], **attribution)
        assert {k:v for k,v in row.items() if k not in ('xmin', 'reserved_at_us')} == request
        wanted_outcome = dict(common, request_sequence=sequence, request_digest=request['request_digest'],
            outcome_kind=1, outcome_bytes='\\x'+receipt.hex(), outcome_sha256='\\x'+hashlib.sha256(receipt).hexdigest(),
            policy_epoch=1, recorder_role='control', recorder_resource=control['resource_id'],
            recorder_epoch=control['epoch'], recorder_incarnation=control['process_incarnation'])
        assert {k:v for k,v in outcome.items() if k not in ('xmin', 'recorded_at_us')} == wanted_outcome
        assert control['acquired_at_us'] <= row['reserved_at_us'] <= outcome['recorded_at_us'] < control['lease_expires_at_us']
    evidence = dict(native_rows=len(wanted), original_requests=len(inputs), dictionary_recorded_us=dictionary_time,
        event_received_us=received, event_recorded_us=recorded_at)
    return evidence, archive_expected(state, initialized, json.loads(artifact), dictionary_time,
        received, recorded_at, transcript[0]['clock_resolution_us'])
