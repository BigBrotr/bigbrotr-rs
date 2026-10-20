"""Literal epoch1/epoch2 progress expectations over existing scientific models.

The216/72-byte encoding is reused; semantic counts, coordinates and event/input
meaning are reconstructed here. No SQL or native engine is opened by this file.
"""
import copy
import hashlib
import importlib.util
import json
import re
import stat
import sys
from pathlib import Path

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
for _directory in ('relay-source','ingest-succession','network-transport','control-migrations'):
    sys.path.insert(0,str(SOURCE/'experiments'/_directory))
if 'bootstrap_oracle' not in sys.modules:
    _spec=importlib.util.spec_from_file_location('bootstrap_oracle',Path(__file__).with_name('oracle.py'))
    _bootstrap=importlib.util.module_from_spec(_spec)
    sys.modules['bootstrap_oracle']=_bootstrap
    _spec.loader.exec_module(_bootstrap)

import installed_progress_replay as typed
from installed_sync_oracle import decode, expected as initial_science, SIGNED_SHA256
from initial_succession_oracle import only, same

MIGRATIONS = {
    27: ('0027_acquisition_abandonment.sql', '34f9d690076e0acaef0c22df5a3d0079c1d0436f7d2e687429d4e9b407d16093'),
    28: ('0028_acquisition_gap_guard.sql', '043c290a469137589d05317c778714de0d6b6b2f45eaa7f577438c64a5035326'),
    29: ('0029_initial_retirement.sql', '8d5c6a860e47ff6cd23b4c46518e226a93cc6be282bd96f7d9bbb6c0c01a4658'),
    30: ('0030_initial_succession.sql', '55049328c3f0be13a22af199f1599af157ce02e5a74760a296909469693be1f4'),
    31: ('0031_initial_empty_retirement.sql', '52819215086abb15c06b25ebb536b16955256e4707aaf0f1a89060c66fc96b8a'),
    32: ('0032_initial_activation.sql', '1af4c64b3c1a311cac185d7ec8f015974b339ab60138a7454a3047d2d50b072f'),
    33: ('0033_activated_initial_recovery.sql', '4f22eb019e831126bf3f5c712d0ddf1e8213dfdfcdf9afaf5b38c80418cb5804'),
    34: ('0034_activated_control_retirement.sql', 'd3ccd19efd29a5c5db849c8bd21db45e4699076122d0bb6cd659fdd736d0b87a'),
    35: ('0035_activated_source_closure.sql', '80b5441cbb2de1f782ce6458e1c2b26a2f3730f51e61619af49f1e8d8adc375a'),
    36: ('0036_activated_acquisition_abandonment.sql', '19e2390b46d9eba7e4eac765eafc30fc7b2cac03e03c0918df64ed0b87642e54'),
    37: ('0037_activated_initial_retirement.sql', 'ea882fd887d0800ab1880646026ac85eeb6485f5fa4fd3323eee09b4bf96eac8'),
    38: ('0038_repeatable_ingest_succession.sql', 'bc5c90d25e5ae533e67dd328fbe1da32af3c19e8c03615241fa2359572aac087'),
    39: ('0039_producer_progress.sql', '267045f6f5c9ee44c1a9dc3335c7092c056606662bd4016cb7c2a81b19c421f0'),
}
EXTRA32 = {'ingest_successions', 'ingest_activations', 'ingest_terminal_transactions'}
PROFILES = {
    'initial': dict(epoch=1, request=4, mutation_revision=4, dependencies=tuple((1,n) for n in range(1,13)), key='b1'),
    'activated': dict(epoch=2, request=2, mutation_revision=2, dependencies=((1,3), *tuple((2,n) for n in range(1,10))), key='f4'),
}


def sha(value):
    return hashlib.sha256(value).hexdigest()


def check_upgrade(context):
    assert set(context) in ({'before','after','physical_before','physical_after','status','responses',
        'catalog_before','catalog_after'}, {'before','after','physical_before','physical_after','status','responses',
        'catalog_before','catalog_after','reference_catalog32'})
    before, after = context['before'], context['after']
    assert len(after) == 77 and not any(after[t] for t in (*typed.TABLES, 'producer_checkpoints'))
    old = typed.legacy_state(after)
    version = max(row['migration'] for row in before['schema_versions'])
    assert version in (26, 32)
    previous = {row['migration']:row for row in before['schema_versions']}
    current = {row['migration']:row for row in after['schema_versions']}
    assert len(current) == len(after['schema_versions'])
    assert set(current) == set(previous) | set(range(version + 1, 40))
    assert all(current[n] == row for n, row in previous.items())
    transactions = set()
    for n in range(version + 1, 40):
        row = current[n]
        name, digest = MIGRATIONS[n]
        typed.xid(row['xmin']); transactions.add(row['xmin'])
        assert row == dict(migration=n, name=name, checksum='\\x'+digest,
            engine='postgres-control', applied_by='bigbrotr_migrator', reader_min=1,
            reader_max=1, writer_floor=1, applied_at_us=row['applied_at_us'], xmin=row['xmin'])
        assert type(row['applied_at_us']) is int and row['applied_at_us'] >= max(
            r['applied_at_us'] for r in before['schema_versions'])
    assert len(transactions) == 1 and not transactions & {r['xmin'] for rows in before.values() for r in rows}
    for table in before:
        if table != 'schema_versions':
            same({table:before[table]}, {table:old[table]})
    assert all(after[table] == [] for table in set(after) - set(before))
    assert all(context['physical_after'][table] == file for table, file in context['physical_before'].items())
    assert len(context['physical_after']) == 77
    assert [r['action'] for r in context['responses']] == ['apply', 'reapply']
    assert all(r['returncode'] == 0 and r['stderr'] == '' for r in context['responses'])


def project32(state):
    value = typed.legacy_state(state)
    for table in EXTRA32:
        assert value.pop(table) == [], 'unmodeled later lifecycle row'
    value['schema_versions'] = [r for r in value['schema_versions'] if r['migration'] <= 32]
    assert len(value) == 72
    return value


def input_custody(cuts, after, inputs):
    assert set(cuts) == {'mutation','proof-only'}
    assert cuts['mutation'] == cuts['proof-only'] == after
    assert set(after) == {'.', *inputs}
    for name, metadata in after.items():
        assert set(metadata) == {'mode','inode','device','links','bytes','sha256'}
        assert metadata['inode'] > 0 and metadata['device'] >= 0
        if name == '.':
            assert stat.S_ISDIR(metadata['mode']) and metadata['sha256'] is None
        else:
            assert stat.S_ISREG(metadata['mode']) and metadata['links'] == 1
            assert {key:metadata[key] for key in ('bytes','sha256')} == inputs[name]


def ready(state, kind, phase, wire):
    spec = PROFILES[kind]
    assert len(state) == 77 and phase in ('mutation', 'proof-only')
    head = [r for r in state['producer_checkpoints'] if r['producer_epoch'] == spec['epoch']]
    jobs = [r for r in state['acquisition_progress'] if r['producer_epoch'] == spec['epoch']]
    if len(head) != 1 or len(jobs) != 1:
        return False
    head, job = head[0], jobs[0]
    proof = phase == 'proof-only'
    return (head['checkpoint_revision'] == spec['mutation_revision'] + proof
        and head['complete_through_request'] == spec['request']
        and head['dependency_count'] == len(spec['dependencies']) and head['progress_jobs'] == 1
        and job['progress_revision'] == job['classified_events'] == job['successful_events'] == 1 + proof
        and job['rejected_events'] == 0 and job['event_ordinal'] == job['wire_sequence'] == 1
        and job['classification'] == (5 if proof else 2)
        and job['mutation_request'] == (None if proof else spec['request'])
        and typed.bytea(job['request_id'], 48) == wire['request'][1].encode())


def wires_checked(opened, closed, snapshots, event, gates):
    assert 3 <= len(opened) == len(closed) <= 128
    phases, nonces = {}, set()
    for number, (first, last) in enumerate(zip(opened, closed), 1):
        opened_fields={'number','connected_at_us','clock_resolution_us','request','request_at_us','request_monotonic_ns'}
        assert set(first)==opened_fields
        extra={'frames','completion','closed_at_us','closed_monotonic_ns'}
        if 'progress_phase' in last:
            extra|={'progress_phase','event_ids','event_json_sha256','sent_at_us'}
            if last['progress_phase']=='proof-only': extra.add('progress_release')
        assert set(last)==opened_fields|extra
        assert first['number'] == last['number'] == number
        assert all(last[k] == v for k,v in first.items())
        request = last['request']
        assert request[0] == 'REQ' and request[2] == {'kinds':[1], 'limit':3} and len(request) == 3
        assert re.fullmatch('[0-9a-f]{48}', request[1]) and int(request[1][:32],16)
        assert request[1][:32] not in nonces; nonces.add(request[1][:32])
        assert last['connected_at_us'] <= last['request_at_us'] <= last['closed_at_us']
        assert 0 <= last['closed_at_us'] - last['request_at_us'] < 3_000_000
        assert 0 <= last['closed_monotonic_ns'] - first['request_monotonic_ns'] < 3_000_000_000
        if 'progress_phase' not in last:
            assert number > 1 and last['frames'] == [] and last['completion'] == 'quiet-eof'
            continue
        phase = last['progress_phase']
        assert phase in ('mutation','proof-only') and phase not in phases
        assert last['frames'] == ['signed','EOSE'] and last['event_ids'] == [event['id']]
        assert last['event_json_sha256'] == [sha(json.dumps(event,ensure_ascii=False,separators=(',',':')).encode())]
        assert last['completion'] == 'request-and-websocket-close'
        assert last['request_at_us'] <= last['sent_at_us'] <= last['closed_at_us']
        phases[phase] = last
    assert list(phases) == ['mutation','proof-only'] and phases['mutation']['number'] == 1
    observations = {s['stage']:s for s in snapshots}
    for phase, wire in phases.items():
        cut = observations['progress-'+phase]
        assert wire['closed_at_us'] <= cut['server_us'] <= cut['observed_by_us']
        assert json.loads(gates['progress-'+phase+'-observation.json']) == dict(
            format='bigbrotr-progress-transport-observation/1', phase=phase, observation=cut, wire=wire)
    gate = json.loads(gates['progress-release.json'])
    assert gate == dict(format='bigbrotr-progress-transport-release/1',
        mutation_request=phases['mutation']['request'][1],
        observation_sha256=sha(gates['progress-mutation-observation.json'].encode()))
    assert phases['proof-only']['progress_release'] == dict(gate, release_sha256=sha(gates['progress-release.json'].encode()))
    assert phases['proof-only']['sent_at_us'] >= observations['progress-mutation']['observed_by_us']
    return phases


def typed_checked(kind, before, after, snapshots, phases, event, native, inputs, receipts,
                  scope, observer, witness, control):
    spec = PROFILES[kind]
    epoch = spec['epoch']
    dataset = before['datasets'][0]['dataset_id']
    # Select by literal epoch, never aggregate row order.
    producer = only(after['producer_epochs'], producer_epoch=epoch)
    slot = producer['producer_slot_id']
    common = dict(dataset_id=dataset, producer_slot_id=slot, producer_epoch=epoch)
    manifest = native['default', '0101']
    manifest_sha = sha(bytes.fromhex(manifest))
    states = [before, *(s['control'] for s in snapshots), after]
    for state in states:
        assert set(state) == set(before) and len(state) == 77
        assert state['schema_versions'] == before['schema_versions']
        for request in state['producer_requests']:
            if request['producer_epoch'] != epoch or request['request_sequence'] != spec['request']:
                assert all(request[key] is None for key in typed.REQUEST_ADDITIONS)
            else:
                wire = phases['mutation']
                assert {key:request[key] for key in typed.REQUEST_ADDITIONS} == dict(
                    progress_request_id='\\x'+wire['request'][1].encode().hex(),
                    progress_event_ordinal=1, progress_wire_sequence=1,
                    progress_input_sha256='\\x'+sha(json.dumps(event,ensure_ascii=False,separators=(',',':')).encode()))
    cuts = {s['stage']:s for s in snapshots}
    mutation, proof = (cuts['progress-'+phase] for phase in ('mutation','proof-only'))
    assert mutation['inputs'] == proof['inputs']
    for table in ('producer_requests','producer_outcomes','producer_epochs','jobs','acquisition_jobs','job_admissions',
                  'producer_checkpoint_dependencies'):
        same({table:mutation['control'][table]}, {table:proof['control'][table]})
    assert int(only(mutation['control']['producer_checkpoints'])['xmin']) != int(only(proof['control']['producer_checkpoints'])['xmin'])
    def check(cut, phase=None, setup=False):
        state = cut['control']
        if not setup:
            wire = phases[phase]
            assert ready(state,kind,phase,wire)
            actual = only(state['acquisition_progress'])
            accepted = only(state['jobs'], request_key='\\x'+spec['key']*16)
            assert accepted['checkpoint_revision'] == 0 and accepted['state'] == 'queued'
            detail = only(state['acquisition_jobs'],job_id=accepted['job_id'])
            is_proof = phase == 'proof-only'
            job = dict(common, job_id=accepted['job_id'],scope_id=detail['scope_id'],
                progress_revision=1+is_proof,request_id='\\x'+wire['request'][1].encode().hex(),
                event_ordinal=1,wire_sequence=1,input_sha256='\\x'+sha(json.dumps(event,ensure_ascii=False,separators=(',',':')).encode()),
                classification=5 if is_proof else 2, mutation_request=None if is_proof else spec['request'],
                classified_events=1+is_proof,successful_events=1+is_proof,rejected_events=0,
                observed_at_us=actual['observed_at_us'],clock_resolution_us=actual['clock_resolution_us'],
                clock_anomaly_flags=actual['clock_anomaly_flags'],xmin=actual['xmin'])
            if is_proof:
                digest = typed.certification_digest(event,scope,observer,observer,witness,job,wire).hex()
                assert all(job['xmin'] != row['xmin'] for rows in typed.legacy_state(state).values() for row in rows)
            else:
                raw = inputs[spec['request']]
                observation = decode(raw.split(b'\0',1)[1])[3][5]
                assert [job['observed_at_us'],job['clock_resolution_us'],job['clock_anomaly_flags']] == [
                    observation[0],observation[2][1],observation[2][3]]
                assert wire['sent_at_us']-job['clock_resolution_us'] <= observation[0] <= wire['closed_at_us']+job['clock_resolution_us']
                assert job['xmin'] == only(state['producer_outcomes'],producer_epoch=epoch,request_sequence=spec['request'])['xmin']
                digest = sha(raw)
            job['intent_digest'] = '\\x'+digest
            jobs, revision, complete = [job], spec['mutation_revision']+is_proof, spec['request']
            coordinates = spec['dependencies']
        else:
            assert kind == 'initial' and not state['acquisition_progress']
            jobs, revision, complete = [], 2, 2
            coordinates = ((1,1),(1,2),(1,3))
        head = only(state['producer_checkpoints'])
        expected_head = dict(common,progress_format=1,manifest_sha256='\\x'+manifest_sha,
            checkpoint_revision=revision,complete_through_request=complete,progress_jobs=len(jobs),
            dependency_count=len(coordinates),issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],
            issuer_incarnation=control['process_incarnation'],committed_at_us=head['committed_at_us'],xmin=head['xmin'])
        assert control['acquired_at_us'] <= head['committed_at_us'] <= cut['observed_by_us']
        if jobs:
            assert head['xmin'] == jobs[0]['xmin'] and head['committed_at_us'] >= wire['sent_at_us']
        else:
            assert head['xmin'] == only(state['producer_outcomes'],producer_epoch=epoch,request_sequence=2)['xmin']
        dependencies = []
        for source_epoch, sequence in coordinates:
            source = bytes.fromhex(dataset[2:]) + (0).to_bytes(4,'big') + source_epoch.to_bytes(8,'big') + sequence.to_bytes(8,'big')
            value = bytes.fromhex(native['facts',(b'\x01\x20'+source).hex()])
            reference = [[bytes.fromhex(dataset[2:]),0,source_epoch,sequence],value[2:34]]
            first = next(n for n in sorted(receipts) if n <= complete and reference in decode(receipts[n].split(b'\0',1)[1])[5])
            tx = only(state['producer_outcomes'],producer_epoch=epoch,request_sequence=first)['xmin']
            dependencies.append(dict(common,source_shard=0,source_epoch=source_epoch,source_sequence=sequence,
                payload_digest='\\x'+value[2:34].hex(),xmin=tx))
        expected_head['checkpoint_digest'] = '\\x'+typed.head_digest(dict(expected_head,checkpoint_digest='\\x'+'00'*32),
            jobs,dependencies,lambda row:typed.native_reference(row,native)).hex()
        observed_deps = sorted(state['producer_checkpoint_dependencies'],key=lambda r:(r['source_shard'],r['source_epoch'],r['source_sequence']))
        typed.exact_snapshot(head,state['acquisition_progress'],observed_deps,expected_head,jobs,dependencies,native)
    for phase in ('mutation','proof-only'):
        check(cuts['progress-'+phase],phase)
    assert all(before[table] == [] for table in (*typed.TABLES,'producer_checkpoints'))
    for cut in snapshots:
        if cut['stage'] in ('initialized','activated','prepared'):
            if kind == 'initial':
                check(cut,setup=True)
            else:
                assert all(cut['control'][table] == [] for table in (*typed.TABLES,'producer_checkpoints'))
        elif cut['stage'] not in ('progress-mutation','progress-proof-only'):
            for table in (*typed.TABLES,'producer_checkpoints'):
                assert cut['control'][table] == proof['control'][table]
    for table in (*typed.TABLES,'producer_checkpoints'):
        assert after[table] == proof['control'][table]
    return dict(status='PASS_TYPED_CONTEXT',kind=kind,producer_epoch=epoch,
        mutation_revision=spec['mutation_revision'],proof_revision=spec['mutation_revision']+1,
        complete_through_request=spec['request'],dependencies=len(spec['dependencies']),
        acquisition_revision=2,job_checkpoint_revision=0,ack=0)


def verify_initial(context):
    from installed_sync import BOOTSTRAP, COMMANDS, validate_source, validate_queued
    from initial_jobs import unchanged, logged_out, released
    from live_replay import OLD_EVENT
    assert set(context) == {'kind','setup','before','after','snapshots','report','transcript','opened',
        'terminal','stdout','stderr','signal','client_terminal','client_stdout','client_stderr','observations',
        'inputs','input_contents','native','event_bytes','native_result','catalog_after','backend_count','gates',
        'input_files','input_files_after','backend','native_readback'}
    assert context['kind'] == 'initial'
    check_upgrade(context['setup'])
    assert context['setup']['status'] == 'APPLIED_EXACT39'
    same(context['before'], context['setup']['after'])
    assert context['catalog_after'] == context['setup']['catalog_after']
    assert context['backend_count'] == 0
    from installed_family_capture import backend_checked
    assert set(context['backend'])=={'absence','exact','observed_monotonic_ns'}
    backend_checked(dict(absence=context['backend']['absence'],exact=context['backend']['exact'],all=[]), context['backend']['absence']['database'],
        context['before'],context['after'])
    readback=context['native_readback']
    assert set(readback)=={'returncode','stdout','stderr','started_monotonic_ns','finished_monotonic_ns'}
    assert readback['returncode']==0 and readback['stderr']=='' and json.loads(readback['stdout'])==context['native']
    assert 0<context['backend']['observed_monotonic_ns']<=readback['started_monotonic_ns']<=readback['finished_monotonic_ns']
    terminal = context['terminal']
    assert terminal['exit_code'] == 0 and terminal['forced'] is False and terminal['reaped'] is True
    assert 0 < terminal['elapsed_seconds'] < 45 and terminal['pid'] > 0
    assert str(terminal['start_ticks']).isdecimal() and int(terminal['start_ticks']) > 0
    assert terminal['prefix'] == '/lab/sync-signed'
    assert context['stderr'] == context['client_stdout'] == context['client_stderr'] == ''
    assert context['client_terminal'] == {'exit_code':0}
    reports = [json.loads(line) for line in context['stdout'].splitlines()]
    assert reports == [context['report'], dict(format='bigbrotr-initial-ingest/1',state='stopped',
        native='closed',owner_release='committed',source_closure='not-performed',ready=False)]
    assert context['report']['state'] == 'initialized' and context['report']['acquisition'] == 'finite-initial'
    snapshots = context['snapshots']
    assert [s['stage'] for s in snapshots] == ['initialized','prepared','progress-mutation',
        'progress-proof-only','durable','before-stop']
    event_raw = bytes.fromhex(context['event_bytes'])
    assert sha(event_raw) == SIGNED_SHA256
    event = json.loads(event_raw)
    assert event == OLD_EVENT  # Preserve the frozen raw JSON member order too.
    phases = wires_checked(context['opened'],context['transcript'],snapshots,event,context['gates'])
    signal = context['signal']
    quiet = only(context['transcript'],number=signal['round'])
    assert quiet['completion'] == 'quiet-eof' and not quiet['frames']
    assert signal['closed_before_signal'] is False and signal['request'] == quiet['request']
    assert quiet['request_at_us'] <= signal['signal_at_us'] <= quiet['closed_at_us']
    before, after = typed.legacy_state(context['before']), typed.legacy_state(context['after'])
    projected = [typed.legacy_state(s['control']) for s in snapshots]
    initial, prepared, mutation, proof, durable, stopping = projected
    assert not before['owner_epochs'] and not before['producer_requests']
    assert {t for t in before if before[t] != initial[t]} <= BOOTSTRAP
    assert len(initial['producer_requests']) == len(initial['producer_outcomes']) == 2
    source = only(initial['source_streams'])
    assert source['state'] == 'open' and source['last_complete_sequence'] is None, 'source lifecycle changed'
    producer = only(initial['producer_epochs'])
    assert producer['state'] == 'open' and producer['producer_epoch'] == producer['owner_epoch'] == 1
    assert producer['reserved_through_request'] == 2 and producer['outstanding_request_limit'] == 32
    assert producer['xmin'] == only(initial['producer_requests'],request_sequence=2)['xmin']
    for state in (prepared,mutation,proof,durable,stopping,after):
        assert state['source_streams'] == initial['source_streams'], 'source lifecycle changed'
        for table in ('producer_requests','producer_outcomes'):
            assert all(row in state[table] for row in initial[table]), 'retained bootstrap originals changed'
        expected_producer = dict(producer)
        if state is not prepared:
            expected_producer.update(reserved_through_request=4,
                xmin=only(state['producer_requests'],request_sequence=4)['xmin'])
        assert state['producer_epochs'] == [expected_producer], 'producer lifecycle changed'
    unchanged(initial,prepared,COMMANDS)
    assert not prepared['jobs'] and prepared['producer_requests'] == initial['producer_requests']
    observations = context['observations']
    validate_source(prepared,context['report'],observations)
    for state in (mutation,proof,durable):
        unchanged(prepared,state,('jobs','acquisition_jobs','job_admissions','management_commands',
            'management_audit','producer_epochs','producer_requests','producer_outcomes'))
        validate_source(state,context['report'],observations)
        validate_queued(state,observations)
        for table in ('producer_requests','producer_outcomes'):
            same({table:state[table]},{table:after[table]})
        assert all(r in state['management_commands'] for r in prepared['management_commands'])
        assert all(r in state['management_audit'] for r in prepared['management_audit'])
        assert len(state['management_commands']) == len(prepared['management_commands'])+1
        assert len(state['management_audit']) == len(prepared['management_audit'])+1
    logged_out(durable,stopping); released(stopping,after)
    actual = context['native']
    assert set(actual) == {'event','rows','sequence'} and actual['event'] == 'dump-state'
    # One initial manifest write, twelve bootstrap entries (manifest included),
    # eight private-map, twenty attribution and sixteen signed-event entries.
    assert type(actual['sequence']) is int and actual['sequence'] == 1+12+8+20+16
    values = {(r['cf'],bytes.fromhex(r['key'])):bytes.fromhex(r['value']) for r in actual['rows']}
    assert len(values) == len(actual['rows']) == 49
    facts = {int.from_bytes(key[-8:],'big'):decode(value[34:].split(b'\0',1)[1])
        for (cf,key),value in values.items() if cf == 'facts' and key[:2] == b'\x01\x20'}
    dictionary_at, recorded, received = facts[4][4],facts[8][4],facts[11][5][1]
    resolution = phases['mutation']['clock_resolution_us']
    assert phases['mutation']['sent_at_us']-resolution <= received <= phases['mutation']['closed_at_us']+resolution
    assert only(after['jobs'])['created_at_us'] <= dictionary_at <= phases['mutation']['connected_at_us']
    assert received <= recorded <= snapshots[-1]['observed_by_us']
    assert context['native_result'] == dict(native_rows=49,original_requests=4,
        dictionary_recorded_us=dictionary_at,event_received_us=received,event_recorded_us=recorded)
    wanted, inputs, receipts = initial_science(after,context['report'],event,dictionary_at,received,recorded,resolution)
    assert sorted((cf,key,value) for (cf,key),value in values.items()) == wanted
    slot = context['report']['producer_slot_id']
    expected_inputs = {slot+'-0000000000000001-'+f'{n:016x}.cbor':dict(bytes=len(raw),sha256=sha(raw))
        for n,raw in enumerate(inputs,1)}
    assert context['inputs'] == expected_inputs
    assert context['input_contents'] == {slot+'-0000000000000001-'+f'{n:016x}.cbor':raw.hex()
        for n,raw in enumerate(inputs,1)}
    input_custody(context['input_files'],context['input_files_after'],expected_inputs)
    for index, snapshot in enumerate(snapshots):
        assert snapshot['inputs'] == {name:value for name,value in expected_inputs.items()
            if index >= 2 or int(name[-21:-5],16) <= 2}
        assert snapshot['server_us'] <= snapshot['observed_by_us']
        if index:
            assert snapshots[index-1]['observed_by_us'] <= snapshot['server_us']
    control = only(after['owner_epochs'],role='control')
    scope = only(after['acquisition_scopes'])
    enrollment = only(after['relay_enrollments'])
    job = only(after['jobs'])
    manifest = values['default',b'\x01\x01']
    common = dict(dataset_id=after['datasets'][0]['dataset_id'],producer_slot_id='\\x'+slot,
        producer_epoch=1,manifest_sha256='\\x'+sha(manifest))
    for n,(raw,receipt) in enumerate(zip(inputs,receipts),1):
        request = only(after['producer_requests'],request_sequence=n)
        outcome = only(after['producer_outcomes'],request_sequence=n)
        source = dict(relay_endpoint_id=None,relay_access_scope_id=None,relay_enrollment_generation=None,
            relay_route_id=None,acquisition_scope_id=None,acquisition_job_id=None)
        if n == 4:
            source.update(relay_endpoint_id=enrollment['endpoint_id'],relay_access_scope_id=enrollment['access_scope_id'],
                relay_enrollment_generation=1,relay_route_id=enrollment['route_id'],acquisition_scope_id=scope['scope_id'],
                acquisition_job_id=job['job_id'])
        assert request == dict(common,request_sequence=n,request_digest='\\x'+sha(raw),input_bytes=len(raw),input_profile=1,
            issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_role='control',
            issuer_incarnation=control['process_incarnation'],reserved_at_us=request['reserved_at_us'],xmin=request['xmin'],**source)
        assert outcome == dict(common,request_sequence=n,request_digest='\\x'+sha(raw),outcome_kind=1,policy_epoch=1,
            outcome_bytes='\\x'+receipt.hex(),outcome_sha256='\\x'+sha(receipt),recorder_resource=control['resource_id'],
            recorder_epoch=control['epoch'],recorder_role='control',recorder_incarnation=control['process_incarnation'],
            recorded_at_us=outcome['recorded_at_us'],xmin=outcome['xmin'])
        assert control['acquired_at_us'] <= request['reserved_at_us'] <= outcome['recorded_at_us'] < control['lease_expires_at_us']
        assert request['xmin'] != outcome['xmin']
    assert only(after['producer_epochs'])['reserved_through_request'] == 4
    assert only(after['producer_epochs'])['outstanding_request_limit'] == 32
    event_input = decode(inputs[3].split(b'\0',1)[1])[3]
    observation = event_input[5]
    return typed_checked('initial',context['before'],context['after'],snapshots,phases,event,
        {(cf,key.hex()):value.hex() for cf,key,value in wanted},dict(enumerate(inputs,1)),dict(enumerate(receipts,1)),
        event_input[1],observation[1],observation[3],control)


def verify_live(context):
    from installed_live_oracle import verify, native_plan, NetworkScience, BODY, SIGNATURE
    from initial_gap_policy import require_guards
    assert set(context) == {'mode','original','after','native_before','native_after','inputs_before','inputs_after',
        'input_contents','snapshots','response','journey','fixture','server_window','backend','catalog_before',
        'catalog_after','seed','setup','predecessor','progress_setup','progress_catalog_after','progress_gates',
        'progress_input_files','progress_input_files_after'}
    assert context['mode'] == 'installed-live-on' and context['predecessor'] is None
    setup = context['progress_setup']
    check_upgrade(setup)
    assert setup['status'] == 'APPLIED_EXACT39'
    same(context['original'],setup['after']); same(setup['before'],context['setup']['after'])
    assert context['progress_catalog_after'] == setup['catalog_after']
    assert context['catalog_before'] == context['catalog_after']
    require_guards(context['catalog_before'])
    projected = copy.deepcopy(context)
    for key in ('progress_setup','progress_gates','progress_catalog_after','progress_input_files','progress_input_files_after'):
        projected.pop(key)
    projected['original'],projected['after'] = project32(context['original']),project32(context['after'])
    assert [s['stage'] for s in context['snapshots']] == ['activated','prepared','progress-mutation',
        'progress-proof-only','submitted','renewed','before-stop']
    projected['snapshots'] = [dict(s,control=project32(s['control'])) for s in context['snapshots']
        if not s['stage'].startswith('progress-')]
    projected['fixture']['renewal']['current'] = copy.deepcopy(projected['snapshots'][3])
    # The pinned schema32 catalog validates the reused72-table model. Actual39
    # definitions/ACLs remain separate exact before/after pg_dump observations.
    projected['catalog_before'] = projected['catalog_after'] = setup['reference_catalog32']
    base = verify(**projected,split=True)
    input_custody(context['progress_input_files'],context['progress_input_files_after'],context['inputs_after'])
    for cut in context['snapshots'][2:4]:
        intermediate = copy.deepcopy(projected)
        intermediate['snapshots'][2] = dict(cut,stage='submitted',control=project32(cut['control']))
        verify(**intermediate,split=True)
    report = json.loads(context['response']['stdout'].splitlines()[0])
    plan, rows = native_plan(context['seed'],projected['snapshots'][0]['control'],report)
    science = NetworkScience(plan,rows,context['seed']['inputs'])
    actual = {(r['cf'],bytes.fromhex(r['key'])):bytes.fromhex(r['value']) for r in context['native_after']['rows']}
    fact = lambda n:decode(actual['facts',b'\x01\x20'+science.src_key(n)][34:].split(b'\0',1)[1])
    science.network(True,fact(1)[4],fact(8)[5][1],fact(5)[4],fact(8)[5][3][1])
    assert actual == science.rows
    event = dict(pubkey=BODY[1].hex(),created_at=BODY[2],kind=BODY[3],tags=BODY[4],content=BODY[5],
        id=BODY[0].hex(),sig=SIGNATURE.hex())
    phases = wires_checked(context['journey']['opened'],context['journey']['closed'],context['snapshots'],
        event,context['progress_gates'])
    result = typed_checked('activated',context['original'],context['after'],context['snapshots'],phases,event,
        {(cf,key.hex()):value.hex() for (cf,key),value in science.rows.items()},science.requests,science.receipts,
        science.scope,science.ref(2),[science.ref(1),science.ref(3),science.ref(5)],only(context['after']['owner_epochs'],role='control',epoch=8))
    return dict(result,legacy_science=base)


def controls(context, verify):
    verify(context)
    results = []
    def reject(name, change, reason=None):
        value = copy.deepcopy(context)
        change(value)
        try:
            verify(value)
        except (AssertionError,KeyError,TypeError,ValueError,IndexError,StopIteration) as error:
            if reason is not None:
                assert isinstance(error,AssertionError) and str(error)==reason, (name,'wrong rejection',str(error))
            results.append(name)
        else:
            raise AssertionError('progress context accepted counterexample: '+name)
    phase = lambda c:next(s for s in c['snapshots'] if s['stage']=='progress-proof-only')['control']
    after = lambda c:c['after']
    reject('unrelated-sql-row',lambda c:after(c)['network_routes'].append(copy.deepcopy(after(c)['network_routes'][0])))
    reject('missing-dependency',lambda c:after(c)['producer_checkpoint_dependencies'].pop())
    reject('changed-native-value',lambda c:(c['native'] if 'native' in c else c['native_after'])['rows'][0].update(value='00'))
    reject('changed-original-input',lambda c:c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    reject('foreign-progress-issuer',lambda c:after(c)['producer_checkpoints'][0].update(issuer_epoch=999))
    reject('head-complete-overadvance',lambda c:after(c)['producer_checkpoints'][0].update(complete_through_request=5))
    reject('proof-revision-missing',lambda c:phase(c)['acquisition_progress'][0].update(progress_revision=1))
    reject('proof-mutating-request',lambda c:phase(c)['acquisition_progress'][0].update(mutation_request=1))
    reject('proof-reused-request',lambda c:phase(c)['acquisition_progress'][0].update(request_id='\\x'+b'1'.hex()*48))
    reject('unknown-head-field',lambda c:after(c)['producer_checkpoints'][0].update(unknown=True))
    reject('unbound-raw-input',lambda c:after(c)['acquisition_progress'][0].update(input_sha256='\\x'+'00'*32))
    reject('unbound-semantic-input',lambda c:after(c)['acquisition_progress'][0].update(intent_digest='\\x'+'00'*32))
    reject('false-job-completion',lambda c:only(after(c)['jobs'],request_key='\\x'+('b1' if 'kind' in c else 'f4')*16).update(checkpoint_revision=1))
    reject('unknown-relation',lambda c:after(c).update(unmodeled=[]))
    reject('historical-xmin-rewrite',lambda c:after(c)['producer_requests'][0].update(xmin='4294967294'))
    reject('native-sequence-extra-write',lambda c:(c['native'] if 'native' in c else c['native_after']).update(sequence=999))
    reject('input-inode-replacement',lambda c:(c['input_files_after'] if 'kind' in c else c['progress_input_files_after'])[
        next(name for name in (c['input_files_after'] if 'kind' in c else c['progress_input_files_after']) if name!='.')].update(inode=1))
    if 'kind' in context:
        def closing_producer(c):
            for state in [s['control'] for s in c['snapshots'] if s['stage'] in ('durable','before-stop')]+[c['after']]:
                only(state['producer_epochs']).update(state='closing')
        def closed_source(c):
            for state in [s['control'] for s in c['snapshots']]+[c['after']]:
                only(state['source_streams']).update(state='closed',last_complete_sequence=12)
            # Rebind the raw transport captures: reject semantic lifecycle drift,
            # rather than an incidental stale observation checksum.
            rebind_gates(c)
        def bootstrap_rewrite(c):
            for state in [s['control'] for s in c['snapshots'] if s['stage'] not in ('initialized','prepared')]+[c['after']]:
                only(state['producer_requests'],request_sequence=1).update(xmin='4294967294')
            rebind_gates(c)
        reject('coherent-producer-closing',closing_producer,'producer lifecycle changed')
        reject('coherent-source-closed',closed_source,'source lifecycle changed')
        reject('coherent-bootstrap-xmin-rewrite',bootstrap_rewrite,'retained bootstrap originals changed')
        reject('native-before-backend-exclusion',lambda c:c['native_readback'].update(started_monotonic_ns=1))
        reject('unjoined-cli',lambda c:c['terminal'].update(reaped=False))
        reject('backend-not-absent',lambda c:c.update(backend_count=1))
        reject('schema-drift',lambda c:c.update(catalog_after=c['catalog_after']+'changed'))
    else:
        reject('unjoined-cli',lambda c:c['response']['terminal'].update(reaped=False))
        reject('backend-not-absent',lambda c:c['backend']['absence']['attempts'][-1].update(count=1))
        reject('schema-drift',lambda c:c.update(progress_catalog_after=c['progress_catalog_after']+'changed'))
    return dict(status='PASS',rejected=len(results),cases=results)


def rebind_gates(context):
    gates=context['gates'] if 'kind' in context else context['progress_gates']
    wires=[r for r in (context['transcript'] if 'kind' in context else context['journey']['closed'])
        if 'progress_phase' in r]
    for phase in ('mutation','proof-only'):
        wire=only(wires,progress_phase=phase)
        cut=only(context['snapshots'],stage='progress-'+phase)
        gates['progress-'+phase+'-observation.json']=json.dumps(dict(
            format='bigbrotr-progress-transport-observation/1',phase=phase,observation=cut,wire=wire))
        if phase=='mutation':
            gate=dict(format='bigbrotr-progress-transport-release/1',mutation_request=wire['request'][1],
                observation_sha256=sha(gates['progress-mutation-observation.json'].encode()))
            gates['progress-release.json']=json.dumps(gate)
            only(wires,progress_phase='proof-only')['progress_release']=dict(gate,
                release_sha256=sha(gates['progress-release.json'].encode()))


def replay(root, kind):
    """Read original files only; retained custody is not a current resource audit.

    Reuse the family reader's strict JSON and physical-file checks. No SQL,
    engine, subprocess or capture rewrite is required.
    """
    import tomllib
    from installed_family_capture import loads, physical
    from inventory import projection
    assert kind in ('initial','activated')
    root=Path(root).resolve(strict=True); folder=root/'bindings'
    bound={}
    def raw(path):
        path=Path(path)
        assert stat.S_ISREG(path.lstat().st_mode), 'raw capture must be regular'
        value=path.read_bytes(); bound[str(path)]=sha(value); return value
    def read(path): return loads(raw(path))
    report=read(folder/'result.json')
    assert report['status']=='PASS_INSTALLED_'+kind.upper()+'_TYPED_PROGRESS'
    assert report['wave']==kind+'-typed-progress' and report['label']=='org.bigbrotr.acquisition-binding'
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and re.fullmatch('[0-9a-f]{32}',report['owner'])
    assert all(report[key]==[] for key in ('cleanup_errors','missing_owned','unrelated_changed'))
    provenance=read(folder/'progress-source.json')
    assert set(provenance)=={'source','revision','tree','build','files_sha256'}
    assert Path(provenance['source'])==root/'source' and provenance['revision']==report['source']
    assert re.fullmatch('[0-9a-f]{40}',provenance['tree'])
    for name,digest in provenance['files_sha256'].items():
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        assert sha(raw(root/'source'/name))==digest
    build=Path(provenance['build']); assert build.is_absolute()
    built,component=read(build/'build-result.json'),read(build/'component-result.json')
    qualification=read(build.parent/'qualification.json')
    assert component['status']=='PASS' and qualification['status']=='PASS_PRODUCER_PROGRESS_COMPONENTS'
    assert built['source']==component['source']==qualification['source']==typed.require_frozen_profile()['product_source']
    for key in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert built[key]==component[key]==qualification[key]
    assert set(component['executables_sha256'])=={'bigbrotr','ingest_bootstrap_probe','native_components',
        'sync_progress','fact_codec','evidence_intent','control_schema','migrate_entrypoint'}
    for name,digest in component['executables_sha256'].items():
        assert sha(raw(build/name))==digest
    assert component['executables_sha256']['bigbrotr']==built['binary_sha256']
    assert report['build']==dict(built,probe_name='ingest_bootstrap_probe',
        probe_sha256=component['executables_sha256']['ingest_bootstrap_probe'])
    manifest_bytes=raw(build.parent/'build/source-manifest.json')
    assert sha(manifest_bytes)==built['source_manifest_sha256']
    manifest=loads(manifest_bytes)
    assert manifest['revision']==built['source'] and manifest['tree']==built['tree']
    # This is the runtime source-delta rule: all product/build/other tracked
    # inputs match the compiled manifest; only governance/fixture files differ.
    allow=lambda name:name in ('AGENTS.md','PLAN.md') or name.startswith(('docs/','experiments/'))
    actual=provenance['files_sha256']; compiled=manifest['files_sha256']
    assert all(allow(name) or actual.get(name)==compiled.get(name) for name in set(actual)|set(compiled))
    assert actual['migrations/postgres/0039_producer_progress.sql']==typed.FROZEN_PROFILE['migration_sha256']
    initial=kind=='initial'; label='sync-signed' if initial else 'installed-live-on'
    directory=folder/label; prefix=folder/label
    expected_helpers=(('installed_client.py','session_client.py','installed_sync_client.py',
        'installed_sync_expiry.py','installed_sync_window.py') if initial else ('installed_live_client.py','installed_sync_client.py'))
    for name in (*expected_helpers,'installed_progress_transport.py','installed_family_client.py'):
        relative=('experiments/ingest-succession/' if name=='installed_family_client.py' else 'experiments/ingest-bootstrap/')+name
        assert raw(folder/name)==raw(root/'source'/relative)
    assert read(directory/'progress-context.json')==dict(format='bigbrotr-progress-context/1',kind=kind)
    originals=read(folder/'progress-originals-before.json')
    assert set(originals)=={'bb_scope_54d1a18_basic','bb_source_template'}
    assert originals==read(folder/'progress-originals-after.json')
    original_basis=root.parent/'acquisition-scope-54d1a18/engine'
    assert originals['bb_scope_54d1a18_basic']==read(original_basis/'basic-after.json')
    assert read(original_basis/'result.json')['status']=='PASS_SCOPED_ACQUISITION_REGISTRATION'
    assert originals['bb_source_template'] and not originals['bb_source_template']['producer_requests']
    assert not originals['bb_source_template']['owner_epochs']
    inventory=read(folder/'progress-container-inventory.json')
    assert len({c['Id'] for c in inventory})==len(inventory)
    current={c['Id']:projection(c) for c in inventory}
    old=read(folder/'preflight.json')['containers']
    pg_id,client_id=report['postgres_id'],report['client']['id']
    assert client_id not in {c['Id'] for c in old}
    assert set(current)=={c['Id'] for c in old}|{client_id}
    assert all(current[c['Id']]==c for c in old if c['Id']!=pg_id)
    assert report['terminal']==[current[pg_id],current[client_id]]
    for ident,owner,label_key in ((pg_id,report['postgres_owner'],'org.bigbrotr.control-migration-lab'),
            (client_id,report['owner'],report['label'])):
        container=only(inventory,Id=ident); state=container['State']
        assert container['Config']['Labels'][label_key]==owner
        assert state['Status']=='exited' and state['Pid']==0 and not state['Error']
        assert all(state[k] is False for k in ('Running','Paused','Restarting','OOMKilled','Dead'))
        assert state['ExitCode'] in ((0,) if ident==pg_id else (0,143))
    client=only(inventory,Id=client_id)
    assert client['Name']=='/bigbrotr-acquisition-binding-'+report['source'][:7]+'-progress-'+kind
    assert only(client['Mounts'],Destination='/lab')['Source']==str(folder)
    mount=only(client['Mounts'],Destination='/build')
    assert mount['Source']==str(build) and mount['RW'] is False
    assert not any(c['State']['Running'] and any(m.get('Source','')==str(folder) for m in c['Mounts']) for c in inventory)
    context_file=folder/(label+('-progress-verification.json' if initial else '-verification.json'))
    context=read(context_file)
    setup=context['setup'] if initial else context['progress_setup']
    assert setup==read(folder/(label+'-progress-upgrade.json'))
    assert read(folder/(label+'-progress-catalog.json'))==context['catalog_after' if initial else 'progress_catalog_after']
    identity=read(Path(str(prefix)+'.pid.json')); terminal=read(Path(str(prefix)+'.terminal.json'))
    assert set(terminal)==set(identity)|{'exit_code','reaped','forced','elapsed_seconds'}
    assert all(terminal[k]==v for k,v in identity.items())
    assert identity['prefix']=='/lab/'+label and identity['pid']>0 and int(identity['start_ticks'])>0
    assert terminal['exit_code']==0 and terminal['reaped'] is True and terminal['forced'] is False
    assert 0<terminal['elapsed_seconds']<(45 if initial else 55)
    config=tomllib.loads(raw(folder/Path(identity['config']).name).decode())
    database=config['postgres']['control']['database']
    assert database in report['databases'] and config['storage']['ingest_root']=='/lab/'+label+'/store'
    assert config['runtime']['lease_ttl']=='6s' and config['runtime']['graceful_shutdown_timeout']=='2s'
    gates=context['gates'] if initial else context['progress_gates']
    for name,value in gates.items(): assert value==raw(directory/name).decode()
    observations=context['snapshots']
    assert observations==read(folder/(label+('-progress-observations.json' if initial else '-observations.json')))
    for cut in observations:
        path=folder/(label+('-progress-' if initial else '-')+cut['stage']+'-sql.json')
        assert cut['control']==read(path)
    assert context['after']==read(folder/(label+('-progress-after-sql.json' if initial else '-after-sql.json')))
    files=context['input_files'] if initial else context['progress_input_files']
    for phase in ('mutation','proof-only'):
        assert files[phase]==read(directory/('progress-'+phase+'-input-files.json'))
    final_files=context['input_files_after'] if initial else context['progress_input_files_after']
    assert physical(directory/'store/producer-inputs-v1',raw)==final_files
    assert set(context['input_contents'])==set(final_files)-{'.'}
    for name,content in context['input_contents'].items():
        assert raw(directory/'store/producer-inputs-v1'/name).hex()==content
    opened=context['opened'] if initial else context['journey']['opened']
    closed=context['transcript'] if initial else context['journey']['closed']
    for stem,values in (('open',opened),('closed',closed)):
        paths=sorted(directory.glob('relay-'+stem+'-*.json'))
        assert [p.name for p in paths]==[f'relay-{stem}-{n:03d}.json' for n in range(1,len(values)+1)]
        assert [read(p) for p in paths]==values
    assert not (directory/'relay-error.json').exists()
    if initial:
        assert read(folder/(label+'-progress-invocation.json'))==dict(command=[
            'python3','/lab/installed_client.py',identity['config'],'/lab/'+label,'sync'])
        assert terminal==context['terminal']
        for stream in ('stdout','stderr'): assert raw(Path(str(prefix)+'.'+stream)).decode()==context[stream]
        assert context['signal']==read(directory/'signal-observation.json')
        assert context['client_terminal']==read(directory/'client-exit.json')
        for stream in ('stdout','stderr'): assert raw(directory/('client.'+stream)).decode()==context['client_'+stream]
        assert context['observations']==read(directory/'submitted.json')
        assert context['event_bytes']==raw(directory/'event.jsonl').hex()
        assert raw(directory/'event.jsonl')==raw(root/'source/tests/fixtures/ingest/import-event.jsonl')
        assert context['native']==read(folder/(label+'-native.json'))
        assert context['native_readback']==read(folder/(label+'-dump-response.json'))
        assert context['backend']==read(folder/(label+'-backend-exclusion.json'))
        assert context['backend']['absence']==read(folder/(label+'-backends.json'))
        assert context['backend']['absence']['database']==database
        assert typed.legacy_state(context['before'])==read(folder/(label+'-before.json'))
        assert typed.legacy_state(context['after'])==read(folder/(label+'-after.json'))
        answer=verify_initial(context); negative=controls(context,verify_initial)
        case,=report['cases']
        assert case['verification_sha256']==sha(raw(context_file))
    else:
        import initial_succession as seed_owner
        basis=root.parent/seed_owner.SEED/'bindings'
        for name,digest in seed_owner.FROZEN.items(): assert sha(raw(basis/name))==digest
        seed=read(folder/'installed-live-source.json')
        assert context['seed']=={k:seed[k] for k in ('control','native','inputs','sequence')}
        assert seed['control']==read(basis/'retire-signed-retry-after.json')
        assert seed['native']==read(basis/'retire-signed-retry-native.json')
        assert physical(basis/'retire_signed/store',raw)==seed['files']
        assert read(folder/'installed-live-source-preserved.json')==dict(control=True,files=True,inputs=True,artifacts=True)
        assert context['setup']==read(folder/(label+'-policy.json'))
        assert context['original']==read(folder/(label+'-before-sql.json'))
        for stage in ('before','after'):
            dumped=read(folder/(label+'-'+stage+'-dump-response.json'))
            assert dumped['returncode']==0 and dumped['stderr']==''
            assert loads(dumped['stdout'])==context['native_'+stage]==read(folder/(label+'-'+stage+'-native.json'))
        response=read(folder/(label+'-response.json'))
        assert response==context['response'] and response['terminal']==terminal
        assert identity['command']==['/build/bigbrotr','serve','--file',identity['config'],
            '--activate-initial-successor','--roles','control,ingest,sync']
        for stream in ('stdout','stderr'): assert raw(Path(str(prefix)+'.'+stream)).decode()==response[stream]
        assert context['fixture']==read(folder/(label+'-fixture.json'))
        assert context['server_window']==read(folder/(label+'-clock.json'))
        external=read(folder/(label+'-external.json'))
        assert external==dict(control=context['after'],native=context['native_after'],inputs=context['inputs_after'],
            backend=context['backend'],catalog=context['catalog_after'])
        assert context['backend']['absence']==read(folder/(label+'-backends.json'))
        journey=context['journey']
        for name,filename in (('result','client-result.json'),('terminal','client-exit.json')):
            assert journey[name]==read(directory/filename)
        for stream in ('stdout','stderr'): assert journey[stream]==raw(directory/('client.'+stream)).decode()
        assert context['fixture']['signal']==read(directory/'signal-observation.json')
        commands=[read(directory/f'command-{n}.json') for n in range(1,5)]
        assert journey['result']['observations']==commands
        assert read(directory/'prepared.json')==commands[:3] and read(directory/'submitted.json')==commands
        assert read(directory/'commands-finished.json')==commands
        assert raw(directory/'event.jsonl')==raw(basis/'signed-seed/event.jsonl')
        assert raw(directory/'selector-events.json')==raw(root/'source/tests/fixtures/acquisition/selector-events.json')
        answer=verify_live(context); negative=controls(context,verify_live)
        group,=report['cases']; case,=group['cases']
        assert group['cli_invocations']==1 and group['source_preserved']==read(folder/'installed-live-source-preserved.json')
        assert group['source_lab']==str(basis.parent) and group['source_revision']==seed_owner.SOURCE
        assert group['source_owner']==seed_owner.OWNER and group['source_artifacts']==seed_owner.FROZEN
        assert read(folder/'installed-live-cases.json')==group['cases']
    assert case['status']=='PASS' and case['oracle']==answer and case['negative_controls']==negative
    return dict(status='PASS_RAW_CONTEXT_REPLAY',kind=kind,oracle=answer,negative_controls=negative,
        raw_files_sha256=bound,custody='RETAINED_OBSERVATIONS_ONLY_ROOT_CURRENT_AUDIT_SEPARATE')


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    parser.add_argument('--kind',choices=('initial','activated'),required=True)
    arguments=parser.parse_args()
    print(json.dumps(replay(arguments.root,arguments.kind),sort_keys=True))
