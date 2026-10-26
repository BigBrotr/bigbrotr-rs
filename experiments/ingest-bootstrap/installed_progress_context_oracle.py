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
# These are the exact IDs already applied at catalog target26. Reserved
# management/artifact/cursor IDs are not later application chronology.
CORE26 = {1,2,3,4,*range(7,27)}
SUPPORT26 = {501,601,602,604,801,802,*range(805,817)}
PROFILES = {
    'initial': dict(epoch=1, request=4, mutation_revision=4, dependencies=tuple((1,n) for n in range(1,13)), key='b1'),
    'activated': dict(epoch=2, request=2, mutation_revision=2, dependencies=((1,3), *tuple((2,n) for n in range(1,10))), key='f4'),
}


def sha(value):
    return hashlib.sha256(value).hexdigest()


def archive_request_progress(event, transcript):
    """The unchanged Archive peer sends two signed/forged/duplicate quanta.

    The forged signatures remain structurally valid captured inputs and reach
    native certification rejection. All three EVENTs classify in each REQ;
    quiet later REQs produce neither reservations nor classified progress.
    """
    assert 2 <= len(transcript) <= 128
    nonces = set()
    previous_closed = None
    for number, wire in enumerate(transcript, 1):
        assert wire['number'] == number
        command = wire['request']
        assert len(command) == 3 and command[0] == 'REQ'
        assert command[2] == {'kinds': [1], 'limit': 3}
        request = command[1]
        assert re.fullmatch('[0-9a-f]{48}', request) and int(request[:32], 16)
        assert request[:32] not in nonces
        nonces.add(request[:32])
        for key in ('connected_at_us', 'request_at_us', 'closed_at_us'):
            typed.integer(wire[key])
        assert wire['connected_at_us'] <= wire['request_at_us'] <= wire['closed_at_us']
        assert previous_closed is None or previous_closed <= wire['connected_at_us']
        previous_closed = wire['closed_at_us']
        typed.uint(wire['clock_resolution_us'], minimum=1)
        if number <= 2:
            assert wire['frames'] == ['signed', 'forged-signature', 'duplicate', 'EOSE']
            assert wire['completion'] == 'request-and-websocket-close'
            typed.integer(wire['sent_at_us'])
            assert wire['request_at_us'] <= wire['sent_at_us'] <= wire['closed_at_us']
        else:
            assert wire['frames'] == [] and wire['completion'] == 'quiet-eof'
    # send_frame serializes the signed event object this way; this is the
    # original EVENT slice, not the producer-request CBOR or a captured SQL hash.
    raw = json.dumps(event, ensure_ascii=False, separators=(',', ':')).encode()
    return dict(progress_request_id='\\x'+transcript[0]['request'][1].encode().hex(),
        progress_event_ordinal=1, progress_wire_sequence=1,
        progress_input_sha256='\\x'+sha(raw))


def verify_archive_progress(state, transcript, completed_us, event, native, inputs, receipts, control):
    """Exact target40 head for the original one-event Archive recipe/window32.

    Three dictionary operations precede six classified EVENTs: new facts,
    rejected certification, existing proof, then existing/rejected/existing.
    CapturedEventInput preserves invalid signatures; CertificationRejected is a
    durable progress class, unlike a malformed JSON preparation refusal.
    """
    binding = archive_request_progress(event, transcript)
    assert {39, 40} <= {row['migration'] for row in state['schema_versions']}
    assert len(inputs) == len(receipts) == 4
    producer, = state['producer_epochs']
    assert producer['producer_epoch'] == 1 and producer['reserved_through_request'] == 4
    assert producer['outstanding_request_limit'] == 32
    common = dict(dataset_id=producer['dataset_id'], producer_slot_id=producer['producer_slot_id'],
        producer_epoch=1)
    acquisition, = state['acquisition_jobs']
    job = only(state['jobs'], job_id=acquisition['job_id'])
    assert job['request_key'] == '\\x'+'b1'*16
    assert job['state'] == 'queued' and job['checkpoint_revision'] == 0
    observed, = state['acquisition_progress']
    head, = state['producer_checkpoints']
    wire = transcript[1]
    resolution = wire['clock_resolution_us']
    classifications = (2, 6, 5, 5, 6, 5)
    expected_job = dict(common, job_id=job['job_id'], scope_id=acquisition['scope_id'],
        progress_revision=len(classifications), request_id='\\x'+wire['request'][1].encode().hex(),
        event_ordinal=3, wire_sequence=3, input_sha256=binding['progress_input_sha256'],
        classification=classifications[-1], mutation_request=None, classified_events=len(classifications),
        successful_events=4, rejected_events=2, observed_at_us=observed['observed_at_us'],
        clock_resolution_us=resolution, clock_anomaly_flags=4 if resolution > 1 else 0,
        xmin=observed['xmin'])
    dataset = bytes.fromhex(common['dataset_id'][2:])
    domains = {row['dictionary_kind']: bytes.fromhex(row['allocation_domain'][2:])
        for row in state['allocation_domains']}
    ref = lambda kind, ordinal=1: [dataset, kind, domains[kind], ordinal]
    expected_job['intent_digest'] = '\\x'+typed.certification_digest(event, ref(4, 3),
        ref(2), ref(2), [ref(1), ref(3), ref(5)], expected_job, wire).hex()
    assert head['xmin'] == expected_job['xmin']
    # This fresh epoch1 fixture has no XID wrap and serializes all four outcomes
    # before the final proof. Numeric ordering is not a general PG XID rule.
    outcome_xids = [int(typed.xid(row['xmin'])) for row in state['producer_outcomes']]
    assert len(outcome_xids) == 4
    assert int(typed.xid(head['xmin'])) > max(outcome_xids), 'final proof transaction follows outcomes'
    assert control['acquired_at_us'] <= wire['sent_at_us'] <= head['committed_at_us'] <= completed_us
    assert expected_job['observed_at_us'] <= head['committed_at_us'] < control['lease_expires_at_us']
    values = {(cf, key.hex()): value.hex() for cf, key, value in native}
    dependencies = []
    # Literal cumulative union from the four independently encoded receipts:
    # setup1:1..3, observation1:3..7, event1:3..12. No SQL row chooses membership.
    for sequence in range(1, 13):
        source = dataset + bytes(4) + (1).to_bytes(8, 'big') + sequence.to_bytes(8, 'big')
        value = bytes.fromhex(values['facts', (b'\x01\x20'+source).hex()])
        reference = [[dataset, 0, 1, sequence], value[2:34]]
        first = next(n for n, receipt in enumerate(receipts, 1)
            if reference in decode(receipt.split(b'\0', 1)[1])[5])
        outcome = only(state['producer_outcomes'], producer_epoch=1, request_sequence=first)
        dependencies.append(dict(common, source_shard=0, source_epoch=1,
            source_sequence=sequence, payload_digest='\\x'+value[2:34].hex(), xmin=outcome['xmin']))
    expected_head = dict(common, progress_format=1,
        manifest_sha256='\\x'+sha(bytes.fromhex(values['default', '0101'])),
        checkpoint_revision=3+len(classifications), complete_through_request=4, progress_jobs=1, dependency_count=12,
        issuer_resource=control['resource_id'], issuer_epoch=control['epoch'],
        issuer_incarnation=control['process_incarnation'], committed_at_us=head['committed_at_us'],
        xmin=head['xmin'], checkpoint_digest='\\x'+'00'*32)
    expected_head['checkpoint_digest'] = '\\x'+typed.head_digest(expected_head, [expected_job],
        dependencies, lambda row: typed.native_reference(row, values)).hex()
    actual_dependencies = sorted(state['producer_checkpoint_dependencies'],
        key=lambda row: (row['source_shard'], row['source_epoch'], row['source_sequence']))
    return typed.exact_snapshot(head, state['acquisition_progress'], actual_dependencies,
        expected_head, [expected_job], dependencies, values)


def check_upgrade(context, *, ack=False):
    profile=typed.require_frozen_profile(ack=ack)
    assert set(context) in ({'before','after','physical_before','physical_after','status','responses',
        'catalog_before','catalog_after','acl'}, {'before','after','physical_before','physical_after','status','responses',
        'catalog_before','catalog_after','acl','reference_catalog32'})
    before, after = context['before'], context['after']
    assert len(after) == 77 and not any(after[t] for t in (*typed.TABLES, 'producer_checkpoints'))
    old = typed.legacy_state(after,ack=ack)
    previous = {row['migration']:row for row in before['schema_versions']}
    current = {row['migration']:row for row in after['schema_versions']}
    assert len(previous)==len(before['schema_versions']) and len(current)==len(after['schema_versions']), 'duplicate migration receipt'
    core = set(previous)-SUPPORT26
    assert core in (CORE26, CORE26|set(range(27,33))), 'closed migration catalog prefix'
    version = 26 if core==CORE26 else 32
    acl = context['acl']
    assert set(acl)=={'role','relation','before_stdout','after_stdout','grants'}, 'closed activation ACL receipt'
    assert (acl['role'],acl['relation'])==('bigbrotr_control','control.initial_ingest_activations'), 'activation ACL subject'
    privileges = ('SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')
    expected = {p+suffix:False for p in privileges for suffix in ('',' WITH GRANT OPTION')}
    if version==32:
        expected.update(SELECT=True,INSERT=True)
    from installed_family_capture import loads
    prior, current_acl = loads(acl['before_stdout']), loads(acl['after_stdout'])
    assert prior==expected and all(type(v) is bool for v in prior.values()), 'activation ACL baseline'
    expected['SELECT']=True
    assert current_acl==expected and all(type(v) is bool for v in current_acl.values()), 'activation ACL exact read grant'
    assert acl['grants']==[
        dict(statement='GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',stdout=''),
        dict(statement=typed.PROGRESS_GRANTS,stdout='')], 'exact fixture grant statements'
    assert set(previous)==core|SUPPORT26, 'closed migration catalog prefix'
    target=40 if ack else 39
    assert set(current) == set(previous) | set(range(version + 1, target+1)), 'migration extension set'
    assert all(current[n] == row for n, row in previous.items()), 'retained migration changed'
    transactions = set()
    for n in range(version + 1, target+1):
        row = current[n]
        name, digest = ('0040_producer_ack.sql',profile['migration_sha256']) if n==40 else MIGRATIONS[n]
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


def project32(state, *, ack=False):
    value = typed.legacy_state(state,ack=ack)
    for table in EXTRA32:
        assert value.pop(table) == [], 'unmodeled later lifecycle row'
    # Project only the six new core receipts; retain unrelated higher IDs and
    # all their original metadata/xmins for the established schema32 model.
    value['schema_versions'] = [r for r in value['schema_versions'] if not 33 <= r['migration'] <= 38]
    assert len(value) == 72
    return value


def input_custody(cuts, after, inputs, *, preparation_hole=False):
    names=typed.phase_names(preparation_hole)
    assert set(cuts) == set(names)
    if preparation_hole:
        assert cuts['continuation']==after
        assert set(cuts['mutation'])=={'.',*(name for name in inputs if int(name[-21:-5],16)<=5)}
        assert all(metadata==after[name] for name,metadata in cuts['mutation'].items() if name!='.')
        # Directory size may grow as C/D are retained; its custody cannot move.
        assert all(cuts['mutation']['.'][key]==after['.'][key] for key in ('mode','inode','device','links','sha256'))
    else:
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


def ready(state, kind, phase, wire, *, ack=False, preparation_hole=False, post_hole_proof=False):
    assert type(post_hole_proof) is bool and (not post_hole_proof or ack and kind=='initial' and not preparation_hole)
    if preparation_hole:
        assert ack and kind=='initial' and phase in typed.phase_names(True) and len(state)==77
        heads,jobs=state['producer_checkpoints'],state['acquisition_progress']
        if len(heads)!=1 or len(jobs)!=1: return False
        head,job=heads[0],jobs[0]
        later=phase=='continuation'
        return (head['checkpoint_revision']==(8 if later else 5)
            and head['complete_through_request']==(7 if later else 5)
            and head['dependency_count']==(27 if later else 17) and head['progress_jobs']==1
            and job['progress_revision']==job['classified_events']==job['successful_events']==(4 if later else 1)
            and job['rejected_events']==0 and job['event_ordinal']==job['wire_sequence']==(3 if later else 1)
            and job['classification']==(5 if later else 2) and job['mutation_request']==(None if later else 4)
            and typed.bytea(job['request_id'],48)==wire['request'][1].encode())
    spec = typed.POST_HOLE_PROOF_EXPECTATIONS[kind] if post_hole_proof else typed.ACK_EXPECTATIONS[kind] if ack else PROFILES[kind]
    width=3 if ack else 1
    assert len(state) == 77 and phase in ('mutation', 'proof-only')
    head = [r for r in state['producer_checkpoints'] if r['producer_epoch'] == spec['epoch']]
    jobs = [r for r in state['acquisition_progress'] if r['producer_epoch'] == spec['epoch']]
    if len(head) != 1 or len(jobs) != 1:
        return False
    head, job = head[0], jobs[0]
    proof = phase == 'proof-only'
    proof_width=1 if post_hole_proof else width
    return (head['checkpoint_revision'] == spec['mutation_revision'] + proof_width*proof
        and head['complete_through_request'] == spec['request']
        and head['dependency_count'] == len(spec['dependencies']) and head['progress_jobs'] == 1
        and job['progress_revision'] == job['classified_events'] == job['successful_events'] == width+proof_width*proof
        and job['rejected_events'] == 0 and job['event_ordinal'] == job['wire_sequence'] == (proof_width if proof else width)
        and job['classification'] == (5 if proof else 2)
        and job['mutation_request'] == (None if proof else spec['request'])
        and typed.bytea(job['request_id'], 48) == wire['request'][1].encode())


def wires_checked(opened, closed, snapshots, event, gates, *, ack=False, successor=False, preparation_hole=False, post_hole_proof=False):
    assert type(successor) is bool and (not successor or ack)
    assert type(preparation_hole) is bool and (not preparation_hole or ack and not successor)
    assert type(post_hole_proof) is bool and (not post_hole_proof or ack and not successor and not preparation_hole)
    names=typed.phase_names(preparation_hole)
    if ack:
        assert set(gates)=={*('progress-'+name+'-observation.json' for name in names),'progress-release.json'}
    events=typed.ack_event_sequence(successor=successor) if ack else [event]
    assert 3 <= len(opened) == len(closed) <= 128
    phases, nonces = {}, set()
    for number, (first, last) in enumerate(zip(opened, closed), 1):
        opened_fields={'number','connected_at_us','clock_resolution_us','request','request_at_us','request_monotonic_ns'}
        assert set(first)==opened_fields
        extra={'frames','completion','closed_at_us','closed_monotonic_ns'}
        if 'progress_phase' in last:
            extra|={'progress_phase','event_ids','event_json_sha256','sent_at_us'}
            if last['progress_phase']==names[1]: extra.add('progress_release')
            if preparation_hole or post_hole_proof: extra.add('event_json_hex')
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
        assert phase in names and phase not in phases
        if preparation_hole or post_hole_proof:
            events=typed.phase_events(phase,preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)
            assert last['event_json_hex']==[json.dumps(e,ensure_ascii=False,separators=(',',':')).encode().hex() for e in events]
        hole_phase=preparation_hole and phase=='mutation' or post_hole_proof and phase=='proof-only'
        labels=['signed','malformed-event','signed'] if hole_phase else ['signed']*len(events)
        assert last['frames'] == labels+['EOSE'] and last['event_ids'] == [e['id'] for e in events]
        assert last['event_json_sha256'] == [sha(json.dumps(e,ensure_ascii=False,separators=(',',':')).encode()) for e in events]
        assert last['completion'] == 'request-and-websocket-close'
        assert last['request_at_us'] <= last['sent_at_us'] <= last['closed_at_us']
        phases[phase] = last
    assert list(phases) == list(names) and phases['mutation']['number'] == 1
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
    assert phases[names[1]]['progress_release'] == dict(gate, release_sha256=sha(gates['progress-release.json'].encode()))
    assert phases[names[1]]['sent_at_us'] >= observations['progress-mutation']['observed_by_us']
    return phases


def typed_checked(kind, before, after, snapshots, phases, event, native, inputs, receipts,
                  scope, observer, witness, control, *, ack=False, preparation_hole=False, post_hole_proof=False):
    assert not preparation_hole or ack and kind=='initial'
    assert type(post_hole_proof) is bool and (not post_hole_proof or ack and kind=='initial' and not preparation_hole)
    spec = typed.POST_HOLE_PROOF_EXPECTATIONS[kind] if post_hole_proof else typed.PREPARATION_HOLE_EXPECTATIONS[kind] if preparation_hole else typed.ACK_EXPECTATIONS[kind] if ack else PROFILES[kind]
    events=typed.preparation_hole_events() if preparation_hole else typed.ack_event_sequence(successor=kind=='successor') if ack else [event]
    names=typed.phase_names(preparation_hole)
    width=len(events)
    event=events[-1]
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
            if request['producer_epoch'] != epoch or not spec['request']-width < request['request_sequence'] <= spec['request']:
                assert all(request[key] is None for key in typed.REQUEST_ADDITIONS)
            else:
                if preparation_hole:
                    phase,ordinal,index={4:('mutation',1,0),5:('mutation',3,1),
                        6:('continuation',1,2),7:('continuation',2,3)}[request['request_sequence']]
                    wire=phases[phase]
                else:
                    wire = phases['mutation']
                    ordinal=request['request_sequence']-(spec['request']-width)
                    index=ordinal-1
                assert {key:request[key] for key in typed.REQUEST_ADDITIONS} == dict(
                    progress_request_id='\\x'+wire['request'][1].encode().hex(),
                    progress_event_ordinal=ordinal, progress_wire_sequence=ordinal,
                    progress_input_sha256='\\x'+sha(json.dumps(events[index],ensure_ascii=False,separators=(',',':')).encode()))
    cuts = {s['stage']:s for s in snapshots}
    mutation, proof = (cuts['progress-'+phase] for phase in names)
    if preparation_hole:
        assert mutation['inputs'].items() <= proof['inputs'].items()
        for table in ('producer_requests','producer_outcomes','producer_checkpoint_dependencies'):
            assert all(row in proof['control'][table] for row in mutation['control'][table]), 'pre-hole originals changed'
        equal_tables=('jobs','acquisition_jobs','job_admissions')
    else:
        assert mutation['inputs'] == proof['inputs']
        equal_tables=('producer_requests','producer_outcomes','producer_epochs','jobs','acquisition_jobs','job_admissions',
                      'producer_checkpoint_dependencies')
    for table in equal_tables:
        same({table:mutation['control'][table]}, {table:proof['control'][table]})
    assert int(only(mutation['control']['producer_checkpoints'])['xmin']) != int(only(proof['control']['producer_checkpoints'])['xmin'])
    def check(cut, phase=None, setup=False):
        state = cut['control']
        if not setup:
            wire = phases[phase]
            assert ready(state,kind,phase,wire,ack=ack,preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)
            actual = only(state['acquisition_progress'])
            accepted = only(state['jobs'], request_key='\\x'+spec['key']*16)
            assert accepted['checkpoint_revision'] == 0 and accepted['state'] == 'queued'
            detail = only(state['acquisition_jobs'],job_id=accepted['job_id'])
            is_proof = phase == names[1]
            current_event=events[1 if is_proof else 0] if preparation_hole else event
            ordinal=(3 if is_proof else 1) if preparation_hole else width
            classified=(4 if is_proof else 1) if preparation_hole else width*(1+is_proof)
            if post_hole_proof and is_proof:
                current_event,ordinal,classified=events[0],1,4
            job_request=4 if preparation_hole else spec['request']
            job = dict(common, job_id=accepted['job_id'],scope_id=detail['scope_id'],
                progress_revision=classified,request_id='\\x'+wire['request'][1].encode().hex(),
                event_ordinal=ordinal,wire_sequence=ordinal,input_sha256='\\x'+sha(json.dumps(current_event,ensure_ascii=False,separators=(',',':')).encode()),
                classification=5 if is_proof else 2, mutation_request=None if is_proof else job_request,
                classified_events=classified,successful_events=classified,rejected_events=0,
                observed_at_us=actual['observed_at_us'],clock_resolution_us=actual['clock_resolution_us'],
                clock_anomaly_flags=actual['clock_anomaly_flags'],xmin=actual['xmin'])
            if is_proof:
                digest = typed.certification_digest(current_event,scope,observer,observer,witness,job,wire).hex()
                assert all(job['xmin'] != row['xmin'] for rows in typed.legacy_state(state,ack=ack).values() for row in rows)
            else:
                raw = inputs[job_request]
                observation = decode(raw.split(b'\0',1)[1])[3][5]
                assert [job['observed_at_us'],job['clock_resolution_us'],job['clock_anomaly_flags']] == [
                    observation[0],observation[2][1],observation[2][3]]
                assert wire['sent_at_us']-job['clock_resolution_us'] <= observation[0] <= wire['closed_at_us']+job['clock_resolution_us']
                assert job['xmin'] == only(state['producer_outcomes'],producer_epoch=epoch,request_sequence=job_request)['xmin'], 'frozen prefix transaction changed'
                digest = sha(raw)
            job['intent_digest'] = '\\x'+digest
            jobs, revision, complete = [job], spec['mutation_revision']+width*is_proof, spec['request']
            coordinates = spec['dependencies']
            if preparation_hole:
                revision,complete=(8,7) if is_proof else (5,5)
                coordinates=tuple((1,n) for n in range(1,28 if is_proof else 18))
            elif post_hole_proof and is_proof:
                revision=7
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
            if preparation_hole and not is_proof:
                assert head['xmin']==only(state['producer_outcomes'],producer_epoch=epoch,request_sequence=5)['xmin']
                assert head['xmin']!=jobs[0]['xmin'], 'hole must not update classified prefix'
            else:
                assert head['xmin'] == jobs[0]['xmin']
            assert head['committed_at_us'] >= wire['sent_at_us']
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
    for phase in names:
        check(cuts['progress-'+phase],phase)
    assert all(before[table] == [] for table in (*typed.TABLES,'producer_checkpoints'))
    for cut in snapshots:
        if cut['stage'] in ('initialized','activated','prepared'):
            if kind == 'initial':
                check(cut,setup=True)
            else:
                assert all(cut['control'][table] == [] for table in (*typed.TABLES,'producer_checkpoints'))
        elif cut['stage'] not in tuple('progress-'+phase for phase in names):
            for table in (*typed.TABLES,'producer_checkpoints'):
                assert cut['control'][table] == proof['control'][table]
    for table in (*typed.TABLES,'producer_checkpoints'):
        assert after[table] == proof['control'][table]
    if preparation_hole:
        return dict(status='PASS_TYPED_PREPARATION_HOLE',kind=kind,producer_epoch=epoch,
            mutation_revision=5,continuation_revision=8,complete_through_request=7,dependencies=27,
            acquisition_revision=4,job_checkpoint_revision=0,ack=7,preparation_refusals=1,
            frozen_prefix_ordinal=1,post_hole_mutations=3,window=2,
            scope='Initial A/hole/B then genuine C/D/B-proof REQ; no EOSE/workflow completion or full-window proof')
    if post_hole_proof:
        return dict(status='PASS_TYPED_POST_HOLE_PROOF',kind=kind,producer_epoch=epoch,
            mutation_revision=6,proof_revision=7,complete_through_request=6,dependencies=22,
            acquisition_revision=4,job_checkpoint_revision=0,ack=6,preparation_refusals=1,
            frozen_prefix_ordinal=1,post_hole_proofs=1,window=2,
            proof_observation='Exact wire and source path plus later serial REQ and unchanged effects; no per-proof success response',
            scope='Initial A/B/C then actual A/hole/A REQ; no extra typed/native mutation or ACK; no full-window or EOSE/workflow claim')
    return dict(status='PASS_TYPED_CONTEXT',kind=kind,producer_epoch=epoch,
        mutation_revision=spec['mutation_revision'],proof_revision=spec['mutation_revision']+width,
        complete_through_request=spec['request'],dependencies=len(spec['dependencies']),
        acquisition_revision=2*width,job_checkpoint_revision=0,ack=spec['request'] if ack else 0)


def verify_initial(context, *, ack=False, preparation_hole=False, post_hole_proof=False):
    from installed_sync import BOOTSTRAP, COMMANDS, validate_source, validate_queued
    from initial_jobs import unchanged, logged_out, released
    from live_replay import OLD_EVENT
    assert type(preparation_hole) is bool and (not preparation_hole or ack)
    assert type(post_hole_proof) is bool and (not post_hole_proof or ack and not preparation_hole)
    typed.require_frozen_profile(ack=ack,preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)
    names=typed.phase_names(preparation_hole)
    assert set(context) == {'kind','setup','before','after','snapshots','report','transcript','opened',
        'terminal','stdout','stderr','signal','client_terminal','client_stdout','client_stderr','observations',
        'inputs','input_contents','native','event_bytes','native_result','catalog_after','backend_count','gates',
        'input_files','input_files_after','backend','native_readback'} | ({'producer_ack_profile','ack_event_recipe'} if ack else set())
    assert context['kind'] == 'initial'
    check_upgrade(context['setup'],ack=ack)
    assert context['setup']['status'] == ('APPLIED_EXACT40' if ack else 'APPLIED_EXACT39')
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
    assert context['stderr'] == context['client_stderr'] == ''
    assert context['client_stdout'] == '{"status": "PASS", "mode": "installed-sync"}\n', 'exact installed-sync client completion'
    assert context['client_terminal'] == {'exit_code':0}
    reports = [json.loads(line) for line in context['stdout'].splitlines()]
    assert reports == [context['report'], dict(format='bigbrotr-initial-ingest/1',state='stopped',
        native='closed',owner_release='committed',source_closure='not-performed',ready=False)]
    assert context['report']['state'] == 'initialized' and context['report']['acquisition'] == 'finite-initial'
    snapshots = context['snapshots']
    assert [s['stage'] for s in snapshots] == ['initialized','prepared',
        *('progress-'+phase for phase in names),'durable','before-stop']
    event_raw = bytes.fromhex(context['event_bytes'])
    assert sha(event_raw) == SIGNED_SHA256
    event = json.loads(event_raw)
    assert event == OLD_EVENT  # Preserve the frozen raw JSON member order too.
    if ack:
        assert context['producer_ack_profile']==('two-slot-post-hole-proof/1' if post_hole_proof
            else 'two-slot-preparation-hole/1' if preparation_hole else 'two-slot-three-events/1')
        events=typed.preparation_hole_events() if preparation_hole else typed.ack_event_sequence()
        assert json.loads(context['ack_event_recipe'])==(typed.preparation_hole_recipe() if preparation_hole
            else dict(format='bigbrotr-ack-post-hole-proof-events/1' if post_hole_proof else 'bigbrotr-ack-events/1',events=events))
        event=events[0]
    phases = wires_checked(context['opened'],context['transcript'],snapshots,event,context['gates'],ack=ack,
        preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)
    signal = context['signal']
    quiet = only(context['transcript'],number=signal['round'])
    assert quiet['completion'] == 'quiet-eof' and not quiet['frames']
    assert signal['closed_before_signal'] is False and signal['request'] == quiet['request']
    assert quiet['request_at_us'] <= signal['signal_at_us'] <= quiet['closed_at_us']
    if post_hole_proof:
        # CLOSE completes collection before SQL dispatch. A later actual REQ
        # requires that the serial owner returned successfully from dispatch.
        # There is still no independent per-proof successful response on wire.
        proof=phases['proof-only']
        assert quiet['number']>proof['number']
        assert quiet['request_at_us']>=proof['closed_at_us']
        assert only(snapshots,stage='progress-proof-only')['observed_by_us']<=signal['signal_at_us']
    before, after = typed.legacy_state(context['before'],ack=ack), typed.legacy_state(context['after'],ack=ack)
    projected = [typed.legacy_state(s['control'],ack=ack) for s in snapshots]
    total=7 if preparation_hole else 6 if ack else 4
    window=2 if ack else 32
    if ack:
        assert all(before[t][0]['document']['settings']['ingest.producer_outstanding_requests']==2
            for t in ('config_revisions','policy_revisions'))
    initial, prepared, mutation, proof, durable, stopping = projected
    assert not before['owner_epochs'] and not before['producer_requests']
    assert {t for t in before if before[t] != initial[t]} <= BOOTSTRAP
    assert len(initial['producer_requests']) == len(initial['producer_outcomes']) == 2
    source = only(initial['source_streams'])
    assert source['state'] == 'open' and source['last_complete_sequence'] is None, 'source lifecycle changed'
    producer = only(initial['producer_epochs'])
    assert producer['state'] == 'open' and producer['producer_epoch'] == producer['owner_epoch'] == 1
    assert producer['reserved_through_request'] == 2 and producer['outstanding_request_limit'] == window
    assert producer['xmin'] == only(initial['producer_requests'],request_sequence=2)['xmin']
    for state in (prepared,mutation,proof,durable,stopping,after):
        assert state['source_streams'] == initial['source_streams'], 'source lifecycle changed'
        for table in ('producer_requests','producer_outcomes'):
            assert all(row in state[table] for row in initial[table]), 'retained bootstrap originals changed'
        expected_producer = dict(producer)
        if state is not prepared:
            current_total=5 if preparation_hole and state is mutation else total
            expected_producer.update(reserved_through_request=current_total,
                xmin=only(state['producer_requests'],request_sequence=current_total)['xmin'])
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
            expected_rows=[r for r in after[table] if not preparation_hole or state is not mutation or r['request_sequence']<=5]
            same({table:state[table]},{table:expected_rows})
        assert all(r in state['management_commands'] for r in prepared['management_commands'])
        assert all(r in state['management_audit'] for r in prepared['management_audit'])
        assert len(state['management_commands']) == len(prepared['management_commands'])+1
        assert len(state['management_audit']) == len(prepared['management_audit'])+1
    logged_out(durable,stopping); released(stopping,after)
    actual = context['native']
    assert set(actual) == {'event','rows','sequence'} and actual['event'] == 'dump-state'
    # One initial manifest write, twelve bootstrap entries (manifest included),
    # eight private-map, twenty attribution and sixteen signed-event entries.
    # Two further new-event batches are sixteen writes each. Under0040 each
    # of the six mutating operations contributes one separate ACK state put.
    if ack:
        assert type(actual['sequence']) is int and actual['sequence']==(112 if preparation_hole else 95), 'ACK native write count differs'
    assert type(actual['sequence']) is int and actual['sequence'] == 1+12+8+20+16+(48+7 if preparation_hole else 32+6 if ack else 0)
    values = {(r['cf'],bytes.fromhex(r['key'])):bytes.fromhex(r['value']) for r in actual['rows']}
    assert len(values) == len(actual['rows']) == (88 if preparation_hole else 75 if ack else 49)
    facts = {int.from_bytes(key[-8:],'big'):decode(value[34:].split(b'\0',1)[1])
        for (cf,key),value in values.items() if cf == 'facts' and key[:2] == b'\x01\x20'}
    dictionary_at, recorded, received = facts[4][4],facts[8][4],facts[11][5][1]
    resolution = phases['mutation']['clock_resolution_us']
    assert phases['mutation']['sent_at_us']-resolution <= received <= phases['mutation']['closed_at_us']+resolution
    assert only(after['jobs'])['created_at_us'] <= dictionary_at <= phases['mutation']['connected_at_us']
    assert received <= recorded <= snapshots[-1]['observed_by_us']
    options={}
    if ack:
        assert context['native_result']==dict(status='CAPTURED_FOR_COMPLETE_ACK_ORACLE')
        extra=[]
        for ordinal,literal in enumerate(events[1:],2):
            first=8+5*(ordinal-1)
            at=facts[first+3][5][1];committed=facts[first][4]
            wire=phases['continuation'] if preparation_hole and ordinal>=3 else phases['mutation']
            quality=wire['clock_resolution_us']
            assert facts[first+3][5][3][1]==quality
            assert wire['sent_at_us']-quality<=at<=wire['closed_at_us']+quality
            assert at<=committed<=snapshots[-1]['observed_by_us']
            extra.append((literal,at,committed,quality,sha(b'bigbrotr/body/1\0'+typed.cbor(typed.body(literal)))))
        options=dict(additional_events=extra,acknowledge=True,preparation_hole=preparation_hole,
            expected_body_digest=sha(b'bigbrotr/body/1\0'+typed.cbor(typed.body(event))))
    else:
        assert context['native_result'] == dict(native_rows=49,original_requests=4,
            dictionary_recorded_us=dictionary_at,event_received_us=received,event_recorded_us=recorded)
    wanted, inputs, receipts = initial_science(after,context['report'],event,dictionary_at,received,recorded,resolution,**options)
    if ack:
        assert sorted((cf,key,value) for (cf,key),value in values.items()) == wanted, 'complete ACK native bytes differ'
    assert sorted((cf,key,value) for (cf,key),value in values.items()) == wanted
    slot = context['report']['producer_slot_id']
    expected_inputs = {slot+'-0000000000000001-'+f'{n:016x}.cbor':dict(bytes=len(raw),sha256=sha(raw))
        for n,raw in enumerate(inputs,1)}
    assert context['inputs'] == expected_inputs
    assert context['input_contents'] == {slot+'-0000000000000001-'+f'{n:016x}.cbor':raw.hex()
        for n,raw in enumerate(inputs,1)}
    input_custody(context['input_files'],context['input_files_after'],expected_inputs,preparation_hole=preparation_hole)
    for index, snapshot in enumerate(snapshots):
        assert snapshot['inputs'] == {name:value for name,value in expected_inputs.items()
            if int(name[-21:-5],16) <= (2 if index<2 else 5 if preparation_hole and index==2 else total)}
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
        if n >= 4:
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
    assert only(after['producer_epochs'])['reserved_through_request'] == total
    assert only(after['producer_epochs'])['outstanding_request_limit'] == window
    event_input = decode(inputs[3].split(b'\0',1)[1])[3]
    observation = event_input[5]
    return typed_checked('initial',context['before'],context['after'],snapshots,phases,event,
        {(cf,key.hex()):value.hex() for cf,key,value in wanted},dict(enumerate(inputs,1)),dict(enumerate(receipts,1)),
        event_input[1],observation[1],observation[3],control,ack=ack,preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)


def verify_live(context, *, ack=False):
    from installed_live_oracle import verify, native_plan, NetworkScience, BODY, SIGNATURE, ack_observations
    from initial_gap_policy import require_guards
    assert set(context) == {'mode','original','after','native_before','native_after','inputs_before','inputs_after',
        'input_contents','snapshots','response','journey','fixture','server_window','backend','catalog_before',
        'catalog_after','seed','setup','predecessor','progress_setup','progress_catalog_after','progress_gates',
        'progress_input_files','progress_input_files_after'} | ({'producer_ack_profile','ack_event_recipe'} if ack else set())
    assert context['mode'] == 'installed-live-on' and context['predecessor'] is None
    setup = context['progress_setup']
    check_upgrade(setup,ack=ack)
    assert setup['status'] == ('APPLIED_EXACT40' if ack else 'APPLIED_EXACT39')
    same(context['original'],setup['after']); same(setup['before'],context['setup']['after'])
    assert context['progress_catalog_after'] == setup['catalog_after']
    assert context['catalog_before'] == context['catalog_after']
    require_guards(context['catalog_before'])
    if ack:
        assert context['producer_ack_profile']=='two-slot-three-events/1'
        assert json.loads(context['ack_event_recipe'])==dict(format='bigbrotr-ack-events/1',events=typed.ack_event_sequence())
        for table in ('config_revisions','policy_revisions'):
            assert only(context['original'][table],revision=2)['document']['settings']['ingest.producer_outstanding_requests']==2
    projected = copy.deepcopy(context)
    if ack:
        projected.pop('producer_ack_profile'); projected.pop('ack_event_recipe')
    for key in ('progress_setup','progress_gates','progress_catalog_after','progress_input_files','progress_input_files_after'):
        projected.pop(key)
    projected['original'],projected['after'] = project32(context['original'],ack=ack),project32(context['after'],ack=ack)
    assert [s['stage'] for s in context['snapshots']] == ['activated','prepared','progress-mutation',
        'progress-proof-only','submitted','renewed','before-stop']
    projected['snapshots'] = [dict(s,control=project32(s['control'],ack=ack)) for s in context['snapshots']
        if not s['stage'].startswith('progress-')]
    projected['fixture']['renewal']['current'] = copy.deepcopy(projected['snapshots'][3])
    # The pinned schema32 catalog validates the reused72-table model. Actual39
    # definitions/ACLs remain separate exact before/after pg_dump observations.
    projected['catalog_before'] = projected['catalog_after'] = setup['reference_catalog32']
    base = verify(**projected,split=True,ack=ack)
    input_custody(context['progress_input_files'],context['progress_input_files_after'],context['inputs_after'])
    for cut in context['snapshots'][2:4]:
        intermediate = copy.deepcopy(projected)
        intermediate['snapshots'][2] = dict(cut,stage='submitted',control=project32(cut['control'],ack=ack))
        verify(**intermediate,split=True,ack=ack)
    report = json.loads(context['response']['stdout'].splitlines()[0])
    plan, rows = native_plan(context['seed'],projected['snapshots'][0]['control'],report)
    science = NetworkScience(plan,rows,context['seed']['inputs'],acknowledge=ack)
    actual = {(r['cf'],bytes.fromhex(r['key'])):bytes.fromhex(r['value']) for r in context['native_after']['rows']}
    fact = lambda n:decode(actual['facts',b'\x01\x20'+science.src_key(n)][34:].split(b'\0',1)[1])
    if ack:
        first, = [wire for wire in context['journey']['closed']
            if wire.get('progress_phase') == 'mutation']
        event,extras=ack_observations(science,actual,first,context['server_window'])
        science.network(True,fact(1)[4],fact(8)[5][1],fact(5)[4],fact(8)[5][3][1],
            event=event,additional_events=extras)
    else:
        science.network(True,fact(1)[4],fact(8)[5][1],fact(5)[4],fact(8)[5][3][1])
    assert actual == science.rows
    event = dict(pubkey=BODY[1].hex(),created_at=BODY[2],kind=BODY[3],tags=BODY[4],content=BODY[5],
        id=BODY[0].hex(),sig=SIGNATURE.hex())
    phases = wires_checked(context['journey']['opened'],context['journey']['closed'],context['snapshots'],
        event,context['progress_gates'],ack=ack)
    result = typed_checked('activated',context['original'],context['after'],context['snapshots'],phases,event,
        {(cf,key.hex()):value.hex() for (cf,key),value in science.rows.items()},science.requests,science.receipts,
        science.scope,science.ref(2),[science.ref(1),science.ref(3),science.ref(5)],only(context['after']['owner_epochs'],role='control',epoch=8),ack=ack)
    return dict(result,legacy_science=base)


def controls(context, verify):
    verify(context)
    preparation_hole=context.get('producer_ack_profile')=='two-slot-preparation-hole/1'
    post_hole_proof=context.get('producer_ack_profile')=='two-slot-post-hole-proof/1'
    results = []
    def reject(name, change, reason=None):
        value = copy.deepcopy(context)
        change(value)
        if context.get('producer_ack_profile') in ('two-slot-three-events/1','two-slot-preparation-hole/1','two-slot-post-hole-proof/1'):
            rebind_gates(value)
        try:
            verify(value)
        except (AssertionError,KeyError,TypeError,ValueError,IndexError,StopIteration) as error:
            if reason is not None:
                assert isinstance(error,AssertionError) and str(error)==reason, (name,'wrong rejection',str(error))
            results.append(name)
        else:
            raise AssertionError('progress context accepted counterexample: '+name)
    phase = lambda c:next(s for s in c['snapshots'] if s['stage']=='progress-'+typed.phase_names(preparation_hole)[1])['control']
    after = lambda c:c['after']
    upgrade = lambda c:c['setup'] if 'kind' in c else c['progress_setup']
    ledger = lambda c,stage:upgrade(c)[stage]['schema_versions']
    def change_acl(c, stage, **values):
        acl=upgrade(c)['acl']
        observed=json.loads(acl[stage+'_stdout']); observed.update(values)
        acl[stage+'_stdout']=json.dumps(observed)
    reject('activation-read-missing',lambda c:change_acl(c,'after',SELECT=False),'activation ACL exact read grant')
    reject('activation-write-overgrant',lambda c:change_acl(c,'after',UPDATE=True),'activation ACL exact read grant')
    reject('activation-grant-option',lambda c:change_acl(c,'after',**{'SELECT WITH GRANT OPTION':True}),
        'activation ACL exact read grant')
    reject('activation-baseline-overgrant',lambda c:change_acl(c,'before',DELETE=True),'activation ACL baseline')
    reject('activation-wrong-subject',lambda c:upgrade(c)['acl'].update(role='postgres'),'activation ACL subject')
    reject('activation-wide-grant-statement',lambda c:upgrade(c)['acl']['grants'][0].update(
        statement='GRANT ALL ON control.initial_ingest_activations TO bigbrotr_control;'),'exact fixture grant statements')
    reject('duplicate-prior-migration',lambda c:ledger(c,'before').append(copy.deepcopy(
        only(ledger(c,'before'),migration=501))),'duplicate migration receipt')
    reject('missing-core-prefix-migration',lambda c:ledger(c,'before').remove(
        only(ledger(c,'before'),migration=25)),'closed migration catalog prefix')
    reject('missing-unrelated-migration',lambda c:ledger(c,'after').remove(
        only(ledger(c,'after'),migration=501)),'migration extension set')
    reject('rewritten-unrelated-migration',lambda c:only(ledger(c,'after'),migration=501).update(
        xmin='4294967294'),'retained migration changed')
    reject('foreign-migration-receipt',lambda c:ledger(c,'after').append(dict(
        only(ledger(c,'after'),migration=501),migration=900)),'migration extension set')
    reject('unrelated-sql-row',lambda c:after(c)['network_routes'].append(copy.deepcopy(after(c)['network_routes'][0])))
    reject('missing-dependency',lambda c:after(c)['producer_checkpoint_dependencies'].pop())
    reject('changed-native-value',lambda c:(c['native'] if 'native' in c else c['native_after'])['rows'][0].update(value='00'))
    reject('changed-original-input',lambda c:c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    reject('foreign-progress-issuer',lambda c:after(c)['producer_checkpoints'][0].update(issuer_epoch=999))
    reject('head-complete-overadvance',lambda c:after(c)['producer_checkpoints'][0].update(complete_through_request=(8 if preparation_hole
            else typed.ACK_EXPECTATIONS['initial' if 'kind' in c else 'activated']['request']+1
            if c.get('producer_ack_profile') in ('two-slot-three-events/1','two-slot-post-hole-proof/1') else 5)))
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
        reject('missing-client-completion',lambda c:c.update(client_stdout=''),'exact installed-sync client completion')
        reject('foreign-client-completion',lambda c:c.update(
            client_stdout='{"status": "PASS", "mode": "journey"}\n'),'exact installed-sync client completion')
        reject('extra-client-output',lambda c:c.update(client_stdout=c['client_stdout']+'unexpected\n'),
            'exact installed-sync client completion')
        reject('native-before-backend-exclusion',lambda c:c['native_readback'].update(started_monotonic_ns=1))
        reject('unjoined-cli',lambda c:c['terminal'].update(reaped=False))
        reject('backend-not-absent',lambda c:c.update(backend_count=1))
        reject('schema-drift',lambda c:c.update(catalog_after=c['catalog_after']+'changed'))
    else:
        reject('unjoined-cli',lambda c:c['response']['terminal'].update(reaped=False))
        reject('backend-not-absent',lambda c:c['backend']['absence']['attempts'][-1].update(count=1))
        reject('schema-drift',lambda c:c.update(progress_catalog_after=c['progress_catalog_after']+'changed'))
    if context.get('producer_ack_profile') in ('two-slot-three-events/1','two-slot-preparation-hole/1','two-slot-post-hole-proof/1'):
        results.extend(typed.ack_counterexamples(context,verify,'initial' if 'kind' in context else 'activated',
            preparation_hole=preparation_hole,post_hole_proof=post_hole_proof))
    if preparation_hole:
        mutation=lambda c:only(c['snapshots'],stage='progress-mutation')['control']
        def rehash(c,state):
            native={(r['cf'],r['key']):r['value'] for r in c['native']['rows']}
            head=only(state['producer_checkpoints'])
            jobs=sorted(state['acquisition_progress'],key=lambda r:r['job_id'])
            dependencies=sorted(state['producer_checkpoint_dependencies'],key=lambda r:(
                r['source_shard'],r['source_epoch'],r['source_sequence']))
            head['checkpoint_digest']='\\x'+typed.head_digest(head,jobs,dependencies,
                lambda row:typed.native_reference(row,native)).hex()
        def advance_prefix(c):
            state=mutation(c); job=only(state['acquisition_progress'])
            request=only(state['producer_requests'],request_sequence=5)
            outcome=only(state['producer_outcomes'],request_sequence=5)
            original=bytes.fromhex(c['input_contents'][next(name for name in c['input_contents'] if name.endswith('0000000000000005.cbor'))])
            observation=decode(original.split(b'\0',1)[1])[3][5]
            # Forge a structurally valid prefix by falsely classifying the
            # preparation hole as rejected between the two successful events.
            job.update(progress_revision=3,classified_events=3,successful_events=2,rejected_events=1,event_ordinal=3,
                wire_sequence=3,input_sha256=request['progress_input_sha256'],intent_digest=request['request_digest'],
                mutation_request=5,observed_at_us=observation[0],clock_resolution_us=observation[2][1],
                clock_anomaly_flags=observation[2][3],xmin=outcome['xmin'])
            rehash(c,state)
        def unbind_later_mutation(c):
            for state in [s['control'] for s in c['snapshots'][2:]]+[c['after']]:
                request=only(state['producer_requests'],request_sequence=5)
                request.update({field:None for field in typed.REQUEST_ADDITIONS})
        def drop_closure(c):
            state=mutation(c); state['producer_checkpoint_dependencies'].pop()
            only(state['producer_checkpoints'])['dependency_count']-=1
            rehash(c,state)
        def replace_hole(c):
            wire,=[row for row in c['transcript'] if row.get('progress_phase')=='mutation']
            forged=dict(typed.preparation_hole_events()[0],sig='00'*64)
            raw=json.dumps(forged,ensure_ascii=False,separators=(',',':')).encode()
            wire['event_json_hex'][1]=raw.hex();wire['event_json_sha256'][1]=sha(raw)
            wire['frames'][1]='forged-signature'
        reject('coherent-head-skips-preparation-hole',advance_prefix)
        reject('prefix-xmin-updated-by-post-hole-mutation',lambda c:only(mutation(c)['acquisition_progress']).update(
            xmin=only(mutation(c)['producer_outcomes'],request_sequence=5)['xmin']),'frozen prefix transaction changed')
        reject('post-hole-mutation-loses-raw-binding',unbind_later_mutation)
        reject('post-hole-dependency-omitted-with-valid-checksum',drop_closure)
        reject('classified-signature-rejection-substituted-for-preparation-hole',replace_hole)
        reject('post-hole-ordinal-compacted',lambda c:only(mutation(c)['producer_requests'],request_sequence=5).update(
            progress_event_ordinal=2,progress_wire_sequence=2))
        reject('new-request-does-not-reset-ordinal',lambda c:only(phase(c)['producer_requests'],request_sequence=6).update(
            progress_event_ordinal=4,progress_wire_sequence=4))
        reject('new-request-proof-counts-skipped-hole',lambda c:only(phase(c)['acquisition_progress']).update(
            classified_events=5,successful_events=5,progress_revision=5))
    if post_hole_proof:
        proof_wire=lambda c:next(r for r in c['transcript'] if r.get('progress_phase')=='proof-only')
        def advance_post_hole_proof(c):
            # All constructors run outside reject()'s verifier catch. This head
            # is canonical but falsely checkpoints ordinal3 beyond the hole.
            native={(r['cf'],r['key']):r['value'] for r in c['native']['rows']}
            for state in [s['control'] for s in c['snapshots'][3:]]+[c['after']]:
                job=only(state['acquisition_progress'])
                job.update(progress_revision=5,classified_events=5,successful_events=5,
                    event_ordinal=3,wire_sequence=3)
                head=only(state['producer_checkpoints']);head['checkpoint_revision']=8
                dependencies=sorted(state['producer_checkpoint_dependencies'],key=lambda r:(
                    r['source_shard'],r['source_epoch'],r['source_sequence']))
                head['checkpoint_digest']='\\x'+typed.head_digest(head,[job],dependencies,
                    lambda row:typed.native_reference(row,native)).hex()
        def replace_post_hole_input(c):
            wire=proof_wire(c); forged=dict(typed.ack_event_sequence()[0],sig='00'*64)
            raw=json.dumps(forged,ensure_ascii=False,separators=(',',':')).encode()
            wire['event_json_hex'][1]=raw.hex();wire['event_json_sha256'][1]=sha(raw)
            wire['frames'][1]='forged-signature'
        reject('coherent-proof-head-skips-preparation-hole',advance_post_hole_proof)
        reject('native-signature-rejection-substituted-for-proof-preparation-hole',replace_post_hole_input)
        reject('post-hole-proof-input-spool-write',lambda c:c['input_files']['proof-only'].update(
            {next(name for name in c['input_files']['proof-only'] if name!='.'):
                dict(next(value for name,value in c['input_files']['proof-only'].items() if name!='.'),inode=1)}))
        reject('post-hole-proof-dependency-xmin-rewrite',lambda c:phase(c)['producer_checkpoint_dependencies'][0].update(
            xmin='4294967294'))
    return dict(status='PASS',rejected=len(results),cases=results)


def rebind_gates(context):
    gates=context['gates'] if 'kind' in context else context['progress_gates']
    wires=[r for r in (context['transcript'] if 'kind' in context else context['journey']['closed'])
        if 'progress_phase' in r]
    names=typed.phase_names(context.get('producer_ack_profile')=='two-slot-preparation-hole/1')
    for phase in names:
        wire=only(wires,progress_phase=phase)
        cut=only(context['snapshots'],stage='progress-'+phase)
        gates['progress-'+phase+'-observation.json']=json.dumps(dict(
            format='bigbrotr-progress-transport-observation/1',phase=phase,observation=cut,wire=wire))
        if phase=='mutation':
            gate=dict(format='bigbrotr-progress-transport-release/1',mutation_request=wire['request'][1],
                observation_sha256=sha(gates['progress-mutation-observation.json'].encode()))
            gates['progress-release.json']=json.dumps(gate)
            only(wires,progress_phase=names[1])['progress_release']=dict(gate,
                release_sha256=sha(gates['progress-release.json'].encode()))



def verify_hole_recovery(context, seed):
    """Full original preservation plus only fresh Control and native closing."""
    import installed_cycle_model as model
    from installed_cycle_replay import CONTEXT_FIELDS, process_closed
    from installed_family_capture import backend_checked, number
    typed.require_frozen_profile(ack=True,preparation_hole=True)
    assert set(context)==CONTEXT_FIELDS
    case=context['case']; retry=case['name']=='sync-signed-retry'
    assert set(case)=={'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert case['name'] in ('sync-signed-recover','sync-signed-retry')
    assert (case['clone'],case['origin'],case['operation'])==('sync-signed','initial','recover-initial')
    assert case['status']=='RUNNING' and case['custody']=='OBSERVED_CLOSED_AND_JOINED'
    assert case['predecessor']==('sync-signed-recover' if retry else None)
    assert re.fullmatch('[a-z0-9_]{1,63}',case['database'])
    assert case['config']=='/lab/'+case['database']+'.toml'
    before,after=context['before'],context['after']
    assert set(before)==set(after)==set(seed[0]) and len(before)==77
    assert context['inputs_before']==context['inputs_after']==seed[2] and len(seed[2])==7
    assert set(context['input_contents'])==set(seed[2])
    for name,value in context['input_contents'].items():
        raw=bytes.fromhex(value)
        assert seed[2][name]==dict(bytes=len(raw),sha256=sha(raw))
    assert context['snapshots']==[] and context['fixture']==dict(cleanup_errors=[])
    assert context['catalog_before']==context['catalog_after']
    window=context['server_window']
    assert set(window)=={'before_us','after_us'}
    number(window['before_us'],1,integer=True)
    number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    reports=process_closed(context['response'],case)
    backend_checked(context['backend'],case['database'],before,after)
    producer=only(seed[0]['producer_epochs'])
    assert producer['producer_epoch']==1 and producer['state']=='open' and producer['reserved_through_request']==7
    assert only(seed[0]['producer_checkpoints'])['complete_through_request']==7
    assert only(seed[0]['producer_checkpoints'])['checkpoint_revision']==8
    assert only(seed[0]['acquisition_progress'])['classified_events']==4
    expected_native=copy.deepcopy(seed[1]); values=model.native_map(expected_native)
    key=('receipts','0131'+producer['producer_slot_id'][2:]+(1).to_bytes(8,'big').hex())
    marker=b'\x01\x31'+(7).to_bytes(8,'big')*2+bytes(8)
    assert values[key]==marker.hex() and expected_native['sequence']==112
    values[key]=(b'\x02\x31'+marker[2:]+b'\x01').hex()
    expected_native.update(rows=model.native_rows(values),sequence=113)
    assert context['native_before']==(expected_native if retry else seed[1])
    assert context['native_after']==expected_native, 'recovery permits only one closing write'
    # All prior scientific, authorization, checkpoint and audit rows survive.
    retained=copy.deepcopy(before)
    if retry:
        prior=only(retained['owner_epochs'],role='control',epoch=2)
        assert prior['state']=='released' and prior['exclusion_method']=='storage_fenced'
        role=only(retained['role_instances'],role_instance_id=prior['role_instance_id'])
        retained['owner_epochs'].remove(prior); retained['role_instances'].remove(role)
        current=only(retained['producer_epochs'])
        assert current['state']=='closing'
        model.fresh_xmin(current['xmin'],seed[0])
        current.update(state='open',xmin=producer['xmin'])
    same(retained,seed[0])
    m=model.Model(before,context['native_before'],seed[2],window,initial=True)
    m.completed_control(after)
    if not retry:
        changed=only(after['producer_epochs']); model.fresh_xmin(changed['xmin'],m.pg)
        only(m.pg['producer_epochs']).update(state='closing',xmin=changed['xmin'])
    same(m.pg,after)
    assert reports==[dict(format='bigbrotr-ingest-recovery/1',state='positive-outcomes-resolved',
        dataset_id=producer['dataset_id'][2:],owner_resource_id=producer['owner_resource_id'][2:],
        producer_slot_id=producer['producer_slot_id'][2:],producer_epoch='1',issued_through='7',
        reserved_through='7',resolved_requests='7',native='closed',owner_release='committed',ready=False)]
    return dict(status='PASS',case=case['name'],tables=77,input_files=7,native_rows=88,native_sequence=113,
        product_native_writes=0 if retry else 1,native_acknowledged=7,native_retired=0,
        typed_heads_dependencies_original_xmins_preserved=True,source_and_job_completion=False)


def hole_recovery_controls(context, seed):
    verify_hole_recovery(context,seed)
    changes=[
        ('unknown-process',lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN')),
        ('unjoined-process',lambda c:c['response']['terminal'].update(reaped=False)),
        ('non-config-only-recovery',lambda c:c['response']['terminal']['command'].append('--close-source')),
        ('backend-not-excluded',lambda c:c['backend'].update(all=[dict(pid=1)])),
        ('missing-closing-or-extra-write',lambda c:c['native_after'].update(sequence=112)),
        ('native-fact-lost',lambda c:c['native_after']['rows'].pop()),
        ('original-input-lost',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))),
        ('original-request-xmin-rewritten',lambda c:c['after']['producer_requests'][0].update(xmin='4294967000')),
        ('original-outcome-xmin-rewritten',lambda c:c['after']['producer_outcomes'][0].update(xmin='4294967001')),
        ('historical-head-xmin-rewritten',lambda c:c['after']['producer_checkpoints'][0].update(xmin='4294967002')),
        ('post-hole-frame-binding-lost',lambda c:only(c['after']['producer_requests'],request_sequence=5).update(progress_event_ordinal=None)),
        ('old-prefix-reclassified',lambda c:c['after']['acquisition_progress'][0].update(classified_events=5)),
        ('dependency-lost',lambda c:c['after']['producer_checkpoint_dependencies'].pop()),
        ('historical-control-issuer-rewritten',lambda c:c['after']['producer_checkpoints'][0].update(issuer_epoch=2)),
        ('old-control-fencing-lost',lambda c:only(c['after']['owner_epochs'],role='control',epoch=1).update(exclusion_digest=None)),
        ('native-noop-instead-of-recovery',lambda c:c.update(native_after=copy.deepcopy(seed[1]))),
        ('job-falsely-completed',lambda c:c['after']['jobs'][0].update(state='completed')),
        ('source-falsely-closed',lambda c:c['after']['source_streams'][0].update(state='closed')),
        ('catalog-drift',lambda c:c.update(catalog_after=c['catalog_after']+'changed')),
    ]
    if context['case']['name']=='sync-signed-retry':
        changes.append(('retry-additional-native-write',lambda c:c['native_after'].update(sequence=114)))
    rejected=[]
    for name,change in changes:
        other=copy.deepcopy(context); change(other)
        try: verify_hole_recovery(other,seed)
        except (AssertionError,KeyError,TypeError,ValueError,IndexError,StopIteration): rejected.append(name)
        else: raise AssertionError('accepted hole-recovery corruption: '+name)
    return dict(status='PASS',rejected=len(rejected),cases=rejected)


def replay_hole_recovery(folder, cases, live, raw, read, database, config, finished):
    """Bind the existing cycle capture files to both initial recovery processes."""
    from installed_family_capture import number
    assert raw(folder/'installed_cycle_client.py')==raw(SOURCE/'experiments/ingest-succession/installed_cycle_client.py')
    assert cases==read(folder/'sync-signed-recovery-cases.json')
    assert [case['name'] for case in cases]==['sync-signed-recover','sync-signed-retry']
    seed=(live['after'],live['native'],live['inputs']); expected=seed; result=[]
    previous=live['snapshots'][-1]['observed_by_us']
    for case in cases:
        name=case['name']; item=lambda suffix:read(folder/(name+'-'+suffix+'.json'))
        context=item('verification')
        fields={'name','clone','origin','operation','predecessor','database','config','custody'}
        assert context['case']=={key:case[key] for key in fields}|dict(status='RUNNING')
        assert case['status']=='PASS' and case['returncode']==0
        assert case['database']==database and case['config']==config
        before,after,native,after_native,inputs,after_inputs=[item(suffix) for suffix in
            ('before','after','before-native','after-native','inputs','after-inputs')]
        same(expected[0],before); assert expected[1:]==(native,inputs)
        same(before,context['before']); same(after,context['after'])
        assert (native,after_native,inputs,after_inputs)==tuple(context[key] for key in
            ('native_before','native_after','inputs_before','inputs_after'))
        for stage,value in (('before',native),('after',after_native)):
            answer=item(stage+'-dump-response')
            assert set(answer)=={'returncode','stdout','stderr','seconds'}
            assert type(answer['returncode']) is int and answer['returncode']==0 and answer['stderr']==''
            number(answer['seconds'],0,120)
            assert json.loads(answer['stdout'])==value
        response=item('response'); assert response==context['response']
        identity,terminal=read(folder/(name+'.pid.json')),read(folder/(name+'.terminal.json'))
        assert set(identity)=={'pid','start_ticks','command','config','prefix'}
        assert all(terminal[key]==value for key,value in identity.items()) and terminal==response['terminal']
        for stream in ('stdout','stderr'):
            assert raw(folder/(name+'.'+stream)).decode()==response[stream]
        window=item('clock'); assert window==context['server_window']==case['server_window']
        number(window['before_us'],previous,finished,integer=True)
        previous=number(window['after_us'],window['before_us'],finished,integer=True)
        assert item('fixture')==context['fixture'] and item('observations')==context['snapshots']
        backend=item('backends')
        assert backend==context['backend'] and item('control-backends')==backend['absence']
        assert item('external')==dict(control=after,native=after_native,inputs=after_inputs,
            backend=backend,catalog=live['catalog_after'])
        assert context['catalog_before']==context['catalog_after']==live['catalog_after']
        assert item('custody')==dict(status='OBSERVED_CLOSED_AND_JOINED',database=case['database'],backend_absent=True,errors=[])
        assert not (folder/(name+'-external-unavailable.json')).exists()
        assert set(item('provisional-sql'))==set(before) and item('provisional-inputs')==inputs
        assert context['input_contents']==live['input_contents']
        answer=verify_hole_recovery(context,seed); controls=hole_recovery_controls(context,seed)
        assert case['oracle']==answer and case['negative_controls']==controls
        result.append(dict(name=name,oracle=answer,negative_controls=controls))
        expected=(after,after_native,after_inputs)
    return result


def bind_ack_modules(provenance, raw):
    """Bind the actual pure-file verifier modules to the captured source manifest."""
    stdlib=Path(stat.__file__).resolve().parent
    for module in tuple(sys.modules.values()):
        path=getattr(module,'__file__',None)
        if path is None: continue
        path=Path(path).resolve()
        if path.is_relative_to(stdlib): continue
        assert path.is_relative_to(SOURCE), 'foreign ACK replay module'
        name=path.relative_to(SOURCE).as_posix()
        assert name in provenance['files_sha256'], 'unbound ACK replay module'
        assert sha(raw(path))==provenance['files_sha256'][name], 'ACK replay source changed'


def replay(root, kind, *, retained_initial_capture=False, ack=False, preparation_hole=False, post_hole_proof=False):
    """Read original files only; retained custody is not a current resource audit.

    Reuse the family reader's strict JSON and physical-file checks. No SQL,
    engine, subprocess or capture rewrite is required.
    """
    import tomllib
    from installed_family_capture import loads, physical
    from inventory import projection
    assert kind in ('initial','activated')
    assert type(ack) is bool and not (ack and retained_initial_capture)
    assert type(preparation_hole) is bool and (not preparation_hole or ack and kind=='initial')
    assert type(post_hole_proof) is bool and (not post_hole_proof or ack and kind=='initial' and not preparation_hole)
    profile=typed.require_frozen_profile(ack=ack,preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)
    assert type(retained_initial_capture) is bool and (not retained_initial_capture or kind=='initial')
    root=Path(root).resolve(strict=True); folder=root/'bindings'
    bound={}
    def raw(path):
        path=Path(path)
        assert stat.S_ISREG(path.lstat().st_mode), 'raw capture must be regular'
        value=path.read_bytes(); bound[str(path)]=sha(value); return value
    def read(path): return loads(raw(path))
    report=read(folder/'result.json')
    if retained_initial_capture:
        # Only the complete7239 capture whose original verifier rejected the
        # established client completion line. Never adopt another failed run.
        assert report['status']=='FAIL' and report['cases']==[]
        assert sha(raw(folder/'result.json'))=='cee2afb1c1170a0fec63cbad5c5c32ef33142f960c1dfd3e4520ac21b5adeba8'
        assert sha(raw(root/'driver.log'))=='49670a7daed23aa50554b7581a215e56b854c6cd2359e716c43eff20a1fd7db2'
    else:
        assert report['status']=='PASS_INSTALLED_'+kind.upper()+('_PRODUCER_ACK' if ack else '_TYPED_PROGRESS')+('_POST_HOLE_PROOF' if post_hole_proof else '_PREPARATION_HOLE' if preparation_hole else '')
    assert report['wave']==kind+('-producer-ack' if ack else '-typed-progress')+('-post-hole-proof' if post_hole_proof else '-preparation-hole' if preparation_hole else '') and report['label']=='org.bigbrotr.acquisition-binding'
    assert re.fullmatch('[0-9a-f]{40}',report['source']) and re.fullmatch('[0-9a-f]{32}',report['owner'])
    assert all(report[key]==[] for key in ('cleanup_errors','missing_owned','unrelated_changed'))
    provenance=read(folder/'progress-source.json')
    assert set(provenance)=={'source','revision','tree','build','files_sha256'}
    assert Path(provenance['source'])==root/'source' and provenance['revision']==report['source']
    assert re.fullmatch('[0-9a-f]{40}',provenance['tree'])
    if retained_initial_capture:
        assert (provenance['revision'],provenance['tree'])==(
            '7239c7885c732298839504f013a7b3a7e832d300','e6423360e8be6e3c351861d8d5031d473c5b0ce5')
    for name,digest in provenance['files_sha256'].items():
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        assert sha(raw(root/'source'/name))==digest
        if retained_initial_capture and name!='experiments/ingest-bootstrap/installed_progress_context_oracle.py':
            # Run the corrected reader from its own checkout. Every other
            # tracked input/helper must still match the actual capture source.
            assert sha(raw(SOURCE/name))==digest, 'retained replay helper/source changed'
    build=Path(provenance['build']); assert build.is_absolute()
    built,component=read(build/'build-result.json'),read(build/'component-result.json')
    qualification=read(build.parent/'qualification.json')
    if preparation_hole or post_hole_proof:
        typed.preparation_hole_qualification(built,component,qualification,post_hole_proof=post_hole_proof)
    assert component['status']=='PASS' and qualification['status']==('PASS_PRODUCER_ACK_COMPONENTS' if ack else 'PASS_PRODUCER_PROGRESS_COMPONENTS')
    assert built['source']==component['source']==qualification['source']==profile['product_source']
    for key in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert built[key]==component[key]==qualification[key]
    assert set(component['executables_sha256'])=={'bigbrotr','ingest_bootstrap_probe','native_components',
        'sync_progress','fact_codec','evidence_intent','control_schema','migrate_entrypoint'}
    if ack:
        assert component['executables_sha256']==typed.ack_executables(preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)
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
    if ack:
        assert actual['migrations/postgres/0040_producer_ack.sql']==profile['migration_sha256']
    initial=kind=='initial'; label='sync-signed' if initial else 'installed-live-on'
    directory=folder/label; prefix=folder/label
    expected_helpers=(('installed_client.py','session_client.py','installed_sync_client.py',
        'installed_sync_expiry.py','installed_sync_window.py') if initial else ('installed_live_client.py','installed_sync_client.py'))
    for name in (*expected_helpers,'installed_progress_transport.py','installed_family_client.py'):
        relative=('experiments/ingest-succession/' if name=='installed_family_client.py' else 'experiments/ingest-bootstrap/')+name
        assert raw(folder/name)==raw(root/'source'/relative)
    assert read(directory/'progress-context.json')==dict(format='bigbrotr-ack-post-hole-proof-context/1' if post_hole_proof
        else 'bigbrotr-ack-preparation-hole-context/1' if preparation_hole
        else 'bigbrotr-ack-context/1' if ack else 'bigbrotr-progress-context/1',kind=kind)
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
    assert client['Name']=='/bigbrotr-acquisition-binding-'+report['source'][:7]+'-progress-'+kind+('-ack' if ack else '')+('-post-hole-proof' if post_hole_proof else '-preparation-hole' if preparation_hole else '')
    assert only(client['Mounts'],Destination='/lab')['Source']==str(folder)
    mount=only(client['Mounts'],Destination='/build')
    assert mount['Source']==str(build) and mount['RW'] is False
    assert not any(c['State']['Running'] and any(m.get('Source','')==str(folder) for m in c['Mounts']) for c in inventory)
    context_file=folder/(label+('-progress-verification.json' if initial else '-verification.json'))
    context=read(context_file)
    if retained_initial_capture:
        assert sha(raw(context_file))=='9758b6bf4b96ef4e5ae0d88336ef354cbb50711b519ce91c29d9d2980271a5c3'
    if ack:
        assert context['ack_event_recipe']==raw(directory/'progress-ack-events.json').decode()
        assert read(directory/'progress-ack-events.json')==(typed.preparation_hole_recipe() if preparation_hole
            else dict(format='bigbrotr-ack-post-hole-proof-events/1' if post_hole_proof
                else 'bigbrotr-ack-events/1',events=typed.ack_event_sequence()))
    setup=context['setup'] if initial else context['progress_setup']
    assert setup==read(folder/(label+'-progress-upgrade.json'))
    assert setup['acl']==read(folder/(label+'-progress-acl.json'))
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
    if ack:
        assert config['ingest']['producer_outstanding_requests']==2
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
    for phase in typed.phase_names(preparation_hole):
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
        assert typed.legacy_state(context['before'],ack=ack)==read(folder/(label+'-before.json'))
        assert typed.legacy_state(context['after'],ack=ack)==read(folder/(label+'-after.json'))
        checker=lambda value:verify_initial(value,ack=ack,preparation_hole=preparation_hole,post_hole_proof=post_hole_proof)
        answer=checker(context); negative=controls(context,checker)
        if not retained_initial_capture:
            case,=report['cases']
            assert case['verification_sha256']==sha(raw(context_file))
            if preparation_hole:
                from installed_family_capture import instant
                recovery=replay_hole_recovery(folder,case['recovery'],context,raw,read,
                    database,identity['config'],instant(report['finished_at']))
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
        checker=lambda value:verify_live(value,ack=ack)
        answer=checker(context); negative=controls(context,checker)
        group,=report['cases']; case,=group['cases']
        assert group['cli_invocations']==1 and group['source_preserved']==read(folder/'installed-live-source-preserved.json')
        assert group['source_lab']==str(basis.parent) and group['source_revision']==seed_owner.SOURCE
        assert group['source_owner']==seed_owner.OWNER and group['source_artifacts']==seed_owner.FROZEN
        assert read(folder/'installed-live-cases.json')==group['cases']
    if retained_initial_capture:
        verifier=Path(__file__).resolve(strict=True)
        verifier_sha=sha(raw(verifier))
        assert verifier==SOURCE/'experiments/ingest-bootstrap/installed_progress_context_oracle.py'
        # Imported fixture modules must come from the compared checkout; a
        # foreign PYTHONPATH module cannot borrow the checked on-disk identity.
        stdlib=Path(stat.__file__).resolve().parent
        for module in tuple(sys.modules.values()):
            path=getattr(module,'__file__',None)
            if path is None:
                continue
            path=Path(path).resolve()
            if path.is_relative_to(stdlib):
                continue
            assert path.is_relative_to(SOURCE), 'foreign retained replay module'
            name=path.relative_to(SOURCE).as_posix()
            assert name in provenance['files_sha256'], 'unbound retained replay module'
            assert path==verifier or sha(raw(path))==provenance['files_sha256'][name]
        return dict(status='PASS_RETAINED_INITIAL_CAPTURE_REPLAY',kind=kind,
            original_report_status=report['status'],original_source=provenance['revision'],
            original_tree=provenance['tree'],original_capture_sha256=dict(
                driver_log=bound[str(root/'driver.log')],report=bound[str(folder/'result.json')],
                context=bound[str(context_file)]),
            verifier=dict(path=str(verifier),sha256=verifier_sha),oracle=answer,negative_controls=negative,
            raw_files_sha256=bound,custody='RETAINED_OBSERVATIONS_ONLY_ROOT_CURRENT_AUDIT_SEPARATE')
    if ack:
        bind_ack_modules(provenance,raw)
    assert case['status']=='PASS' and case['oracle']==answer and case['negative_controls']==negative
    return dict(status='PASS_RAW_ACK_POST_HOLE_PROOF_REPLAY' if post_hole_proof else 'PASS_RAW_ACK_PREPARATION_HOLE_REPLAY' if preparation_hole else 'PASS_RAW_ACK_CONTEXT_REPLAY' if ack else 'PASS_RAW_CONTEXT_REPLAY',kind=kind,oracle=answer,negative_controls=negative,
        **(dict(recovery=recovery) if preparation_hole else {}),
        raw_files_sha256=bound,custody='RETAINED_OBSERVATIONS_ONLY_ROOT_CURRENT_AUDIT_SEPARATE')


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    parser.add_argument('--kind',choices=('initial','activated'),required=True)
    parser.add_argument('--producer-ack',action='store_true')
    parser.add_argument('--post-hole-proof',action='store_true',help='Initial-only A/B/C then A/hole/A, exact measured9cef build.')
    parser.add_argument('--preparation-hole',action='store_true',help='Initial-only A/hole/B then C/D/B-proof, exact separately pinned build.')
    parser.add_argument('--retained-initial-capture',action='store_true',
        help='Verify only the pinned complete7239 initial capture; preserve its original FAIL report.')
    arguments=parser.parse_args()
    print(json.dumps(replay(arguments.root,arguments.kind,
        retained_initial_capture=arguments.retained_initial_capture,ack=arguments.producer_ack,
        preparation_hole=arguments.preparation_hole,post_hole_proof=arguments.post_hole_proof),sort_keys=True))
