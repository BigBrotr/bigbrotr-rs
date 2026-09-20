#!/usr/bin/env python3
"""Isolated exact-Git PG/native/private event consumer qualification, not public reads.

The trusted fixture owns its scientific files for the complete experiment. It does
not claim installed policy, retention pins, a certified cut or a current read view.
All failure results are saved before assertions; terminal resource exclusion is a
separate owner-verified host audit.
"""
import argparse
import copy
import hashlib
import struct
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from signed_publication_cases import command, event, execute, send, sha
from signed_publication_oracle import EVENT, corpus, check, input_expectations
from fault_cases import Process


def cut_digest(read):
    body = b'bigbrotr/archive-cut-inspection/1\0' + bytes.fromhex(read['dataset_id'])
    body += struct.pack('>QQ', int(read['topology_generation']), len(read['bounds']))
    for b in read['bounds']:
        body += struct.pack('>IQQ', b['shard_id'], b['owner_epoch'], b['complete_through'])
    body += struct.pack('>Q', len(read['receipts']))
    for r in read['receipts']:
        body += bytes.fromhex(r['dataset_id']) + struct.pack('>HQ', r['schema_version'], int(r['topology_generation']))
        body += bytes.fromhex(r['batch_digest']) + struct.pack('>Q', r['record_count']) + bytes(r['attempt_id'])
        body += struct.pack('>QQ', r['publication_ordinal'], len(r['input_ranges']))
        for bound in r['input_ranges']:
            body += struct.pack('>IQQQ', *(bound[k] for k in ('shard_id','owner_epoch','first_sequence','last_sequence')))
    body += bytes([read['snapshot_id'] is not None])
    if read['snapshot_id'] is not None:
        body += struct.pack('>Q', read['snapshot_id'])
    return hashlib.sha256(body).hexdigest()


def cut_expectation(mode):
    """Independent literal wire recipe; does not import a product encoder."""
    receipts = [r for r in input_expectations(mode) if r['publication_ordinal'] in (2, 3, 5)]
    snapshot, count = (4, 12) if mode == 'history' else (3, 10)
    read = dict(dataset_id='01'*16, topology_generation='0',
                bounds=[dict(shard_id=0, owner_epoch=1, complete_through=count)],
                receipts=receipts, snapshot_id=snapshot)
    observation = dict(format='bigbrotr-native-cut/1', snapshot_id=snapshot,
                       request_digest=cut_digest(read),
                       family_counts=[6, 1, 1, 2, 2] if mode == 'history' else [6, 1, 1, 1, 1])
    return dict(event='captured-cut', observation=observation, read=read,
                frontier='not-implemented', pin='not-implemented')


def main(root, capture_cut=False):
    if capture_cut:
        frozen = json.loads((Path(__file__).resolve().parents[2]/'tests/fixtures/protocol/archive-cut.json').read_text())
        assert cut_digest(frozen['read']) == frozen['request_digest']
    resources = json.loads((root/'resources.json').read_text())
    source = root/'source'
    assert Path(__file__).resolve().parents[2] == source
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    out = root/'out/native-tests'
    ready = json.loads((out/'ready.json').read_text())
    assert ready['source'] == resources['source']
    artifacts = [('worker', root/'out/archive-worker/bigbrotr-archive-worker'),
                 ('publisher', out/'archive-publication-probe'), ('reader', out/'archive-read-probe')]
    if not capture_cut:
        artifacts.append(('event_reader', out/'archive-event-probe'))
    for label, path in artifacts:
        assert sha(path) == ready[label]
    evidence = root/('cut-cases.json' if capture_cut else 'event-cases.json')
    assert not evidence.exists()
    result = dict(source=resources['source'], tree=resources['tree'], status='INCOMPLETE',
                  artifacts=ready, harness_sha256=sha(Path(__file__)),
                  oracle_sha256=sha(source/'experiments/archive-engine/signed_publication_oracle.py'),
                  cases=[])
    def save():
        evidence.write_text(json.dumps(result, indent=2)+'\n')
    def sql(db, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=query)
    binary = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib', runner]
    inputs = root/('out/cut-inputs' if capture_cut else 'out/event-inputs')
    inputs.mkdir()
    def run_case(label, request, wanted, interact=None):
        case = dict(name=label, status='INCOMPLETE')
        result['cases'].append(case)
        request_path = inputs/(label+'.json')
        request_path.write_text(json.dumps(request)+'\n')
        case['input_sha256'] = sha(request_path)
        temporary = inputs/(label+'-temp')
        temporary.mkdir(mode=0o700)
        save()
        try:
            execute(binary+['/out/native-tests/archive-event-probe', '/out/event-inputs/'+request_path.name,
                            '/out/event-inputs/'+temporary.name], 35, case, interact)
            save()
            assert case['exit'] == 0 and not case['stderr'] and not case['overflow']
            observed = json.loads(case['stdout'])
            assert observed['active_permits'] == 0
            assert observed['result']['status'] == ('observed' if wanted else 'refused')
            if wanted:
                assert observed['result']['event'] == wanted
                snapshot = request['expected'].get('snapshot_id')
                if snapshot is None:
                    snapshot = int(sql(request['database'], 'SELECT max(snapshot_id) FROM ducklake_snapshot;'))
                assert int(observed['result']['snapshot_id']) == snapshot
            else:
                assert 'event' not in observed['result'] and 'snapshot_id' not in observed['result']
            case['status'] = 'PASS'
            return observed
        finally:
            save()
    def identity(event_value, records, mode):
        def reference(sequence):
            row = next(r for b in records for r in b['facts'] if r['sequence'] == sequence)
            return dict(source_record_id=dict(dataset_id='01'*16, shard_id=0, owner_epoch='1', sequence=str(sequence)), payload_digest=row['digest'])
        return dict(expected=dict(dataset='01'*16, event_id=event_value['id'],
                    scope=dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1'),
                    payload=reference(7), eligibility=reference(8)),
                    catalog_id='5a'*16, data_root=f'/out/signed_{mode}/kind=7/',
                    database='bbpub_'+mode, max_row_bytes=1024*1024, deadline_ms=15000, cancel_file=None)
    def metadata(db):
        names = json.loads(sql(db,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='public';"))
        assert all(name.startswith('ducklake_') and name.replace('_','').isalnum() for name in names)
        return {name: json.loads(sql(db, f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM public.{name} t;")) for name in names}
    def files(mode):
        home = root/f'out/signed_{mode}/kind=7'
        return {str(p.relative_to(home)): dict(bytes=p.stat().st_size, sha256=sha(p))
                for p in sorted(home.rglob('*.parquet'))}
    save()
    try:
        for mode in (['normal', 'history'] if capture_cut else ['normal', 'large', 'history']):
            value = EVENT
            if mode == 'large':
                recipe = json.loads((source/'tests/fixtures/protocol/archive-event-response.json').read_text())
                value = dict(recipe['event'], content=recipe['content_repeat']*recipe['content_count'])
            records, _ = corpus(value, history=mode == 'history')
            db = 'bbpub_'+mode
            sql('postgres', f'CREATE DATABASE {db}; CREATE DATABASE bbpub_control_{mode};')
            sql(db, f'REVOKE ALL ON DATABASE {db} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
            parent = {}
            result[mode+'_publisher'] = parent
            try:
                execute(binary+['/out/native-tests/archive-publication-probe', mode, '5432', '--capture-cut' if capture_cut else '--event-read'], 180, parent)
            finally:
                save()
            assert parent['exit'] == 0 and not parent['stderr'] and not parent['overflow']
            values = [json.loads(line) for line in parent['stdout'].splitlines()]
            assert next(v['batches'] for v in values if v['event']=='retained-inputs') == records
            if mode in ['normal', 'history']:
                reader = result[mode+'_independent_readback'] = {}
                try:
                    execute(binary+['/out/native-tests/archive-read-probe', mode], 45, reader)
                finally:
                    save()
                assert reader['exit'] == 0 and not reader['stderr']
                assert [v['expected'] for v in values if v['event']=='inputs-reopened'] == input_expectations(mode)
                # This slice consumes installed initialization/retained inputs;
                # its dedicated mode omits previously qualified ACL fault races.
                check(mode, [v for v in values if v['event'] not in ('inputs-reopened', 'captured-cut')], json.loads(reader['stdout']))
            request = identity(value, records, mode)
            before, original_files = metadata(db), files(mode)
            result[mode+'_metadata_before'] = before
            assert len(original_files) == (11 if mode == 'history' else 8)
            result[mode+'_files'] = original_files
            if capture_cut:
                captured = [v for v in values if v['event'] == 'captured-cut']
                assert captured == [cut_expectation(mode)], 'independent complete cut and request digest'
                marker = json.loads(sql('bbpub_control_'+mode,
                    "SELECT row_to_json(t) FROM (SELECT encode(dataset_id,'hex') AS dataset, encode(catalog_id,'hex') AS catalog, protocol_version, protection_class, baseline_snapshot FROM control.archive_persistence)t;"))
                assert marker == dict(dataset='01'*16, catalog='5a'*16, protocol_version=1,
                                      protection_class='local-synced', baseline_snapshot=1)
                protocols = json.loads(sql('bbpub_control_'+mode,
                    "SELECT json_agg(persistence_protocol ORDER BY ordinal) FROM control.archive_attempts;"))
                assert protocols == [1]*(5 if mode == 'history' else 4)
                assert int(sql(db, 'SELECT max(snapshot_id) FROM ducklake_snapshot;')) == (4 if mode == 'history' else 3)
                result['cases'].append(dict(name=mode+'-captured-cut', status='PASS',
                    cut=captured[0], persistence=marker, attempt_protocols=protocols))
                save()
                continue
            run_case(mode+'-complete', request, value)
            if mode == 'history':
                assert int(sql(db, 'SELECT max(snapshot_id) FROM ducklake_snapshot;')) == 4
                request['expected']['snapshot_id'] = 3
                run_case('history-exact-previous', request, value)
            if mode == 'normal':
                # Publication produced dictionary snapshot2 and signed-event
                # snapshot3. The older snapshots MUST NOT fall back to current,
                # whose complete signed body was independently observed above.
                for snapshot in [0, 1, 2, 4, 2**63-1, 2**63, 2**64-1]:
                    selected = copy.deepcopy(request)
                    selected['expected']['snapshot_id'] = snapshot
                    run_case('snapshot-'+str(snapshot), selected, None)
                request['expected']['snapshot_id'] = 3
                run_case('snapshot-exact', request, value)
                for variant in ['scope', 'digest', 'event-id', 'missing', 'row-limit', 'zero-limit', 'wrong-catalog', 'unavailable-database']:
                    bad = copy.deepcopy(request)
                    if variant == 'scope': bad['expected']['scope']['ordinal'] = '2'
                    elif variant == 'digest': bad['expected']['payload']['payload_digest'] = '00'*32
                    elif variant == 'event-id': bad['expected']['event_id'] = '00'*32
                    elif variant == 'missing': bad['expected']['payload']['source_record_id']['sequence'] = '99'
                    elif variant == 'row-limit': bad['max_row_bytes'] = 32
                    elif variant == 'zero-limit': bad['max_row_bytes'] = 0
                    elif variant == 'wrong-catalog': bad['catalog_id'] = '5b'*16
                    else: bad['database'] = 'absent'
                    run_case(variant, bad, None)
                body_files = [name for name in original_files if '/events/' in name]
                assert len(body_files) == 1
                path = root/f'out/signed_{mode}/kind=7'/body_files[0]
                original = path.read_bytes()
                for fault in ['missing-file', 'corrupt-footer']:
                    retained = path.with_suffix('.retained')
                    path.rename(retained)
                    try:
                        if fault == 'corrupt-footer': path.write_bytes(original[:-4]+b'FAIL')
                        run_case(fault, request, None)
                    finally:
                        if path.exists(): path.unlink()
                        retained.rename(path)
                    assert path.read_bytes() == original
                    run_case(fault+'-restored', request, value)
                for cancellation in [True, False]:
                    holder = Process(['docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
                                      '-U', 'postgres', '-d', db], input_pipe=True)
                    try:
                        send(holder, "BEGIN; LOCK TABLE public.ducklake_snapshot IN ACCESS EXCLUSIVE MODE; SELECT json_build_object('event','locked','pid',pg_backend_pid());\n")
                        pid = event(holder, 'locked')['pid']
                        blocked = dict(request, deadline_ms=15000 if cancellation else 6000)
                        label = 'cancel-blocked' if cancellation else 'deadline-blocked'
                        marker = inputs/(label+'.cancel')
                        if cancellation: blocked['cancel_file'] = '/out/event-inputs/'+marker.name
                        def interact(_parent):
                            until = time.monotonic()+5
                            while time.monotonic() < until:
                                rows = json.loads(sql(db, f"SELECT coalesce(json_agg(json_build_object('pid',pid,'user',usename,'application',application_name,'wait_type',wait_event_type,'blockers',pg_blocking_pids(pid))),'[]'::json) FROM pg_stat_activity WHERE datname=current_database() AND {pid}=ANY(pg_blocking_pids(pid));"))
                                if rows:
                                    assert all(r['user']=='bigbrotr_receipt_reader' and r['wait_type']=='Lock' for r in rows)
                                    result['cases'][-1]['blocking'] = rows
                                    save()
                                    if cancellation: marker.touch(exist_ok=False)
                                    return
                                time.sleep(.02)
                            raise AssertionError('actual blocked native reader was not observed')
                        seen = run_case(label, blocked, None, interact)
                        assert seen['cancelled'] is cancellation
                        assert seen['elapsed_ms'] < (5000 if cancellation else 7000)
                        if not cancellation:
                            assert seen['elapsed_ms'] >= blocked['deadline_ms']
                    finally:
                        primary_failure = sys.exc_info()[0] is not None
                        cleanup_error = None
                        try:
                            send(holder, 'ROLLBACK;\n\\q\n')
                            assert holder.wait(5) == 0 and not holder.stderr
                        except BaseException as error:
                            cleanup_error = type(error).__name__
                            if not primary_failure:
                                raise
                        finally:
                            holder.stop()
                            result.setdefault('holders', []).append(dict(
                                cancellation=cancellation, exit=holder.process.poll(),
                                stdout=holder.stdout.decode(errors='replace'),
                                stderr=holder.stderr.decode(errors='replace'),
                                overflow=holder.overflow, cleanup_error=cleanup_error,
                                remote_exclusion='UNKNOWN'))
                            save()
                    # Native driver destruction may follow local reap; wait for
                    # independently observed PG reader disappearance, not an ACK.
                    until = time.monotonic()+5
                    while int(sql(db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_receipt_reader';")) and time.monotonic()<until:
                        time.sleep(.02)
                    assert int(sql(db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_receipt_reader';")) == 0
                    run_case(label+'-retry', request, value)
            result[mode+'_metadata_after'] = metadata(db)
            assert result[mode+'_metadata_after'] == before and files(mode) == original_files
            result[mode+'_unchanged'] = True
            # A real metadata-engine restart; all scientific objects stay owned
            # and unchanged, and the next read uses fresh parent/worker processes.
            previous_start = sql(db, 'SELECT pg_postmaster_start_time()::text;')
            # The owned lab container keeps its network namespace while its
            # explicitly configured wrapper restarts the actual postmaster.
            # SIGQUIT exercises crash recovery, not just a configuration reload.
            command('docker', 'exec', pg, '/bin/bash', '-c',
                    'kill -QUIT "$(head -n1 /var/lib/postgresql/data/postmaster.pid)"')
            until = time.monotonic()+10
            while True:
                remaining = until-time.monotonic()
                assert remaining > 0
                restart = subprocess.run(['docker','exec',pg,'psql','-XAtq','-U','postgres','-d',db,
                                          '-c','SELECT pg_postmaster_start_time()::text;'], capture_output=True, text=True,
                                         timeout=min(2, remaining))
                if restart.returncode == 0 and restart.stdout.strip() != previous_start:
                    result[mode+'_restart'] = dict(before=previous_start, after=restart.stdout.strip(), signal='SIGQUIT')
                    break
                assert time.monotonic()<until
                time.sleep(.05)
            run_case(mode+'-pg-restarted', request, value)
            result[mode+'_metadata_restarted'] = metadata(db)
            result[mode+'_files_restarted'] = files(mode)
            assert result[mode+'_metadata_restarted'] == before and result[mode+'_files_restarted'] == original_files
            save()
        result['status'] = 'PASS'
    except BaseException as error:
        result['failure'] = str(error) if isinstance(error, AssertionError) else type(error).__name__
        raise
    finally:
        save()
    print(json.dumps(dict(status=result['status'], cases=len(result['cases']), source=resources['source'])))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    parser.add_argument('--capture-cut', action='store_true')
    args = parser.parse_args()
    main(args.root.resolve(), args.capture_cut)
