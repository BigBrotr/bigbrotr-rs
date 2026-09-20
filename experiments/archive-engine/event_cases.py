#!/usr/bin/env python3
"""Isolated exact-Git PG/native/private event consumer qualification, not public reads.

The trusted fixture owns its scientific files for the complete experiment. It does
not claim installed policy, retention pins, a certified cut or a current read view.
All failure results are saved before assertions; terminal resource exclusion is a
separate owner-verified host audit.
"""
import copy
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from signed_publication_cases import command, event, execute, send, sha
from signed_publication_oracle import EVENT, corpus, expected, check, input_expectations
from fault_cases import Process


def main(root):
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
    for label, path in [('worker', root/'out/archive-worker/bigbrotr-archive-worker'),
                        ('publisher', out/'archive-publication-probe'),
                        ('reader', out/'archive-read-probe'), ('event_reader', out/'archive-event-probe')]:
        assert sha(path) == ready[label]
    evidence = root/'event-cases.json'
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
    inputs = root/'out/event-inputs'
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
                assert int(observed['result']['snapshot_id']) == int(sql(request['database'], 'SELECT max(snapshot_id) FROM ducklake_snapshot;'))
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
        for mode in ['normal', 'large']:
            value = EVENT
            if mode == 'large':
                recipe = json.loads((source/'tests/fixtures/protocol/archive-event-response.json').read_text())
                value = dict(recipe['event'], content=recipe['content_repeat']*recipe['content_count'])
            records, _ = corpus(value)
            db = 'bbpub_'+mode
            sql('postgres', f'CREATE DATABASE {db}; CREATE DATABASE bbpub_control_{mode};')
            sql(db, f'REVOKE ALL ON DATABASE {db} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
            parent = {}
            result[mode+'_publisher'] = parent
            try:
                execute(binary+['/out/native-tests/archive-publication-probe', mode, '5432', '--event-read'], 180, parent)
            finally:
                save()
            assert parent['exit'] == 0 and not parent['stderr'] and not parent['overflow']
            values = [json.loads(line) for line in parent['stdout'].splitlines()]
            assert next(v['batches'] for v in values if v['event']=='retained-inputs') == records
            if mode == 'normal':
                reader = result['independent_readback'] = {}
                try:
                    execute(binary+['/out/native-tests/archive-read-probe', mode], 45, reader)
                finally:
                    save()
                assert reader['exit'] == 0 and not reader['stderr']
                assert [v['expected'] for v in values if v['event']=='inputs-reopened'] == input_expectations(mode)
                # This slice consumes installed initialization/retained inputs;
                # its dedicated mode omits previously qualified ACL fault races.
                check(mode, [v for v in values if v['event']!='inputs-reopened'], json.loads(reader['stdout']))
                assert json.loads(reader['stdout']) == expected(mode)
            request = identity(value, records, mode)
            before, original_files = metadata(db), files(mode)
            result[mode+'_metadata_before'] = before
            assert len(original_files) == 8
            result[mode+'_files'] = original_files
            run_case(mode+'-complete', request, value)
            if mode == 'normal':
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
    assert len(sys.argv) == 2
    main(Path(sys.argv[1]).resolve())
