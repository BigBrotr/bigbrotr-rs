#!/usr/bin/env python3
"""Signed HTTP job: file publication, killed SQL completion, exact-unit restart."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import time
from signed_publication_cases import command, sha


def run(root, state, sql, policy, empty_manifest):
    source = root / 'source'
    private = root / 'out/private-grant-http'
    db, catalog = 'bbpub_control_normal', 'bbpub_normal'
    runner, pg = state['container_id'], state['postgres_id']
    client = ['docker', 'exec', runner, 'python3', '-B',
              '/out/build/source/experiments/export-execution/recovery_client.py']
    result = dict(status='INCOMPLETE', source=state['source'])
    path = root / 'recovery-cases.json'
    assert not path.exists()

    def save():
        path.write_text(json.dumps(result, indent=2) + '\n')

    def until(observe, predicate, message, seconds=15):
        end = time.monotonic() + seconds
        while True:
            value = observe()
            if predicate(value):
                return value
            if time.monotonic() >= end:
                raise TimeoutError(message)
            time.sleep(.02)

    def activate(revision, settings):
        assert sql(db, "SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'") == '0'
        selected = copy.deepcopy(policy)
        selected['settings'].update(settings)
        document = json.dumps(selected, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(b'bigbrotr/product-policy/1\0' + document.encode()).hexdigest()
        sql(db, f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT {revision},dataset_id,{revision-1},config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision={revision-1}; UPDATE control.datasets SET policy_revision={revision}; COMMIT;")

    def launch(mode):
        with (root/f'recovery-{mode}.stdout').open('xb') as out, (root/f'recovery-{mode}.stderr').open('xb') as err:
            return subprocess.Popen(client + [mode], stdout=out, stderr=err)

    def completed(process, mode):
        assert process.wait(timeout=15) == 0, (root/f'recovery-{mode}.stderr').read_text()
        assert not (root/f'recovery-{mode}.stderr').read_text()

    process = lock = None
    lock_pid = None
    frozen_backend = None
    try:
        # Defer execution using the real configured native-memory admission limit.
        # This permits a deterministic completed logout before any new reader/unit.
        activate(3, {'administration.enabled': True, 'export.memory.max_bytes': 268435456})
        sql(db, 'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
        process = launch('accept')
        completed(process, 'accept')
        accepted = json.loads((private/'executor-signed-accept.json').read_text())
        assert accepted['status'] == 'PASS' and accepted['logged_out']
        result['acceptance'] = accepted
        job = accepted['receipt']['job_id']
        assert len(job) == 32 and all(c in '0123456789abcdef' for c in job)
        where = f"j.job_id=decode('{job}','hex')"

        def target():
            return json.loads(sql(db, f"SELECT jsonb_build_object('state',j.state,'revision',j.job_revision::text,'checkpoint',j.checkpoint_revision::text,'artifact',encode(u.artifact_id,'hex'),'artifacts',(SELECT count(*) FROM control.artifacts a WHERE a.job_id=j.job_id),'files',(SELECT count(*) FROM control.artifact_files f WHERE f.artifact_id=u.artifact_id),'export_retained',(SELECT state='active' FROM control.snapshot_leases WHERE lease_id=e.pin_lease_id)) FROM control.jobs j JOIN control.export_jobs e USING(job_id) LEFT JOIN control.export_execution_units u USING(job_id) WHERE {where}"))

        result['accepted_sql'] = target()
        assert result['accepted_sql']['state'] == 'accepted' and result['accepted_sql']['artifact'] is None
        assert result['accepted_sql']['checkpoint'] == '0' and result['accepted_sql']['export_retained']
        activate(4, {'administration.enabled': False, 'export.memory.max_bytes': 1073741824})
        lock_name = 'bigbrotr_export_fixture_completion_lock'
        lock_query = "BEGIN; LOCK TABLE control.artifacts IN SHARE MODE; SELECT pg_sleep(60); ROLLBACK;"
        with (root/'completion-lock.stdout').open('xb') as out, (root/'completion-lock.stderr').open('xb') as err:
            lock = subprocess.Popen(['docker', 'exec', '--env', 'PGAPPNAME='+lock_name, pg,
                'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, '-c', lock_query], stdout=out, stderr=err)
        lock_pid = until(lambda: sql(db, f"SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l USING(pid) WHERE a.application_name='{lock_name}' AND l.relation='control.artifacts'::regclass AND l.mode='ShareLock' AND l.granted"), bool, 'completion lock')
        assert lock_pid.isdecimal()
        process = launch('crash')
        until(lambda: (private/'executor-crash-ready.json').exists(), bool, 'crash runtime readiness')
        def waiter():
            return sql(db, f"SELECT jsonb_build_object('pid',a.pid,'backend_start',a.backend_start,'query',a.query,'query_start',a.query_start,'observed_at',clock_timestamp(),'wait',a.wait_event,'wait_type',a.wait_event_type,'blocking_pids',pg_blocking_pids(a.pid)) FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid AND o.control_backend_start=a.backend_start WHERE o.role='control' AND o.state='active' AND a.usename='bigbrotr_control' AND a.datname=current_database() AND a.wait_event_type='Lock' AND a.query LIKE 'INSERT INTO control.artifacts(%' AND {lock_pid}=ANY(pg_blocking_pids(a.pid))")
        first_wait = json.loads(until(waiter, bool, 'actual completion INSERT blocked'))
        (private/'executor-crash-freeze').write_text('freeze parent at observed completion wait\n')
        until(lambda: (private/'executor-crash-frozen.json').exists(), bool, 'parent freeze acknowledgement')
        frozen = json.loads((private/'executor-crash-frozen.json').read_text())
        current_wait = json.loads(waiter())
        assert current_wait['pid'] == first_wait['pid'] and current_wait['backend_start'] == first_wait['backend_start']
        backend_pid = str(current_wait['pid'])
        assert backend_pid.isdecimal()
        # Freeze only this positively identified PostgreSQL backend. Independent
        # sessions still inspect the lock manager. No timeout/rollback can advance
        # the pending INSERT while the parent is killed, even on a slow host.
        backend_stat = command('docker','exec',pg,'cat',f'/proc/{backend_pid}/stat').split()
        frozen_backend = (backend_pid, backend_stat[21])
        command('docker','exec',pg,'sh','-c','kill -STOP "$1"','fixture',backend_pid)
        until(lambda: command('docker','exec',pg,'cat',f'/proc/{backend_pid}/stat').split()[2], lambda v:v=='T', 'backend SIGSTOP observation', 2)
        pending_query = f"SELECT count(*) FROM pg_locks WHERE pid={backend_pid} AND relation='control.artifacts'::regclass AND mode='RowExclusiveLock' AND NOT granted"
        assert sql(db,pending_query) == '1'
        result['blocked_completion'] = dict(parent=frozen, initial=first_wait, frozen_backend=current_wait, backend_start_ticks=backend_stat[21], ungranted_row_exclusive_lock=True)
        result['before_crash'] = target()
        assert result['before_crash']['state'] == 'running' and result['before_crash']['checkpoint'] == '0'
        assert result['before_crash']['artifacts'] == result['before_crash']['files'] == 0
        artifact = result['before_crash']['artifact']
        assert len(artifact) == 32 and all(c in '0123456789abcdef' for c in artifact)
        directory = root/'out/signed_normal/export-destination'/artifact
        expected_event = json.loads((source/'experiments/identity-contract/signed-fixture.json').read_text())['events']['E_public']
        expected_bytes = (json.dumps({k:expected_event[k] for k in ['id','pubkey','created_at','kind','tags','content','sig']}, ensure_ascii=False, separators=(',', ':'))+'\n').encode()
        expected_manifest = copy.deepcopy(empty_manifest)
        expected_manifest.update(artifact_id=artifact, job_id=job)
        expected_manifest['selection']['event_id'] = expected_event['id']
        expected_manifest['accepted_revisions']['policy'] = '3'
        expected_manifest['files'] = [dict(file_id='events', path='events.jsonl', format='nostr-jsonl/1', bytes=str(len(expected_bytes)), rows='1', sha256=hashlib.sha256(expected_bytes).hexdigest())]
        assert (directory/'events.jsonl').read_bytes() == expected_bytes
        assert json.loads((directory/'manifest.json').read_text()) == expected_manifest
        output_hashes = {str((directory/name).relative_to(root/'out')):sha(directory/name) for name in ['events.jsonl', 'manifest.json']}
        result['output_hashes'] = output_hashes
        result['before_inodes'] = {name:(directory/name).stat().st_ino for name in ['events.jsonl', 'manifest.json']}
        result['literal_manifest'] = expected_manifest
        readers_query = "SELECT coalesce(jsonb_agg(jsonb_build_object('lease',to_jsonb(l),'lease_xmin',l.xmin::text,'pin',to_jsonb(p),'pin_xmin',p.xmin::text,'claims',(SELECT jsonb_agg(jsonb_build_object('row',to_jsonb(c),'xmin',c.xmin::text) ORDER BY claim_id) FROM control.retention_claims c WHERE c.lease_id=l.lease_id)) ORDER BY lease_id),'[]'::jsonb) FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) WHERE p.purpose='reader' AND l.state='active'"
        old_readers = json.loads(sql(db, readers_query))
        assert len(old_readers) == 1
        claims = old_readers[0]['claims']
        assert len(claims) == 1 and claims[0]['row']['state'] == 'active'
        assert claims[0]['row']['first_sequence'] == 1 and claims[0]['row']['last_sequence'] == 10
        result['unresolved_readers_before_crash'] = old_readers
        save()
        (private/'executor-crash-now').write_text('actual output present; SQL INSERT observed blocked\n')
        completed(process, 'crash')
        result['kill'] = json.loads((private/'executor-crash-killed.json').read_text())
        # The waiter is still frozen and really queued AFTER the parent died.
        assert command('docker','exec',pg,'cat',f'/proc/{backend_pid}/stat').split()[2] == 'T'
        assert sql(db,pending_query) == '1'
        result['kill_while_insert_pending'] = True
        command('docker','exec',pg,'sh','-c','kill -CONT "$1"','fixture',backend_pid)
        frozen_backend = None
        assert sql(db, f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={lock_pid} AND application_name='{lock_name}'") == 't'
        lock.wait(timeout=10)
        lock_pid = None
        for database in [db, catalog]:
            until(lambda: sql(database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','CatalogManager','bigbrotr_receipt_reader')"), lambda n:n=='0', 'product backend absent')
        result['after_crash'] = target()
        assert result['after_crash'] == result['before_crash']
        assert json.loads(sql(db, readers_query)) == old_readers
        assert all(sha(root/'out'/name) == digest for name,digest in output_hashes.items())
        save()
        process = launch('restart')
        until(lambda: (private/'executor-restart-ready.json').exists(), bool, 'restart readiness')
        result['after_restart'] = until(target, lambda value:value['state']=='succeeded', 'automatic recovered completion')
        assert result['after_restart']['artifact'] == artifact
        assert result['after_restart']['checkpoint'] == '1' and result['after_restart']['export_retained']
        assert result['after_restart']['artifacts'] == result['after_restart']['files'] == 1
        until(lambda: json.loads(sql(db, readers_query)), lambda rows:rows==old_readers, 'fresh reader released without touching unresolved old reader')
        (private/'executor-restart-stop').write_text('completed SQL observed independently\n')
        completed(process, 'restart')
        result['terminal_job'] = target()
        assert result['terminal_job'] == result['after_restart']
        assert all(sha(root/'out'/name) == digest for name,digest in output_hashes.items())
        inventory = json.loads(sql(db, f"SELECT jsonb_build_object('artifact',encode(a.artifact_id,'hex'),'manifest',encode(a.manifest_sha256,'hex'),'manifest_bytes',a.manifest_bytes::text,'bytes',f.byte_count::text,'rows',f.row_count::text,'sha256',encode(f.sha256,'hex'),'reader_released',l.state='released') FROM control.jobs j JOIN control.artifacts a USING(job_id) JOIN control.artifact_files f USING(artifact_id) JOIN control.snapshot_leases l ON l.lease_id=a.verification_pin_id WHERE {where}"))
        raw_manifest = (directory/'manifest.json').read_bytes()
        assert inventory == dict(artifact=artifact, manifest=hashlib.sha256(raw_manifest).hexdigest(), manifest_bytes=str(len(raw_manifest)), bytes=str(len(expected_bytes)), rows='1', sha256=hashlib.sha256(expected_bytes).hexdigest(), reader_released=True)
        result['inventory'] = inventory
        result['after_inodes'] = {name:(directory/name).stat().st_ino for name in ['events.jsonl', 'manifest.json']}
        assert all(result['before_inodes'][name] != result['after_inodes'][name] for name in result['before_inodes'])
        result['totals'] = json.loads(sql(db, "SELECT jsonb_build_object('jobs',(SELECT count(*) FROM control.jobs),'units',(SELECT count(*) FROM control.export_execution_units),'artifacts',(SELECT count(*) FROM control.artifacts),'files',(SELECT count(*) FROM control.artifact_files))"))
        assert result['totals'] == dict(jobs=2, units=2, artifacts=2, files=2)
        result['retained_old_readers'] = json.loads(sql(db, readers_query))
        assert result['retained_old_readers'] == old_readers
        result['terminal'] = {database:int(sql(database, "SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,catalog]}
        assert result['terminal'] == {db:0,catalog:0}
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'") == '0'
        assert not list((private/'scratch').iterdir())
        result['status'] = 'PASS'
        return result
    except BaseException as error:
        result['failure'] = type(error).__name__
        raise
    finally:
        if frozen_backend is not None:
            pid, start_ticks = frozen_backend
            observed = command('docker','exec',pg,'cat',f'/proc/{pid}/stat').split()
            assert observed[21] == start_ticks
            command('docker','exec',pg,'sh','-c','kill -CONT "$1"','fixture',pid)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        if lock_pid is not None:
            sql(db, f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={lock_pid} AND application_name='bigbrotr_export_fixture_completion_lock'")
        if lock is not None and lock.poll() is None:
            lock.wait(timeout=10)
        save()
