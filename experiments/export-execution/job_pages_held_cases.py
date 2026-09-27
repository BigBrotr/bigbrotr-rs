#!/usr/bin/env python3
"""Held cursor COMMIT/reply: authority timeout, SIGTERM and sampled local custody."""
import json
import time
from signed_publication_cases import Process, command, event, send, sha


def run(root, state, sql):
    db = 'bbpub_control_normal'
    private = root/'out/private-grant-http'
    result = dict(status='INCOMPLETE',source=state['source'],cases=[])
    path = root/'job-pages-held-cases.json'; assert not path.exists()
    client = proxy = None
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def query(text): return sql(db,text)
    def rows(table):
        return json.loads(query(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{table} t"))
    def ledger():
        return {table:rows(table) for table in ['jobs','job_admissions','job_input_bounds',
            'export_jobs','export_execution_units','artifacts','artifact_files',
            'snapshot_leases','archive_snapshot_pins','retention_claims']}
    def sessions():
        return json.loads(query("SELECT jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'xmin',xmin::text) ORDER BY session_id) FROM control.browser_sessions"))
    def owner():
        return json.loads(query("SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=control_backend_pid AND backend_start=control_backend_start)) FROM control.owner_epochs WHERE role='control' AND state='active'"))
    def backends():
        return {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
    def now(): return int(query("SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint"))

    try:
        before = result['before'] = ledger()
        assert len(before['jobs']) == 6
        for index, (stage, trigger) in enumerate([
            ('commit','deadline'),('reply','deadline'),('commit','signal'),('reply','signal')]):
            record = dict(stage=stage,trigger=trigger,status='INCOMPLETE')
            result['cases'].append(record); save()
            proxy = Process(['docker','exec','-i',state['container_id'],'python3','-B',
                '/out/build/source/experiments/archive-engine/native_commit_proxy.py',
                '--mode','hold-'+stage,'--target','control-cursor-retention'],input_pipe=True)
            hello = record['proxy_ready'] = event(proxy,'ready')
            assert hello['source_sha256'] == sha(root/'source/experiments/archive-engine/native_commit_proxy.py')
            if index == 0:
                client = Process(['docker','exec','-i',state['container_id'],'python3','-B',
                    '/out/build/source/experiments/export-execution/job_pages_held_client.py',str(hello['port'])],input_pipe=True)
            else: send(client,json.dumps(dict(port=hello['port']))+'\n')
            record['ready'] = event(client,f'ready-{index}'); assert record['ready']['index'] == index
            current = record['owner'] = owner()
            assert current['present'] and [current[k] for k in ['resource','epoch','incarnation']] == [record['ready']['owner'][k] for k in ['owner_resource_id','owner_epoch','process_incarnation']]
            if index:
                previous = result['cases'][index-1]['owner']
                assert current['resource'] == previous['resource'] and int(current['epoch']) == int(previous['epoch'])+1 and current['incarnation'] != previous['incarnation']
            record['sessions_before'] = sessions()
            record['retention_before'] = rows('cursor_key_lifecycle')
            assert len(record['retention_before']) == 1
            record['clock_before_us'] = now(); save()
            send(client,'submit\n'); record['submitted'] = event(client,f'submitted-{index}')
            held = record['held'] = event(proxy,'held')
            assert held['stage'] == stage and held['backend_pid'] == current['pid']
            assert held['data_queries'] == held['data_commits'] == 1
            assert held['server_complete'] == (stage == 'reply') and held['client_complete'] == 0 and not held['rejection_sequence']
            record['held_owner'] = owner(); assert record['held_owner'] == current
            send(client,'observe\n'); record['observing'] = event(client,f'observing-{index}')
            record['client_terminal'] = event(client,f'terminal-{index}',timeout=12)
            assert record['client_terminal'] == dict(event=f'terminal-{index}',index=index,status='PASS')
            record['client'] = json.loads((private/f'job-pages-held-{stage}-{trigger}.json').read_text())
            record['clock_after_us'] = now()
            record['after_local_exit'] = dict(owner=owner(),backends=backends(),retention=rows('cursor_key_lifecycle'))
            save()
            sample = record['client']
            assert sample['status'] == 'PASS' and sample['violations'] == 0 and sample['observed_interval_seconds'] >= .5
            assert sample['drain_observation']['interval_seconds'] >= .5
            assert sample['terminal']['elapsed_seconds'] < 8 and sample['terminal']['exit'] == 4
            # A held frontend COMMIT prevents the proxy from reading local EOF.
            # Its separate server endpoint can outlive the terminated driver.
            # Do not misreport this as leaked local custody or start a new owner.
            remote_present = stage == 'commit'
            assert record['after_local_exit']['owner'] == {**current,'present':remote_present}
            assert record['after_local_exit']['backends'] == {db:int(remote_present),'bbpub_normal':0}
            retention = record['after_local_exit']['retention']
            prior = record['retention_before']
            if stage == 'commit': assert retention == prior
            else:
                assert len(retention) == 1 and retention[0]['xmin'] != prior[0]['xmin']
                row = retention[0]['row']
                assert {k:v for k,v in row.items() if k != 'decrypt_until_us'} == {k:v for k,v in prior[0]['row'].items() if k != 'decrypt_until_us'}
                assert record['clock_before_us']+930_000_000 <= row['decrypt_until_us'] <= record['clock_after_us']+930_000_000
            send(proxy,'stop\n'); assert proxy.wait(5) == 0
            record['proxy_terminal'] = event(proxy,'terminal')
            assert record['proxy_terminal']['unexpected'] == 0 and record['proxy_terminal']['client_complete'] == 0
            assert not proxy.stderr and not proxy.overflow
            proxy.stop(); proxy = None
            deadline = time.monotonic()+5
            while any(backends().values()):
                assert time.monotonic() < deadline
                time.sleep(.01)
            record['after_proxy_close'] = dict(owner=owner(),backends=backends(),retention=rows('cursor_key_lifecycle'))
            record['ledger'] = ledger(); record['sessions_after'] = sessions(); save()
            assert record['after_proxy_close'] == dict(owner={**current,'present':False},backends={db:0,'bbpub_normal':0},retention=retention)
            assert record['ledger'] == before and record['sessions_after'] == record['sessions_before']
            record['status'] = 'PASS'; save()
        send(client,json.dumps(dict(operation='stop'))+'\n')
        result['finished'] = event(client,'finished')
        assert client.wait(10) == 0 and not client.stderr and not client.overflow
        result['after'] = ledger(); assert result['after'] == before
        # The last failed owner intentionally remains active-but-absent; recovery is
        # qualified separately and must never be simulated by editing owner rows.
        result['status'] = 'PASS'; return result
    except BaseException as error:
        result['failure'] = type(error).__name__; raise
    finally:
        for name, process in [('client',client),('proxy',proxy)]:
            if process:
                try:
                    if name == 'proxy' and process.process.poll() is None: send(process,'stop\n')
                    process.stop()
                except BaseException as error:
                    result.setdefault('cleanup_errors',[]).append(dict(process=name,error=type(error).__name__))
                    result['status'] = 'INCOMPLETE'
                result[name+'_output'] = process.stdout.decode()
                result[name+'_error'] = process.stderr.decode()
        save()
