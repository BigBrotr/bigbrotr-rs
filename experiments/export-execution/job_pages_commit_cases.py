#!/usr/bin/env python3
"""Independent durable oracle for lost cursor-retention COMMIT and recovery."""
import json
from signed_publication_cases import Process, command, event, send, sha
from job_pages_cases import EnvelopeOracle


def run(root, state, sql):
    db = 'bbpub_control_normal'
    private = root/'out/private-grant-http'
    prepared = json.loads((root/'job-pages-cases.json').read_text())
    assert prepared['status'] == 'PASS' and len(prepared['consumer_expected']) == 6
    result = dict(status='INCOMPLETE', source=state['source'], cases=[])
    path = root/'job-pages-commit-cases.json'
    assert not path.exists()
    client = proxy = None

    def save(): path.write_text(json.dumps(result, indent=2)+'\n')
    def query(text): return sql(db, text)
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
        baseline = result['retention_before'] = rows('cursor_key_lifecycle')
        assert len(baseline) == 1 and baseline[0]['row']['decrypt_until_us'] is None
        for index, mode in enumerate(['before-forward', 'after-complete']):
            record = dict(mode=mode, status='INCOMPLETE')
            result['cases'].append(record); save()
            proxy = Process(['docker','exec','-i',state['container_id'],'python3','-B',
                '/out/build/source/experiments/archive-engine/native_commit_proxy.py',
                '--mode',mode,'--target','control-cursor-retention'], input_pipe=True)
            hello = record['proxy_ready'] = event(proxy,'ready')
            assert hello['source_sha256'] == sha(root/'source/experiments/archive-engine/native_commit_proxy.py')
            if index == 0:
                client = Process(['docker','exec','-i',state['container_id'],'python3','-B',
                    '/out/build/source/experiments/export-execution/job_pages_commit_client.py',str(hello['port'])], input_pipe=True)
            else: send(client,json.dumps(dict(port=hello['port']))+'\n')
            ready = record['ready'] = event(client,'ready-'+('before' if index == 0 else 'after'))
            current = record['owner'] = owner()
            assert current['present'] and [current[k] for k in ['resource','epoch','incarnation']] == [ready['owner'][k] for k in ['owner_resource_id','owner_epoch','process_incarnation']]
            if index:
                previous = result['cases'][0]['owner']
                assert current['resource'] == previous['resource'] and int(current['epoch']) == int(previous['epoch'])+1 and current['incarnation'] != previous['incarnation']
            record['sessions_before'] = sessions()
            record['clock_before_us'] = now(); save()
            send(client,'submit\n')
            fault = record['fault'] = event(proxy,'fault')
            assert fault['backend_pid'] == current['pid']
            assert fault['data_queries'] == fault['data_commits'] == fault['faults'] == 1
            assert fault['server_complete'] == index and fault['client_complete'] == 0 and not fault['rejection_sequence']
            failed = record['failed'] = event(client,'failed-'+('before' if index == 0 else 'after'))
            assert failed['exit'] == 4 and failed['parent_absent'] and failed['lock_available'] and failed['scratch_empty'] and 'CommitUnknown' in failed['error']
            record['clock_after_us'] = now()
            record['failed_owner'] = owner()
            record['retention'] = rows('cursor_key_lifecycle')
            record['ledger'] = ledger(); record['sessions_after'] = sessions()
            record['backends'] = backends(); save()
            assert record['failed_owner'] == {**current,'present':False}
            assert record['ledger'] == before and record['sessions_after'] == record['sessions_before']
            assert not any(record['backends'].values())
            if index == 0: assert record['retention'] == baseline
            else:
                assert len(record['retention']) == 1
                row = record['retention'][0]['row']; prior = baseline[0]['row']
                assert {k:v for k,v in row.items() if k != 'decrypt_until_us'} == {k:v for k,v in prior.items() if k != 'decrypt_until_us'}
                assert record['clock_before_us']+930_000_000 <= row['decrypt_until_us'] <= record['clock_after_us']+930_000_000
                assert record['retention'][0]['xmin'] != baseline[0]['xmin']
            send(proxy,'stop\n'); assert proxy.wait(5) == 0
            record['proxy_terminal'] = event(proxy,'terminal')
            assert record['proxy_terminal']['unexpected'] == 0 and not proxy.stderr and not proxy.overflow
            proxy.stop(); proxy = None
            record['status'] = 'PASS'; save()

        oracle = EnvelopeOracle((private/'job-page-keys/ring').read_bytes())
        result['oracle'] = oracle.identity
        send(client,'recover\n'); result['recovery_ready'] = event(client,'ready-recover')
        assert int(result['recovery_ready']['owner']['owner_epoch']) == int(current['epoch'])+1
        result['recovery_sessions_before'] = sessions()
        result['pages'] = []
        for name, expected in [('first',prepared['consumer_expected'][:2]),
                               ('second',prepared['consumer_expected'][2:4]),
                               ('repeat',prepared['consumer_expected'][2:4])]:
            clock_before = now()
            send(client,name+'\n'); page = event(client,name)
            clock_after = now()
            decoded = oracle.open(json.loads((private/'page-commit-cursors.private').read_text())[name])
            retention = rows('cursor_key_lifecycle')
            result['pages'].append(dict(response=page,decoded=decoded,retention=retention,
                clock_before_us=clock_before,clock_after_us=clock_after)); save()
            assert page['status'] == 200 and page['has_next']
            body = page['page']
            assert body['format'] == 'bigbrotr-job-list/1' and body['authority'] == 'observation-only' and body['effective_page_size'] == 2
            assert [{k:v for k,v in item.items() if k != 'observed_at_us'} for item in body['items']] == expected
            assert all(item['observed_at_us'] == body['observed_at_us'] for item in body['items'])
            assert decoded['cut'] == 6 and decoded['expires'] == decoded['issued']+900_000_000
            assert len(retention) == 1 and retention[0]['row']['decrypt_until_us'] == decoded['expires']+30_000_000
            if name == 'first':
                committed = result['cases'][1]['retention'][0]
                assert clock_before <= decoded['issued'] <= clock_after
                assert clock_before > committed['row']['decrypt_until_us']-930_000_000
                assert retention[0]['row']['decrypt_until_us'] > committed['row']['decrypt_until_us']
                assert retention[0]['xmin'] != committed['xmin']
                assert {k:v for k,v in retention[0]['row'].items() if k != 'decrypt_until_us'} == {k:v for k,v in committed['row'].items() if k != 'decrypt_until_us'}
            first = result['pages'][0]
            assert retention == first['retention']
            assert (decoded['issued'],decoded['expires']) == (first['decoded']['issued'],first['decoded']['expires'])
        result['after'] = ledger(); result['recovery_sessions_after'] = sessions(); save()
        assert result['after'] == before and result['recovery_sessions_after'] == result['recovery_sessions_before']
        send(client,'stop\n'); result['terminal'] = event(client,'terminal')
        assert result['terminal'] == dict(event='terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
        assert client.wait(10) == 0 and not client.stderr and not client.overflow
        result['backends'] = backends()
        assert not any(result['backends'].values())
        assert query("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state<>'released'") == '0'
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
