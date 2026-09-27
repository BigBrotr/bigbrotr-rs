#!/usr/bin/env python3
"""Installed page consumer: independent SQL/ownership oracle and real browsers."""
import json
import time
from signed_publication_cases import Process, command, event, send, sha
from browser_cases import IMAGE


def run(root, state, sql, start, finish, tools):
    out = root/'out'
    engine, mount = state['job_pages_browser_pair'].split('-')
    prepared = json.loads((root/'job-pages-cases.json').read_text())
    assert prepared['status'] == 'PASS' and len(prepared['consumer_expected']) == 6
    runner, pg, db = state['container_id'], state['postgres_id'], 'bbpub_control_normal'
    result = dict(status='INCOMPLETE', source=state['source'], engine=engine, mount=mount, commands=[])
    path = root/'job-pages-browser-cases.json'
    assert not path.exists()
    server = holder = None
    def save(): path.write_text(json.dumps(result, indent=2)+'\n')
    def query(text): return sql(db, text)
    def ledger():
        tables = ['jobs','job_admissions','job_input_bounds','export_jobs','export_execution_units',
                  'artifacts','artifact_files','snapshot_leases','archive_snapshot_pins','retention_claims']
        return {name:json.loads(query(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t")) for name in tables}
    def sessions():
        return json.loads(query("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def bind(p, target, ro=False): return ['--mount',f'type=bind,src={p},dst={target}'+(',readonly' if ro else '')]
    try:
        before = result['before'] = ledger()
        case = out/'browser-cases'/('job-pages-'+engine+'-'+mount)
        case.mkdir(parents=True, mode=0o700)
        (case/'input.json').write_text(json.dumps(dict(expected=prepared['consumer_expected']))+'\n')
        server = Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/browser_server.py',engine+'-'+mount,mount],input_pipe=True)
        result['ready'] = event(server,'ready'); save()
        ident = start('job-pages-browser',IMAGE,
            ['/tools/node/bin/node','/out/frontend-source/experiments/export-execution/job_pages_browser.mjs',engine,mount],
            bind(out,'/out',True)+bind(case,'/case')+bind(tools,'/tools',True)+
            ['--env','PLAYWRIGHT_BROWSERS_PATH=/ms-playwright','--env','HOME=/tmp'],'container:'+pg)
        result['browser_container'] = ident; save()
        expected = ['node-before','node-after','browser-before','browser-after','restart',
                    'hold-account','verify-blocked','release-account','deactivate']
        index = 0; boundary = None; deadline = time.monotonic()+210
        while True:
            assert time.monotonic() < deadline, 'browser host deadline'
            entry = case/f'command-{index+1}.json'
            if entry.exists():
                assert entry.stat().st_size < 1024
                request = json.loads(entry.read_text())
                assert index < len(expected) and request == dict(name=expected[index])
                name = request['name']; detail = dict(name=name)
                # Preserve the observation before assertions, not merely booleans.
                detail['ledger'] = ledger(); detail['sessions'] = sessions()
                result['commands'].append(detail); save()
                assert detail['ledger'] == before
                if name.endswith('-before'): boundary = detail['sessions']
                elif name.endswith('-after'): assert detail['sessions'] == boundary
                elif name == 'restart':
                    send(server,json.dumps(dict(operation='restart'))+'\n')
                    detail['stopped'] = event(server,'restart-terminal')
                    detail['ready'] = event(server,'restarted')
                    assert detail['stopped'] == dict(event='restart-terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
                    assert int(detail['ready']['owner']['owner_epoch']) == int(result['ready']['owner']['owner_epoch'])+1
                    assert sessions() == detail['sessions']
                elif name == 'hold-account':
                    current_backend = json.loads(query("SELECT jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start)) FROM pg_stat_activity WHERE usename='bigbrotr_control' AND datname=current_database()"))
                    assert len(current_backend) == 1
                    detail['control_backend'] = current_backend[0]
                    holder = Process(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input_pipe=True)
                    send(holder,"BEGIN; SELECT json_build_object('event','locked','pid',pg_backend_pid()) FROM control.local_accounts WHERE username='alice' FOR UPDATE;\n")
                    detail['holder'] = event(holder,'locked'); held_pid = detail['holder']['pid']; held_at = time.monotonic()
                elif name == 'verify-blocked':
                    until = time.monotonic()+2
                    while True:
                        waiting = json.loads(query(f"SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start,'query',query,'wait',wait_event_type)),'[]') FROM pg_stat_activity WHERE {held_pid}=ANY(pg_blocking_pids(pid)) AND datname=current_database() AND usename='bigbrotr_control'"))
                        if waiting: break
                        assert time.monotonic() < until; time.sleep(.01)
                    assert len(waiting) == 1 and waiting[0]['wait'] == 'Lock'
                    assert {key:waiting[0][key] for key in ['pid','backend_start']} == current_backend[0]
                    assert 'FROM control.local_accounts' in waiting[0]['query'] and 'FOR UPDATE' in waiting[0]['query']
                    detail['waiter'] = waiting[0]
                elif name == 'release-account':
                    send(holder,'ROLLBACK;\n\\q\n'); assert holder.wait(5) == 0; holder.stop(); holder = None
                    detail['held_seconds'] = time.monotonic()-held_at
                    assert detail['held_seconds'] < 3
                elif name == 'deactivate':
                    # Isolated account-state transition; not account-editor evidence.
                    query("BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; UPDATE control.local_accounts SET active=false WHERE username='alice'; COMMIT;")
                save(); index += 1
                temporary = case/f'response-{index}.tmp'
                temporary.write_text(json.dumps(dict(state='verified'))+'\n')
                temporary.rename(case/f'response-{index}.json')
            c = json.loads(command('docker','inspect',ident))[0]
            assert c['Config']['Labels'][state['owner_label']] == state['owner']
            if not c['State']['Running']: break
            time.sleep(.025)
        result['container_ok'] = finish(ident,'job-pages-browser',5)
        result['browser'] = json.loads((case/'browser-report.json').read_text())
        result['after'] = ledger(); result['terminal_sessions'] = sessions(); save()
        assert result['container_ok'] and result['browser']['status'] == 'PASS' and index == len(expected)
        assert result['after'] == before
        send(server,json.dumps(dict(operation='stop'))+'\n')
        result['terminal'] = event(server,'stop-terminal')
        assert result['terminal'] == dict(event='stop-terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
        assert server.wait(12) == 0 and not server.stderr and not server.overflow
        result['backends'] = {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        assert not any(result['backends'].values())
        assert query("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state<>'released'") == '0'
        manifest = json.loads((out/'build/source-manifest.json').read_text())
        assert all(sha(out/'frontend-source'/name) == digest for name,digest in manifest['files_sha256'].items())
        result['bundles'] = json.loads((out/'frontend-result.json').read_text())['bundles']
        for name,identity in result['bundles'].items():
            bundle = out/'bundles'/name
            assert sha(bundle/'manifest.json') == identity['manifest_sha256']
            for item in json.loads((bundle/'manifest.json').read_text())['files']:
                assert (bundle/item['path']).stat().st_size == item['bytes'] and sha(bundle/item['path']) == item['sha256']
        result['status'] = 'PASS'; return result
    except BaseException as error:
        result['failure'] = type(error).__name__; raise
    finally:
        for name,process in [('holder',holder),('server',server)]:
            if process:
                try: process.stop()
                except BaseException as error:
                    result.setdefault('cleanup_errors',[]).append(dict(process=name,error=type(error).__name__))
                    result['status'] = 'INCOMPLETE'
        save()
