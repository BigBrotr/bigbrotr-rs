#!/usr/bin/env python3
"""Independent SQL oracle for the first installed resume/reload browser journey."""
import hashlib
import json
import re
import time
from signed_publication_cases import Process, command, event, send
from resume_commit_cases import accepted_resume
from browser_cases import IMAGE


def run(root, state, sql, start, finish, tools):
    out=root/'out';runner=state['container_id'];pg=state['postgres_id'];db='bbpub_control_normal'
    result=dict(status='INCOMPLETE',source=state['source'],native_stores={},commands=[])
    path=root/'resume-browser-cases.json';assert not path.exists()
    server=None
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def bind(p,target,ro=False):return ['--mount',f'type=bind,src={p},dst={target}'+(',readonly' if ro else '')]
    def query(text):return sql(db,text)
    def observe():
        tables=['jobs','job_input_bounds','export_jobs','export_execution_units','artifacts','artifact_files','snapshot_leases','archive_snapshot_pins','retention_claims','export_resume_commands','management_commands','management_audit']
        values={}
        for name in tables:
            values[name]=json.loads(query(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t"))
        return values
    def sessions():
        return json.loads(query("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    try:
        # Native store operations are separate from the installed UI. Fresh
        # isolated browser contexts contain only the synthetic non-secret data.
        for engine in ['chromium','firefox','webkit']:
            case=out/'browser-cases'/('resume-store-'+engine);case.mkdir(parents=True,mode=0o700)
            ident=start('resume-store-'+engine,IMAGE,['/tools/node/bin/node','/out/frontend-source/experiments/export-execution/resume_store_browser.mjs','/case',engine],
                        bind(out,'/out',True)+bind(case,'/case')+bind(tools,'/tools',True)+['--env','PLAYWRIGHT_BROWSERS_PATH=/ms-playwright','--env','HOME=/tmp'], 'none')
            ok=finish(ident,'resume-store-'+engine,90)
            value=json.loads((case/'report.json').read_text());result['native_stores'][engine]=dict(container=ident,container_ok=ok,report=value);save()
            assert ok and value['status']=='PASS' and len(value['cases'])==5
        query('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control;')
        assert json.loads(query("SELECT json_agg(username ORDER BY username) FROM control.local_accounts"))==['alice']
        # Isolated fixture provisioning, not an account-editor qualification.
        # The second account authenticates independently and has status.read only.
        bob='b7'*16
        query(f"BEGIN; INSERT INTO control.principals SELECT decode('{bob}','hex'),dataset_id,'local',created_at_us,database_role,local_os_uid FROM control.principals WHERE kind='installation'; INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision) SELECT decode('{bob}','hex'),dataset_id,'bob',false,true,credential_revision,1 FROM control.local_accounts WHERE username='alice'; INSERT INTO control.local_credentials SELECT decode('{bob}','hex'),dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash FROM control.local_credentials WHERE principal_id=(SELECT principal_id FROM control.local_accounts WHERE username='alice'); INSERT INTO control.principal_grants SELECT dataset_id,decode('{bob}','hex'),'status.read','dataset',dataset_id FROM control.datasets; COMMIT;")
        assert json.loads(query("SELECT json_agg(username ORDER BY username) FROM control.local_accounts"))==['alice','bob']
        before=result['before']=observe();assert len(before['jobs'])==1
        job=before['jobs'][0]['row'];assert (job['state'],job['job_revision'])==('paused',7)
        case=out/'browser-cases/resume-chromium-root';case.mkdir(parents=True,mode=0o700)
        (case/'input.json').write_text(json.dumps(dict(job=job['job_id'][2:],actor=job['principal_id'][2:]))+'\n')
        server=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/resume_browser_server.py','root'],input_pipe=True)
        result['ready']=event(server,'ready');owner=result['ready']['owner'];save()
        ident=start('resume-chromium-root',IMAGE,['/tools/node/bin/node','/out/frontend-source/experiments/export-execution/resume_browser.mjs'],
                    bind(out,'/out',True)+bind(case,'/case')+bind(tools,'/tools',True)+['--env','PLAYWRIGHT_BROWSERS_PATH=/ms-playwright','--env','HOME=/tmp'],'container:'+pg)
        result['browser_container']=ident;save()
        expected=['before-loss','accepted','before-retry','after-retry','restart','after-restart-retry','deactivate','reactivate','forgotten']
        index=0;boundary=None;after=None;deadline=time.monotonic()+150
        while True:
            assert time.monotonic()<deadline,'browser host deadline'
            entry=case/f'command-{index+1}.json'
            if entry.exists():
                assert entry.stat().st_size<1024
                request=json.loads(entry.read_text());name=request['name'];assert name==expected[index]
                assert set(request)==({'name','key','body'} if name=='accepted' else {'name'})
                detail=dict(name=name);reply=dict(state='verified')
                if name=='before-loss':
                    assert observe()==before;boundary=sessions();detail['sessions']=boundary
                elif name=='accepted':
                    key=request['key'];assert re.fullmatch('[0-9a-f]{32}',key) and key!='0'*32
                    assert request['body']==dict(expected_job_revision='7',expected_config_revision='1',expected_policy_revision='2')
                    after=result['accepted']=observe()
                    accepted=result['receipt']=accepted_resume(before,after,dict(resource=owner['owner_resource_id'],epoch=owner['owner_epoch'],incarnation=owner['process_incarnation']),key)
                    reply['receipt']=dict(format='bigbrotr-export-resume/1',job_id=job['job_id'][2:],command_key=key,job_revision='8',accepted_at_us=str(accepted['accepted_at_us']),created=True,execution='unobserved',authority='acceptance-only')
                    assert sessions()==boundary;detail['session_unchanged']=True;detail['key']=key
                elif name=='before-retry':boundary=sessions();detail['sessions']=boundary;assert observe()==after
                elif name in ['after-retry','after-restart-retry']:
                    assert sessions()==boundary and observe()==after;detail['sessions_unchanged']=True;detail['ledgers_unchanged']=True
                elif name=='restart':
                    send(server,json.dumps(dict(operation='restart'))+'\n')
                    detail['stopped']=event(server,'restart-terminal');detail['ready']=event(server,'restarted')
                    assert detail['stopped']==dict(event='restart-terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
                    current=detail['ready']['owner'];assert int(current['owner_epoch'])==int(owner['owner_epoch'])+1 and current['process_incarnation']!=owner['process_incarnation']
                    query("BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; SELECT 1 FROM control.local_accounts WHERE username='alice' FOR UPDATE; DELETE FROM control.principal_grants WHERE action='data.read'; COMMIT;")
                    assert sessions()==boundary and observe()==after
                elif name in ['deactivate','reactivate']:
                    query("BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; UPDATE control.local_accounts SET active="+('false' if name=='deactivate' else 'true')+" WHERE username='alice'; COMMIT;")
                    assert observe()==after
                elif name=='forgotten':assert observe()==after;detail['ledgers_unchanged']=True
                result['commands'].append(detail);save();index+=1
                temporary=case/f'response-{index}.tmp';temporary.write_text(json.dumps(reply)+'\n');temporary.rename(case/f'response-{index}.json')
            c=json.loads(command('docker','inspect',ident))[0]
            assert c['Config']['Labels'][state['owner_label']]==state['owner']
            if not c['State']['Running']:break
            time.sleep(.025)
        result['browser_container_ok']=finish(ident,'resume-chromium-root',5)
        result['browser']=json.loads((case/'browser-report.json').read_text());save()
        assert result['browser_container_ok'] and result['browser']['status']=='PASS' and index==len(expected)
        send(server,json.dumps(dict(operation='stop'))+'\n');result['terminal']=event(server,'stop-terminal')
        assert result['terminal']==dict(event='stop-terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
        assert server.wait(12)==0 and not server.stderr and not server.overflow
        result['after']=observe();assert result['after']==after
        result['backends']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        assert not any(result['backends'].values())
        assert query("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
        manifest=json.loads((out/'build/source-manifest.json').read_text())
        assert all(sha(out/'frontend-source'/name)==digest for name,digest in manifest['files_sha256'].items())
        bundles=json.loads((out/'frontend-result.json').read_text())['bundles']
        for mount,identity in bundles.items():
            bundle=out/'bundles'/mount;assert sha(bundle/'manifest.json')==identity['manifest_sha256']
            for item in json.loads((bundle/'manifest.json').read_text())['files']:
                assert (bundle/item['path']).stat().st_size==item['bytes'] and sha(bundle/item['path'])==item['sha256']
        result['frontend_originals_and_bundles_unchanged']=True
        result['status']='PASS';return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        try:
            if server:server.stop()
        except BaseException as error:
            result['cleanup_error']=type(error).__name__;result['status']='INCOMPLETE';raise
        finally:save()
