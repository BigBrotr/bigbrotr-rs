#!/usr/bin/env python3
"""Resume expiry across actual PG account/source row waits and a live control."""
import json
import re
import secrets
import subprocess
import time
from signed_publication_cases import Process, command, event, send, sha
from resume_browser_oracle import session_mutation
from resume_commit_cases import accepted_resume


def run(root,state,sql):
    result=dict(status='INCOMPLETE',source=state['source'],cases=[])
    path=root/'resume-lock-cases.json';assert not path.exists()
    client=proxy=holder=None
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    tables=['jobs','job_input_bounds','export_jobs','export_execution_units','artifacts','artifact_files','snapshot_leases','archive_snapshot_pins','retention_claims','export_resume_commands','management_commands','management_audit']
    def observe():
        return {name:json.loads(sql(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t")) for name in tables}
    def sessions():
        return json.loads(sql("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'actor',encode(principal_id,'hex'),'dataset',encode(dataset_id,'hex'),'issued',issued_at_us,'revoked',revoked_at_us,'expires',expires_at_us,'idle_expires',idle_expires_at_us,'last_seen',last_seen_at_us,'idle_window',idle_window_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def clock():return int(sql('SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
    def owner():
        raw=sql("SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=control_backend_pid AND backend_start=control_backend_start)) FROM control.owner_epochs WHERE role='control' AND state='active'")
        return json.loads(raw) if raw else None
    def stop_proxy():
        send(proxy,'stop\n');value=event(proxy,'terminal');assert proxy.wait(5)==0 and value['unexpected']==0 and not proxy.stderr and not proxy.overflow;return value
    try:
        sql('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control;')
        for name in ['account-expiry','source-expiry','source-live']:
            record=dict(name=name,status='INCOMPLETE');result['cases'].append(record);save()
            before=record['prepared']=observe();job=before['jobs'][0]['row']
            assert len(before['jobs'])==1 and (job['state'],job['job_revision'])==('paused',7)
            actor=job['principal_id'][2:];job_id=job['job_id'][2:];key=secrets.token_hex(16);record['key']=key
            mode='pass' if name=='account-expiry' else 'hold-resume-authority'
            proxy=Process(['docker','exec','-i',state['container_id'],'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode',mode,'--target','control-export-resume'],input_pipe=True)
            hello=record['proxy_ready']=event(proxy,'ready');assert hello['source_sha256']==sha(root/'source/experiments/archive-engine/native_commit_proxy.py')
            client=Process(['docker','exec','-i',state['container_id'],'python3','-B','/out/build/source/experiments/export-execution/resume_lock_client.py',name,str(hello['port']),job_id],input_pipe=True)
            ready=record['ready']=event(client,'ready',timeout=20);current=record['owner']=owner()
            assert current['present'] and [current[k] for k in ['resource','epoch','incarnation']]==[ready['owner'][k] for k in ['owner_resource_id','owner_epoch','process_incarnation']]
            baseline=record['login_baseline']=observe();login_sessions=record['login_sessions']=sessions()
            record['login_audit']=session_mutation(before,baseline,login_sessions,'issue',actor)
            private=json.loads((root/'out/private-grant-http'/('job-resume-lock-'+name+'.private')).read_text())
            assert private['id']==ready['session'] and re.fullmatch('[0-9a-f]{32}',private['id'])
            assert all(re.fullmatch('[0-9a-f]{64}',private[k]) for k in ['token','csrf'])
            holder=Process(['docker','exec','-i',state['postgres_id'],'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','bbpub_control_normal'],input_pipe=True)
            send(holder,"SET application_name='resume-lock-holder'; SET statement_timeout='4s'; SET idle_in_transaction_session_timeout='8s'; SELECT json_build_object('event','holder-open','pid',pg_backend_pid());\n")
            opened=record['holder']=event(holder,'holder-open')
            audit_id=secrets.token_hex(16)
            provision=f"""BEGIN;
WITH t AS (SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint n)
INSERT INTO control.browser_sessions(session_id,dataset_id,principal_id,credential_revision,authentication_revision,token_digest,csrf_digest,issued_at_us,expires_at_us,idle_window_us,last_seen_at_us,idle_expires_at_us)
SELECT decode('{private['id']}','hex'),dataset_id,principal_id,credential_revision,authentication_revision,decode('{private['token']}','hex'),decode('{private['csrf']}','hex'),n,n+60000000,1250000,n,n+1250000 FROM control.local_accounts,t WHERE principal_id=decode('{actor}','hex');
INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
SELECT decode('{audit_id}','hex'),dataset_id,principal_id,'browser.session.issue','accepted',issued_at_us,1,session_id,2 FROM control.browser_sessions WHERE session_id=decode('{private['id']}','hex');
SELECT json_build_object('session',jsonb_build_object('id',encode(s.session_id,'hex'),'actor',encode(s.principal_id,'hex'),'dataset',encode(s.dataset_id,'hex'),'issued',s.issued_at_us,'revoked',s.revoked_at_us,'expires',s.expires_at_us,'idle_expires',s.idle_expires_at_us,'last_seen',s.last_seen_at_us,'idle_window',s.idle_window_us,'xmin',s.xmin::text),'audit',jsonb_build_object('row',to_jsonb(a),'xmin',a.xmin::text)) FROM control.browser_sessions s JOIN control.management_audit a ON a.subject_id=s.session_id WHERE s.session_id=decode('{private['id']}','hex'); COMMIT;"""
            # PostgreSQL error DETAIL can include the entire rejected row. Never
            # send these session digests through the generic SQL error reporter.
            provisioned=subprocess.run(['docker','exec','-i',state['postgres_id'],'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','bbpub_control_normal'],input=provision,text=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=5)
            assert provisioned.returncode==0, 'short session provisioning failed (details withheld)'
            seeded=record['short_session_seed']=json.loads(provisioned.stdout);del private,provision,provisioned
            if name=='account-expiry':
                send(holder,f"BEGIN; DO $$ BEGIN PERFORM 1 FROM control.local_accounts WHERE principal_id=decode('{actor}','hex') FOR UPDATE; END $$; SELECT json_build_object('event','locked');\n")
                event(holder,'locked',timeout=2)
            send(client,json.dumps(dict(operation='resume',key=key))+'\n');record['submitted']=event(client,'resume-submitted')
            if name!='account-expiry':
                held=record['proxy_held']=event(proxy,'held',timeout=2)
                assert held['backend_pid']==current['pid'] and held['resume_authority_requests']==2 and held['stage']=='resume-authority'
                send(holder,f"BEGIN; DO $$ BEGIN PERFORM 1 FROM control.source_streams s JOIN control.job_input_bounds b USING(dataset_id,shard_id,owner_epoch) WHERE b.job_id=decode('{job_id}','hex') ORDER BY s.dataset_id,s.shard_id,s.owner_epoch FOR UPDATE OF s; END $$; SELECT json_build_object('event','locked');\n")
                event(holder,'locked',timeout=2);send(proxy,'release\n')
            deadline=time.monotonic()+1.5
            while True:
                rows=json.loads(sql(f"SELECT coalesce(json_agg(json_build_object('pid',w.pid,'start',w.backend_start,'wait_type',w.wait_event_type,'wait',w.wait_event,'blocker',h.pid,'blocker_start',h.backend_start,'time_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'authority_query',w.query='SELECT control.require_export_execution_authority($1::bytea)','account_query',w.query LIKE '%FROM control.local_accounts%FOR UPDATE%')),'[]') FROM pg_stat_activity w JOIN pg_stat_activity h ON h.pid=ANY(pg_blocking_pids(w.pid)) WHERE w.pid={current['pid']} AND h.pid={opened['pid']} AND w.wait_event_type='Lock'"))
                if rows:break
                assert time.monotonic()<deadline,'actual PG lock wait missing';time.sleep(.01)
            assert len(rows)==1;blocked=record['blocked']=rows[0]
            assert blocked['start']==current['start'] and blocked['time_us']<seeded['session']['idle_expires']
            assert blocked['authority_query'] if name!='account-expiry' else blocked['account_query']
            if name.endswith('expiry'):
                until=seeded['session']['idle_expires']+50000
                while clock()<until:time.sleep(.01)
            record['release_time_us']=clock()
            assert (record['release_time_us']>=seeded['session']['idle_expires'])==name.endswith('expiry')
            send(holder,"ROLLBACK; SELECT json_build_object('event','unlocked');\n\\q\n");event(holder,'unlocked');assert holder.wait(5)==0 and not holder.stderr;holder.stop();holder=None
            response=record['response']=event(client,'resume-response',timeout=5);assert response['elapsed_seconds']<4
            assert response['status']==(202 if name=='source-live' else 401)
            after=record['after']=observe();record['sessions_after']=sessions()
            expected={**baseline,'management_audit':baseline['management_audit']+[seeded['audit']]}
            # Canonicalize row ordering independently of SQL JSONB text sorting.
            canonical=lambda rows:sorted(rows,key=lambda x:json.dumps(x,sort_keys=True))
            for table in expected:expected[table]=canonical(expected[table]);after[table]=canonical(after[table])
            if name=='source-live':
                receipt=record['receipt']=accepted_resume(expected,after,current,key)
                detail=next(v['row'] for v in after['export_resume_commands'] if v not in expected['export_resume_commands'])
                assert detail['session_id']=='\\x'+seeded['session']['id']
                assert record['release_time_us']<=receipt['accepted_at_us']<seeded['session']['idle_expires']
                expected_receipt=dict(format='bigbrotr-export-resume/1',command_key=key,job_id=job_id,job_revision='8',accepted_at_us=str(receipt['accepted_at_us']),created=True,execution='unobserved',authority='acceptance-only')
                assert response['body']==expected_receipt
            else:assert after==expected
            expected_sessions=login_sessions+[seeded['session']]
            assert sorted(record['sessions_after'],key=lambda x:x['id'])==sorted(expected_sessions,key=lambda x:x['id'])
            send(client,json.dumps(dict(operation='health',key=key))+'\n');record['health']=event(client,'health-response');assert record['health']['status']==200
            while clock()<seeded['session']['idle_expires']+50000:time.sleep(.01)
            send(client,json.dumps(dict(operation='historical-short',key=key))+'\n');record['expired_retry']=event(client,'historical-short-response');assert record['expired_retry']['status']==401
            if name=='source-live':
                send(client,json.dumps(dict(operation='historical-long',key=key))+'\n');retried=record['historical_retry']=event(client,'historical-long-response');assert retried['status']==200 and retried['body']=={**expected_receipt,'created':False}
            send(client,json.dumps(dict(operation='stop'))+'\n');record['client_terminal']=event(client,'terminal');assert client.wait(10)==0 and not client.stderr and not client.overflow
            record['proxy_terminal']=stop_proxy();assert record['proxy_terminal']['data_queries']==int(name=='source-live')
            assert record['proxy_terminal']['data_commits']==record['proxy_terminal']['server_complete']==record['proxy_terminal']['client_complete']==int(name=='source-live')
            client.stop();client=None;proxy.stop();proxy=None
            record['terminal']=observe();assert all(canonical(record['terminal'][t])==after[t] for t in tables)
            record['terminal_sessions']=sessions();assert record['terminal_sessions']==record['sessions_after']
            record['status']='PASS';save()
        result['backends']={db:int(command('docker','exec',state['postgres_id'],'psql','-XAtq','-U','postgres','-d',db,'-c',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for db in ['bbpub_normal','bbpub_control_normal']}
        assert result['backends']==dict(bbpub_normal=0,bbpub_control_normal=0) and owner() is None
        result['status']='PASS';return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        errors=[]
        for name,p in [('holder',holder),('client',client),('proxy',proxy)]:
            if p:
                try:
                    if name=='holder':send(p,'ROLLBACK;\n\\q\n')
                    elif name=='client':send(p,json.dumps(dict(operation='stop'))+'\n')
                    else:send(p,'stop\n')
                    p.wait(10)
                except BaseException as error:errors.append(dict(process=name,error=type(error).__name__))
                finally:
                    try:p.stop()
                    except BaseException as error:errors.append(dict(process=name+'-stop',error=type(error).__name__))
                result[name+'_observation']=dict(stdout=p.stdout.decode(errors='replace'),stderr=p.stderr.decode(errors='replace'),exit=p.process.poll(),overflow=p.overflow)
        try:result['terminal_observation']=observe();result['terminal_sessions']=sessions();result['terminal_owner']=owner()
        except BaseException as error:errors.append(dict(process='sql-observer',error=type(error).__name__))
        result['cleanup_errors']=errors
        if errors:result['status']='INCOMPLETE'
        save()
