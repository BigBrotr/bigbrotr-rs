#!/usr/bin/env python3
"""Real Control renewal and unchanged paused unit during blocked resume checking."""
import json
import time
from signed_publication_cases import Process, command, event, send
from resume_browser_oracle import session_mutation


def run(root, state, sql):
    result=dict(status='INCOMPLETE', source=state['source'])
    path=root/'resume-checker-cases.json'; assert not path.exists()
    client=None
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def observe():
        tables=['jobs','job_input_bounds','export_jobs','export_execution_units','artifacts','artifact_files',
                'snapshot_leases','archive_snapshot_pins','retention_claims','export_resume_commands','management_commands','management_audit']
        return {name:json.loads(sql(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t")) for name in tables}
    def identities():
        return json.loads(sql("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'actor',encode(principal_id,'hex'),'dataset',encode(dataset_id,'hex'),'issued',issued_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def idle():
        return json.loads(sql("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def owner():
        return json.loads(sql("SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,'incarnation',encode(process_incarnation,'hex'),'expires',lease_expires_at_us,'pid',control_backend_pid,'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=control_backend_pid AND backend_start=control_backend_start)) FROM control.owner_epochs WHERE role='control' AND state='active'"))
    try:
        sql('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control;')
        before=result['prepared']=observe(); assert len(before['jobs'])==1
        job=before['jobs'][0]['row']; assert (job['state'],job['job_revision'])==('paused',7)
        client=Process(['docker','exec','-i',state['container_id'],'python3','-B',
            '/out/build/source/experiments/export-execution/resume_checker_client.py',job['job_id'][2:]],input_pipe=True)
        ready=result['ready']=event(client,'ready',timeout=20)
        assert ready['job']==job['job_id'][2:] and ready['key']=='f6'*16
        baseline=result['baseline']=observe(); sessions=result['login_sessions']=identities()
        result['login_audit']=session_mutation(before,baseline,sessions,'issue',job['principal_id'][2:])
        result['idle_before']=idle(); save()
        send(client,'run\n'); result['blocked']=event(client,'blocked',timeout=15)
        first=result['owner_before']=owner(); assert first['present']
        assert [first[k] for k in ['resource','epoch','incarnation']]==[ready['owner'][k] for k in ['owner_resource_id','owner_epoch','process_incarnation']]
        started=time.monotonic(); time.sleep(2.2)
        second=result['owner_after']=owner(); result['renewal_observation_seconds']=time.monotonic()-started
        assert {k:v for k,v in first.items() if k!='expires'}=={k:v for k,v in second.items() if k!='expires'}
        assert second['expires']-first['expires']>1_000_000
        send(client,'signal\n')
        result['client_terminal']=event(client,'terminal',timeout=12); result['client_exit']=client.wait(10)
        result['checker']=json.loads((root/'out/private-grant-http/resume-checker-result.json').read_text()); save()
        result['after']=observe(); result['idle_after']=idle(); result['terminal_owner']=owner()
        assert result['after']==baseline and result['idle_after']==result['idle_before']
        assert result['terminal_owner'] is not None and not result['terminal_owner']['present']
        assert all(result['terminal_owner'][k]==first[k] for k in ['resource','epoch','incarnation','pid','start'])
        result['backends']={db:int(command('docker','exec',state['postgres_id'],'psql','-XAtq','-U','postgres','-d',db,'-c',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for db in ['bbpub_normal','bbpub_control_normal']}
        assert result['backends']=={'bbpub_normal':0,'bbpub_control_normal':0}
        assert result['client_exit']==0 and not client.stderr and not client.overflow
        assert result['checker']['status']=='PASS' and result['checker']['marker_restored']
        assert result['client_terminal']==dict(event='terminal',status='PASS')
        result['status']='PASS'; return result
    except BaseException as error:
        result['failure']=type(error).__name__
        # Preserve observable failure state before the enclosing lab tears down
        # its containers. A killed docker-exec wrapper is not tracee exclusion.
        try:
            if client: result['failure_client_exit']=client.wait(20)
        except BaseException as observation_error:
            result['failure_client_wait']=type(observation_error).__name__
        try:
            checker=root/'out/private-grant-http/resume-checker-result.json'
            if checker.exists(): result['checker']=json.loads(checker.read_text())
            result['failure_after']=observe(); result['failure_idle']=idle()
            result['failure_owner']=owner()
        except BaseException as observation_error:
            result['failure_observation']=type(observation_error).__name__
        raise
    finally:
        try:
            if client: client.stop()
        except BaseException as error:
            result['status']='INCOMPLETE'; result['cleanup_error']=type(error).__name__; raise
        finally:
            if client:
                result['client_observation']=dict(exit=client.process.poll(),stdout=client.stdout.decode(errors='replace'),stderr=client.stderr.decode(errors='replace'),overflow=client.overflow)
            save()
