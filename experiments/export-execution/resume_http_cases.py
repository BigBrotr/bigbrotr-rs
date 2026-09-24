#!/usr/bin/env python3
"""Independent PostgreSQL/installed HTTP oracle over the original paused unit."""
import hashlib
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
import resume_cases
from signed_publication_cases import Process, command, event, send


def main(root, profile=None):
    assert profile in (None, 'commits', 'custody', 'client', 'prepare-browser', 'checker', 'locks', 'revoke', 'expiry')
    state=json.loads((root/'resources.json').read_text()); runner=state['container_id']; pg=state['postgres_id']; db='bbpub_control_normal'
    path=root/'resume-http-cases.json'; assert not path.exists()
    result=dict(status='INCOMPLETE',source=state['source'],tree=state['tree'],cases={},scope='Actual installed resume HTTP and application-role SQL; native execution disabled to observe exact accepted revision. No resumed artifact output or COMMIT-fault qualification.')
    if profile=='commits': result['scope']='Actual installed resume HTTP with native execution disabled; selected Control COMMIT lost before forwarding and after server completion, then original-key resolution.'
    if profile=='custody': result['scope']='Actual installed resume HTTP with native execution disabled; held server-completed reply and SIGTERM, sampled destination exclusion during actual pending Control driver drain.'
    if profile=='client': result['scope']='Actual TypeScript client on Node26.4.0 against installed HTTP/PostgreSQL; native export execution disabled. Explicit loopback Origin and in-memory cookie jar, no browser qualification.'
    if profile=='prepare-browser': result['scope']='Verified signed seed and paused7/config1/policy2 fixture prepared for a separate production browser run. This result alone does not exercise the browser or resume HTTP.'
    if profile=='checker': result['scope']='Actual resume destination openat held, Control renewal and SIGTERM; unchanged paused unit and original pin. Priming marker is deliberately incompatible; no accepted resume or output qualification.'
    if profile=='revoke': result['scope']='Authenticated scope revocation while a real resume marker open waits; fresh authorization must refuse acceptance. No export execution or arbitrary kernel-stall qualification.'
    if profile=='expiry': result['scope']='Actual installed resume COMMIT held across idle/maximum-clamped session expiry and a timely positive control. No inconsistent session fixture or separate maximum-predicate discriminator.'
    if profile=='locks': result['scope']='Actual installed HTTP across PostgreSQL account/source row waits with valid short seeded sessions, plus timely acceptance. No configurable-login lifetime or arbitrary clock fault qualification.'
    client=None
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def sql(text): return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def exchange(name, operation, **fields):
        assert name not in result['cases']
        send(client,json.dumps(dict(request=name,operation=operation,**fields))+'\n')
        value=event(client,name,timeout=12);assert value['operation']==operation
        result['cases'][name]=value;save();return value
    def resume(name, key='f1'*16, revision='7', policy='2', status=200, **fields):
        value=exchange(name,'resume',job=job,key=key,body=dict(expected_job_revision=revision,expected_config_revision='1',expected_policy_revision=policy),**fields)
        assert value['status']==status, value
        if status in [200,202]:
            stored=json.loads(sql(f"SELECT json_build_object('accepted_at_us',c.accepted_at_us::text,'job_revision',(r.expected_job_revision+1)::text) FROM control.management_commands c JOIN control.export_resume_commands r USING(actor_id,request_key) JOIN control.export_jobs e ON e.job_id=r.job_id AND e.actor_id=r.actor_id AND e.dataset_id=r.dataset_id WHERE r.request_key=decode('{key}','hex') AND r.job_id=decode('{job}','hex')"))
            assert stored['job_revision']==str(int(revision)+1)
            assert value['observation']==dict(format='bigbrotr-export-resume/1',command_key=key,job_id=job,job_revision=stored['job_revision'],accepted_at_us=stored['accepted_at_us'],created=status==202,execution='unobserved',authority='acceptance-only')
            assert int(value['observation']['accepted_at_us'])>0
        return value['observation']
    def start(name):
        nonlocal client
        client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/resume_http_client.py',name],input_pipe=True)
        ready=event(client,'ready');result[name+'-ready']=ready;save();return client
    def stop(name):
        exchange(name,'stop');assert client.wait(10)==0 and not client.stderr and not client.overflow;client.stop()
    def observe():
        tables=['jobs','job_input_bounds','export_jobs','export_execution_units','artifacts','artifact_files','snapshot_leases','archive_snapshot_pins','retention_claims','export_resume_commands','management_commands','management_audit']
        result={}
        for name in tables:
            where=" WHERE action='job.export.resume'" if name in ['management_commands','management_audit'] else ''
            result[name]=json.loads(sql(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t{where}"))
        return result
    def sessions():
        return json.loads(sql("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    try:
        resume_cases.main(root)
        command('docker','exec',runner,'python3','-B','/out/build/source/experiments/export-execution/resume_http_client.py','prepare')
        # Offline provisioning, not a management grant-change qualification.
        sql('GRANT SELECT ON control.export_execution_units,control.artifacts,control.artifact_files,control.export_resume_commands TO bigbrotr_control; GRANT UPDATE(principal_id) ON control.principal_grants TO bigbrotr_control; GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        policy=json.loads(sql('SELECT document::text FROM control.policy_revisions WHERE revision=1'));policy['settings']['administration.enabled']=True
        document=json.dumps(policy,sort_keys=True,separators=(',',':'));digest=hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
        sql(f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 2,dataset_id,1,config_revision,schema_version,$p${document}$p$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1; UPDATE control.datasets SET policy_revision=2; COMMIT;")
        job=sql('SELECT encode(job_id,\'hex\') FROM control.jobs');result['job']=job
        result['before']=observe();save()
        started=time.monotonic();time.sleep(60);result['rate_wait_seconds']=time.monotonic()-started;save()
        if profile=='revoke':
            import resume_revoke_cases
            result['revoke']=resume_revoke_cases.run(root,state,sql)
            assert result['revoke']['status']=='PASS'
            result['status']='PASS';return
        if profile=='expiry':
            import resume_lock_cases
            result['expiry']=resume_lock_cases.run(root,state,sql,profile='expiry')
            assert result['expiry']['status']=='PASS'
            result['status']='PASS';return
        if profile=='locks':
            import resume_lock_cases
            result['locks']=resume_lock_cases.run(root,state,sql)
            assert result['locks']['status']=='PASS'
            result['status']='PASS';return
        if profile=='checker':
            import resume_checker_cases
            result['checker']=resume_checker_cases.run(root,state,sql)
            assert result['checker']['status']=='PASS'
            result['status']='PASS';return
        if profile=='prepare-browser':
            result['status']='PASS';return
        if profile=='client':
            import resume_client_cases
            result['client']=resume_client_cases.run(root,state,sql,observe)
            assert result['client']['status']=='PASS'
            result['status']='PASS';return
        if profile=='custody':
            import resume_custody_cases
            result['custody']=resume_custody_cases.run(root,state,sql,observe)
            assert result['custody']['status']=='PASS'
            result['status']='PASS';return
        if profile=='commits':
            import resume_commit_cases
            result['commits']=resume_commit_cases.run(root,state,sql,observe)
            assert result['commits']['status']=='PASS'
            result['status']='PASS';return
        client=start('missing-insert');exchange('first-login','login',username='alice')
        resume('missing-insert-refused',status=503)
        assert exchange('sessions-still-available','me')['status']==200
        stop('missing-terminal');client=None
        assert observe()==result['before']
        sql('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control; REVOKE ALL ON control.artifact_files,control.consumer_frontiers,control.consumer_frontier_revisions FROM bigbrotr_control;')
        client=start('initial');exchange('current-login','login',username='alice')
        result['sessions_before']=sessions()
        old=resume('old-receipt-after-session-deletion',key='e4'*16,revision='4',policy='1')
        grant=sql("DELETE FROM control.principal_grants WHERE action='data.read' RETURNING to_jsonb(principal_grants)::text")
        sql("DELETE FROM control.principal_grants WHERE action IN ('roles.control','status.read','principals.manage')")
        assert resume('old-receipt-without-input-grants',key='e4'*16,revision='4',policy='1')==old
        resume('new-input-denied',status=403)
        resume('key-body-conflict',key='e4'*16,status=409)
        assert observe()==result['before']
        sql("INSERT INTO control.principal_grants SELECT * FROM jsonb_populate_record(NULL::control.principal_grants,$g$"+grant+"$g$::jsonb)")
        resume('stale-revision',revision='6',status=409)
        resume('csrf-denied',wrong_csrf=True,status=401)
        exchange('without-destination','restart',destination=False)
        resume('missing-destination',status=503)
        assert resume('historical-without-destination',key='e4'*16,revision='4',policy='1')==old
        exchange('destination-restored','restart',destination=True)
        exchange('marker-unavailable','marker',available=False)
        resume('marker-refused',status=503)
        exchange('marker-restored','marker',available=True)
        assert observe()==result['before']
        result['expired_original_scheduling_lease']=json.loads(sql("SELECT json_build_object('active',l.state='active','expired',l.expires_at_us<(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) FROM control.export_jobs e JOIN control.snapshot_leases l ON l.lease_id=e.pin_lease_id"))
        assert result['expired_original_scheduling_lease']==dict(active=True,expired=True)
        accepted=resume('new-resume-accepted',status=202)
        result['accepted']=observe();save()
        assert {**accepted,'created':False}==resume('same-receipt')
        result['sessions_after']=sessions();assert result['sessions_before']==result['sessions_after']
        exchange('restart-without-destination','restart',destination=False)
        sql("DELETE FROM control.principal_grants WHERE action='data.read'")
        assert {**accepted,'created':False}==resume('same-receipt-after-restart-and-revocation')
        sql("UPDATE control.local_accounts SET active=false WHERE username='alice'")
        resume('deactivated-session-denied',status=401)
        stop('final-terminal');client=None
        result['after']=observe();before=result['before'];after=result['after'];assert result['accepted']==after
        assert before['jobs'][0]['row']['state']=='paused' and before['jobs'][0]['row']['job_revision']==7
        assert len(after['jobs'])==1 and after['jobs'][0]['row']['state']=='queued' and after['jobs'][0]['row']['job_revision']==8 and after['jobs'][0]['row']['reason_code'] is None
        changed={'state','job_revision','reason_code','owner_resource_id','owner_epoch','owner_incarnation'}
        assert {k:v for k,v in before['jobs'][0]['row'].items() if k not in changed}=={k:v for k,v in after['jobs'][0]['row'].items() if k not in changed}
        for table in before.keys()-{'jobs','export_resume_commands','management_commands','management_audit'}:assert before[table]==after[table],table
        for table in ['export_resume_commands','management_commands','management_audit']:
            assert len(before[table])==1 and len(after[table])==2 and before[table][0] in after[table]
        detail,command_row,audit=[next(v for v in after[t] if v not in before[t]) for t in ['export_resume_commands','management_commands','management_audit']]
        assert detail['xmin']==command_row['xmin']==audit['xmin']==after['jobs'][0]['xmin']
        detail,command_row,audit=detail['row'],command_row['row'],audit['row']
        original=before['export_jobs'][0]['row']
        assert detail['dataset_id']==original['dataset_id']==command_row['dataset_id']==audit['dataset_id']
        assert detail['actor_id']==original['actor_id']==command_row['actor_id']==audit['actor_id']
        assert detail['job_id']=='\\x'+job and detail['request_key']==command_row['request_key']==audit['subject_id']=='\\x'+'f1'*16
        assert detail['expected_job_revision']==7 and command_row['expected_config_revision']==audit['config_revision']==1 and command_row['policy_revision']==audit['policy_revision']==2
        assert command_row['audit_id']==audit['audit_id'] and command_row['action']==audit['action']=='job.export.resume' and audit['outcome']=='accepted'
        assert str(command_row['accepted_at_us'])==str(audit['created_at_us'])==accepted['accepted_at_us']
        payload=b'bigbrotr/export-resume/1\0'+b''.join(bytes.fromhex(detail[k][2:]) for k in ['dataset_id','actor_id','request_key','job_id'])+b''.join(n.to_bytes(8,'big') for n in [7,1,2])
        assert command_row['request_digest']=='\\x'+hashlib.sha256(payload).hexdigest()
        result['terminal']=json.loads(sql("SELECT json_build_object('active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'),'backends',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()),'extra_grants',(SELECT count(*) FROM control.principal_grants WHERE action IN ('roles.control','status.read','principals.manage')))"))
        assert result['terminal']==dict(active_control=0,backends=0,extra_grants=0)
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        if client:
            try:
                stop('failure-terminal')
            except BaseException as error:
                result['failure_cleanup']=type(error).__name__
            finally:
                client.stop()
        save()


if __name__=='__main__':
    assert len(sys.argv) in (2,3)
    main(Path(sys.argv[1]).resolve(),sys.argv[2] if len(sys.argv)==3 else None)
