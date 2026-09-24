#!/usr/bin/env python3
"""Full durable oracle for authenticated revocation during resume filesystem work."""
import hashlib
import json
from signed_publication_cases import Process, command, event, send


def run(root,state,sql):
    result=dict(status='INCOMPLETE',source=state['source']);path=root/'resume-revoke-cases.json';assert not path.exists()
    client=None
    tables=['jobs','job_input_bounds','export_jobs','export_execution_units','artifacts','artifact_files','snapshot_leases','archive_snapshot_pins','retention_claims','export_resume_commands','management_commands','management_audit','local_accounts','principal_grants','scope_grants','scope_grant_changes','reauthentication_proofs']
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def observe():return {t:json.loads(sql(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{t} t")) for t in tables}
    def sessions():return json.loads(sql("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'actor',encode(principal_id,'hex'),'dataset',encode(dataset_id,'hex'),'issued',issued_at_us,'expires',expires_at_us,'idle_expires',idle_expires_at_us,'last_seen',last_seen_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def added(before,after):
        assert len(after)==len(before)+1 and all(x in after for x in before)
        return next(x for x in after if x not in before)
    try:
        sql('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control;')
        before=result['prepared']=observe();assert len(before['jobs'])==1 and not before['reauthentication_proofs']
        job=before['jobs'][0]['row'];assert (job['state'],job['job_revision'])==('paused',7)
        result['sessions_prepared']=sessions();assert not result['sessions_prepared']
        client=Process(['docker','exec','-i',state['container_id'],'python3','-B','/out/build/source/experiments/export-execution/resume_revoke_client.py',job['job_id'][2:]],input_pipe=True)
        result['ready']=event(client,'ready',timeout=20)
        baseline=result['baseline']=observe();result['sessions_before']=sessions()
        owner=result['owner_before']=json.loads(sql("SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=control_backend_pid AND backend_start=control_backend_start)) FROM control.owner_epochs WHERE role='control' AND state='active'"))
        assert owner['present'] and [owner[k] for k in ['resource','epoch','incarnation']]==[result['ready']['owner'][k] for k in ['owner_resource_id','owner_epoch','process_incarnation']]
        for t in set(tables)-{'management_audit','reauthentication_proofs'}:assert before[t]==baseline[t],t
        assert len(result['sessions_before'])==1;session=result['sessions_before'][0]
        assert session['actor']==job['principal_id'][2:] and session['dataset']==job['dataset_id'][2:] and session['revoked'] is None
        audits=[x for x in baseline['management_audit'] if x not in before['management_audit']]
        assert len(audits)==2 and len(baseline['management_audit'])==len(before['management_audit'])+2 and all(x in baseline['management_audit'] for x in before['management_audit'])
        by_action={x['row']['action']:x for x in audits};assert set(by_action)=={'browser.session.issue','browser.session.reauthenticate'}
        assert by_action['browser.session.issue']['row']['subject_id']=='\\x'+session['id'] and by_action['browser.session.issue']['xmin']==session['xmin']
        proof=added(before['reauthentication_proofs'],baseline['reauthentication_proofs']);p=proof['row']
        assert p['proof_id']=='\\x'+result['ready']['proof']['proof_id'] and p['session_id']=='\\x'+session['id'] and p['request_key']=='\\x'+'e7'*16
        assert by_action['browser.session.reauthenticate']['row']['subject_id']==p['proof_id'] and by_action['browser.session.reauthenticate']['xmin']==proof['xmin']
        for audit in audits:
            row=audit['row'];assert row['actor_id']==job['principal_id'] and row['dataset_id']==job['dataset_id'] and row['outcome']=='accepted' and (row['config_revision'],row['policy_revision'])==(1,2)
        save();send(client,'run\n')
        result['client_terminal']=event(client,'terminal',timeout=25);assert client.wait(10)==0 and not client.stderr and not client.overflow
        observed=result['client']=json.loads((root/'out/private-grant-http/job-resume-revoke-result.json').read_text());assert observed['status']=='PASS' and observed['marker_restored']
        after=result['after']=observe();result['sessions_after']=sessions();assert result['sessions_after']==result['sessions_before']
        mutable={'local_accounts','scope_grants','scope_grant_changes','reauthentication_proofs','management_commands','management_audit'}
        for t in set(tables)-mutable:assert before[t]==baseline[t]==after[t],t
        assert len(after['local_accounts'])==len(baseline['local_accounts'])==1
        account=after['local_accounts'][0];assert account['row']=={**baseline['local_accounts'][0]['row'],'grant_revision':5} and baseline['local_accounts'][0]['row']['grant_revision']==4
        assert not after['reauthentication_proofs']
        detail,receipt,audit=[added(baseline[t],after[t]) for t in ['scope_grant_changes','management_commands','management_audit']]
        assert account['xmin']==detail['xmin']==receipt['xmin']==audit['xmin']
        d,c,a=detail['row'],receipt['row'],audit['row']
        assert d['actor_id']==d['principal_id']==c['actor_id']==a['actor_id']==job['principal_id'] and d['dataset_id']==c['dataset_id']==a['dataset_id']==job['dataset_id']
        assert d['request_key']==c['request_key']==a['subject_id']=='\\x'+'e7'*16 and c['audit_id']==a['audit_id']
        assert d['action']=='history.read' and d['enabled'] is False and (d['scope_generation'],d['expected_grant_revision'],d['resulting_grant_revision'])==(2,4,5)
        assert d['proof_id']==p['proof_id'] and d['session_id']==p['session_id'] and d['proof_expires_at_us']==p['expires_at_us']
        assert (d['actor_credential_revision'],d['actor_authentication_revision'],d['actor_grant_revision'])==(p['credential_revision'],p['authentication_revision'],4)
        assert all(d[k] is None for k in ['catalog_id','frontier_revision','pin_lease_id'])
        assert (d['dictionary_kind'],d['allocation_domain'],d['ordinal'])==(4,'\\x'+'04'*16,1)
        payload=b'bigbrotr/scope-grant-change/1\0'+b''.join(bytes.fromhex(x[2:]) for x in [job['dataset_id'],job['principal_id'],d['request_key'],job['principal_id'],job['dataset_id']])+b'\0\4'+bytes.fromhex('04'*16)+(1).to_bytes(8,'big')+b'\1\0'+b''.join(n.to_bytes(8,'big') for n in [1,2,4])+b'\0'
        assert c['request_digest']==p['request_digest']=='\\x'+hashlib.sha256(payload).hexdigest()
        assert c['action']==a['action']=='scope-grant.change' and a['outcome']=='accepted' and c['expected_config_revision']==a['config_revision']==1 and c['policy_revision']==a['policy_revision']==2
        assert c['accepted_at_us']==a['created_at_us'] and observed['blocked_before']['wall']*1e6<=c['accepted_at_us']<=observed['blocked_after']['wall']*1e6
        assert len(after['scope_grants'])==len(baseline['scope_grants'])==3
        for old in baseline['scope_grants']:
            new=next(x for x in after['scope_grants'] if x['row']['action']==old['row']['action'])
            if old['row']['action']=='history.read':assert new['row']=={**old['row'],'enabled':False,'generation':2,'actor_id':d['actor_id'],'request_key':d['request_key']} and new['xmin']==detail['xmin']
            else:assert old==new
        expected=dict(format='bigbrotr-scope-grant/1',command_key='e7'*16,principal_id=job['principal_id'][2:],scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),enabled=False,scope_generation='2',account_grant_revision='5',accepted_at_us=str(c['accepted_at_us']),created=True,application='unobserved',authority='acceptance-only')
        assert observed['grant_response']['receipt']==expected and observed['grant_retry']['receipt']=={**expected,'created':False}
        assert observed['response']['status']==observed['retry']['status']==403 and observed['health']['status']==200
        result['backends']={db:int(command('docker','exec',state['postgres_id'],'psql','-XAtq','-U','postgres','-d',db,'-c',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for db in ['bbpub_normal','bbpub_control_normal']}
        assert result['backends']==dict(bbpub_normal=0,bbpub_control_normal=0) and sql("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        result['status']='PASS';return result
    except BaseException as error:result['failure']=type(error).__name__;raise
    finally:
        if client:
            try:
                if client.process.poll() is None:client.wait(12)
            except BaseException as error:result['cleanup_wait']=type(error).__name__
            try:client.stop()
            except BaseException as error:result['status']='INCOMPLETE';result['cleanup_error']=type(error).__name__
            result['client_observation']=dict(exit=client.process.poll(),stdout=client.stdout.decode(errors='replace'),stderr=client.stderr.decode(errors='replace'),overflow=client.overflow)
        try:
            result['terminal_observation']=observe();result['terminal_sessions']=sessions()
            if result['status']=='PASS':assert result['terminal_observation']==result['after'] and result['terminal_sessions']==result['sessions_after']
        except BaseException as error:result['status']='INCOMPLETE';result['terminal_observation_error']=type(error).__name__
        save()
