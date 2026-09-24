#!/usr/bin/env python3
"""Same resume key across unforwarded COMMIT, committed reply loss and recovery."""
import hashlib
import json
from signed_publication_cases import Process, command, event, send, sha


def accepted_resume(before, after, current, key):
    """Fixed paused7/config1/policy2 fixture; compare the complete atomic transition."""
    job=before['jobs'][0]['row']['job_id'][2:]
    assert len(after['jobs'])==1 and (after['jobs'][0]['row']['state'],after['jobs'][0]['row']['job_revision'],after['jobs'][0]['row']['reason_code'])==('queued',8,None)
    changed={'state','job_revision','reason_code','owner_resource_id','owner_epoch','owner_incarnation'}
    assert {k:v for k,v in before['jobs'][0]['row'].items() if k not in changed}=={k:v for k,v in after['jobs'][0]['row'].items() if k not in changed}
    assert after['jobs'][0]['row']['owner_resource_id']=='\\x'+current['resource'] and after['jobs'][0]['row']['owner_epoch']==int(current['epoch']) and after['jobs'][0]['row']['owner_incarnation']=='\\x'+current['incarnation']
    for table in before.keys()-{'jobs','export_resume_commands','management_commands','management_audit'}:assert before[table]==after[table],table
    for table in ['export_resume_commands','management_commands','management_audit']:
        assert len(after[table])==len(before[table])+1 and all(v in after[table] for v in before[table]),table
    d,c,a=[next(v for v in after[t] if v not in before[t]) for t in ['export_resume_commands','management_commands','management_audit']]
    assert d['xmin']==c['xmin']==a['xmin']==after['jobs'][0]['xmin']
    d,c,a=d['row'],c['row'],a['row'];receipt=c
    assert c['actor_id']==d['actor_id']==a['actor_id']==before['jobs'][0]['row']['principal_id']
    assert c['dataset_id']==d['dataset_id']==a['dataset_id']==before['jobs'][0]['row']['dataset_id']
    assert c['request_key']==d['request_key']==a['subject_id']=='\\x'+key and d['job_id']=='\\x'+job
    assert d['expected_job_revision']==7 and c['expected_config_revision']==a['config_revision']==1 and c['policy_revision']==a['policy_revision']==2
    assert c['audit_id']==a['audit_id'] and c['action']==a['action']=='job.export.resume' and c['accepted_at_us']==a['created_at_us'] and a['outcome']=='accepted'
    payload=b'bigbrotr/export-resume/1\0'+b''.join(bytes.fromhex(d[k][2:]) for k in ['dataset_id','actor_id','request_key','job_id'])+b''.join(n.to_bytes(8,'big') for n in [7,1,2])
    assert c['request_digest']=='\\x'+hashlib.sha256(payload).hexdigest()
    return receipt


def run(root, state, sql, observe):
    result=dict(status='INCOMPLETE',source=state['source'],cases=[])
    path=root/'resume-commit-cases.json';assert not path.exists()
    runner=state['container_id'];pg=state['postgres_id'];client=proxy=None
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def owner():
        return json.loads(sql("SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=control_backend_pid AND backend_start=control_backend_start)) FROM control.owner_epochs WHERE role='control' AND state='active'"))
    def backends():
        return {db:int(command('docker','exec',pg,'psql','-XAtq','-U','postgres','-d',db,'-c',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for db in ['bbpub_control_normal','bbpub_normal']}
    try:
        sql('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control;')
        before=result['before']=observe();assert len(before['jobs'])==1
        job=before['jobs'][0]['row']['job_id'][2:]
        assert (before['jobs'][0]['row']['state'],before['jobs'][0]['row']['job_revision'])==('paused',7)
        for index,mode in enumerate(['before-forward','after-complete']):
            record=dict(mode=mode,status='INCOMPLETE');result['cases'].append(record);save()
            proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode',mode,'--target','control-export-resume'],input_pipe=True)
            hello=record['proxy_ready']=event(proxy,'ready')
            assert hello['source_sha256']==sha(root/'source/experiments/archive-engine/native_commit_proxy.py')
            if index==0:
                client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/resume_commit_client.py',str(hello['port']),job],input_pipe=True)
            else:send(client,json.dumps(dict(port=hello['port']))+'\n')
            ready=record['ready']=event(client,'ready-'+('before' if index==0 else 'after'))
            current=record['owner']=owner();save()
            assert current['present'] and current['resource']==ready['owner']['owner_resource_id'] and current['epoch']==ready['owner']['owner_epoch'] and current['incarnation']==ready['owner']['process_incarnation']
            if index:
                previous=result['cases'][0]['owner']
                assert current['resource']==previous['resource'] and int(current['epoch'])==int(previous['epoch'])+1 and current['incarnation']!=previous['incarnation']
            send(client,'submit\n')
            fault=record['fault']=event(proxy,'fault')
            assert fault['backend_pid']==current['pid'] and fault['data_queries']==fault['data_commits']==fault['faults']==1
            assert fault['server_complete']==index and fault['client_complete']==0 and not fault['rejection_sequence']
            failed=record['failed']=event(client,'failed-'+('before' if index==0 else 'after'))
            assert failed['exit']==4 and failed['parent_absent'] and failed['lock_available'] and failed['scratch_empty'] and 'CommitUnknown' in failed['error']
            record['failed_owner']=owner();assert record['failed_owner']=={**current,'present':False}
            after=record['after']=observe();record['backends']=backends();save()
            assert record['backends']=={'bbpub_control_normal':0,'bbpub_normal':0}
            if index==0:assert after==before
            else:
                result['committed_receipt']=accepted_resume(before,after,current,'f3'*16)
            send(proxy,'stop\n');assert proxy.wait(5)==0
            record['proxy_terminal']=event(proxy,'terminal')
            assert record['proxy_terminal']['unexpected']==0 and not proxy.stderr and not proxy.overflow
            proxy.stop();proxy=None;record['status']='PASS';save()
        sql("DELETE FROM control.principal_grants WHERE action='data.read'")
        send(client,'retry\n');retried=result['retried']=event(client,'retried');save()
        assert retried['status']==200 and retried['receipt']==dict(format='bigbrotr-export-resume/1',command_key='f3'*16,job_id=job,job_revision='8',accepted_at_us=str(result['committed_receipt']['accepted_at_us']),created=False,execution='unobserved',authority='acceptance-only')
        assert int(retried['owner']['owner_epoch'])==int(current['epoch'])+1 and retried['owner']['process_incarnation']!=current['incarnation']
        assert observe()==after
        send(client,'stop\n');result['client_terminal']=event(client,'final-terminal')
        assert client.wait(10)==0 and not client.stderr and not client.overflow
        assert result['client_terminal']==dict(event='final-terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
        client.stop();client=None
        result['terminal']=observe();assert result['terminal']==after
        result['backends']=backends();assert result['backends']=={'bbpub_control_normal':0,'bbpub_normal':0}
        assert sql("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        result['status']='PASS';return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        errors=[]
        for name,process in [('client',client),('proxy',proxy)]:
            if process:
                try:
                    if name=='proxy' and process.process.poll() is None:send(process,'stop\n')
                    process.stop()
                except BaseException as error:errors.append(dict(process=name,error=type(error).__name__))
                result[name+'_output']=process.stdout.decode();result[name+'_error']=process.stderr.decode()
        result['cleanup_errors']=errors
        if errors:result['status']='INCOMPLETE'
        save()
