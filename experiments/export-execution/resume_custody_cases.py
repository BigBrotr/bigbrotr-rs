#!/usr/bin/env python3
"""Hold the real resume reply while SIGTERM drains its actual Control driver."""
import json
from signed_publication_cases import Process, command, event, send, sha
from resume_commit_cases import accepted_resume


def run(root, state, sql, observe):
    result=dict(status='INCOMPLETE',source=state['source'])
    path=root/'resume-custody-cases.json';assert not path.exists()
    private=root/'out/private-grant-http';client=proxy=None
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def observation():
        return json.loads(sql("""SELECT json_build_object(
          'owner',(SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,
            'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,
            'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity
              WHERE pid=control_backend_pid AND backend_start=control_backend_start))
            FROM control.owner_epochs WHERE role='control' AND state='active'),
          'catalog_backends',(SELECT count(*) FROM pg_stat_activity WHERE datname='bbpub_normal'))"""))
    try:
        sql('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control;')
        before=result['before']=observe();assert len(before['jobs'])==1
        job=before['jobs'][0]['row']['job_id'][2:]
        assert (before['jobs'][0]['row']['state'],before['jobs'][0]['row']['job_revision'])==('paused',7)
        proxy=Process(['docker','exec','-i',state['container_id'],'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode','hold-reply','--target','control-export-resume'],input_pipe=True)
        ready=result['proxy_ready']=event(proxy,'ready')
        assert ready['source_sha256']==sha(root/'source/experiments/archive-engine/native_commit_proxy.py')
        client=Process(['docker','exec','-i',state['container_id'],'python3','-B','/out/build/source/experiments/archive-engine/export_job_custody_client.py',str(ready['port']),job],input_pipe=True)
        result['submitted']=event(client,'submitted');held=result['held']=event(proxy,'held');save()
        assert held['stage']=='reply' and held['server_complete']==held['data_queries']==held['data_commits']==1 and held['client_complete']==0 and not held['rejection_sequence']
        current=result['held_state']=observation();owner=current['owner'];save()
        assert owner['present'] and owner['pid']==held['backend_pid'] and current['catalog_backends']==0
        submitted=result['submitted'];ready=submitted['owner']
        assert submitted['key']=='f4'*16 and submitted['command']==dict(expected_job_revision='7',expected_config_revision='1',expected_policy_revision='2')
        assert [owner[k] for k in ['resource','epoch','incarnation']]==[ready[k] for k in ['owner_resource_id','owner_epoch','process_incarnation']]
        send(client,'signal\n');result['signal']=event(client,'signal_sent')
        # Native execution is disabled: zero catalog backends is an independent
        # observation, not evidence of a manager join-call or continuous custody.
        after_signal=result['after_signal']=observation();assert after_signal==current
        (private/'job-resume-custody-catalog-gone.json').write_text(json.dumps(after_signal)+'\n');save()
        result['client_observed']=event(client,'observed',timeout=15)
        result['client_exit']=client.wait(10)
        result['client_result']=json.loads((private/'job-resume-custody-result.json').read_text())
        result['terminal_owner']=observation()
        after=result['after']=observe();save()
        result['committed_receipt']=accepted_resume(before,after,owner,'f4'*16)
        assert result['terminal_owner']==dict(owner={**owner,'present':False},catalog_backends=0)
        result['backends']={db:int(command('docker','exec',state['postgres_id'],'psql','-XAtq','-U','postgres','-d',db,'-c',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for db in ['bbpub_control_normal','bbpub_normal']}
        assert result['backends']=={'bbpub_control_normal':0,'bbpub_normal':0}
        send(proxy,'stop\n');result['proxy_exit']=proxy.wait(5);result['proxy_terminal']=event(proxy,'terminal');save()
        assert result['proxy_exit']==0 and not proxy.stderr and not proxy.overflow
        assert result['proxy_terminal']['unexpected']==result['proxy_terminal']['client_complete']==0
        if result['client_result']['status']=='FAIL':result['status']='FAIL';save()
        assert result['client_exit']==0 and not client.stderr and not client.overflow
        assert result['client_result']['status']=='PASS' and not result['client_result']['violations']
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
