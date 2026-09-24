#!/usr/bin/env python3
"""Real typed-client receipts checked against independently observed PostgreSQL."""
import hashlib
import json
import os
from pathlib import Path
import tarfile
from signed_publication_cases import Process, command, event, send
from resume_commit_cases import accepted_resume


def node_tools(root, prior):
    old=root.parent/'private-panel-no-preload-b6d9d7e'
    retained=json.loads((old/'resources.json').read_text())
    assert retained['owner']=='f062862798d1c5a5606e31e87618683e' and retained['state']=='stopped-retained'
    for entry in retained['containers']:
        c=next(c for c in prior if c['Id']==entry['id'])
        assert c['Config']['Labels']['org.bigbrotr.private-panel']==retained['owner']
        assert not c['State']['Running'] and c['State']['Pid']==0
    tools=old/'tools';archive=tools/'node.tar.xz'
    digest=hashlib.sha256(archive.read_bytes()).hexdigest()
    assert digest=='5c4286dcd5bbd5acb1ccc7eb0e088bd5eb1e3affad671ee9364004f8f6a4a431'
    files=0
    with tarfile.open(archive) as tar:
        for member in tar:
            relative=Path(*Path(member.name).parts[1:])
            assert not relative.is_absolute() and '..' not in relative.parts
            p=tools/'node'/relative
            if member.isfile():
                assert p.is_file() and not p.is_symlink()
                assert hashlib.sha256(p.read_bytes()).hexdigest()==hashlib.sha256(tar.extractfile(member).read()).hexdigest()
                files+=1
            elif member.issym():assert p.is_symlink() and os.readlink(p)==member.linkname
            else:assert member.isdir() and p.is_dir() and not p.is_symlink()
    identity=dict(root=str(old),owner=retained['owner'],archive_sha256=digest,files_verified=files,
                  node_sha256=hashlib.sha256((tools/'node/bin/node').read_bytes()).hexdigest())
    path=root/'node-client-tools.json'
    if path.exists():assert json.loads(path.read_text())==identity
    else:path.write_text(json.dumps(identity,indent=2)+'\n')
    return tools


def run(root, state, sql, observe):
    result=dict(status='INCOMPLETE',source=state['source'],cases={})
    path=root/'resume-client-cases.json';assert not path.exists()
    runner=state['container_id'];client=server=None
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def request(name, operation, **fields):
        assert name not in result['cases']
        send(client,json.dumps(dict(name=name,operation=operation,**fields))+'\n')
        value=event(client,name);result['cases'][name]=value;save();return value
    def resume(name, key='f5'*16, revision='7', policy='2', status=200):
        value=request(name,'resume',key=key,command=dict(expected_job_revision=revision,expected_config_revision='1',expected_policy_revision=policy))
        assert value['status']==status and value['requests']==1,value
        if status in (200,202):
            stored=json.loads(sql(f"SELECT json_build_object('time',c.accepted_at_us::text,'revision',(d.expected_job_revision+1)::text) FROM control.export_resume_commands d JOIN control.management_commands c USING(actor_id,request_key) WHERE d.job_id=decode('{job}','hex') AND d.request_key=decode('{key}','hex')"))
            assert value['observation']==dict(format='bigbrotr-export-resume/1',command_key=key,job_id=job,job_revision=stored['revision'],accepted_at_us=stored['time'],created=status==202,execution='unobserved',authority='acceptance-only')
        else:assert value['failure']=='api' and 'observation' not in value
        return value.get('observation')
    def sessions():
        return json.loads(sql("SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    try:
        sql('GRANT INSERT ON control.export_resume_commands TO bigbrotr_control;')
        before=result['before']=observe();assert len(before['jobs'])==1
        job=before['jobs'][0]['row']['job_id'][2:]
        assert (before['jobs'][0]['row']['state'],before['jobs'][0]['row']['job_revision'])==('paused',7)
        server=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/resume_http_client.py','typed-server'],input_pipe=True)
        result['server_ready']=event(server,'ready');save()
        owner=result['server_ready']['owner']
        client=Process(['docker','exec','-i',runner,'env','-i','/tools/node/bin/node','/out/build/source/experiments/export-execution/resume_client.mjs',job],input_pipe=True)
        result['client_ready']=event(client,'ready');assert result['client_ready']['node']=='v26.4.0'
        assert request('login','login')['status']==201
        result['sessions_before']=sessions()
        resume('historical',key='e4'*16,revision='4',policy='1')
        resume('stale-revision',revision='6',status=409)
        assert observe()==before
        receipt=resume('accepted',status=202)
        after=result['accepted']=observe()
        result['committed_receipt']=accepted_resume(before,after,dict(resource=owner['owner_resource_id'],epoch=owner['owner_epoch'],incarnation=owner['process_incarnation']),'f5'*16)
        assert resume('retry')=={**receipt,'created':False}
        resume('changed-body',revision='6',status=409)
        send(server,json.dumps(dict(request='restart-terminal',operation='restart',destination=False))+'\n')
        result['restarted']=event(server,'restart-terminal');save()
        restarted=result['restarted']
        assert restarted['stopped']==dict(exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
        assert int(restarted['owner']['owner_epoch'])==int(owner['owner_epoch'])+1 and restarted['owner']['process_incarnation']!=owner['process_incarnation']
        sql("DELETE FROM control.principal_grants WHERE action='data.read'")
        assert resume('retry-after-restart')=={**receipt,'created':False}
        result['sessions_after']=sessions();assert result['sessions_after']==result['sessions_before']
        sql("UPDATE control.local_accounts SET active=false WHERE username='alice'")
        resume('deactivated-session',status=401)
        result['client_terminal']=request('client-terminal','stop')
        assert client.wait(10)==0 and not client.stderr and not client.overflow
        assert [x['status'] for x in result['client_terminal']['traffic']]==[201,200,409,202,200,409,200,401]
        send(server,json.dumps(dict(request='server-terminal',operation='stop'))+'\n')
        result['server_terminal']=event(server,'server-terminal')
        assert result['server_terminal']==dict(event='server-terminal',operation='stop',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
        assert server.wait(10)==0 and not server.stderr and not server.overflow
        result['after']=observe();assert result['after']==after
        result['backends']={db:int(command('docker','exec',state['postgres_id'],'psql','-XAtq','-U','postgres','-d',db,'-c',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for db in ['bbpub_control_normal','bbpub_normal']}
        assert result['backends']=={'bbpub_control_normal':0,'bbpub_normal':0}
        assert sql("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        node_tools(root,json.loads((root/'preflight.json').read_text())['containers'])
        result['status']='PASS';return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        errors=[]
        for name,process in [('client',client),('server',server)]:
            if process:
                try:process.stop()
                except BaseException as error:errors.append(dict(process=name,error=type(error).__name__))
                result[name+'_output']=process.stdout.decode();result[name+'_error']=process.stderr.decode()
        result['cleanup_errors']=errors
        if errors:result['status']='INCOMPLETE'
        save()
