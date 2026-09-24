#!/usr/bin/env python3
"""Lost pause COMMIT replies resolve the same unit without retrying a paused job."""
import hashlib
import json
import time
from signed_publication_cases import Process, event, send, sha


def run(root, state, sql, job, before, snapshot):
    db='bbpub_control_normal';runner=state['container_id'];source=root/'source'
    result=dict(status='INCOMPLETE',source=state['source'],job=job,before=before,cases=[])
    path=root/'pause-commit-cases.json';assert not path.exists()
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def target(observation):return next(v['row'] for v in observation['jobs'] if v['row']['job_id']=='\\x'+job)
    def backends():
        return {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
    def preserved(old,new):
        for table in old:
            for value in old[table]:
                if table=='jobs' and value['row']['job_id']=='\\x'+job:continue
                assert value in new[table]
    def files(observation):
        units=[v for v in observation['export_execution_units'] if v['row']['job_id']=='\\x'+job];assert len(units)==1
        unit=units[0]['row'];assert unit['max_output_bytes']==1536 and unit['max_temporary_bytes']==3072
        artifact=unit['artifact_id'][2:];assert len(artifact)==32 and all(c in '0123456789abcdef' for c in artifact)
        destination=root/'out/signed_normal/export-destination'
        assert not (destination/artifact).exists() and not (destination/('.artifact-'+artifact+'.partial')).exists()
        lock=destination/('.artifact-'+artifact+'.lock');info=lock.lstat()
        assert info.st_mode==0o100600 and info.st_nlink==1 and info.st_size==0
        return dict(unit=units[0],namespace=sorted(p.name for p in destination.iterdir()),
                    mutex=dict(dev=info.st_dev,ino=info.st_ino,mode=info.st_mode,nlink=info.st_nlink,size=info.st_size,sha256=hashlib.sha256(lock.read_bytes()).hexdigest()))
    def cleanup(record,client,proxy):
        errors=[]
        for name,process in [('client',client),('proxy',proxy)]:
            if process is not None:
                try:
                    if name=='proxy' and process.process.poll() is None:send(process,'stop\n')
                    process.stop()
                except BaseException as error:errors.append(dict(process=name,error=type(error).__name__))
                record[name+'_output']=process.stdout.decode();record[name+'_error']=process.stderr.decode()
        record['cleanup_errors']=errors;save();assert not errors
    try:
        for index,mode in enumerate(['before-forward','after-complete']):
            record=dict(mode=mode,status='INCOMPLETE',before=snapshot());result['cases'].append(record)
            proxy=client=None
            try:
                proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode',mode,'--target','control-export-pause'],input_pipe=True)
                ready=record['proxy_ready']=event(proxy,'ready');assert ready['source_sha256']==sha(source/'experiments/archive-engine/native_commit_proxy.py')
                client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/commit_client.py',mode,str(ready['port']),'4'],input_pipe=True)
                fault=record['fault']=event(proxy,'fault')
                failed=record['failed']=event(client,'failed');assert client.wait(10)==0 and not client.stderr and not client.overflow
                owner=record['owner']=json.loads(sql(db,"SELECT jsonb_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=control_backend_pid AND backend_start=control_backend_start)) FROM control.owner_epochs WHERE role='control' AND state='active'"))
                assert owner['pid']==fault['backend_pid'] and not owner['present']
                assert owner['resource']==failed['owner']['owner_resource_id'] and owner['epoch']==failed['owner']['owner_epoch'] and owner['incarnation']==failed['owner']['process_incarnation']
                assert fault['data_queries']==fault['data_commits']==fault['faults']==1
                assert fault['server_complete']==index and fault['client_complete']==0 and not fault['rejection_sequence']
                assert failed['exit']==4 and failed['parent_absent'] and failed['lock_available'] and 'CommitUnknown' in failed['stderr']
                after=record['after']=snapshot();row=target(after)
                record['backends']=backends();record['files']=files(after);save()
                assert record['backends']=={db:0,'bbpub_normal':0}
                assert row['state']==('running' if index==0 else 'paused') and row['job_revision']==(3 if index==0 else 5)
                assert row['reason_code']==(None if index==0 else 'resource_limit')
                assert row['checkpoint_revision']==0 and row['completed_at_us'] is None and row['result_manifest_digest'] is None
                assert row['owner_epoch']==int(owner['epoch'])
                preserved(record['before'],after)
                for table in ['artifacts','artifact_files']:assert after[table]==before[table]
                added=[v['row'] for v in after['archive_snapshot_pins'] if v not in record['before']['archive_snapshot_pins']]
                assert len(added)==1 and added[0]['purpose']=='reader'
                reader=record['released_reader']=added[0]['lease_id']
                assert next(v['row']['state'] for v in after['snapshot_leases'] if v['row']['lease_id']==reader)=='released'
                claims=[v['row'] for v in after['retention_claims'] if v['row']['lease_id']==reader]
                assert len(claims)==1 and claims[0]['state']=='released'
                if index:assert record['files']==result['cases'][0]['files']
                send(proxy,'stop\n');assert proxy.wait(5)==0
                record['proxy_terminal']=event(proxy,'terminal');assert record['proxy_terminal']['unexpected']==0 and not proxy.stderr and not proxy.overflow
            finally:cleanup(record,client,proxy)
            record['status']='PASS';save()
        client=None;record=result['restart']=dict(before=snapshot())
        try:
            client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/commit_client.py','restart','0','4'],input_pipe=True)
            record['ready']=event(client,'ready')
            time.sleep(1.5)
            record['observed']=snapshot();record['files']=files(record['observed']);save()
            send(client,'stop\n');assert client.wait(10)==0
            record['client_terminal']=event(client,'terminal');record['terminal']=snapshot();record['backends']=backends();save()
            assert record['client_terminal']==dict(event='terminal',exit=0,lock_available=True)
            assert not client.stderr and not client.overflow
            assert record['before']==record['observed']==record['terminal']==result['cases'][-1]['after']
            assert record['files']==result['cases'][-1]['files']
            assert record['backends']=={db:0,'bbpub_normal':0}
            assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        finally:cleanup(record,client,None)
        result['status']='PASS'
        return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:save()
