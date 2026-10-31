#!/usr/bin/env python3
"""Automatic completion: committed reply loss, unforwarded COMMIT, same-unit retry."""
import copy
import hashlib
import json
import time
from signed_publication_cases import Process, command, event, send, sha


def run(root, state, sql, policy, empty_manifest):
    source, runner = root/'source', state['container_id']
    private, db = root/'out/private-grant-http', 'bbpub_control_normal'
    result = dict(status='INCOMPLETE', source=state['source'], cases=[], output_hashes={})
    path = root/'completion-commit-cases.json'
    assert not path.exists()
    def save(): path.write_text(json.dumps(result, indent=2)+'\n')
    def activate(revision, settings):
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        selected=copy.deepcopy(policy);selected['settings'].update(settings)
        document=json.dumps(selected,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(b'bigbrotr/product-policy/3\0'+document.encode()).hexdigest()
        sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT {revision},dataset_id,{revision-1},config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision={revision-1}; UPDATE control.datasets SET policy_revision={revision}; COMMIT;")
    def snapshots():
        return json.loads(sql(db,"SELECT jsonb_object_agg(encode(j.job_id,'hex'),jsonb_build_object('job',to_jsonb(j),'job_xmin',j.xmin::text,'unit',(SELECT to_jsonb(u) FROM control.export_execution_units u WHERE u.job_id=j.job_id),'unit_xmin',(SELECT u.xmin::text FROM control.export_execution_units u WHERE u.job_id=j.job_id),'artifact',(SELECT jsonb_build_object('row',to_jsonb(a),'xmin',a.xmin::text,'files',(SELECT jsonb_agg(jsonb_build_object('row',to_jsonb(f),'xmin',f.xmin::text) ORDER BY file_id) FROM control.artifact_files f WHERE f.artifact_id=a.artifact_id)) FROM control.artifacts a WHERE a.job_id=j.job_id),'export_pin',(SELECT jsonb_build_object('lease',to_jsonb(l),'xmin',l.xmin::text) FROM control.export_jobs e JOIN control.snapshot_leases l ON l.lease_id=e.pin_lease_id WHERE e.job_id=j.job_id))) FROM control.jobs j"))
    def pins():
        return json.loads(sql(db,"SELECT jsonb_object_agg(encode(l.lease_id,'hex'),jsonb_build_object('lease',to_jsonb(l),'lease_xmin',l.xmin::text,'pin',to_jsonb(p),'pin_xmin',p.xmin::text,'claims',(SELECT jsonb_agg(jsonb_build_object('row',to_jsonb(c),'xmin',c.xmin::text) ORDER BY claim_id) FROM control.retention_claims c WHERE c.lease_id=l.lease_id))) FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id)"))
    def backends():
        return {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
    def preserved(old, new):
        for key,value in old.items(): assert new[key]==value,key
    def owner():
        return json.loads(sql(db,"SELECT jsonb_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=control_backend_pid AND backend_start=control_backend_start)) FROM control.owner_epochs WHERE role='control' AND state='active'"))
    expected_event=json.loads((source/'experiments/identity-contract/signed-fixture.json').read_text())['events']['E_public']
    expected_bytes=(json.dumps({k:expected_event[k] for k in ['id','pubkey','created_at','kind','tags','content','sig']},ensure_ascii=False,separators=(',',':'))+'\n').encode()
    def output(job, row):
        artifact=row['unit']['artifact_id'].removeprefix('\\x')
        assert len(artifact)==32 and all(c in '0123456789abcdef' for c in artifact)
        directory=root/'out/signed_normal/export-destination'/artifact
        expected=copy.deepcopy(empty_manifest);expected.update(artifact_id=artifact,job_id=job)
        expected['selection']['event_id']=expected_event['id'];expected['accepted_revisions']['policy']='5'
        expected['files']=[dict(file_id='events',path='events.jsonl',format='nostr-jsonl/1',bytes=str(len(expected_bytes)),rows='1',sha256=hashlib.sha256(expected_bytes).hexdigest())]
        raw=(directory/'manifest.json').read_bytes()
        assert (directory/'events.jsonl').read_bytes()==expected_bytes and json.loads(raw)==expected
        hashes={str((directory/name).relative_to(root/'out')):sha(directory/name) for name in ['events.jsonl','manifest.json']}
        result['output_hashes'].update(hashes)
        inventory=row['artifact']
        if inventory is not None:
            a=inventory['row'];files=inventory['files']
            assert a['artifact_id']=='\\x'+artifact and a['manifest_sha256']=='\\x'+hashlib.sha256(raw).hexdigest()
            assert a['manifest_bytes']==len(raw) and a['total_bytes']==len(raw)+len(expected_bytes) and a['file_count']==1
            assert len(files)==1
            f=files[0]['row'];assert f['file_id']=='events' and f['relative_path']=='events.jsonl' and f['format']=='nostr-jsonl/1'
            assert f['byte_count']==len(expected_bytes) and f['row_count']==1 and f['sha256']=='\\x'+hashlib.sha256(expected_bytes).hexdigest()
        return dict(artifact=artifact,hashes=hashes,inodes={name:(directory/name).stat().st_ino for name in ['events.jsonl','manifest.json']},manifest=expected)
    def stop_wrappers(record, processes):
        # Actual parent/driver joins are asserted in normal flow. These wrappers
        # alone cannot exclude inner processes; outer lab stops owned containers.
        errors=[]
        for name,process in processes:
            if process is not None:
                try:
                    if name=='proxy' and process.process.poll() is None: send(process,'stop\n')
                    process.stop()
                except BaseException as error: errors.append(dict(process=name,error=type(error).__name__))
                record[name+'_output']=process.stdout.decode();record[name+'_error']=process.stderr.decode()
        record['cleanup_errors']=errors;save();assert not errors
    try:
        activate(5,{'administration.enabled':True,'export.memory.max_bytes':268435456})
        def login_buckets():
            return json.loads(sql(db,"SELECT jsonb_build_object('observed_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'buckets',(SELECT jsonb_agg(jsonb_build_object('kind',kind,'bucket',bucket,'window_started_at_us',window_started_at_us,'attempts',attempts) ORDER BY kind,bucket) FROM control.login_buckets))"))
        result['login_before']=login_buckets();save()
        command('docker','exec',runner,'python3','-B','/out/build/source/experiments/export-execution/recovery_client.py','accept-commits',timeout=85)
        result['login_after']=login_buckets()
        result['login_responses']=json.loads((private/'executor-commit-accept-login.json').read_text());save()
        acceptance=json.loads((private/'executor-commit-accept.json').read_text())
        assert acceptance['status']=='PASS' and acceptance['logged_out'] and len(acceptance['receipts'])==2
        jobs=sorted(r['job_id'] for r in acceptance['receipts'])
        assert len(set(jobs))==2 and all(len(j)==32 and all(c in '0123456789abcdef' for c in j) for j in jobs)
        result['acceptance']=acceptance
        before=snapshots();assert len(before)==4
        for job in jobs: assert before[job]['job']['state']=='accepted' and before[job]['unit'] is None
        baseline_pins=pins();activate(6,{'administration.enabled':False,'export.memory.max_bytes':1073741824})
        result['before']=before;result['pins_before']=baseline_pins;save()
        for job,mode in zip(jobs,['after-complete','before-forward']):
            record=dict(mode=mode,job=job,status='INCOMPLETE');result['cases'].append(record)
            proxy=client=None
            try:
                record['before']=snapshots();record['pins_before']=pins()
                proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode',mode,'--target','control-export-complete'],input_pipe=True)
                ready=record['proxy_ready']=event(proxy,'ready')
                assert ready['source_sha256']==sha(source/'experiments/archive-engine/native_commit_proxy.py')
                client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/commit_client.py',mode,str(ready['port'])],input_pipe=True)
                fault=record['fault']=event(proxy,'fault')
                failed=record['failed']=event(client,'failed');assert client.wait(10)==0 and not client.stderr and not client.overflow
                observed_owner=record['owner']=owner()
                assert observed_owner['pid']==fault['backend_pid'] and not observed_owner['present']
                assert observed_owner['resource']==failed['owner']['owner_resource_id'] and observed_owner['epoch']==failed['owner']['owner_epoch'] and observed_owner['incarnation']==failed['owner']['process_incarnation']
                assert fault['data_queries']==fault['data_commits']==fault['faults']==1
                assert fault['server_complete']==int(mode=='after-complete') and fault['client_complete']==0 and not fault['rejection_sequence']
                assert failed['exit']==4 and failed['parent_absent'] and failed['lock_available'] and 'CommitUnknown' in failed['stderr']
                record['backends']=backends();assert record['backends']=={db:0,'bbpub_normal':0}
                after=record['after']=snapshots();new_pins=record['pins_after']=pins()
                preserved({k:v for k,v in record['before'].items() if k!=job},after)
                preserved(record['pins_before'],new_pins)
                added=set(new_pins)-set(record['pins_before']);assert len(added)==1
                reader=record['unknown_reader']=next(iter(added))
                assert new_pins[reader]['pin']['purpose']=='reader' and new_pins[reader]['lease']['state']=='active'
                assert new_pins[reader]['pin']['issuer_epoch']==int(observed_owner['epoch'])
                row=after[job]
                assert row['job']['state']==('succeeded' if mode=='after-complete' else 'running')
                assert row['job']['checkpoint_revision']==int(mode=='after-complete')
                assert (row['artifact'] is not None)==(mode=='after-complete') and row['export_pin']['lease']['state']=='active'
                record['output']=output(job,row)
                if mode=='after-complete': assert row['artifact']['row']['verification_pin_id']=='\\x'+reader
                send(proxy,'stop\n');assert proxy.wait(5)==0
                record['proxy_terminal']=event(proxy,'terminal')
                assert record['proxy_terminal']['unexpected']==0 and not proxy.stderr and not proxy.overflow
            finally: stop_wrappers(record,[('client',client),('proxy',proxy)])
            record['status']='PASS';save()
        # Observe no new durable rows, row versions or file replacement for the
        # completed job. These snapshots do not trace every filesystem read/write.
        before_restart=snapshots();pins_before_restart=pins()
        client=None;record=result['restart']={}
        try:
            client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/commit_client.py','restart','0'],input_pipe=True)
            record['ready']=event(client,'ready')
            end=time.monotonic()+15
            while True:
                terminal=snapshots();current_pins=pins()
                new=set(current_pins)-set(pins_before_restart)
                if all(v['job']['state']=='succeeded' for v in terminal.values()) and len(new)==1 and current_pins[next(iter(new))]['lease']['state']=='released': break
                if time.monotonic()>=end: raise TimeoutError('completion COMMIT resolution after restart')
                time.sleep(.05)
            record['observed']=terminal;record['pins_observed']=current_pins;save()
            send(client,'stop\n');assert client.wait(10)==0
            record['client_terminal']=event(client,'terminal');assert record['client_terminal']==dict(event='terminal',exit=0,lock_available=True)
            assert not client.stderr and not client.overflow
            assert snapshots()==terminal and pins()==current_pins
            preserved({k:v for k,v in before_restart.items() if k!=jobs[1]},terminal)
            preserved(pins_before_restart,current_pins)
            assert len(new)==1
            final_reader=next(iter(new));assert current_pins[final_reader]['pin']['purpose']=='reader'
            assert terminal[jobs[1]]['artifact']['row']['verification_pin_id']=='\\x'+final_reader
            record['outputs']={}
            for index,job in enumerate(jobs):
                observed=output(job,terminal[job]);record['outputs'][job]=observed
                old=result['cases'][index]['output']
                assert observed['artifact']==old['artifact'] and observed['hashes']==old['hashes'] and observed['manifest']==old['manifest']
                assert all((observed['inodes'][name]==old['inodes'][name])==(index==0) for name in observed['inodes'])
                assert terminal[job]['export_pin']['lease']['state']=='active'
            record['terminal']=terminal;record['pins_terminal']=current_pins;record['backends']=backends()
            assert record['backends']=={db:0,'bbpub_normal':0}
            assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        finally: stop_wrappers(record,[('client',client)])
        result['status']='PASS'
        return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally: save()
