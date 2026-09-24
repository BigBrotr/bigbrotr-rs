#!/usr/bin/env python3
"""Independent real-PG oracles for selected acceptance COMMIT loss and retry.

Source/engines/signed archive are prepared by export_job_http_cases. Old pins are
retained obligations, even after an actually fenced successor can accept work.
No recovery release, destination driver-custody or exported file is inferred.
"""
import json
from pathlib import Path
from signed_publication_cases import Process, event, send, sha
from signed_publication_oracle import EVENT

TABLES=('management_commands','management_audit','jobs','job_input_bounds',
        'export_jobs','snapshot_leases','archive_snapshot_pins','retention_claims')


def run(root, sql, observe, retained):
    resources=json.loads((root/'resources.json').read_text())
    source=root/'source';runner=resources['container_id'];db='bbpub_control_normal'
    path=root/'export-job-commit-cases.json';assert not path.exists()
    result=dict(source=resources['source'],status='INCOMPLETE',cases=[])
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def query(text):return json.loads(sql(db,text))
    def rows():
        fields=[]
        for table in TABLES:
            digest="encode(sha256(convert_to(to_jsonb(t)::text||':'||xmin::text,'UTF8')),'hex')"
            fields.append(f"'{table}',(SELECT coalesce(jsonb_agg({digest} ORDER BY {digest}),'[]'::jsonb) FROM control.{table} t)")
        return query('SELECT jsonb_build_object('+','.join(fields)+');')
    def unchanged_subset(old,new):
        assert set(old)==set(new)==set(TABLES)
        for table in TABLES:
            assert len(set(new[table]))==len(new[table])
            assert set(old[table])<=set(new[table]),table
    def owner():
        return query("""SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,
          'incarnation',encode(process_incarnation,'hex'),'backend_pid',control_backend_pid,
          'backend_start',control_backend_start,'backend_present',EXISTS(SELECT 1 FROM pg_stat_activity
          WHERE pid=control_backend_pid AND backend_start=control_backend_start))
          FROM control.owner_epochs WHERE role='control' AND state='active'""")
    def pins():
        return query("""SELECT coalesce(jsonb_object_agg(encode(l.lease_id,'hex'),jsonb_build_object(
          'purpose',p.purpose,'state',l.state,'revision',l.lease_revision::text,
          'issuer_epoch',p.issuer_epoch::text,'issuer_incarnation',encode(p.issuer_incarnation,'hex'),
          'job',encode(p.export_job_id,'hex'),'claims',(SELECT jsonb_agg(jsonb_build_array(
            shard_id,owner_epoch::text,first_sequence::text,last_sequence::text,purpose,state))
            FROM control.retention_claims WHERE lease_id=l.lease_id))),'{}'::jsonb)
          FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id)""")
    def details():
        return query("""SELECT coalesce(jsonb_object_agg(encode(e.request_key,'hex'),jsonb_build_object(
          'job_id',encode(e.job_id,'hex'),'accepted_at_us',c.accepted_at_us::text,
          'export_pin',encode(e.pin_lease_id,'hex'),'allocation_pin',encode(e.allocation_pin_id,'hex'),
          'event_id',encode(e.event_id,'hex'),'state',j.state,'complete',j.input_complete,
          'bounds',(SELECT jsonb_agg(jsonb_build_array(shard_id,owner_epoch::text,last_complete_sequence::text)
              ORDER BY shard_id,owner_epoch) FROM control.job_input_bounds WHERE job_id=e.job_id))), '{}'::jsonb)
          FROM control.export_jobs e JOIN control.management_commands c USING(actor_id,request_key)
          JOIN control.jobs j USING(job_id)""")
    def backends():
        return {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for database in [db,'bbpub_normal']}
    def state():return dict(counts=observe(),rows=rows(),pins=pins(),jobs=details())
    try:
        for index,mode in enumerate(['before-forward','after-complete']):
            record=dict(mode=mode,status='INCOMPLETE');result['cases'].append(record);save()
            proxy=client=None
            try:
                record['before']=state();save()
                assert record['before']['counts']==dict(commands=index,details=index,audit=index,jobs=index,export_pins=index,reader_pins=index,claims=2*index)
                proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py',
                    '--mode',mode,'--target','control-export-accept'],input_pipe=True)
                ready=record['proxy_ready']=event(proxy,'ready')
                assert ready['source_sha256']==sha(source/'experiments/archive-engine/native_commit_proxy.py')
                client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/export_job_commit_client.py',mode,str(ready['port'])],input_pipe=True)
                ready=record['client_ready']=event(client,'ready')
                original=record['original_owner']=owner()
                assert original['backend_present'] and original['epoch']==ready['owner']['owner_epoch']
                assert original['resource']==ready['owner']['owner_resource_id'] and original['incarnation']==ready['owner']['process_incarnation']
                save();send(client,'submit\n')
                fault=record['fault']=event(proxy,'fault')
                assert fault['backend_pid']==original['backend_pid']
                assert fault['data_queries']==fault['data_commits']==fault['faults']==1
                assert fault['server_complete']==index and fault['client_complete']==0
                assert not fault['rejection_sequence']
                record['failed']=event(client,'failed');save()
                assert record['failed']['exit']==4 and record['failed']['parent_absent'] and record['failed']['lock_available']
                assert 'CommitUnknown' in record['failed']['stderr']
                record['after_fault']=state();record['failed_owner']=owner();record['backends_after_fault']=backends();save()
                assert record['failed_owner']=={**original,'backend_present':False}
                assert record['backends_after_fault']=={db:0,'bbpub_normal':0}
                after=record['after_fault'];before=record['before']
                count=index+int(mode=='after-complete')
                assert after['counts']==dict(commands=count,details=count,audit=count,jobs=count,export_pins=count,reader_pins=index+1,claims=count+index+1)
                unchanged_subset(before['rows'],after['rows'])
                for key,value in before['pins'].items():assert after['pins'][key]==value
                old_reader=set(after['pins'])-set(before['pins'])
                old_reader={p for p in old_reader if after['pins'][p]['purpose']=='reader'}
                assert len(old_reader)==1;old_reader=next(iter(old_reader))
                assert after['pins'][old_reader]['state']=='active' and after['pins'][old_reader]['issuer_epoch']==original['epoch']
                key=ready['key'];assert (key in after['jobs'])==(mode=='after-complete')
                send(proxy,'stop\n');record['proxy_exit']=proxy.wait(5);record['proxy_terminal']=event(proxy,'terminal')
                assert record['proxy_exit']==0 and not proxy.stderr and not proxy.overflow and record['proxy_terminal']['unexpected']==0
                save();send(client,'retry\n')
                retried=record['retried']=event(client,'retried')
                successor=record['successor']=owner()
                assert successor['backend_present'] and successor['resource']==original['resource']
                assert int(successor['epoch'])==int(original['epoch'])+1 and successor['incarnation']!=original['incarnation']
                assert successor['epoch']==retried['owner']['owner_epoch']
                observed=record['after_retry']=state();save()
                assert observed['counts']==dict(commands=index+1,details=index+1,audit=index+1,jobs=index+1,export_pins=index+1,reader_pins=index+1,claims=2*(index+1))
                unchanged_subset(after['rows'],observed['rows'])
                for pin,value in after['pins'].items():assert observed['pins'][pin]==value
                detail=observed['jobs'][key];receipt=retried['receipt']
                assert {field:receipt[field] for field in ['job_id','accepted_at_us']}=={field:detail[field] for field in ['job_id','accepted_at_us']}
                assert detail['event_id']==EVENT['id'] and detail['state']=='accepted' and detail['complete'] and detail['bounds']==[[0,'1','10']]
                export=observed['pins'][detail['export_pin']]
                assert export['job']==detail['job_id'] and export['purpose']=='export' and export['state']=='active'
                assert export['claims']==[[0,'1','1','10','export','active']]
                if mode=='before-forward':
                    new_reader=detail['allocation_pin'];assert new_reader!=old_reader and new_reader not in after['pins']
                    assert set(observed['pins'])-set(after['pins'])=={new_reader,detail['export_pin']}
                    pin=observed['pins'][new_reader]
                    assert pin['purpose']=='reader' and pin['state']=='released' and pin['issuer_epoch']==successor['epoch']
                    assert pin['claims']==[[0,'1','1','10','reader','released']]
                    assert export['issuer_epoch']==successor['epoch']
                else:
                    assert observed==after and detail['allocation_pin']==old_reader
                    assert export['issuer_epoch']==original['epoch']
                record['retry_digest_set']=retained();save()
                send(client,'stop\n');record['client_exit']=client.wait(10);record['client_terminal']=event(client,'terminal')
                assert record['client_exit']==0 and not client.stderr and not client.overflow
                assert record['client_terminal']==dict(event='terminal',exit=0,lock_available=True)
                record['terminal']=state();record['terminal_backends']=backends();save()
                assert record['terminal']==observed and retained()==record['retry_digest_set']
                assert record['terminal_backends']=={db:0,'bbpub_normal':0}
                assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
                record['status']='PASS'
            finally:
                # These wrappers are not remote exclusion. The launcher owns any
                # unresolved child/backend; no PASS if their completion is absent.
                errors=[]
                for name,process in [('client',client),('proxy',proxy)]:
                    if process is not None:
                        try:
                            if name=='proxy' and process.process.poll() is None:send(process,'stop\n')
                            process.stop()
                        except BaseException as error:errors.append(dict(stage=name,error=type(error).__name__))
                        record[name+'_output']=process.stdout.decode();record[name+'_error']=process.stderr.decode()
                record['cleanup_errors']=errors;save()
                assert not errors
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:save()
    return result
