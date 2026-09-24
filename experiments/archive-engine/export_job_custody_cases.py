#!/usr/bin/env python3
"""Real committed acceptance, held reply and sampled destination custody.

Catalog closure is a lower bound, not an observation of the Rust join call.
The source-isolated mutant supplies the causal discrimination. No pin recovery.
"""
import json
import time
from signed_publication_cases import Process, event, send, sha
from signed_publication_oracle import EVENT


def run(root, sql, observe, retained):
    resources=json.loads((root/'resources.json').read_text())
    source=root/'source';runner=resources['container_id'];db='bbpub_control_normal'
    path=root/'export-job-custody-cases.json';assert not path.exists()
    private=root/'out/private-grant-http';proxy=client=None
    result=dict(status='INCOMPLETE',source=resources['source'])
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def query(text):return json.loads(sql(db,text))
    def observation():
        return query("""SELECT json_build_object(
          'owner',(SELECT json_build_object('resource',encode(resource_id,'hex'),'epoch',epoch::text,
            'incarnation',encode(process_incarnation,'hex'),'pid',control_backend_pid,
            'start',control_backend_start,'present',EXISTS(SELECT 1 FROM pg_stat_activity
              WHERE pid=control_backend_pid AND backend_start=control_backend_start))
            FROM control.owner_epochs WHERE role='control' AND state='active'),
          'catalog_backends',(SELECT count(*) FROM pg_stat_activity WHERE datname='bbpub_normal'))""")
    try:
        result['before']=dict(counts=observe(),rows=retained());save()
        assert not any(result['before']['counts'].values())
        proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py',
            '--mode','hold-reply','--target','control-export-accept'],input_pipe=True)
        ready=result['proxy_ready']=event(proxy,'ready')
        assert ready['source_sha256']==sha(source/'experiments/archive-engine/native_commit_proxy.py')
        client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/export_job_custody_client.py',str(ready['port'])],input_pipe=True)
        result['submitted']=event(client,'submitted')
        held=result['held']=event(proxy,'held')
        assert held['stage']=='reply' and held['server_complete']==1 and held['client_complete']==0
        assert held['data_queries']==held['data_commits']==1 and not held['rejection_sequence']
        original=result['held_state']=observation();owner=original['owner']
        assert owner['present'] and owner['pid']==held['backend_pid'] and original['catalog_backends']==1
        ready=result['submitted']['owner']
        assert [owner[k] for k in ['resource','epoch','incarnation']]==[ready[k] for k in ['owner_resource_id','owner_epoch','process_incarnation']]
        send(client,'signal\n');result['signal']=event(client,'signal_sent')
        deadline=time.monotonic()+1.5
        while True:
            observed=observation()
            if observed['catalog_backends']==0:break
            assert time.monotonic()<deadline,'catalog backend did not close before sampling deadline'
            time.sleep(.01)
        assert observed['owner']==owner
        # The client only uses samples taken after this positive SQL observation.
        (private/'custody-catalog-gone.json').write_text(json.dumps(observed)+'\n')
        result['catalog_gone']=observed;save()
        result['client_observed']=event(client,'observed',timeout=15)
        result['client_exit']=client.wait(10)
        result['client_result']=json.loads((private/'custody-result.json').read_text());save()
        # Always record real terminal SQL, including for the expected mutant FAIL.
        result['terminal']=dict(counts=observe(),rows=retained(),observation=observation())
        result['jobs']=query("""SELECT json_agg(json_build_object('id',encode(e.job_id,'hex'),
          'key',encode(e.request_key,'hex'),'event_id',encode(e.event_id,'hex'),'state',j.state,
          'complete',j.input_complete,'allocation_pin',encode(e.allocation_pin_id,'hex'),
          'export_pin',encode(e.pin_lease_id,'hex'),'bounds',(SELECT json_agg(json_build_array(
          shard_id,owner_epoch::text,last_complete_sequence::text) ORDER BY shard_id,owner_epoch)
          FROM control.job_input_bounds WHERE job_id=e.job_id))) FROM control.export_jobs e JOIN control.jobs j USING(job_id)""")
        result['pins']=query("""SELECT json_agg(json_build_object('id',encode(l.lease_id,'hex'),
          'purpose',p.purpose,'state',l.state,'epoch',p.issuer_epoch::text,'job',encode(p.export_job_id,'hex'),
          'claims',(SELECT json_agg(jsonb_build_array(shard_id,owner_epoch::text,first_sequence::text,
          last_sequence::text,purpose,state)) FROM control.retention_claims WHERE lease_id=l.lease_id)))
          FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) WHERE l.state='active'""")
        save()
        assert result['terminal']['counts']==dict(commands=1,details=1,audit=1,jobs=1,export_pins=1,reader_pins=1,claims=2)
        assert result['terminal']['observation']==dict(owner={**owner,'present':False},catalog_backends=0)
        assert len(result['jobs'])==1 and len(result['pins'])==2
        job=result['jobs'][0]
        assert job['key']==result['submitted']['key'] and job['event_id']==EVENT['id']
        assert job['state']=='accepted' and job['complete'] and job['bounds']==[[0,'1','10']]
        for pin in result['pins']:
            purpose=pin['purpose'];assert purpose in ('reader','export') and pin['state']=='active' and pin['epoch']==owner['epoch']
            assert pin['id']==job['allocation_pin' if purpose=='reader' else 'export_pin']
            assert pin['job']==(None if purpose=='reader' else job['id'])
            assert pin['claims']==[[0,'1','1','10',purpose,'active']]
        send(proxy,'stop\n');result['proxy_exit']=proxy.wait(5);result['proxy_terminal']=event(proxy,'terminal');save()
        assert result['proxy_exit']==0 and not proxy.stderr and not proxy.overflow
        assert result['proxy_terminal']['unexpected']==0 and result['proxy_terminal']['client_complete']==0
        if result['client_result']['status']=='FAIL':
            result['status']='FAIL';save()
        assert result['client_exit']==0 and not client.stderr and not client.overflow
        assert result['client_result']['status']=='PASS' and not result['client_result']['violations']
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        errors=[]
        for name,process in [('client',client),('proxy',proxy)]:
            if process is not None:
                try:
                    if name=='proxy' and process.process.poll() is None:send(process,'stop\n')
                    process.stop()
                except BaseException as error:errors.append(dict(stage=name,error=type(error).__name__))
                result[name+'_output']=process.stdout.decode();result[name+'_error']=process.stderr.decode()
        result['cleanup_errors']=errors;save()
        assert not errors
    return result
