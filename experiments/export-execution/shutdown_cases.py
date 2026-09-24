#!/usr/bin/env python3
"""Actual SIGTERM while a positively identified policy poll waits on PostgreSQL."""
import json
import hashlib
import time
from signed_publication_cases import Process, event, send


def run(root, state, sql):
    result = dict(status='INCOMPLETE', source=state['source'], cases={})
    path = root/'shutdown-cases.json'; assert not path.exists()
    db = 'bbpub_control_normal'
    def save(): path.write_text(json.dumps(result, indent=2)+'\n')
    def wait(predicate, seconds=4):
        end=time.monotonic()+seconds
        while True:
            value=predicate()
            if value:return value
            assert time.monotonic()<end, 'bounded observation deadline'
            time.sleep(.02)
    try:
        for name,delay in [('released-before-deadline',.3),('held-past-deadline',6),('private-released-before-deadline',.6)]:
            observed=result['cases'][name]={};save()
            client=holder=None
            try:
                if name.startswith('private-'):
                    policy=json.loads(sql(db,'SELECT document FROM control.policy_revisions WHERE revision=2'))
                    policy['settings']['administration.enabled']=True
                    document=json.dumps(policy,sort_keys=True,separators=(',',':'))
                    digest=hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
                    sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 3,dataset_id,2,config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=2; UPDATE control.datasets SET policy_revision=3; COMMIT;")
                client=Process(['docker','exec','-i',state['container_id'],'python3','-B','/out/build/source/experiments/export-execution/shutdown_client.py',name],input_pipe=True)
                observed['ready']=event(client,'ready');save()
                backend=json.loads(sql(db,"SELECT json_build_object('pid',pid,'backend_start',backend_start) FROM pg_stat_activity WHERE usename='bigbrotr_control' AND datname=current_database()"))
                holder=Process(['docker','exec','-i',state['postgres_id'],'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input_pipe=True)
                send(holder,"BEGIN; SELECT json_build_object('event','locked','pid',pg_backend_pid()) FROM control.datasets FOR UPDATE;\n")
                observed['holder']=event(holder,'locked');pid=observed['holder']['pid']
                def blocked():
                    return json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start,'query',query,'wait_event_type',wait_event_type)),'[]') FROM pg_stat_activity WHERE {pid}=ANY(pg_blocking_pids(pid)) AND datname=current_database() AND usename='bigbrotr_control'"))
                waiter,=wait(blocked)
                assert {k:waiter[k] for k in backend}==backend and waiter['wait_event_type']=='Lock'
                assert waiter['query']=='SELECT substring(dataset_id FROM 1 FOR 17), substring(instance_id FROM 1 FOR 17) FROM control.datasets LIMIT 2 FOR UPDATE'
                observed['waiter']=waiter;save()
                send(client,json.dumps(dict(operation='signal'))+'\n');event(client,'signalled')
                started=time.monotonic();time.sleep(delay)
                with client.condition:
                    observed['early_terminal']=any(json.loads(line).get('event')=='terminal' for line in bytes(client.stdout).split(b'\n')[:-1])
                send(holder,'ROLLBACK;\n\\q\n');assert holder.wait(5)==0;holder.stop();holder=None
                observed['held_after_signal_seconds']=time.monotonic()-started
                observed['terminal']=event(client,'terminal',timeout=10)
                if name.startswith('private-'):
                    observed['admission']=event(client,'admission')
                    assert observed['admission']==dict(event='admission',parent_running=True,closed=True)
                assert client.wait(5)==0 and not client.stderr and not client.overflow
                observed['owners']=json.loads(sql(db,"SELECT json_agg(json_build_object('epoch',epoch::text,'state',state) ORDER BY epoch) FROM control.owner_epochs WHERE role='control'"))
                observed['backends']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
                terminal=observed['terminal']
                safe=all(terminal[k] for k in ['parent_absent','lock_available','scratch_empty']) and not any(observed['backends'].values()) and terminal['elapsed_seconds']<8
                if name.endswith('released-before-deadline'):
                    expected=safe and not observed['early_terminal'] and terminal['exit']==0 and not terminal['stderr'] and json.loads(terminal['stdout'].splitlines()[-1]).get('release')=='committed' and observed['owners'][-1]['state']=='released'
                else:
                    expected=safe and observed['early_terminal'] and terminal['exit']!=0 and len(terminal['stdout'].splitlines())==1 and observed['owners'][-1]['state']=='active'
                observed['status']='PASS' if expected else 'FAIL';save()
            finally:
                if holder:holder.stop()
                if client:client.stop()
        result['status']='PASS' if all(c['status']=='PASS' for c in result['cases'].values()) else 'FAIL'
        save();assert result['status']=='PASS';return result
    finally:save()
