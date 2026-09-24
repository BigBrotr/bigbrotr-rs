#!/usr/bin/env python3
"""An undersized immutable output grant parks once; process restart cannot retry it."""
import copy
import hashlib
import json
import stat
import time
from signed_publication_cases import Process, command, event, send


def run(root, state, sql, policy, *, busy=False, pause_commits=False):
    runner=state['container_id'];db='bbpub_control_normal';private=root/'out/private-grant-http'
    result=dict(status='INCOMPLETE',source=state['source'],cases=[])
    path=root/'parking-cases.json';assert not path.exists()
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def activate(revision, settings):
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        selected=copy.deepcopy(policy);selected['settings'].update(settings)
        document=json.dumps(selected,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
        sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT {revision},dataset_id,{revision-1},config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision={revision-1}; UPDATE control.datasets SET policy_revision={revision}; COMMIT;")
    def rows(table, order):
        return json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY {order}),'[]') FROM control.{table} t"))
    def snapshot():
        return {t:rows(t,key) for t,key in [('jobs','job_id'),('export_execution_units','job_id'),('artifacts','artifact_id'),('artifact_files','artifact_id,file_id'),('snapshot_leases','lease_id'),('archive_snapshot_pins','lease_id'),('retention_claims','claim_id')]}
    def target(observation):
        return next(v['row'] for v in observation['jobs'] if v['row']['job_id']=='\\x'+job)
    def backends():
        return {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
    try:
        activate(3,{'administration.enabled':True,'export.memory.max_bytes':268435456})
        sql(db,'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
        command('docker','exec',runner,'python3','-B','/out/build/source/experiments/export-execution/recovery_client.py','accept')
        accepted=json.loads((private/'executor-signed-accept.json').read_text());assert accepted['status']=='PASS' and accepted['logged_out']
        result['acceptance']=accepted;job=accepted['receipt']['job_id']
        before=result['before']=snapshot();assert len(before['jobs'])==2 and target(before)['state']=='accepted'
        #399 signed bytes + the independently specified1228byte manifest cannot
        #fit this logical output grant. The actual reader must still execute.
        activate(4,{'administration.enabled':False,'export.memory.max_bytes':1073741824,'export.artifact.max_bytes':1536})
        save()
        assert not (busy and pause_commits)
        if pause_commits:
            from pause_commit_cases import run as check_commits
            result['pause_commits']=check_commits(root,state,sql,job,before,snapshot)
            result['status']='PASS'
            return result
        if busy:
            from busy_cases import run as check_busy
            result['busy']=check_busy(root,state,sql,job,before,snapshot)
            result['status']='PASS'
            return result
        for label in ['attempt','restart']:
            record=dict(label=label,status='INCOMPLETE');result['cases'].append(record);client=None
            try:
                client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/parking_client.py',label],input_pipe=True)
                record['ready']=event(client,'ready');save()
                end=time.monotonic()+8
                while True:
                    current=snapshot();row=target(current)
                    readers=[v['row'] for v in current['archive_snapshot_pins'] if v not in before['archive_snapshot_pins']]
                    released={v['row']['lease_id'] for v in current['snapshot_leases'] if v['row']['state']=='released'}
                    if label=='restart' or (len(readers)==1 and readers[0]['lease_id'] in released):break
                    if time.monotonic()>=end:raise TimeoutError('known reader cleanup before parking')
                    time.sleep(.05)
                # Sample beyond several250ms queue intervals. This is a bounded
                # no-retry observation, not an aged-queue/capacity qualification.
                time.sleep(1.5)
                record['observed']=snapshot();save()
                send(client,'stop\n');assert client.wait(10)==0
                record['client_terminal']=event(client,'terminal');record['terminal']=snapshot();record['backends']=backends();save()
                assert not client.stderr and not client.overflow
                assert record['backends']=={db:0,'bbpub_normal':0}
                assert record['observed']==record['terminal']
                assert record['client_terminal']['alive_before_stop'] and record['client_terminal']['exit']==0,'known refusal stopped Control instead of parking'
                assert record['client_terminal']['parent_absent'] and record['client_terminal']['lock_available'] and record['client_terminal']['scratch_empty']
                assert not record['client_terminal']['stderr']
                final=record['terminal'];row=target(final)
                assert row['state']=='paused' and row['job_revision']==4 and row['checkpoint_revision']==0 and row['reason_code']=='resource_limit'
                assert row['result_manifest_digest'] is None and row['completed_at_us'] is None
                for table in ['artifacts','artifact_files']:assert final[table]==before[table]
                for old in before['jobs']:assert old['row']['job_id']=='\\x'+job or old in final['jobs']
                for table in ['export_execution_units','snapshot_leases','archive_snapshot_pins','retention_claims']:
                    for old in before[table]:assert old in final[table]
                units=[v['row'] for v in final['export_execution_units'] if v['row']['job_id']=='\\x'+job];assert len(units)==1
                unit=units[0];assert unit['max_output_bytes']==1536 and unit['max_temporary_bytes']==3072
                artifact=unit['artifact_id'][2:];assert len(artifact)==32 and all(c in '0123456789abcdef' for c in artifact)
                destination=root/'out/signed_normal/export-destination'
                record['files_present']={name:(destination/name).exists() for name in [artifact,'.artifact-'+artifact+'.partial','.artifact-'+artifact+'.lock']}
                assert record['files_present']=={artifact:False,'.artifact-'+artifact+'.partial':False,'.artifact-'+artifact+'.lock':True}
                lock=destination/('.artifact-'+artifact+'.lock');info=lock.lstat()
                assert stat.S_ISREG(info.st_mode) and info.st_nlink==1
                record['namespace']=sorted(p.name for p in destination.iterdir())
                record['mutex']={key:getattr(info,'st_'+key) for key in ['dev','ino','mode','nlink','size']}
                record['mutex']['sha256']=hashlib.sha256(lock.read_bytes()).hexdigest()
                added=[v['row'] for v in final['archive_snapshot_pins'] if v not in before['archive_snapshot_pins']];assert len(added)==1 and added[0]['purpose']=='reader'
                assert next(v['row']['state'] for v in final['snapshot_leases'] if v['row']['lease_id']==added[0]['lease_id'])=='released'
                assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
                if label=='restart':
                    assert final==result['cases'][0]['terminal']
                    for key in ['namespace','mutex']:assert record[key]==result['cases'][0][key]
            finally:
                if client is not None:
                    client.stop();record['client_output']=client.stdout.decode();record['client_error']=client.stderr.decode()
                save()
            record['status']='PASS';save()
        result['status']='PASS'
        return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:save()
