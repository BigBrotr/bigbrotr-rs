#!/usr/bin/env python3
"""Independent SQL/HTTP status oracle; no synthetic status or mocked response."""
import copy
import hashlib
import json
import time
from signed_publication_cases import Process, command, event, send


def run(root, state, sql, policy):
    db = 'bbpub_control_normal'; runner = state['container_id']
    result = dict(status='INCOMPLETE', source=state['source'], cases={})
    path = root/'status-cases.json'; assert not path.exists()
    client = None
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def exchange(name, operation, **fields):
        send(client,json.dumps(dict(operation=operation,**fields))+'\n')
        observed=event(client, {'revoke-history':'revoked'}.get(operation,operation),timeout=8)
        result['cases'][name]=observed;save();return observed
    def session_rows():
        # No token/CSRF digest, salt or credential material in evidence.
        return json.loads(sql(db,"SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'principal',encode(principal_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def ledgers():
        return {table:json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY {key}),'[]') FROM control.{table} t")) for table,key in [('jobs','job_id'),('export_execution_units','job_id'),('artifacts','artifact_id'),('artifact_files','artifact_id,file_id'),('snapshot_leases','lease_id'),('archive_snapshot_pins','lease_id'),('retention_claims','claim_id')]}
    def status_expected(observation):
        assert observation['status']==200, observation
        value=observation['observation']
        assert set(value)==set(expected)|{'observed_at_us'}
        assert {k:v for k,v in value.items() if k!='observed_at_us'}==expected
        assert int(value['observed_at_us'])>int(value['created_at_us'])
    def start(name):
        return Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/status_client.py',name],input_pipe=True)
    def stop(name):
        send(client,json.dumps(dict(operation='stop'))+'\n');assert client.wait(10)==0
        result[name]=event(client,'terminal');assert not client.stderr and not client.overflow
        client.stop()
    try:
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        selected=copy.deepcopy(policy);selected['settings'].update({'administration.enabled':True,'export.memory.max_bytes':268435456})
        document=json.dumps(selected,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
        sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 3,dataset_id,2,config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=2; UPDATE control.datasets SET policy_revision=3; COMMIT;")
        sql(db,'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
        # Isolated fixture account creation, not a qualification of account management.
        # Its independently authenticated HTTP session has only status.read.
        bob='b7'*16
        sql(db,f"BEGIN; INSERT INTO control.principals SELECT decode('{bob}','hex'),dataset_id,'local',created_at_us,database_role,local_os_uid FROM control.principals WHERE kind='installation'; INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision) SELECT decode('{bob}','hex'),dataset_id,'bob',false,true,credential_revision,1 FROM control.local_accounts WHERE username='alice'; INSERT INTO control.local_credentials SELECT decode('{bob}','hex'),dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash FROM control.local_credentials WHERE principal_id=(SELECT principal_id FROM control.local_accounts WHERE username='alice'); INSERT INTO control.principal_grants SELECT dataset_id,decode('{bob}','hex'),'status.read','dataset',dataset_id FROM control.datasets; COMMIT;")
        job=sql(db,"SELECT encode(job_id,'hex') FROM control.jobs")
        row=json.loads(sql(db,"SELECT jsonb_build_object('job_id',encode(job_id,'hex'),'state',state,'job_revision',job_revision::text,'checkpoint_revision',checkpoint_revision::text,'created_at_us',created_at_us::text,'completed_at_us',completed_at_us::text,'reason_code',reason_code) FROM control.jobs"))
        assert row['state']=='succeeded' and row['job_revision']=='4' and row['checkpoint_revision']=='1' and row['reason_code'] is None
        expected=dict(format='bigbrotr-job-status/1',kind='export',authority='observation-only',**row)
        result['expected']=expected;result['before']=ledgers();save()
        # The seed consumed four account attempts. Preserve its real counters.
        started=time.monotonic();time.sleep(60);result['rate_wait_seconds']=time.monotonic()-started
        client=start('initial');result['ready']=event(client,'ready')
        alice=exchange('alice-login','login',username='alice')['observation']['principal_id']
        result['sessions_before_reads']=session_rows()
        status_expected(exchange('owner-status','status',job=job))
        missing=exchange('missing-status','status',job='ff'*16)
        assert missing['status']==404 and missing['observation']['error']['code']=='job_unavailable'
        result['sessions_after_reads']=session_rows();assert result['sessions_after_reads']==result['sessions_before_reads']
        # A real signed-event request adds one durable accepted job. The finite
        # executor cannot fit in this policy's256MiB; acceptance remains available.
        accepted=exchange('signed-acceptance','accept')
        job=accepted['receipt']['job_id']
        row=json.loads(sql(db,f"SELECT jsonb_build_object('job_id',encode(job_id,'hex'),'state',state,'job_revision',job_revision::text,'checkpoint_revision',checkpoint_revision::text,'created_at_us',created_at_us::text,'completed_at_us',completed_at_us::text,'reason_code',reason_code) FROM control.jobs WHERE job_id=decode('{job}','hex')"))
        assert row['state']=='accepted' and row['job_revision']=='1' and row['checkpoint_revision']=='0' and row['completed_at_us'] is None
        expected=dict(format='bigbrotr-job-status/1',kind='export',authority='observation-only',**row)
        result['accepted_expected']=expected
        old=result['before'];result['before_observation']=ledgers()
        assert len(result['before_observation']['jobs'])==2 and all(v in result['before_observation']['jobs'] for v in old['jobs'])
        status_expected(exchange('accepted-zero-checkpoint','status',job=job))
        exchange('input-revocation','revoke-history',principal=alice)
        assert sql(db,"SELECT enabled FROM control.scope_grants WHERE action='history.read'")=='f'
        status_expected(exchange('status-after-input-revocation','status',job=job))
        exchange('bob-login','login',username='bob')
        other=exchange('other-actor-status','status',job=job)
        absent=exchange('bob-missing','status',job='ff'*16)
        assert other['status']==absent['status']==404
        assert {k:v for k,v in other['observation']['error'].items() if k!='correlation_id'}=={k:v for k,v in absent['observation']['error'].items() if k!='correlation_id'}
        assert exchange('bob-still-healthy','me')['status']==200
        exchange('alice-login-again','login',username='alice')
        # Privileged fixture grant change uses the same dataset/account lock order;
        # this does not advertise a production dataset-grant editor.
        sql(db,f"BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; SELECT 1 FROM control.local_accounts WHERE principal_id=decode('{alice}','hex') FOR UPDATE; DELETE FROM control.principal_grants WHERE principal_id=decode('{alice}','hex') AND action IN ('status.read','data.read'); UPDATE control.local_accounts SET grant_revision=grant_revision+1 WHERE principal_id=decode('{alice}','hex'); COMMIT;")
        assert exchange('status-grant-revoked','status',job=job)['status']==403
        assert exchange('control-after-denial','me')['status']==200
        sql(db,f"BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; SELECT 1 FROM control.local_accounts WHERE principal_id=decode('{alice}','hex') FOR UPDATE; INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'status.read','dataset',dataset_id FROM control.local_accounts WHERE principal_id=decode('{alice}','hex'); UPDATE control.local_accounts SET grant_revision=grant_revision+1 WHERE principal_id=decode('{alice}','hex'); COMMIT;")
        status_expected(exchange('no-data-grant','status',job=job))
        sql(db,f"BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; UPDATE control.local_accounts SET active=false WHERE principal_id=decode('{alice}','hex'); COMMIT;")
        assert exchange('deactivated-session','status',job=job)['status']==401
        assert ledgers()==result['before_observation']
        stop('initial-terminal');client=None
        sql(db,f"BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; UPDATE control.local_accounts SET active=true WHERE principal_id=decode('{alice}','hex'); COMMIT;")
        client=start('restart');result['restart_ready']=event(client,'ready')
        exchange('restart-login','login',username='alice')
        status_expected(exchange('restart-status','status',job=job))
        stop('restart-terminal');client=None
        result['after']=ledgers();assert result['after']==result['before_observation']
        result['backends']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        assert result['backends']=={db:0,'bbpub_normal':0}
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        result['status']='PASS';return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        if client: client.stop()
        save()
