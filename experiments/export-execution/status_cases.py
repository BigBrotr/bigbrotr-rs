#!/usr/bin/env python3
"""Independent SQL/HTTP status oracle; no synthetic status or mocked response."""
import copy
import hashlib
import json
import struct
import time
from signed_publication_cases import Process, command, event, send, sha
from signed_publication_oracle import EVENT


def run(root, state, sql, policy, *, admission=False):
    db = 'bbpub_control_normal'; runner = state['container_id']
    result = dict(status='INCOMPLETE', source=state['source'], cases={})
    path = root/'status-cases.json'; assert not path.exists()
    client = None
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def exchange(name, operation, **fields):
        assert name not in result['cases']
        send(client,json.dumps(dict(operation=operation,request=name,**fields))+'\n')
        observed=event(client,name,timeout=8)
        assert observed['operation']==operation
        result['cases'][name]=observed;save();return observed
    def session_rows():
        # No token/CSRF digest, salt or credential material in evidence.
        return json.loads(sql(db,"SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'principal',encode(principal_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def ledgers():
        tables=[('jobs','job_id'),('export_execution_units','job_id'),('artifacts','artifact_id'),('artifact_files','artifact_id,file_id'),('snapshot_leases','lease_id'),('archive_snapshot_pins','lease_id'),('retention_claims','claim_id')]
        if admission:
            tables += [('job_admissions','admission_ordinal'),('export_jobs','job_id'),('job_input_bounds','job_id,shard_id,owner_epoch'),('management_commands','actor_id,request_key'),('management_audit','audit_id')]
        result={}
        for table,key in tables:
            # Later login/grant changes have their own receipts; this comparison
            # retains every export-acceptance receipt and its original row version.
            where=" WHERE action='job.export.accept'" if table in ('management_commands','management_audit') else ''
            result[table]=json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY {key}),'[]') FROM control.{table} t"+where))
        return result
    def status_expected(observation):
        assert observation['status']==200, observation
        value=observation['observation']
        assert set(value)==set(expected)|{'observed_at_us'}
        assert {k:v for k,v in value.items() if k!='observed_at_us'}==expected
        assert int(value['observed_at_us'])>int(value['created_at_us'])
    def retained_output():
        destination=root/'out/signed_normal/export-destination'
        return {str(path.relative_to(destination)):sha(path)
                for entry in result['before']['artifacts']
                for name in ['manifest.json','events.jsonl']
                for path in [destination/entry['row']['artifact_id'][2:]/name]}
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
        digest=hashlib.sha256(b'bigbrotr/product-policy/3\0'+document.encode()).hexdigest()
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
        if admission: result['retained_output_before']=retained_output();save()
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
        if admission:
            binding=sha(root/'out/signed_normal/export-destination/destination.json')
            submitted=dict(kind='export',format='signed-events-jsonl',
                query=dict(version=1,resource='event_history',mode='snapshot',
                    scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
                    filters=[dict(ids=[EVENT['id']])]),
                allocation=dict(catalog_id='5a'*16,frontier_revision='1'),
                destination=dict(id='e1'*16,binding_sha256=binding),
                expected_config_revision='1',expected_policy_revision='3')
            assert accepted['submitted']==submitted and accepted['request_key']=='e8'*16
            assert accepted['receipt']['command_key']=='e8'*16
            # Independent encoding of the concrete request; equality among SQL
            # rows alone would miss a consistently stored wrong selection.
            fingerprint=hashlib.sha256(b'bigbrotr/export-event/1\0'+bytes.fromhex('01'*16+alice+'e8'*16+'01'*16)
                +struct.pack('>H',4)+bytes.fromhex('04'*16)+struct.pack('>Q',1)+bytes.fromhex(EVENT['id'])
                +struct.pack('>H',100)+bytes.fromhex('5a'*16)+struct.pack('>Q',1)+bytes.fromhex('e1'*16+binding)
                +bytes([1,1])+struct.pack('>QQ',1,3)).hexdigest()
            # Check the actual command/job/pin closure using its returned identity,
            # not merely presence of an unrelated mapping row or successful HTTP.
            closure=json.loads(sql(db,f"""SELECT jsonb_build_object(
                'job',encode(j.job_id,'hex'),'ordinal',a.admission_ordinal::text,
                'command_key',encode(c.request_key,'hex'),'actor',encode(j.principal_id,'hex'),
                'action',c.action,'kind',j.kind,'config',j.config_revision::text,'policy',j.policy_revision::text,
                'accepted_at_us',c.accepted_at_us::text,'request_digest',encode(c.request_digest,'hex'),
                'selection',jsonb_build_object('event',encode(e.event_id,'hex'),'dataset',encode(e.dataset_id,'hex'),
                    'dictionary_kind',e.dictionary_kind,'domain',encode(e.allocation_domain,'hex'),'ordinal',e.ordinal::text,
                    'catalog',encode(e.catalog_id,'hex'),'frontier',e.frontier_revision::text,'page_size',e.page_size,
                    'destination',encode(e.destination_id,'hex'),'binding',encode(e.destination_binding_sha256,'hex'),
                    'format',e.format,'visibility',e.visibility),
                'same_actor',j.principal_id=e.actor_id AND e.actor_id=c.actor_id,
                'same_request',j.request_key=e.request_key AND e.request_key=c.request_key,
                'same_digest',j.request_digest=c.request_digest,'same_time',j.created_at_us=c.accepted_at_us,
                'same_dataset',j.dataset_id=e.dataset_id AND e.dataset_id=c.dataset_id,
                'audit_bound',u.action=c.action AND u.actor_id=c.actor_id AND u.subject_id=c.request_key AND u.outcome='accepted',
                'export_pin',p.export_job_id=j.job_id AND p.purpose='export' AND p.lease_id=e.pin_lease_id,
                'lease_active',l.state='active','input_complete',j.input_complete,'protocol',j.export_job_protocol,
                'bounds',(SELECT jsonb_agg(jsonb_build_array(shard_id,owner_epoch::text,last_complete_sequence::text) ORDER BY shard_id,owner_epoch) FROM control.job_input_bounds WHERE job_id=j.job_id),
                'active_export_claims',(SELECT count(*) FROM control.retention_claims WHERE lease_id=l.lease_id AND state='active' AND purpose='export' AND first_sequence=1 AND last_sequence=10),
                'jobs',(SELECT count(*) FROM control.jobs),'mappings',(SELECT count(*) FROM control.job_admissions))
                FROM control.jobs j JOIN control.job_admissions a USING(job_id)
                JOIN control.export_jobs e USING(job_id) JOIN control.management_commands c ON c.actor_id=e.actor_id AND c.request_key=e.request_key
                JOIN control.management_audit u ON u.audit_id=c.audit_id
                JOIN control.archive_snapshot_pins p ON p.lease_id=e.pin_lease_id
                JOIN control.snapshot_leases l ON l.lease_id=p.lease_id WHERE j.job_id=decode('{job}','hex')"""))
            assert closure==dict(job=job,ordinal='2',command_key='e8'*16,actor=alice,
                action='job.export.accept',kind='export',config='1',policy='3',
                accepted_at_us=str(accepted['receipt']['accepted_at_us']),request_digest=fingerprint,
                selection=dict(event=EVENT['id'],dataset='01'*16,dictionary_kind=4,domain='04'*16,ordinal='1',
                    catalog='5a'*16,frontier='1',page_size=100,destination='e1'*16,binding=binding,
                    format='signed-events-jsonl',visibility='private'),same_actor=True,
                same_request=True,same_digest=True,same_time=True,same_dataset=True,audit_bound=True,
                export_pin=True,lease_active=True,input_complete=True,protocol=1,bounds=[[0,'1','10']],
                active_export_claims=1,jobs=2,mappings=2),closure
            result['admission_closure']=closure; result['before_identical_retry']=ledgers();save()
            for table,rows in result['before'].items():
                assert all(row in result['before_identical_retry'][table] for row in rows),table
            for table in ['export_execution_units','artifacts','artifact_files']:
                assert result['before_identical_retry'][table]==result['before'][table],table
            result['retained_output_after_acceptance']=retained_output()
            assert result['retained_output_after_acceptance']==result['retained_output_before']
            retry=exchange('signed-identical-retry','retry')
            assert retry['receipt']==dict(accepted['receipt'],created=False)
            assert retry['submitted']==submitted and retry['request_key']=='e8'*16
            result['after_identical_retry']=ledgers()
            assert result['after_identical_retry']==result['before_identical_retry']
            save()
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
        if admission:
            result['retained_output_after_restart']=retained_output()
            assert result['retained_output_after_restart']==result['retained_output_before']
        result['backends']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        assert result['backends']=={db:0,'bbpub_normal':0}
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        result['status']='PASS';return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        if client: client.stop()
        save()
