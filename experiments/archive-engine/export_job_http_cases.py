#!/usr/bin/env python3
"""Isolated real engines and HTTP job acceptance, with independent SQL oracles."""
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys
import time
sys.dont_write_bytecode=True
from signed_publication_cases import command,execute,sha
from signed_publication_oracle import check,input_expectations,EVENT
from event_cases import cut_expectation


def main(root, profile='ordinary'):
    assert profile in ('ordinary','checker','responsive','commits','custody')
    checker=profile in ('checker','responsive')
    resources=json.loads((root/'resources.json').read_text());source=root/'source'
    assert Path(__file__).resolve().parents[2]==source
    assert command('git','-C',str(source),'rev-parse','HEAD')==resources['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    runner,pg=resources['container_id'],resources['postgres_id']
    for identity in [runner,pg]:
        item=json.loads(command('docker','inspect',identity))[0]
        assert item['Config']['Labels'][resources['owner_label']]==resources['owner']
        assert item['State']['Running'] and not item['State']['OOMKilled']
    ready=json.loads((root/'out/native-tests/ready.json').read_text())
    for key,name in [('control','native-tests/bigbrotr'),('worker','archive-worker/bigbrotr-archive-worker'),('publisher','native-tests/archive-publication-probe'),('reader','native-tests/archive-read-probe')]:
        assert sha(root/'out'/name)==ready[key]
    result=dict(source=resources['source'],tree=resources['tree'],status='INCOMPLETE',cases=[],artifacts=ready)
    path=root/'export-job-http-cases.json';assert not path.exists()
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def case(name,**details):
        result['cases'].append(dict(case=name,status='PASS',**details));save()
    def sql(database,query):return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=query)
    db='bbpub_control_normal'
    client=['docker','exec',runner,'python3','-B','/out/build/source/experiments/archive-engine/export_job_http.py']
    oldclient=['docker','exec',runner,'python3','-B','/out/build/source/experiments/archive-engine/private_grant_http.py']
    native=['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner]
    private=root/'out/private-grant-http'
    def observe():
        return json.loads(sql(db,"""SELECT json_build_object(
          'commands',(SELECT count(*) FROM control.management_commands WHERE action='job.export.accept'),
          'details',(SELECT count(*) FROM control.export_jobs),
          'audit',(SELECT count(*) FROM control.management_audit WHERE action='job.export.accept'),
          'jobs',(SELECT count(*) FROM control.jobs WHERE export_job_protocol=1 AND state='accepted' AND input_complete),
          'export_pins',(SELECT count(*) FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) WHERE l.state='active' AND p.purpose='export'),
          'reader_pins',(SELECT count(*) FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) WHERE l.state='active' AND p.purpose='reader'),
          'claims',(SELECT count(*) FROM control.retention_claims WHERE state='active'));"""))
    def retained(export_only=False):
        # Compare exact row images and PostgreSQL xmin for every acceptance ledger.
        fields=[]
        for table in ['management_commands','management_audit','jobs','job_input_bounds','export_jobs','snapshot_leases','archive_snapshot_pins','retention_claims']:
            where=" WHERE action='job.export.accept'" if export_only and table in ['management_commands','management_audit'] else ""
            fields.append(f"'{table}',(SELECT jsonb_build_array(count(*),encode(sha256(convert_to(coalesce(string_agg(to_jsonb(t)::text||':'||xmin::text,chr(10) ORDER BY to_jsonb(t)::text),''),'UTF8')),'hex')) FROM control.{table} t{where})")
        return json.loads(sql(db,'SELECT jsonb_build_object('+','.join(fields)+');'))
    try:
        for database in ['bbpub_normal',db]:
            sql('postgres',f'CREATE DATABASE {database};')
            sql(database,f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        sql(db,f'GRANT CONNECT ON DATABASE {db} TO bigbrotr_control;')
        publisher=result['publisher']={}
        execute(native+['/out/native-tests/archive-publication-probe','normal','5432','--capture-cut'],180,publisher);save()
        assert publisher['exit']==0 and not publisher['stderr'] and not publisher['overflow']
        values=[json.loads(line) for line in publisher['stdout'].splitlines() if line.startswith('{')]
        assert [v for v in values if v.get('event')=='captured-cut']==[cut_expectation('normal')]
        assert [v['expected'] for v in values if v.get('event')=='inputs-reopened']==input_expectations('normal')
        readback=result['independent_readback']={}
        execute(native+['/out/native-tests/archive-read-probe','normal'],45,readback);save()
        assert readback['exit']==0 and not readback['stderr'] and not readback['overflow']
        check('normal',[v for v in values if v.get('event') not in ('inputs-reopened','captured-cut')],json.loads(readback['stdout']))
        files={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        assert len(files)==8
        command(*oldclient,'prepare')
        permissions='GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;'
        sql(db,permissions)
        policy=json.loads((source/'tests/fixtures/configuration/receipt-policy-v3.json').read_text())
        policy['settings'].update({'administration.enabled':True,'read.execution.max_memory_bytes':1073741824})
        document=json.dumps(policy,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(b'bigbrotr/product-policy/3\0'+document.encode()).hexdigest()
        sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 2,dataset_id,1,config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1; UPDATE control.datasets SET policy_revision=2; COMMIT;")
        command(*oldclient,'enroll')
        result['unselected']=json.loads(command(*client,'unselected'));assert result['unselected']['status']=='PASS'
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        case('unselected-destination-refuses-job-without-poisoning812-runtime')
        command(*client,'destination')
        serve=['docker','exec',runner,'/out/bigbrotr','serve','--roles','control','--file','/out/private-grant-http/instance.toml']
        refused={};execute(serve,15,refused)
        assert refused['exit']==3 and not refused['stdout'] and 'Prerequisite' in refused['stderr'],refused
        case('selected-destination-requires-explicit813-before-private-listener',observation=refused)
        upgraded={};execute(['docker','exec',runner,'/out/bigbrotr','migrate','apply','--target','813','--user','postgres','--password-ref','file:/out/private-grant-http/postgres','--file','/out/private-grant-http/instance.toml'],30,upgraded)
        assert upgraded['exit']==0 and not upgraded['stderr'] and not upgraded['overflow'],upgraded
        sql(db,permissions)
        sql(db,'REVOKE UPDATE ON control.source_streams FROM bigbrotr_control;')
        assert sql(db,"SELECT has_any_column_privilege('bigbrotr_control','control.source_streams','UPDATE')")=='f'
        refused={};execute(serve,15,refused)
        assert refused['exit']==3 and not refused['stdout'] and 'Prerequisite' in refused['stderr'],refused
        sql(db,'GRANT UPDATE ON control.source_streams TO bigbrotr_control;')
        case('source-row-lock-privilege-is-required-before-private-listener',observation=refused)
        http=result['http']={}
        with (root/'http-client.stdout').open('xb') as out,(root/'http-client.stderr').open('xb') as err:
            process=subprocess.Popen(client+['exercise' if profile=='ordinary' else 'commits' if profile=='custody' else profile],stdout=out,stderr=err)
            try:
                for phase in ([] if profile in ('commits','custody') else ['checker-blocked'] if checker else ['before-accept','accepted','retried','restarted']):
                    end=time.monotonic()+90
                    while not (private/(phase+'.waiting')).exists():
                        assert process.poll() is None,'HTTP client ended before '+phase
                        if time.monotonic()>=end:raise TimeoutError(phase)
                        time.sleep(.02)
                    observed=observe();result[phase]=dict(sql=observed,rows=retained());
                    if not checker:result[phase]['export_rows']=retained(True)
                    save()
                    if checker:
                        def owner():
                            return json.loads(sql(db,"SELECT json_build_object('epoch',epoch,'expires',lease_expires_at_us) FROM control.owner_epochs WHERE role='control' AND state='active'"))
                        first=owner();time.sleep(2.2);second=owner()
                        result['checker_renewal']=dict(before=first,after=second);save()
                        assert first['epoch']==second['epoch'] and second['expires']-first['expires']>1_000_000
                        assert result[phase]['rows']==retained(), 'Blocked checker changed export/pin state'
                    count=0 if phase in ('before-accept','checker-blocked') else 1
                    assert observed==dict(commands=count,details=count,audit=count,jobs=count,export_pins=count,reader_pins=0,claims=count),observed
                    if phase in ('retried','restarted'):assert result['accepted']==result[phase]
                    (private/(phase+'.ready')).write_text('observed\n')
                http['exit']=process.wait(timeout=90)
            finally:
                if process.poll() is None:process.kill();process.wait(timeout=10)
                http.update(stdout=(root/'http-client.stdout').read_text(),stderr=(root/'http-client.stderr').read_text(),overflow=False);save()
        result['cases']+=json.loads((private/'export-job-http-cases.json').read_text());save()
        if not checker:assert http['exit']==0 and not http['stderr'],http
        if profile in ('commits','custody'):
            assert len(result['cases'])==4 and all(c['status']=='PASS' for c in result['cases'])
            if profile=='commits':
                from export_job_commit_cases import run
            else:
                from export_job_custody_cases import run
            result[profile]=run(root,sql,observe,retained)
            assert result[profile]['status']=='PASS'
            result['files']=files
            assert files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
            result['status']='PASS';save()
            print(json.dumps(dict(status='PASS',prerequisites=4,profile=profile,source=resources['source'])))
            return
        if checker:
            result['checker']=json.loads((private/'checker-result.json').read_text())
            result['final_sql']=observe();assert result['final_sql']==result['checker-blocked']['sql']
            assert retained()==result['checker-blocked']['rows']
            result['active_control']=int(sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'"))
            assert result['active_control']==1 # Deliberately retained on fail-stop.
            result['terminal_backends']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for database in [db,'bbpub_normal']}
            assert result['terminal_backends']=={db:0,'bbpub_normal':0}
            result['files']=files
            assert files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
            save()
            assert http['exit']==0 and not http['stderr'],http
            assert len(result['cases'])==5 and all(c['status']=='PASS' for c in result['cases'])
            assert result['checker']['status']=='PASS'
            result['status']='PASS';save()
            print(json.dumps(dict(status='PASS',cases=len(result['cases']),source=resources['source'])))
            return
        assert len(result['cases'])==11 and all(c['status']=='PASS' for c in result['cases'])
        result['final_export_rows']=retained(True);assert result['final_export_rows']==result['accepted']['export_rows']
        result['final_sql']=observe();assert result['final_sql']==result['accepted']['sql']
        result['terminal_backends']={database:json.loads(sql(database,"SELECT json_build_object('backends',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()))")) for database in [db,'bbpub_normal']}
        assert result['terminal_backends']=={db:dict(backends=0),'bbpub_normal':dict(backends=0)}
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        result['actions']=json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_array(action,enabled,generation::text) ORDER BY action) FROM control.scope_grants"))
        assert result['actions']==[['export',False,'2'],['history.read',True,'1'],['private-export',True,'1']]
        result['acceptance']=json.loads((private/'export-accepted.json').read_text())
        actor=(private/'principal').read_text();binding=sha(private/'destination/destination.json')
        expected=hashlib.sha256(b'bigbrotr/export-event/1\0'+bytes.fromhex('01'*16+actor+'d1'*16+'01'*16)
            +struct.pack('>H',4)+bytes.fromhex('04'*16)+struct.pack('>Q',1)+bytes.fromhex(EVENT['id'])
            +struct.pack('>H',100)+bytes.fromhex('5a'*16)+struct.pack('>Q',1)+bytes.fromhex('e1'*16+binding)
            +bytes([1,1])+struct.pack('>QQ',1,2)).hexdigest()
        result['fingerprint']=dict(expected=expected,actual=sql(db,"SELECT encode(request_digest,'hex') FROM control.management_commands WHERE action='job.export.accept'"),actor=actor,binding=binding)
        assert result['fingerprint']['actual']==expected
        receipt=result['acceptance']['receipt']
        row=json.loads(sql(db,"SELECT json_build_object('job_id',encode(e.job_id,'hex'),'accepted_at_us',c.accepted_at_us::text) FROM control.export_jobs e JOIN control.management_commands c USING(actor_id,request_key)"))
        assert row=={key:receipt[key] for key in ['job_id','accepted_at_us']}
        result['files']=files
        assert files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:save()
    print(json.dumps(dict(status=result['status'],cases=len(result['cases']),source=resources['source'])))


if __name__=='__main__':main(Path(sys.argv[1]).resolve(),sys.argv[2] if len(sys.argv)>2 else 'ordinary')
