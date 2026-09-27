#!/usr/bin/env python3
"""Original signed archive/job -> installed headless Control -> filesystem/SQL."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
from signed_publication_cases import command, execute, sha
from signed_publication_oracle import check, input_expectations
from event_cases import cut_expectation


def main(root, profile=None):
    state = json.loads((root/'resources.json').read_text()); source = root/'source'
    assert Path(__file__).resolve().parents[2] == source
    assert command('git','-C',str(source),'rev-parse','HEAD') == state['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    runner, pg = state['container_id'], state['postgres_id']
    for ident in [runner, pg]:
        c = json.loads(command('docker','inspect',ident))[0]
        assert c['Config']['Labels'][state['owner_label']] == state['owner'] and c['State']['Running']
    result = dict(status='INCOMPLETE',source=state['source'],cases={})
    path = root/'runtime-cases.json'; assert not path.exists()
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def sql(db,text): return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    native = ['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner]
    def run(name,args,seconds=60):
        r = result['cases'][name] = {}; execute(native+args,seconds,r); save()
        assert r['exit'] == 0 and not r['stderr'] and not r['overflow'],name
        return [json.loads(line) for line in r['stdout'].splitlines()]
    db = 'bbpub_control_normal'; private = root/'out/private-grant-http'
    try:
        for database in ['bbpub_normal',db]:
            sql('postgres',f'CREATE DATABASE {database};')
            sql(database,f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        sql(db,f'GRANT CONNECT ON DATABASE {db} TO bigbrotr_control;')
        seed = run('seed',['/out/native-tests/archive-publication-probe','normal','5432','--export-jobs'],180)
        assert len([v for v in seed if v.get('status')=='PASS']) == 5
        events = [v for v in seed if 'event' in v]
        assert [v for v in events if v['event']=='captured-cut'] == [cut_expectation('normal')]
        assert [v['expected'] for v in events if v['event']=='inputs-reopened'] == input_expectations('normal')
        readback = run('independent-read',['/out/native-tests/archive-read-probe','normal'])
        assert len(readback)==1
        check('normal',[v for v in events if v['event'] not in ('inputs-reopened','captured-cut')],readback[0])
        files = {str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        assert len(files)==8
        client = ['docker','exec',runner,'python3','-B','/out/build/source/experiments/export-execution/runtime_client.py']
        admission = profile in ('job-admission','job-pages','job-pages-prepare','job-pages-commits')
        def seed_rows():
            return {table:json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{table} t"))
                    for table in ['jobs','export_jobs','job_input_bounds','management_commands','management_audit','snapshot_leases','archive_snapshot_pins','retention_claims']}
        if admission: result['pre815_seed']=seed_rows();save()
        command(*client,'prepare',*(['815'] if admission else []))
        if admission:
            result['post815_seed']=seed_rows()
            assert result['post815_seed']==result['pre815_seed']
            result['backfilled_admission']=json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_object('job',encode(j.job_id,'hex'),'ordinal',a.admission_ordinal::text,'protocol',j.export_job_protocol)) FROM control.job_admissions a JOIN control.jobs j USING(job_id)"))
            seed_job=result['pre815_seed']['jobs'][0]['row']['job_id'][2:]
            assert result['backfilled_admission']==[dict(job=seed_job,ordinal='1',protocol=1)]
            result['migration815_checksum']=sql(db,"SELECT encode(checksum,'hex') FROM control.schema_versions WHERE migration=815")
            assert result['migration815_checksum']==sha(source/'migrations/postgres/0815_job_admission.sql');save()
        sql(db,'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        policy = json.loads((source/'experiments/archive-engine/receipt-policy.json').read_text())
        policy['settings'].update({'administration.enabled':False,'archive.worker_memory_bytes':671088640,'export.memory.max_bytes':1073741824,'export.artifact.max_bytes':2097152,'export.spill.max_bytes':134217728})
        document = json.dumps(policy,sort_keys=True,separators=(',',':'))
        digest = hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
        sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 2,dataset_id,1,config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1; UPDATE control.datasets SET policy_revision=2; COMMIT;")
        def observe():
            return json.loads(sql(db,"SELECT jsonb_build_object('states',(SELECT jsonb_agg(state ORDER BY job_id) FROM control.jobs),'units',(SELECT count(*) FROM control.export_execution_units),'artifacts',(SELECT count(*) FROM control.artifacts),'files',(SELECT count(*) FROM control.artifact_files),'readers_active',(SELECT count(*) FROM control.snapshot_leases l JOIN control.archive_snapshot_pins p USING(lease_id) WHERE p.purpose='reader' AND l.state='active'))"))
        result['before'] = observe(); save()
        assert result['before']['states'] == ['accepted'] and result['before']['artifacts'] == 0
        # Observe through the real container and installed config decoder, after
        # setup has consumed disk. Keep the default reserve; do not tune it down
        # to turn a resource-deferred job into a passing execution fixture.
        growth = 2*policy['settings']['export.artifact.max_bytes'] + 65*1024**2
        capacity = result['execution_capacity'] = json.loads(command(*client, 'capacity', str(growth)))
        save()
        reserve = int(capacity['reserve']['value'])
        assert reserve == 64*1024**3 and capacity['growth_bytes'] == growth
        assert all(v['free_bytes'] >= reserve+growth for v in capacity['filesystems']), 'Insufficient actual export completion headroom'
        with (root/'runtime-client.stdout').open('xb') as out,(root/'runtime-client.stderr').open('xb') as err:
            process = subprocess.Popen(client+['run'],stdout=out,stderr=err)
            try:
                deadline=time.monotonic()+15
                while not (private/'executor-ready.json').exists():
                    assert process.poll() is None, 'runtime client exited before readiness'
                    if time.monotonic()>=deadline: raise TimeoutError('installed readiness')
                    time.sleep(.05)
                result['ready']=json.loads((private/'executor-ready.json').read_text()); save()
                deadline=time.monotonic()+10
                while time.monotonic()<deadline:
                    observed=observe()
                    if observed['states']==['succeeded'] and observed['readers_active']==result['before']['readers_active']: break
                    assert process.poll() is None, 'runtime ended while processing'
                    time.sleep(.1)
                result['after']=observe();save()
                (private/'executor-stop').write_text('host observation ended\n')
                result['runtime_exit']=process.wait(timeout=15);save()
            finally:
                if process.poll() is None: process.kill();process.wait(timeout=10)
        assert result['runtime_exit']==0 and not (root/'runtime-client.stderr').read_text()
        result['terminal']={database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        result['active_control']=int(sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'"))
        result['unchanged_archive_files']=files
        assert files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        save()
        assert result['terminal']=={db:0,'bbpub_normal':0} and result['active_control']==0
        assert result['after']==dict(states=['succeeded'],units=1,artifacts=1,files=1,readers_active=result['before']['readers_active']), 'Installed runtime did not execute the retained job'
        row=json.loads(sql(db,"SELECT jsonb_build_object('job',encode(j.job_id,'hex'),'artifact',encode(a.artifact_id,'hex'),'checkpoint',j.checkpoint_revision::text,'manifest',encode(a.manifest_sha256,'hex'),'manifest_bytes',a.manifest_bytes::text,'bytes',f.byte_count::text,'rows',f.row_count::text,'sha256',encode(f.sha256,'hex'),'reader_released',l.state='released','export_retained',(SELECT state='active' FROM control.snapshot_leases WHERE lease_id=e.pin_lease_id)) FROM control.jobs j JOIN control.export_jobs e USING(job_id) JOIN control.artifacts a USING(job_id) JOIN control.artifact_files f ON f.artifact_id=a.artifact_id JOIN control.snapshot_leases l ON l.lease_id=a.verification_pin_id"))
        artifact=root/'out/signed_normal/export-destination'/row['artifact']
        raw=(artifact/'manifest.json').read_bytes(); manifest=json.loads(raw)
        assert (artifact/'events.jsonl').read_bytes()==b'' and row['rows']==row['bytes']=='0'
        assert row['checkpoint']=='1' and row['reader_released'] and row['export_retained']
        assert row['sha256']==hashlib.sha256(b'').hexdigest() and row['manifest']==hashlib.sha256(raw).hexdigest()
        assert str(len(raw))==row['manifest_bytes']
        expected_manifest=dict(format='bigbrotr-signed-events-export/1',artifact_id=row['artifact'],job_id=row['job'],
            visibility='private',fidelity='full-signed-events',origin='retained-scoped-archive',
            source=dict(cut=dict(dataset_id='01'*16,topology_generation='0',bounds=[dict(stream=dict(shard_id=0,owner_epoch='1'),last_complete_sequence='10')]),catalog_id='5a'*16,frontier_revision='1',snapshot_id='3'),
            selection=dict(event_id='ab'*32,scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1')),
            accepted_revisions=dict(configuration='1',policy='1'),included_families=['signed-events'],
            excluded_families=['internal-payload-envelopes','eligibility-facts','first-local-receive','relay-provenance','imported-claims','measurements','documents','dictionaries','coverage','derived-results','deployment-state'],
            files=[dict(file_id='events',path='events.jsonl',format='nostr-jsonl/1',bytes='0',rows='0',sha256=hashlib.sha256(b'').hexdigest())])
        assert manifest == expected_manifest
        result['output']=row
        result['output_hashes']={str((artifact/name).relative_to(root/'out')):sha(artifact/name) for name in ['events.jsonl','manifest.json']}
        save()
        if profile == 'shutdown':
            from shutdown_cases import run as shutdown
            result['shutdown'] = shutdown(root, state, sql)
        elif profile in ('job-pages','job-pages-prepare','job-pages-commits'):
            from job_pages_cases import run as pages
            result['job_pages'] = pages(root, state, sql, policy, prepare_consumer=profile!='job-pages')
            if profile == 'job-pages-commits':
                from job_pages_commit_cases import run as commit_pages
                result['job_pages_commits'] = commit_pages(root, state, sql)
                save()
                assert result['job_pages_commits']['status'] == 'PASS'
        elif profile in ('job-status','job-admission'):
            from status_cases import run as status
            result['job_status'] = status(root, state, sql, policy, admission=admission)
        elif profile in ('revocation', 'resume-output'):
            from revocation_cases import run as revoke
            result['revocation'] = revoke(root, state, sql, policy, expected_manifest)
            result['output_hashes'].update(result['revocation']['output_hashes'])
            if profile == 'resume-output':
                from resume_output_cases import run as resume_output
                result['resume_output'] = resume_output(root, state, sql, expected_manifest, result['revocation'])
                result['output_hashes'].update(result['resume_output']['output_hashes'])
        elif profile in ('parking', 'busy', 'pause-commits'):
            from parking_cases import run as park
            result['parking'] = park(root, state, sql, policy, busy=profile=='busy', pause_commits=profile=='pause-commits')
        else:
            from recovery_cases import run as recover
            result['recovery'] = recover(root, state, sql, policy, expected_manifest)
            result['output_hashes'].update(result['recovery']['output_hashes'])
            save()
            from commit_cases import run as commit_faults
            result['completion_commits'] = commit_faults(root, state, sql, policy, expected_manifest)
            result['output_hashes'].update(result['completion_commits']['output_hashes'])
        assert files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally: save()


if __name__=='__main__':
    assert len(sys.argv)==2 or sys.argv[2:] in (['job-pages-commits'], ['job-pages-prepare'], ['job-pages'], ['resume-output'], ['shutdown'], ['parking'], ['busy'], ['pause-commits'], ['revocation'], ['job-status'], ['job-admission'])
    main(Path(sys.argv[1]).resolve(), sys.argv[2] if len(sys.argv)==3 else None)
