#!/usr/bin/env python3
"""Current Rust adapter and original signed archive, checked independently in SQL/bytes."""
import hashlib
import json
from pathlib import Path
import sys
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'archive-engine'))
from signed_publication_cases import command, execute, sha
from signed_publication_oracle import check, input_expectations
from event_cases import cut_expectation

def main(root):
    state=json.loads((root/'resources.json').read_text());source=root/'source'
    assert Path(__file__).resolve().parents[2]==source
    assert command('git','-C',str(source),'rev-parse','HEAD')==state['source']
    runner,pg=state['container_id'],state['postgres_id']
    for ident in [runner,pg]:
        c=json.loads(command('docker','inspect',ident))[0]
        assert c['Config']['Labels'][state['owner_label']]==state['owner'] and c['State']['Running']
    result=dict(status='INCOMPLETE',source=state['source'],tree=state['tree'],seed_source=state['artifact_origin'],cases={})
    path=root/'adapter-cases.json';assert not path.exists()
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def sql(db,text):return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    native=['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner]
    def run(name,cmd,seconds=60):
        out=result['cases'][name]={};execute(native+cmd,seconds,out);save()
        assert out['exit']==0 and not out['stderr'] and not out['overflow'],name
        return [json.loads(line) for line in out['stdout'].splitlines()]
    db='bbpub_control_normal'
    try:
        for database in ['bbpub_normal',db]:
            sql('postgres',f'CREATE DATABASE {database};')
            sql(database,f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        sql(db,f'GRANT CONNECT ON DATABASE {db} TO bigbrotr_control;')
        seed=run('seed',['/out/native-tests/archive-publication-probe','normal','5432','--export-jobs'],180)
        assert len([v for v in seed if v.get('status')=='PASS'])==5
        events=[v for v in seed if 'event' in v]
        assert [v for v in events if v['event']=='captured-cut']==[cut_expectation('normal')]
        assert [v['expected'] for v in events if v['event']=='inputs-reopened']==input_expectations('normal')
        native_read=run('seed-independent-read',['/out/native-tests/archive-read-probe','normal'])
        assert len(native_read)==1
        check('normal',[v for v in events if v['event'] not in ('inputs-reopened','captured-cut')],native_read[0])
        before_files={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))};assert len(before_files)==8
        before=sql(db,'SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM control.jobs j')
        probe='/out/adapter-artifacts/export-execution-probe'
        assert run('migrate',[probe,'migrate'])==[dict(case='current-rust-migrator-applies501-to-accepted813-job',status='PASS')]
        assert sql(db,'SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM control.jobs j')==before
        assert sql(db,"SELECT encode(checksum,'hex') FROM control.schema_versions WHERE migration=501")==sha(source/'migrations/postgres/0501_artifacts.sql')
        sql(db,"GRANT SELECT,INSERT ON control.export_execution_units,control.artifacts,control.artifact_files TO bigbrotr_control; GRANT UPDATE(artifact_id) ON control.artifacts TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;")
        assert run('missing-row-lock',[probe,'denied'])==[dict(case='missing-execution-row-lock-privilege-refused',status='PASS')]
        sql(db,'GRANT UPDATE(principal_id) ON control.principal_grants TO bigbrotr_control;')
        fault=state.get('adapter_profile')=='rollback'
        outcomes=run('execute',[probe,'rollback' if fault else 'run'])
        if fault:
            assert outcomes==[dict(case=name,status='PASS') for name in ['separate-reservation-commit-and-identical-retry-retain-original-caps','catalog-loss-inside-completion-rolls-back-and-preserves-control-session','catalog-loss-rollback-reader-release-and-control-driver-join']]
            observation=json.loads((root/'out/rollback-observation.json').read_text())
            assert observation['status']=='PASS' and observation['insert_wait_observed'] and observation['catalog_driver_joined'] and observation['catalog_backend_absent']
            assert observation['completion']=='ArchiveUnavailable' and not observation['commit_started']
            assert observation['before']==observation['after'] and observation['same_session_resolution']=='Ok(Some(Running))'
            assert observation['before']['job']['state']=='running' and observation['before']['artifacts']==observation['before']['files']==0
            reader_pin=observation['reader_pin'];assert len(reader_pin)==32 and all(c in '0123456789abcdef' for c in reader_pin)
            row=json.loads(sql(db,f"SELECT jsonb_build_object('states',(SELECT jsonb_agg(state ORDER BY state) FROM control.jobs),'checkpoint_sum',(SELECT sum(checkpoint_revision) FROM control.jobs),'artifacts',(SELECT count(*) FROM control.artifacts),'files',(SELECT count(*) FROM control.artifact_files),'units',(SELECT count(*) FROM control.export_execution_units),'fresh_reader_released',(SELECT count(*) FROM control.archive_snapshot_pins p JOIN control.snapshot_leases l USING(lease_id) WHERE p.purpose='reader' AND l.state='released' AND l.lease_id=decode('{reader_pin}','hex')),'exports_active',(SELECT count(*) FROM control.archive_snapshot_pins p JOIN control.snapshot_leases l USING(lease_id) WHERE p.purpose='export' AND l.state='active'),'active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'),'backends',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()))"))
            assert row==dict(states=['queued','running'],checkpoint_sum=0,artifacts=0,files=0,units=2,fresh_reader_released=1,exports_active=2,active_control=0,backends=0)
            artifact=root/'out/signed_normal/export-destination'/observation['artifact']
            assert (artifact/'events.jsonl').read_bytes()==b''
            manifest=json.loads((artifact/'manifest.json').read_text())
            assert manifest['artifact_id']==observation['artifact'] and manifest['job_id']==observation['job']
            assert manifest['selection']['event_id']=='ab'*32
            assert manifest['files']==[dict(file_id='events',path='events.jsonl',format='nostr-jsonl/1',bytes='0',rows='0',sha256=hashlib.sha256(b'').hexdigest())]
            assert before_files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
            result.update(status='PASS',terminal_sql=row,rollback=observation,output_hashes={str((artifact/name).relative_to(root/'out')):sha(artifact/name) for name in ['events.jsonl','manifest.json']},unchanged_archive_files=before_files)
            return
        assert outcomes==[dict(case=name,status='PASS') for name in ['separate-reservation-commit-and-identical-retry-retain-original-caps','native-empty-and-signed-event-output-complete-after-browser-logout','known-completion-reader-release-and-actual-driver-joins']]
        rows=json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_object('job',encode(j.job_id,'hex'),'artifact',encode(a.artifact_id,'hex'),'state',j.state,'checkpoint',j.checkpoint_revision::text,'manifest',encode(a.manifest_sha256,'hex'),'manifest_bytes',a.manifest_bytes::text,'bytes',f.byte_count::text,'rows',f.row_count::text,'sha256',encode(f.sha256,'hex'),'reader_released',l.state='released','export_retained',(SELECT state='active' FROM control.snapshot_leases WHERE lease_id=e.pin_lease_id)) ORDER BY f.row_count) FROM control.jobs j JOIN control.export_jobs e USING(job_id) JOIN control.artifacts a USING(job_id) JOIN control.artifact_files f ON f.artifact_id=a.artifact_id JOIN control.snapshot_leases l ON l.lease_id=a.verification_pin_id"))
        assert len(rows)==2 and [r['rows'] for r in rows]==['0','1']
        expected_event=json.loads((source/'experiments/identity-contract/signed-fixture.json').read_text())['events']['E_public']
        expected_bytes=(json.dumps({k:expected_event[k] for k in ['id','pubkey','created_at','kind','tags','content','sig']},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        output_hashes={}
        for row in rows:
            assert row['state']=='succeeded' and row['checkpoint']=='1' and row['reader_released'] and row['export_retained']
            artifact=root/'out/signed_normal/export-destination'/row['artifact']
            data=(artifact/'events.jsonl').read_bytes();raw=(artifact/'manifest.json').read_bytes();manifest=json.loads(raw)
            assert data==(expected_bytes if row['rows']=='1' else b'')
            assert sha(artifact/'events.jsonl')==row['sha256'] and str(len(data))==row['bytes']
            assert hashlib.sha256(raw).hexdigest()==row['manifest'] and str(len(raw))==row['manifest_bytes']
            assert manifest==dict(format='bigbrotr-signed-events-export/1',artifact_id=row['artifact'],job_id=row['job'],
                visibility='private',fidelity='full-signed-events',origin='retained-scoped-archive',
                source=dict(cut=dict(dataset_id='01'*16,topology_generation='0',bounds=[dict(stream=dict(shard_id=0,owner_epoch='1'),last_complete_sequence='10')]),
                    catalog_id='5a'*16,frontier_revision='1',snapshot_id='3'),
                selection=dict(event_id=expected_event['id'] if row['rows']=='1' else 'ab'*32,
                    scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1')),
                accepted_revisions=dict(configuration='1',policy='1'),included_families=['signed-events'],
                excluded_families=['internal-payload-envelopes','eligibility-facts','first-local-receive','relay-provenance','imported-claims','measurements','documents','dictionaries','coverage','derived-results','deployment-state'],
                files=[dict(file_id='events',path='events.jsonl',format='nostr-jsonl/1',bytes=str(len(data)),rows=row['rows'],sha256=hashlib.sha256(data).hexdigest())])
            for name in ['events.jsonl','manifest.json']:output_hashes[str((artifact/name).relative_to(root/'out'))]=sha(artifact/name)
        result['outputs']=rows;result['output_hashes']=output_hashes;save()
        sql(db,"DELETE FROM control.principal_grants WHERE action='data.read';")
        assert run('resolve',[probe,'resolve'])==[dict(case='fresh-control-resolves-completion-without-input-grants-or-files',status='PASS')]
        assert all(sha(root/'out'/name)==digest for name,digest in output_hashes.items())
        assert before_files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        result['unchanged_archive_files']=before_files
        result['terminal_sql']=json.loads(sql(db,"SELECT jsonb_build_object('jobs',(SELECT count(*) FROM control.jobs WHERE state='succeeded' AND checkpoint_revision=1),'artifacts',(SELECT count(*) FROM control.artifacts),'files',(SELECT count(*) FROM control.artifact_files),'active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'),'backends',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()))"))
        assert result['terminal_sql']==dict(jobs=2,artifacts=2,files=2,active_control=0,backends=0)
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:save()
    print(json.dumps(dict(status=result['status'],source=state['source'])))

if __name__=='__main__':main(Path(sys.argv[1]).resolve())
