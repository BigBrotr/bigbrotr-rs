#!/usr/bin/env python3
"""Actual PostgreSQL metadata constraints after a retained native seed producer.

No filesystem output or product export execution is claimed by this SQL fixture.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
from signed_publication_cases import command, execute, sha
from signed_publication_oracle import check, input_expectations
from event_cases import cut_expectation


def main(root):
    state=json.loads((root/'resources.json').read_text());source=root/'source'
    assert Path(__file__).resolve().parents[2]==source
    assert command('git','-C',str(source),'rev-parse','HEAD')==state['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    runner,pg=state['container_id'],state['postgres_id']
    for identity in [runner,pg]:
        v=json.loads(command('docker','inspect',identity))[0]
        assert v['Config']['Labels'][state['owner_label']]==state['owner'] and v['State']['Running']
    result=dict(status='INCOMPLETE',source=state['source'],tree=state['tree'],seed_source=state['artifact_origin'],scope='SQL reservation and completion constraints only; real seed archive, no runtime export or artifact output')
    path=root/'schema-cases.json';assert not path.exists()
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def sql(database,text):return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=text)
    native=['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner]
    db='bbpub_control_normal'
    try:
        for database in ['bbpub_normal',db]:
            sql('postgres',f'CREATE DATABASE {database};')
            sql(database,f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        sql(db,f'GRANT CONNECT ON DATABASE {db} TO bigbrotr_control;')
        publisher=result['seed_publisher']={}
        execute(native+['/out/native-tests/archive-publication-probe','normal','5432','--export-jobs'],180,publisher);save()
        assert publisher['exit']==0 and not publisher['stderr'] and not publisher['overflow']
        values=[json.loads(line) for line in publisher['stdout'].splitlines()]
        cases=[v for v in values if 'case' in v];assert len(cases)==5 and all(v['status']=='PASS' for v in cases)
        events=[v for v in values if 'event' in v]
        assert [v for v in events if v['event']=='captured-cut']==[cut_expectation('normal')]
        assert [v['expected'] for v in events if v['event']=='inputs-reopened']==input_expectations('normal')
        read=result['independent_seed_read']={};execute(native+['/out/native-tests/archive-read-probe','normal'],45,read);save()
        assert read['exit']==0 and not read['stderr'] and not read['overflow']
        check('normal',[v for v in events if v['event'] not in ('inputs-reopened','captured-cut')],json.loads(read['stdout']))
        original_files={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        assert len(original_files)==8
        before=sql(db,"SELECT json_agg(to_jsonb(j) ORDER BY job_id) FROM control.jobs j")
        migration=source/'migrations/postgres/0501_artifacts.sql';digest=sha(migration)
        refusal=(source/'experiments/export-execution/migration_refusal.sql').read_text()+migration.read_text()+"\nCOMMIT;"
        refused=subprocess.run(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','postgres','-d',db],input=refusal,text=True,capture_output=True,timeout=30)
        result['migration_refusal']=dict(exit=refused.returncode,stdout=refused.stdout,stderr=refused.stderr);save()
        assert refused.returncode==3 and '55000' in refused.stderr and '0501 refuses advanced or inconsistent export history' in refused.stderr
        assert sql(db,"SELECT to_regclass('control.export_execution_units') IS NULL")=='t'
        assert sql(db,"SELECT json_agg(to_jsonb(j) ORDER BY job_id) FROM control.jobs j")==before
        ddl="BEGIN; SELECT pg_advisory_xact_lock(1111651442,1296648018); "+migration.read_text()+f"\nINSERT INTO control.schema_versions VALUES('postgres-control',501,'0501_artifacts.sql',decode('{digest}','hex'),1,1,1,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,current_user); COMMIT;"
        result['migration_stdout']=sql(db,ddl);result['migration_sha256']=digest;save()
        assert sql(db,"SELECT json_agg(to_jsonb(j) ORDER BY job_id) FROM control.jobs j")==before
        trial=source/'experiments/export-execution/schema_cases.sql'
        result['trial_sha256']=sha(trial);result['trial_stdout']=sql(db,trial.read_text());save()
        assert sorted(line for line in result['trial_stdout'].splitlines() if '|' in line)==[
            'claim|BB099','immediate_constraint_bypass|23514','missing_unit|23514','rewrite_unit|23514',
            'run_before_claim_commit|23514']
        assert sql(db,"SELECT json_agg(to_jsonb(j) ORDER BY job_id) FROM control.jobs j")==before
        completion=source/'experiments/export-execution/completion_cases.sql'
        result['completion_sha256']=sha(completion)
        result['completion_stdout']=sql(db,completion.read_text());save()
        rows=[json.loads(line) for line in result['completion_stdout'].splitlines() if line.startswith('[')]
        assert len(rows)==1 and [r['name'] for r in rows[0]]==sorted(['complete','empty','artifact_missing','file_missing','checkpoint_missing','checkpoint_without_artifact','extra_file','manifest_mismatch','wrong_destination','wrong_file','wrong_total','wrong_empty_digest','old_export_pin'])
        assert all(r['code']==('BB099' if r['name'] in ['complete','empty'] else 'BB001' if r['name']=='old_export_pin' else '23514') for r in rows[0])
        result['terminal_sql']=json.loads(sql(db,"SELECT json_build_object('units',(SELECT count(*) FROM control.export_execution_units),'artifacts',(SELECT count(*) FROM control.artifacts),'files',(SELECT count(*) FROM control.artifact_files),'jobs',(SELECT count(*) FROM control.jobs WHERE export_job_protocol=1 AND state='queued' AND checkpoint_revision=0),'active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'),'backends',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()))"))
        assert result['terminal_sql']==dict(units=1,artifacts=0,files=0,jobs=1,active_control=0,backends=0)
        assert original_files=={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        result['unchanged_archive_files']=original_files
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:save()
    print(json.dumps(dict(status=result['status'],source=state['source'],cases=19)))


if __name__=='__main__':main(Path(sys.argv[1]).resolve())
