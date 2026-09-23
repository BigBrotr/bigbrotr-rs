#!/usr/bin/env python3
"""Actual typed scope-grant acceptance after independently checked signed archive.

Native fixture owns its source files/cut, actual Control and local custody. This
is not a browser grant route, private history delivery or completed drain test.
"""
import json
from pathlib import Path
import subprocess
import sys
sys.dont_write_bytecode = True
from signed_publication_cases import command,execute,sha
from signed_publication_oracle import check,input_expectations
from event_cases import cut_expectation


def main(root):
    resources=json.loads((root/'resources.json').read_text())
    source=root/'source'
    assert Path(__file__).resolve().parents[2]==source
    assert command('git','-C',str(source),'rev-parse','HEAD')==resources['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    runner,pg=resources['container_id'],resources['postgres_id']
    for identity in [runner,pg]:
        item=json.loads(command('docker','inspect',identity))[0]
        assert item['Config']['Labels'][resources['owner_label']]==resources['owner']
        assert item['State']['Running'] and not item['State']['OOMKilled']
    ready=json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source']==resources['source'] and ready['source_unchanged']
    for label,path in [('worker',root/'out/archive-worker/bigbrotr-archive-worker'),
                       ('publisher',root/'out/native-tests/archive-publication-probe'),
                       ('reader',root/'out/native-tests/archive-read-probe')]:
        assert sha(path)==ready[label]
    result=dict(source=resources['source'],tree=resources['tree'],status='INCOMPLETE',
                artifacts=ready,harness_sha256=sha(Path(__file__)),
                oracle_sha256=sha(source/'experiments/archive-engine/signed_publication_oracle.py'),cases=[])
    path=root/'scope-grant-cases.json';assert not path.exists()
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def sql(database,query):return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=query)
    binary=['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner]
    try:
        for database in ['bbpub_normal','bbpub_control_normal']:
            sql('postgres',f'CREATE DATABASE {database};')
            sql(database,f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        sql('bbpub_control_normal','GRANT CONNECT ON DATABASE bbpub_control_normal TO bigbrotr_control;')
        publisher=result['publisher']={}
        execute(binary+['/out/native-tests/archive-publication-probe','normal','5432','--scope-grants'],180,publisher);save()
        values=[json.loads(line) for line in publisher['stdout'].splitlines()]
        result['cases']=[v for v in values if 'case' in v];save()
        assert publisher['exit']==0 and not publisher['stderr'] and not publisher['overflow']
        assert len(result['cases'])==5 and len({v['case'] for v in result['cases']})==5
        assert all(v['status']=='PASS' for v in result['cases'])
        values=[v for v in values if 'event' in v]
        assert [v for v in values if v['event']=='captured-cut']==[cut_expectation('normal')]
        assert [v['expected'] for v in values if v['event']=='inputs-reopened']==input_expectations('normal')
        independent=result['independent_readback']={}
        execute(binary+['/out/native-tests/archive-read-probe','normal'],45,independent);save()
        assert independent['exit']==0 and not independent['stderr'] and not independent['overflow']
        check('normal',[v for v in values if v['event'] not in ('inputs-reopened','captured-cut')],json.loads(independent['stdout']))
        query="""SELECT json_build_object(
            'commands',(SELECT count(*) FROM control.management_commands WHERE action='scope-grant.change'),
            'details',(SELECT count(*) FROM control.scope_grant_changes),
            'audit',(SELECT count(*) FROM control.management_audit WHERE action='scope-grant.change'),
            'proofs',(SELECT count(*) FROM control.reauthentication_proofs),
            'disabled_generation2',(SELECT count(*) FROM control.scope_grants WHERE NOT enabled AND generation=2),
            'account_revision',(SELECT grant_revision::text FROM control.local_accounts WHERE username='alice'),
            'active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'),
            'active_pins',(SELECT count(*) FROM control.snapshot_leases WHERE state='active'),
            'backends',(SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()));"""
        result['independent_sql']=json.loads(sql('bbpub_control_normal',query));save()
        assert result['independent_sql']==dict(commands=2,details=2,audit=2,proofs=0,disabled_generation2=1,account_revision='3',active_control=0,active_pins=0,backends=0)
        result['files']={str(p.relative_to(root/'out')):dict(bytes=p.stat().st_size,sha256=sha(p)) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        assert len(result['files'])==8
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__
        raise
    finally:save()
    print(json.dumps(dict(status=result['status'],cases=len(result['cases']),source=resources['source'])))


if __name__=='__main__':main(Path(sys.argv[1]).resolve())
