#!/usr/bin/env python3
"""Host-owned isolated native HTTP qualification; SQL and signed oracle independent."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
sys.dont_write_bytecode = True
from signed_publication_cases import command, execute, sha
from signed_publication_oracle import check, input_expectations
from event_cases import cut_expectation


def main(root, history=False):
    resources = json.loads((root/'resources.json').read_text())
    source = root/'source'
    assert Path(__file__).resolve().parents[2] == source
    assert command('git','-C',str(source),'rev-parse','HEAD') == resources['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner,pg]:
        item=json.loads(command('docker','inspect',identity))[0]
        assert item['Config']['Labels'][resources['owner_label']] == resources['owner']
        assert item['State']['Running'] and not item['State']['OOMKilled']
    ready=json.loads((root/'out/native-tests/ready.json').read_text())
    for key, name in [('control','native-tests/bigbrotr'),('worker','archive-worker/bigbrotr-archive-worker'),('publisher','native-tests/archive-publication-probe'),('reader','native-tests/archive-read-probe')]:
        assert sha(root/'out'/name) == ready[key]
    result=dict(source=resources['source'],tree=resources['tree'],status='INCOMPLETE',cases=[],artifacts=ready)
    path=root/'private-grant-cases.json'
    assert not path.exists()
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def sql(database,query): return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=query)
    native=['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner]
    client=['docker','exec',runner,'python3','-B','/out/build/source/experiments/archive-engine/private_grant_http.py']
    try:
        for database in ['bbpub_normal','bbpub_control_normal']:
            sql('postgres',f'CREATE DATABASE {database};')
            sql(database,f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        sql('bbpub_control_normal','GRANT CONNECT ON DATABASE bbpub_control_normal TO bigbrotr_control;')
        publisher=result['publisher']={}
        execute(native+['/out/native-tests/archive-publication-probe','normal','5432','--capture-cut'],180,publisher);save()
        assert publisher['exit']==0 and not publisher['stderr'] and not publisher['overflow']
        values=[json.loads(line) for line in publisher['stdout'].splitlines() if line.startswith('{')]
        assert [v for v in values if v.get('event')=='captured-cut'] == [cut_expectation('normal')]
        assert [v['expected'] for v in values if v.get('event')=='inputs-reopened'] == input_expectations('normal')
        independent=result['independent_readback']={}
        execute(native+['/out/native-tests/archive-read-probe','normal'],45,independent);save()
        assert independent['exit']==0 and not independent['stderr'] and not independent['overflow']
        check('normal',[v for v in values if v.get('event') not in ('inputs-reopened','captured-cut')],json.loads(independent['stdout']))
        original_files={str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        assert len(original_files)==8
        command(*client,'prepare')
        sql('bbpub_control_normal', 'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        # Fixture-only accepted policy successor. The selected Control process
        # validates both complete documents. This is not a policy-change API test.
        policy=json.loads((source/'experiments/archive-engine/receipt-policy.json').read_text())
        policy['settings']['administration.enabled']=True
        if history: policy['settings']['read.execution.max_memory_bytes']=1073741824
        document=json.dumps(policy,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
        sql('bbpub_control_normal',f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 2,dataset_id,1,config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1; UPDATE control.datasets SET policy_revision=2; COMMIT;")
        command(*client,'enroll')
        # Individual privilege controls, including invoker helper dependencies.
        for index,(table,privilege) in enumerate([
            ('control.archive_frontier_snapshots','SELECT'),('control.archive_pin_recoveries','SELECT'),
            ('control.topology_generations','SELECT'),('control.consumer_frontier_revisions','UPDATE'),
        ]):
            sql('bbpub_control_normal',f'REVOKE {privilege} ON {table} FROM bigbrotr_control;')
            refused={}
            execute(['docker','exec',runner,'/out/bigbrotr','serve','--roles','control','--file','/out/private-grant-http/instance.toml'],15,refused)
            assert refused['exit']==3 and not refused['stdout'] and 'Prerequisite' in refused['stderr'], refused
            sql('bbpub_control_normal',f'GRANT {privilege} ON {table} TO bigbrotr_control;')
            result['cases'].append(dict(case=f'native-reader-startup-privilege-{index}',status='PASS',table=table,privilege=privilege))
            save()
        http=result['http']={}
        if history:
            with (root/'http-client.stdout').open('xb') as out, (root/'http-client.stderr').open('xb') as err:
                process=subprocess.Popen(client+['exercise-history'],stdout=out,stderr=err)
                try:
                    for revision, changes in [
                        (3,{'read.execution.max_memory_bytes':580*1024*1024}),
                        (4,{'read.execution.max_scan_bytes':1}),
                        (5,{'read.response.max_bytes':64}),
                        (6,{}),
                    ]:
                        end=time.monotonic()+90
                        waiting=root/f'out/private-grant-http/policy-{revision}.waiting'
                        while not waiting.exists():
                            assert process.poll() is None, 'HTTP client ended before policy checkpoint'
                            if time.monotonic()>=end: raise TimeoutError('HTTP policy checkpoint')
                            time.sleep(.02)
                        # Independent terminal rows before each policy successor.
                        assert sql('bbpub_control_normal',"SELECT (SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active')+(SELECT count(*) FROM control.snapshot_leases WHERE state='active');")=='0'
                        successor={**policy,'settings':{**policy['settings'],**changes}}
                        document=json.dumps(successor,sort_keys=True,separators=(',',':'))
                        digest=hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
                        sql('bbpub_control_normal',f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT {revision},dataset_id,{revision-1},config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision={revision-1}; UPDATE control.datasets SET policy_revision={revision}; COMMIT;")
                        (root/f'out/private-grant-http/policy-{revision}.ready').write_text('accepted\n')
                    http['exit']=process.wait(timeout=60)
                finally:
                    if process.poll() is None:
                        process.kill();process.wait(timeout=10)
                    http.update(stdout=(root/'http-client.stdout').read_text(),stderr=(root/'http-client.stderr').read_text(),overflow=False)
                    save()
        else: execute(client+['exercise'],120,http);save()
        if (root/'out/private-grant-http/http-cases.json').exists():
            result['cases']+=json.loads((root/'out/private-grant-http/http-cases.json').read_text());save()
        assert http['exit']==0 and not http['stderr'] and not http['overflow'], http
        assert len(result['cases'])==(18 if history else 8) and all(v['status']=='PASS' for v in result['cases'])
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
        result['files_unchanged'] = original_files == {str(p.relative_to(root/'out')):sha(p) for p in sorted((root/'out/signed_normal/kind=7').rglob('*.parquet'))}
        result['files']=original_files
        assert result['files_unchanged']
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__
        raise
    finally: save()
    print(json.dumps(dict(status=result['status'],cases=len(result['cases']),source=resources['source'])))


if __name__=='__main__': main(Path(sys.argv[1]).resolve(), '--history' in sys.argv[2:])
