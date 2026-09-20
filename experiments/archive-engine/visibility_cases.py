#!/usr/bin/env python3
"""Actual PG/native allocation lookup with a separately owned signed corpus.

This fixture owns all source files/cuts for its lifetime. It does not issue a
principal grant or claim the installed HTTP/pin/reauthentication journey passed.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from signed_publication_cases import command, execute, sha
from signed_publication_oracle import corpus, check, input_expectations, expected, cbor, reference, source as source_id
from event_cases import cut_expectation


def main(root):
    resources = json.loads((root/'resources.json').read_text())
    source = root/'source'
    assert Path(__file__).resolve().parents[2] == source
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        item = json.loads(command('docker', 'inspect', identity))[0]
        assert item['Config']['Labels'][resources['owner_label']] == resources['owner']
        assert item['State']['Running'] and not item['State']['OOMKilled']
    out = root/'out/native-tests'
    ready = json.loads((out/'ready.json').read_text())
    assert ready['source'] == resources.get('artifact_source', resources['source']) and ready['source_unchanged']
    for label, path in [('worker', root/'out/archive-worker/bigbrotr-archive-worker'),
                        ('publisher', out/'archive-publication-probe'),
                        ('reader', out/'archive-read-probe'), ('event_reader', out/'archive-event-probe')]:
        assert sha(path) == ready[label]
    evidence = root/'visibility-cases.json'
    assert not evidence.exists()
    result = dict(source=resources['source'], tree=resources['tree'], status='INCOMPLETE',
                  artifacts=ready, harness_sha256=sha(Path(__file__)),
                  oracle_sha256=sha(source/'experiments/archive-engine/signed_publication_oracle.py'), cases=[])
    def save(): evidence.write_text(json.dumps(result, indent=2)+'\n')
    def sql(database, query):
        return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1',
                       '-U','postgres','-d',database,input=query)
    def metadata():
        names = json.loads(sql('bbpub_normal', "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='public';"))
        assert names and all(n.startswith('ducklake_') and n.replace('_','').isalnum() for n in names)
        return {n: json.loads(sql('bbpub_normal',f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM public.{n} t;")) for n in names}
    def files():
        home=root/'out/signed_normal/kind=7'
        return {str(p.relative_to(home)):dict(bytes=p.stat().st_size,sha256=sha(p)) for p in sorted(home.rglob('*.parquet'))}
    def guard():
        return json.loads(sql('bbpub_normal', """SELECT jsonb_build_object(
          'installation',(SELECT jsonb_agg(to_jsonb(t)) FROM bigbrotr_archive.installation t),
          'scientific_schema',(SELECT jsonb_agg(to_jsonb(t)) FROM bigbrotr_archive.scientific_schema t),
          'owner',(SELECT jsonb_agg(to_jsonb(t)) FROM bigbrotr_archive.owner t),
          'attempts',(SELECT jsonb_agg(to_jsonb(t) ORDER BY ordinal) FROM bigbrotr_archive.attempts t),
          'triggers',(SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname,t.tgname,t.tgenabled,pg_get_triggerdef(t.oid)) ORDER BY n.nspname,c.relname,t.tgname)
             FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname IN ('public','bigbrotr_archive')),
          'role_settings',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY setdatabase,setrole),'[]'::jsonb) FROM pg_db_role_setting t),
          'parameter_acl',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY parname),'[]'::jsonb) FROM pg_parameter_acl t));"""))
    binary=['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner]
    inputs=root/'out/visibility-inputs';inputs.mkdir()
    batches,_=corpus();literal=batches[0]['facts'][0]
    expected_scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1')
    def wanted(snapshot):
        return dict(status='observed',snapshot_id=str(snapshot),scope=expected_scope,class_='public',
                    fact_ref=dict(source_record_id=dict(dataset_id='01'*16,shard_id=0,owner_epoch='1',sequence='1'),payload_digest=literal['digest']))
    def run_case(name, request, present, error=None):
        case=dict(name=name,status='INCOMPLETE');result['cases'].append(case);save()
        path=inputs/(name+'.json');path.write_text(json.dumps(request)+'\n')
        temp=inputs/(name+'-temp');temp.mkdir(mode=0o700)
        case['input_sha256']=sha(path)
        try:
            execute(binary+['/out/native-tests/archive-event-probe','/out/visibility-inputs/'+path.name,
                            '/out/visibility-inputs/'+temp.name],35,case)
            save()
            assert case['exit']==0 and not case['stderr'] and not case['overflow']
            observed=json.loads(case['stdout']);assert observed['active_permits']==0
            if present:
                expected=wanted(request['expected']['snapshot_id']);expected['class']=expected.pop('class_')
                assert observed['result']==expected
            else:
                assert observed['result']==dict(status='refused',error=error or 'ObservationUnknown')
            case['status']='PASS';return observed
        finally: save()
    try:
        for database in ['bbpub_normal','bbpub_control_normal']:
            sql('postgres',f'CREATE DATABASE {database};')
            sql(database,f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
        # The Control fixture connects as its non-superuser after initialization.
        # Keep PUBLIC revoked and provision only that required database privilege.
        sql('bbpub_control_normal','GRANT CONNECT ON DATABASE bbpub_control_normal TO bigbrotr_control;')
        assert sql('bbpub_control_normal',"SELECT has_database_privilege('bigbrotr_control','bbpub_control_normal','CONNECT');")=='t'
        publisher=result['publisher']={}
        execute(binary+['/out/native-tests/archive-publication-probe','normal','5432','--capture-cut'],180,publisher);save()
        assert publisher['exit']==0 and not publisher['stderr'] and not publisher['overflow']
        values=[json.loads(line) for line in publisher['stdout'].splitlines()]
        assert [v for v in values if v['event']=='captured-cut']==[cut_expectation('normal')]
        assert [v['expected'] for v in values if v['event']=='inputs-reopened']==input_expectations('normal')
        independent=result['independent_readback']={}
        execute(binary+['/out/native-tests/archive-read-probe','normal'],45,independent);save()
        assert independent['exit']==0 and not independent['stderr'] and not independent['overflow']
        check('normal',[v for v in values if v['event'] not in ('inputs-reopened','captured-cut')],json.loads(independent['stdout']))
        request=dict(expected=dict(dataset='01'*16,scope=expected_scope,topology_generation='0',
                    bounds=[dict(shard_id=0,owner_epoch=1,complete_through=10)],snapshot_id=3),
                    catalog_id='5a'*16,data_root='/out/signed_normal/kind=7/',database='bbpub_normal',
                    max_row_bytes=512,deadline_ms=15000,cancel_file=None)
        before=metadata();original=files();assert len(original)==8
        result.update(metadata_before=before,files_before=original)
        run_case('complete',request,True)
        for prefix in [0,1,5,10]:
            altered=copy.deepcopy(request);altered['expected']['bounds'][0]['complete_through']=prefix
            run_case('prefix-'+str(prefix),altered,prefix>0)
        for variant in ['missing-stream','foreign-epoch','foreign-shard','unrelated-zero','unallocated','domain','scope-kind','scope-dataset','wrong-catalog','unavailable-database','short-reservation']:
            altered=copy.deepcopy(request);read=altered['expected'];error=None
            if variant=='missing-stream':read['bounds'].clear()
            elif variant=='foreign-epoch':read['bounds'][0]['owner_epoch']=2
            elif variant=='foreign-shard':read['bounds'][0]['shard_id']=1
            elif variant=='unrelated-zero':read['bounds'].append(dict(shard_id=1,owner_epoch=1,complete_through=0))
            elif variant=='unallocated':read['scope']['ordinal']='2'
            elif variant=='domain':read['scope']['allocation_domain']='03'*16
            elif variant=='scope-kind':read['scope']['dictionary_kind']=3;error='InvalidInput'
            elif variant=='scope-dataset':read['scope']['dataset_id']='02'*16;error='InvalidInput'
            elif variant=='wrong-catalog':
                altered['catalog_id']='5b'*16;error='Preparation(Custody(Identity))'
            elif variant=='unavailable-database':altered['database']='absent'
            else:altered['max_row_bytes']=511;error='ResourceLimit'
            run_case(variant,altered,variant=='unrelated-zero',error)
        for snapshot in [0,1,2,3,4,2**63]:
            altered=copy.deepcopy(request);altered['expected']['snapshot_id']=snapshot
            run_case('snapshot-'+str(snapshot),altered,snapshot in [2,3], 'InvalidInput' if snapshot==2**63 else None)
        dictionaries=[root/'out/signed_normal/kind=7'/n for n in original if '/dictionaries/' in n]
        assert len(dictionaries)==2
        for fault in ['missing-dictionary','corrupt-dictionary']:
            retained=[]
            try:
                for path in dictionaries:
                    backup=path.with_suffix('.retained');path.rename(backup);retained.append((path,backup))
                    if fault=='corrupt-dictionary':path.write_bytes(b'PAR1invalidPAR1')
                run_case(fault,request,False)
            finally:
                for path,backup in retained:
                    if path.exists():path.unlink()
                    backup.rename(path)
            assert files()==original
            run_case(fault+'-restored',request,True)
        assert metadata()==before and files()==original
        result.update(metadata_after=metadata(),files_after=files())
        # The wrapper restarts this owned postmaster after immediate shutdown.
        before_start=sql('postgres','SELECT pg_postmaster_start_time()::text;')
        command('docker','exec',pg,'/bin/bash','-c','kill -QUIT "$(head -n1 /var/lib/postgresql/data/postmaster.pid)"')
        until=time.monotonic()+10
        attempts=[];after_start=None
        while (remaining:=until-time.monotonic())>0:
            try:
                probe=subprocess.run(['docker','exec',pg,'psql','-XAtq','-U','postgres','-d','postgres','-c','SELECT pg_postmaster_start_time()::text;'],capture_output=True,text=True,timeout=min(2,remaining))
                attempts.append(dict(exit=probe.returncode,start=probe.stdout.strip()))
                if probe.returncode==0 and probe.stdout.strip() and probe.stdout.strip()!=before_start:
                    after_start=probe.stdout.strip();break
            except subprocess.TimeoutExpired:
                attempts.append(dict(timeout=True))
            remaining=until-time.monotonic()
            if remaining>0:time.sleep(min(.05,remaining))
        result['restart']=dict(before=before_start,after=after_start,signal='SIGQUIT',attempts=attempts)
        assert after_start, 'postmaster restart not observed'
        run_case('pg-restarted',request,True)
        assert metadata()==before and files()==original
        # Deliberately bypass publication in this quiescent laboratory. These
        # writes qualify lookup semantics, not admission/frontier/pin authority.
        # Both complete preimages and full readback expectations are independent
        # of the Rust codec/Arrow writer and observed native row renderer.
        repeated=copy.deepcopy(request)
        oracle=expected('normal')
        guard_before=guard()
        assert guard_before['owner']==[dict(singleton=True,ordinal=4,enabled=False)]
        result['guard_before']=guard_before
        mutations=result['quiescent_mutations']=[]
        for mode,sequence,classification,snapshot in [
                ('visibility-repeat',11,'public',4),
                ('visibility-conflict',12,'private',5)]:
            preimage=b'bigbrotr/fact/1\0'+cbor([source_id(sequence),'dictionary',1,reference(4),0,
                        [reference(4),1,'visibility',1,{'class':classification}]])
            digest=hashlib.sha256(preimage).hexdigest()
            path=inputs/(mode+'.json')
            path.write_text(json.dumps(dict(preimage_hex=preimage.hex(),digest=digest))+'\n')
            row=copy.deepcopy(expected('normal')['tables']['dictionaries'][0])
            row['source_record_id']['sequence']=sequence
            row['payload_digest']=digest.upper()
            row['descriptor_value']['class']=classification
            oracle['tables']['dictionaries'].append(row)
            mutation=dict(mode=mode,input_sha256=sha(path),status='INCOMPLETE')
            mutations.append(mutation);save()
            execute(binary+['/out/native-tests/archive-read-probe','normal',mode],45,mutation)
            save()
            assert mutation['exit']==0 and not mutation['stderr'] and not mutation['overflow']
            assert json.loads(mutation['stdout'])==oracle, 'complete independent mutated readback'
            assert sql('bbpub_normal','SELECT max(snapshot_id) FROM ducklake_snapshot;')==str(snapshot)
            mutation['status']='PASS'
            repeated['expected']['snapshot_id']=snapshot
            repeated['expected']['bounds'][0]['complete_through']=sequence
            run_case(mode,repeated,mode=='visibility-repeat')
            repeated['expected']['bounds'][0]['complete_through']=sequence-1
            run_case(mode+'-excluded',repeated,True)
        repeated['expected']['snapshot_id']=4
        repeated['expected']['bounds'][0]['complete_through']=11
        run_case('equal-historical-under-conflicting-head',repeated,True)
        run_case('original-historical-under-conflicting-head',request,True)
        mutated_metadata=metadata();mutated_files=files()
        assert all(mutated_files.get(name)==identity for name,identity in original.items())
        assert len(mutated_files)>len(original)
        result.update(metadata_mutated=mutated_metadata,files_mutated=mutated_files)
        # A fresh native process must read the complete independently expected
        # retained corpus; no current Control frontier is advanced by this probe.
        readback=result['mutated_readback']={}
        execute(binary+['/out/native-tests/archive-read-probe','normal'],45,readback);save()
        assert readback['exit']==0 and not readback['stderr'] and not readback['overflow']
        assert json.loads(readback['stdout'])==oracle
        assert metadata()==mutated_metadata and files()==mutated_files
        result['guard_after']=guard()
        assert result['guard_after']==guard_before
        assert len(result['cases'])==33 and len({c['name'] for c in result['cases']})==33
        assert all(c['status']=='PASS' for c in result['cases'])
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__
        raise
    finally:save()
    print(json.dumps(dict(status=result['status'],cases=len(result['cases']),source=resources['source'])))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('root',type=Path)
    main(parser.parse_args().root.resolve())
