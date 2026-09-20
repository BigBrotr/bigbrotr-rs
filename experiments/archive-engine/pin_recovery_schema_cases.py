#!/usr/bin/env python3
"""Actual installed schema18 constraints; synthetic cuts do not prove native truth."""
import argparse
import json
from pathlib import Path
import re
import sys
sys.dont_write_bytecode = True
from signed_publication_cases import command, execute, sha
from lab import change


def main(root):
    source=Path(__file__).resolve().parents[2]
    state=json.loads((root/'resources.json').read_text())
    assert source in (root/'source',root/'harness-source')
    revision=command('git','-C',str(source),'rev-parse','HEAD')
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    changed=command('git','-C',str(source),'diff','--name-only',state['source'],revision).splitlines()
    assert all(p.startswith('docs/') or p in ('tests/fixtures/storage/archive-pin-recovery-constraints.sql',
        'experiments/archive-engine/pin_recovery_schema_cases.py') for p in changed)
    for key in ['container_id','postgres_id']:
        c=json.loads(command('docker','inspect',state[key]))[0]
        assert c['State']['Running'] and not c['State']['OOMKilled']
        assert c['Config']['Labels']['org.bigbrotr.publication']==state['owner']
    ready=json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source']==state['source'] and sha(root/'out/native-tests/bigbrotr')==ready['control']
    directory=root/'out/pin-recovery-schema';directory.mkdir(mode=0o700)
    report_path=root/'pin-recovery-schema.json';assert not report_path.exists()
    report=dict(status='INCOMPLETE',source=state['source'],harness=revision,changed_paths=changed,
        artifact=ready['control'],commands=[],cases=[],fixtures={n:sha(source/'tests/fixtures/storage'/n)
        for n in ['archive-frontier-constraints.sql','archive-pin-recovery-constraints.sql']})
    def save():report_path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db,text):
        return command('docker','exec','-i',state['postgres_id'],'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def migrate(label,target):
        record=dict(label=label,target=target);report['commands'].append(record);save()
        try:
            execute(['docker','exec',state['container_id'],'/out/native-tests/bigbrotr','migrate','apply',
                '--file','/out/pin-recovery-schema/'+label+'/instance.toml','--target',str(target),
                '--user','postgres','--password-ref','file:/out/pin-recovery-schema/'+label+'/postgres'],75,record)
        finally:save()
        assert record['exit']==0 and not record['stderr'] and not record['overflow']
        assert json.loads(record['stdout'])['current_version']==str(target)
        entries=json.loads((source/'migrations/manifest.json').read_text())['application_order']
        entries=entries[:next(i+1 for i,e in enumerate(entries) if e['version']==target)]
        record['ledger']=json.loads(sql('bbpin_recovery_'+label,"SELECT json_agg(json_build_array(migration,name,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions"))
        assert record['ledger']==sorted([[e['version'],e['name'],sha(source/'migrations/postgres'/e['name'])] for e in entries])
    def fresh(label,target):
        home=directory/label;home.mkdir(mode=0o700)
        secret=home/'postgres';secret.write_bytes((root/'secrets/postgres').read_bytes());secret.chmod(0o600)
        config=(source/'config/recipes/archive-only.toml').read_text();db='bbpin_recovery_'+label
        for key,value in [('endpoint','127.0.0.1:5432'),('database',db)]:config=change(config,'postgres.control',key,value)
        (home/'instance.toml').write_text(config);sql('postgres','CREATE DATABASE '+db);migrate(label,target)
        return db
    setup=(source/'tests/fixtures/storage/archive-frontier-constraints.sql').read_text()
    save()
    try:
        db=fresh('constraints',18)
        case=dict(name='empty-cut-recovery-constraints',status='INCOMPLETE');report['cases'].append(case);save()
        case['output']=sql(db,setup+(source/'tests/fixtures/storage/archive-pin-recovery-constraints.sql').read_text());save()
        objects=[json.JSONDecoder().raw_decode(case['output'][m.start():])[0] for m in re.finditer(r'(?m)^\{"status"\s*:',case['output'])]
        assert len(objects)==2 and objects[-1]['status']=='PASS'
        expected={'recovery_same_holder','recovery_incomplete','recovery_wrong_binding','recovery_wrong_revision',
            'recovery_missing_lease','recovery_rollback','recovery_empty_cut_after_scheduling_expiry',
            'recovery_update','recovery_delete','recovery_truncate','recovery_duplicate_release'}
        assert {c['name'] for c in objects[-1]['cases'] if c['name'].startswith('recovery_')}==expected
        case.update(status='PASS',observed=objects[-1]);save()
        db=fresh('legacy',16)
        sql(db,setup+"""INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,
          topology_generation,holder_kind,holder_role_instance_id,holder_role,holder_incarnation,
          acquired_at_us,updated_at_us,expires_at_us)
          VALUES(pg_temp.id(401),pg_temp.id(1),pg_temp.id(9),1,0,'role_instance',pg_temp.id(3),
          'control',pg_temp.id(6),pg_temp.now_us(),pg_temp.now_us(),pg_temp.now_us()+30000000);""")
        def rows():
            return json.loads(sql(db,"SELECT jsonb_agg(to_jsonb(t)-'archive_pin_protocol') FROM (SELECT *,xmin::text AS xmin FROM control.snapshot_leases)t"))
        before=rows();assert len(before)==1 and before[0]['state']=='active'
        migrate('legacy',18);assert rows()==before
        assert sql(db,'SELECT archive_pin_protocol IS NULL FROM control.snapshot_leases')=='t'
        case=dict(name='legacy16-not-adopted-by18',status='INCOMPLETE',before=before);report['cases'].append(case);save()
        case['output']=sql(db,"""DO $$ DECLARE observed text; detail text; BEGIN
          BEGIN
            INSERT INTO control.archive_pin_recoveries(lease_id,dataset_id,consumer_id,expected_lease_revision,
              coordinator_binding_sha256,exclusion_protocol,recoverer_resource,recoverer_epoch,recoverer_incarnation,recovered_at_us)
              SELECT lease_id,dataset_id,consumer_id,1,decode(repeat('00',32),'hex'),1,
                decode(lpad('5',32,'0'),'hex'),1,holder_incarnation,0 FROM control.snapshot_leases;
          EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS observed=RETURNED_SQLSTATE,detail=MESSAGE_TEXT; END;
          IF observed IS DISTINCT FROM '23514' OR detail IS DISTINCT FROM 'pin recovery requires its exact active qualified lease' THEN
            RAISE EXCEPTION 'wrong legacy refusal: %, %',observed,detail;
          END IF;
        END $$; SELECT count(*) FROM control.archive_pin_recoveries;""")
        assert case['output']=='0' and rows()==before;case['status']='PASS'
        report['status']='PASS'
    finally:save()
    print(json.dumps(dict(status=report['status'],source=state['source'],cases=len(report['cases']))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('root',type=Path)
    main(parser.parse_args().root.resolve())
