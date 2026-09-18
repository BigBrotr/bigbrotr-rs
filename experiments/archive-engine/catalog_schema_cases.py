#!/usr/bin/env python3
"""Fresh schema initialization against an owned real PG16/Tux fixture."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

root=Path(sys.argv[1]).resolve()
state=json.loads((root/'resources.json').read_text())
source=Path(__file__).resolve().parents[2]
assert source==root/'source'
def cmd(*args,input=None):
    r=subprocess.run(args,input=input,text=True,capture_output=True,timeout=60)
    assert r.returncode==0, 'fixture command failed'
    assert len(r.stdout)<2*1024*1024
    return r.stdout.strip()
assert cmd('git','-C',str(source),'rev-parse','HEAD')==state['source']
assert not cmd('git','-C',str(source),'status','--porcelain','--untracked-files=all')
for key in ['container_id','postgres_id']:
    obj=json.loads(cmd('docker','inspect',state[key]))[0]
    assert obj['State']['Running'] and obj['Config']['Labels']['org.bigbrotr.scientific-schema']==state['owner']
runner,pg=state['container_id'],state['postgres_id']
ready=json.loads((root/'out/native-tests/ready.json').read_text())
assert ready['source']==state['source']
report={'source':state['source'],'artifacts':ready,'status':'RUNNING','cases':[]}
path=root/'schema-cases.json'
assert not path.exists()
def save():path.write_text(json.dumps(report,indent=2)+'\n')
def sql(database,query):return cmd('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=query)
def database(suffix,create=True):
    name='bbcat_'+suffix
    sql('postgres','CREATE DATABASE '+name)
    sql(name,'REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE'+(',CREATE' if create else '')+' ON SCHEMA public TO "BootstrapMixed";')
    directory=root/'out'/('schema_'+name);directory.mkdir(mode=0o700)
    return name,'/out/'+directory.name+'/'
def child(database,directory,mode='initialize-catalog'):
    return json.loads(cmd('docker','exec',runner,'python3','-B','/source/experiments/archive-engine/catalog_schema_child.py',database,mode,directory))
def snapshot(database):
    # Catalog dump excludes owner passwords and includes data/DDL for exact retry
    # comparisons. Full native rows are separately retained for schema inspection.
    value=cmd('docker','exec',pg,'pg_dump','--no-owner','--no-privileges','--no-comments','-U','postgres',database)
    # PG16.15 emits a fresh psql \restrict token per dump, not database state.
    stable='\n'.join(line for line in value.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))
    return hashlib.sha256(stable.encode()).hexdigest()
def refusal(label,database,directory,error):
    before=snapshot(database)
    actual=child(database,directory)
    assert actual=={'exit':1,'stdout':'','stderr':'archive catalog bootstrap failed: '+error+'\n'}, actual
    after=snapshot(database);assert before==after
    report['cases'].append({'case':label,'outcome':actual,'database_unchanged_sha256':after});save()
save()
try:
    db,directory=database('science')
    # Exercise the actual product parent, artifact verification, pipes and reap.
    parent=json.loads(cmd('docker','exec',runner,'/out/native-tests/catalog-schema-probe',db,'initialize','success'))
    assert parent=={'success':True,'active_permits':0,'outcome':'Ok(())'}
    names=json.loads(sql(db,"SELECT json_agg(table_name ORDER BY table_name) FROM ducklake_table"))
    assert names==['archive_commits','dictionaries','event_eligibility_facts','event_receive_facts','events','provenance_facts']
    snapshots=json.loads(sql(db,'SELECT json_agg(t ORDER BY snapshot_id) FROM (SELECT snapshot_id,schema_version,next_catalog_id,next_file_id FROM ducklake_snapshot)t'))
    assert snapshots==[{'snapshot_id':0,'schema_version':0,'next_catalog_id':1,'next_file_id':0},{'snapshot_id':1,'schema_version':1,'next_catalog_id':7,'next_file_id':0}]
    schema_hash=hashlib.sha256((source/'contracts/archive/v1/schema.json').read_bytes()).hexdigest()
    assert json.loads(sql(db,'SELECT row_to_json(t) FROM (SELECT author,commit_message,commit_extra_info FROM ducklake_snapshot_changes WHERE snapshot_id=1)t'))=={'author':'BigBrotr','commit_message':'initialize archive schema 1','commit_extra_info':'sha256:'+schema_hash}
    rows={}
    for table in sorted(json.loads((source/'contracts/archive/catalog-native-v1.json').read_text())['tables']):
        rows[table]=json.loads(sql(db,'SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),\'[]\'::jsonb) FROM public.'+table+' t'))
    assert not rows['ducklake_data_file'] and not rows['ducklake_inlined_data_tables']
    assert len(rows['ducklake_schema'])==1 and len(rows['ducklake_table'])==6
    assert all(c['begin_snapshot']==1 and c['end_snapshot'] is None for c in rows['ducklake_column'])
    assert all(t['begin_snapshot']==1 and t['end_snapshot'] is None and t['schema_id']==0 for t in rows['ducklake_table'])
    assert sorted(t['table_id'] for t in rows['ducklake_table'])==list(range(1,7))
    (root/'initial-native-rows.json').write_text(json.dumps(rows,indent=2)+'\n')
    report['cases'].append({'case':'supervised-fresh-initialization','parent':parent,'tables':names,'snapshots':snapshots,'schema_sha256':schema_hash,'native_column_rows':len(rows['ducklake_column'])});save()
    # A different verified empty root proves the existing-DB check itself ran.
    retry=root/'out/schema_retry';retry.mkdir(mode=0o700)
    refusal('completed-initialization-refuses-repeat',db,'/out/schema_retry/','ExistingCatalog')
    for label,ddl in [
        ('table','CREATE TABLE public.canary (value text); INSERT INTO public.canary VALUES (\'preserve\')'),
        ('type','CREATE TYPE public.canary AS ENUM (\'preserve\')'),
        ('schema','CREATE SCHEMA canary'),
        ('function',"CREATE FUNCTION public.canary() RETURNS integer LANGUAGE sql AS 'SELECT 1'"),
        ('collation',"CREATE COLLATION public.canary FROM pg_catalog.\"C\""),
        ('text_search','CREATE TEXT SEARCH CONFIGURATION public.canary (COPY=pg_catalog.simple)'),
        ('large_object','SELECT lo_create(444444)'),
    ]:
        name,data=database(label);sql(name,ddl);refusal('unfamiliar-'+label,name,data,'ExistingCatalog')
    name,data=database('root')
    canary=root/'out'/('schema_'+name)/'preserve';canary.write_bytes(b'preserve')
    refusal('nonempty-root',name,data,'InvalidDataRoot');assert canary.read_bytes()==b'preserve'
    name,data=database('readonly',False);refusal('bootstrap-without-create',name,data,'Bootstrap')
    name,data=database('native_only')
    assert child(name,data,'bootstrap-catalog')=={'exit':0,'stdout':'{"format":"bigbrotr-native-bootstrap/1","status":"initialized"}\n','stderr':''}
    retry=root/'out/schema_native_retry';retry.mkdir(mode=0o700)
    refusal('native-only-catalog-requires-explicit-recovery',name,'/out/schema_native_retry/','ExistingCatalog')
    assert sql('postgres',"SELECT count(*) FROM pg_stat_activity WHERE usename='BootstrapMixed'")=='0'
    report['status']='PASS';save();print(json.dumps({'status':'PASS','cases':len(report['cases'])}))
except BaseException:
    report['status']='FAIL';save();raise
