#!/usr/bin/env python3
"""Actual scientific installation, malformed metadata, atomicity and lock cases."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode=True
SOURCE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(SOURCE/'experiments/control-migrations'))
from fault_cases import Process
root=Path(sys.argv[1]).resolve()
state=json.loads((root/'resources.json').read_text())
runner,pg=state['container_id'],state['postgres_id']
def cmd(*args,input=None):
    result=subprocess.run(args,input=input,text=True,capture_output=True,timeout=60)
    assert result.returncode==0, (args[-3:],result.stdout[-2000:],result.stderr[-2000:])
    assert len(result.stdout)<2*1024*1024
    return result.stdout.strip()
assert SOURCE==root/'source'
assert cmd('git','-C',str(SOURCE),'rev-parse','HEAD')==state['source']
assert not cmd('git','-C',str(SOURCE),'status','--porcelain')
for key in ['container_id','postgres_id']:
    c=json.loads(cmd('docker','inspect',state[key]))[0]
    assert c['State']['Running'] and c['Config']['Labels']['org.bigbrotr.scientific-guard']==state['owner']
ready=json.loads((root/'out/native-tests/ready.json').read_text());assert ready['source']==state['source']
report={'source':state['source'],'artifacts':ready,'status':'RUNNING','cases':[]}
path=root/'guard-cases.json';assert not path.exists()
def save():path.write_text(json.dumps(report,indent=2)+'\n')
def record(name,**details):
    report['cases'].append({'case':name,**details});save();print(name,flush=True)
def sql(db,q):return cmd('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=q)
def create(suffix):
    db='bbcat_'+suffix
    sql('postgres','CREATE DATABASE '+db)
    sql(db,'REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
    (root/'out'/('schema_'+db)).mkdir(mode=0o700)
    return db
def initialize(db):
    assert json.loads(cmd('docker','exec',runner,'/out/native-tests/catalog-schema-probe',db,'initialize','success'))=={'success':True,'active_permits':0,'outcome':'Ok(())'}
def probe(mode,db,expected,port=5432):
    return json.loads(cmd('docker','exec',runner,'/out/native-tests/scientific-guard-probe',mode,db,expected,str(port)))
def native(db,mode):
    assert json.loads(cmd('docker','exec','-e','LD_LIBRARY_PATH=/out/archive-worker/lib',runner,'/out/native-tests/scientific-ddl-probe',db,mode))=={'status':'PASS','case':mode}
def snapshot(db):
    dump=cmd('docker','exec',pg,'pg_dump','-U','postgres',db)
    # Heap row order changes after UPDATE/restore. COPY ordering is not logical
    # identity; sort within each relation, preserving duplicate rows and all bytes.
    lines=[]; copying=None
    for line in dump.splitlines():
        if line.startswith(('\\restrict ','\\unrestrict ')): continue
        if copying is not None:
            if line=='\\.':
                lines.extend(sorted(copying));lines.append(line);copying=None
            else: copying.append(line)
        else:
            lines.append(line)
            if line.startswith('COPY ') and line.endswith(' FROM stdin;'): copying=[]
    assert copying is None
    return hashlib.sha256('\n'.join(lines).encode()).hexdigest()
def damage(db,sql_text):
    # Explicit isolated privileged corruption, never an application repair path.
    sql(db,'BEGIN; SET LOCAL session_replication_role=replica; '+sql_text+'; COMMIT;')
def installed_effects(db):
    # Independent observer includes privileges/ownership that a version marker
    # and the adapter's own compatibility result cannot establish by themselves.
    actual=json.loads(sql(db,"SELECT json_build_object('native_tables',(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner='postgres'),'native_guards',(SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND tgname='bigbrotr_writer_guard' AND tgenabled='O'),'bootstrap_writable',(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND (has_table_privilege('BootstrapMixed',quote_ident(schemaname)||'.'||quote_ident(tablename),'INSERT,UPDATE,DELETE,TRIGGER,TRUNCATE') OR has_any_column_privilege('BootstrapMixed',quote_ident(schemaname)||'.'||quote_ident(tablename),'INSERT,UPDATE'))),'bootstrap_create',has_schema_privilege('BootstrapMixed','public','CREATE'),'guard_owner',(SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname='bigbrotr_archive'),'binding_rows',(SELECT count(*) FROM bigbrotr_archive.scientific_schema),'attempts',(SELECT count(*) FROM bigbrotr_archive.attempts),'owner',(SELECT row_to_json(o) FROM bigbrotr_archive.owner o))"))
    assert actual=={'native_tables':28,'native_guards':28,'bootstrap_writable':0,'bootstrap_create':False,'guard_owner':'postgres','binding_rows':1,'attempts':0,'owner':{'singleton':True,'ordinal':0,'enabled':False}},actual
    return actual
children=[]
try:
    save()
    for mode in ['add','drop','rollback']:
        db=create('native_'+mode);initialize(db);before=snapshot(db);native(db,mode)
        if mode=='rollback':
            assert snapshot(db)==before
            assert probe('install',db,'installed')['commit_started']
        else:
            changed=snapshot(db);assert changed!=before
            assert probe('install',db,'IncompatibleLayout')['commit_started'] is False
            assert snapshot(db)==changed
        record('native-ddl-'+mode)
    db=create('native_only')
    assert json.loads(cmd('docker','exec',runner,'/out/native-tests/catalog-schema-probe',db,'bootstrap','success'))['success']
    probe('install',db,'IncompatibleLayout')
    probe('native-install',db,'installed');probe('native-inspect',db,'inspected')
    probe('inspect',db,'IncompatibleLayout');probe('install',db,'IncompatibleLayout')
    record('explicit-native-only-mode-cannot-be-adopted')
    db=create('science');initialize(db)
    # Late privileged installer failure after companion DDL and most transfers.
    sql(db,"CREATE SCHEMA fixture; CREATE FUNCTION fixture.fail_install() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN IF (SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user)=28 THEN RAISE EXCEPTION 'fixture late transfer failure'; END IF; END $$; CREATE EVENT TRIGGER fail_install ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION fixture.fail_install();")
    before=snapshot(db)
    assert probe('install',db,'Backend')['commit_started'] is False
    assert snapshot(db)==before and sql(db,"SELECT to_regnamespace('bigbrotr_archive') IS NULL")=='t'
    sql(db,'DROP EVENT TRIGGER fail_install; DROP SCHEMA fixture CASCADE')
    record('late-install-failure-rolls-back-all-guard-ddl')
    assert probe('install',db,'installed')['commit_started']
    before=snapshot(db)
    assert probe('install',db,'existing')['commit_started'] is False
    assert probe('inspect',db,'inspected')['commit_started'] is False
    assert snapshot(db)==before
    row=json.loads(sql(db,"SELECT json_build_object('version',format_version,'declaration',encode(declaration_checksum,'hex'),'layout',encode(native_layout_checksum,'hex'),'sql',encode(sql_checksum,'hex'),'snapshot',initialization_snapshot) FROM bigbrotr_archive.scientific_schema"))
    assert row=={'version':1,'declaration':hashlib.sha256((SOURCE/'contracts/archive/v1/schema.json').read_bytes()).hexdigest(),'layout':hashlib.sha256((SOURCE/'contracts/archive/v1/native-layout.json').read_bytes()).hexdigest(),'sql':hashlib.sha256((SOURCE/'migrations/catalog/0002_scientific_schema.sql').read_bytes()).hexdigest(),'snapshot':1}
    probe('native-inspect',db,'IncompatibleLayout');probe('native-install',db,'IncompatibleLayout')
    record('install-binding-retry-and-fresh-inspection',binding=row,effects=installed_effects(db))
    before=snapshot(db)
    refused=subprocess.run(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','postgres','-d',db],input="BEGIN; SET LOCAL session_replication_role=replica; INSERT INTO public.ducklake_metadata VALUES('data_inlining_row_limit',NULL,NULL,NULL); COMMIT;",text=True,capture_output=True,timeout=15)
    assert refused.returncode==3 and '23502' in refused.stderr and 'ducklake_metadata' in refused.stderr
    assert snapshot(db)==before
    record('native-metadata-value-not-null',sqlstate='23502',owner='PostgreSQL physical constraint')
    target="table_id=(SELECT table_id FROM public.ducklake_table WHERE table_name='events') AND column_name='created_at'"
    controls=[('type',"UPDATE public.ducklake_column SET column_type='int64' WHERE "+target,"UPDATE public.ducklake_column SET column_type='uint64' WHERE "+target),
        ('parent',"UPDATE public.ducklake_column SET parent_column=1 WHERE "+target,"UPDATE public.ducklake_column SET parent_column=NULL WHERE "+target),
        ('nullable',"UPDATE public.ducklake_column SET nulls_allowed=true WHERE "+target,"UPDATE public.ducklake_column SET nulls_allowed=false WHERE "+target),
        ('default',"UPDATE public.ducklake_column SET default_value='0' WHERE "+target,"UPDATE public.ducklake_column SET default_value='NULL' WHERE "+target),
        ('order',"UPDATE public.ducklake_column SET column_order=1000 WHERE "+target,"UPDATE public.ducklake_column SET column_order=column_id WHERE "+target),
        ('oversized-name',"UPDATE public.ducklake_column SET column_name=repeat('x',1000000) WHERE "+target,"UPDATE public.ducklake_column SET column_name='created_at' WHERE length(column_name)=1000000"),
        ('missing-table',"UPDATE public.ducklake_table SET table_name='absent' WHERE table_name='events'","UPDATE public.ducklake_table SET table_name='events' WHERE table_name='absent'"),
        ('unknown-metadata-scope',"INSERT INTO public.ducklake_metadata VALUES('probe','x','unexpected',NULL)","DELETE FROM public.ducklake_metadata WHERE key='probe'"),
        ('null-scope-id',"INSERT INTO public.ducklake_metadata VALUES('data_inlining_row_limit','0','table',NULL)","DELETE FROM public.ducklake_metadata WHERE key='data_inlining_row_limit'"),
        ('unknown-scope-id',"INSERT INTO public.ducklake_metadata VALUES('data_inlining_row_limit','0','table',99)","DELETE FROM public.ducklake_metadata WHERE key='data_inlining_row_limit'"),
        ('duplicate-settings',"INSERT INTO public.ducklake_metadata VALUES('data_inlining_row_limit','0',NULL,NULL),('data_inlining_row_limit','0',NULL,NULL)","DELETE FROM public.ducklake_metadata WHERE key='data_inlining_row_limit'"),
        ('binding-hash',"UPDATE bigbrotr_archive.scientific_schema SET declaration_checksum=decode(repeat('00',32),'hex')","UPDATE bigbrotr_archive.scientific_schema SET declaration_checksum=decode('"+row['declaration']+"','hex')")]
    saved=sql(db,'SELECT row_to_json(c) FROM public.ducklake_column c WHERE '+target)
    controls.extend([
        ('missing-column','DELETE FROM public.ducklake_column WHERE '+target,"INSERT INTO public.ducklake_column SELECT * FROM jsonb_populate_record(NULL::public.ducklake_column,'"+saved.replace("'","''")+"'::jsonb)"),
        ('duplicate-column','INSERT INTO public.ducklake_column SELECT * FROM public.ducklake_column WHERE '+target,'DELETE FROM public.ducklake_column WHERE ctid IN (SELECT ctid FROM public.ducklake_column WHERE '+target+' LIMIT 1)'),
        ('initial-default',"UPDATE public.ducklake_column SET initial_default='0' WHERE "+target,"UPDATE public.ducklake_column SET initial_default=NULL WHERE "+target),
        ('null-table-path',"UPDATE public.ducklake_table SET path=NULL WHERE table_name='events'","UPDATE public.ducklake_table SET path='events/' WHERE table_name='events'"),
    ])
    for column,restore in [('column_name',"'created_at'"),('column_type',"'uint64'"),('column_order','column_id'),('begin_snapshot','1'),('nulls_allowed','false'),('default_value',"'NULL'"),('default_value_type',"'literal'"),('default_value_dialect',"'duckdb'")]:
        controls.append(('null-'+column,'UPDATE public.ducklake_column SET '+column+'=NULL WHERE '+target,'UPDATE public.ducklake_column SET '+column+'='+restore+(' WHERE column_name IS NULL' if column=='column_name' else ' WHERE '+target)))
    for name,change,restore in controls:
        before=snapshot(db);damage(db,change);changed=snapshot(db)
        assert changed!=before
        assert probe('inspect',db,'IncompatibleLayout')['commit_started'] is False
        assert probe('activate',db,'IncompatibleLayout')['commit_started'] is False
        assert snapshot(db)==changed
        assert sql(db,"SELECT (SELECT count(*) FROM bigbrotr_archive.attempts)=0 AND (SELECT ordinal=0 AND NOT enabled FROM bigbrotr_archive.owner) AND NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname LIKE 'bb_archive_%')")=='t'
        damage(db,restore);assert snapshot(db)==before
        record('refuse-'+name)
    # Valid optional zero-inlining values at every selected scope remain valid.
    damage(db,"INSERT INTO public.ducklake_metadata VALUES('data_inlining_row_limit','0',NULL,NULL),('data_inlining_row_limit','0','schema',0); INSERT INTO public.ducklake_metadata SELECT 'data_inlining_row_limit','0','table',table_id FROM public.ducklake_table")
    probe('inspect',db,'inspected');damage(db,"DELETE FROM public.ducklake_metadata WHERE key='data_inlining_row_limit'")
    record('all-zero-inlining-scopes')
    # Actual wire ambiguity: companion marker and guards must all commit or none.
    for mode,counts in [('before-forward',[1,0,0,0,1]),('after-complete',[1,1,1,0,1])]:
        name=create('lost_'+('before' if mode=='before-forward' else 'after'));initialize(name);before=snapshot(name)
        proxy=Process(['docker','exec','-i',runner,'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--mode',mode,'--deadline-seconds','60'],input_pipe=True);children.append(proxy)
        r=proxy.event('ready',10);assert r['proxy_sha256']==hashlib.sha256((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest()
        assert probe('install',name,'CommitUnknown',r['listen_port'])['commit_started']
        receipt=proxy.event('receipt',10);assert proxy.wait(5)==0 and not proxy.stderr
        assert [receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']]==counts
        assert sql(name,"SELECT to_regnamespace('bigbrotr_archive') IS NOT NULL")==('f' if mode=='before-forward' else 't')
        if mode=='before-forward': assert snapshot(name)==before
        else: installed_effects(name)
        probe('install',name,'installed' if mode=='before-forward' else 'existing');probe('inspect',name,'inspected')
        record('lost-install-'+mode,wire=receipt,effects=installed_effects(name))
    # Only after all negative activation controls, obtain one real retained intent.
    assert probe('activate',db,'provisioned-discarded')['commit_started']
    probe('inspect',db,'inspected')
    record('scientific-activation')
    login='bb_archive_'+'14'*16
    def session(name,query):
        process=Process(['docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input_pipe=True)
        children.append(process)
        process.process.stdin.write(("SET application_name='"+name+"'; BEGIN; "+query+"; SELECT 'ready';\n").encode());process.process.stdin.flush()
        return process
    def ready_session(process):
        end=time.monotonic()+5
        while time.monotonic()<end:
            if b'ready\n' in process.stdout: return
            assert process.process.poll() is None and not process.overflow
            time.sleep(.02)
        raise RuntimeError('session-not-ready')
    def release(process):
        process.process.stdin.write(b'ROLLBACK;\n');process.process.stdin.close()
        assert process.wait(10)==0 and not process.stderr
    def blocking(waiter,holder):
        end=time.monotonic()+5
        while time.monotonic()<end:
            result=sql(db,"SELECT EXISTS(SELECT 1 FROM pg_stat_activity w JOIN pg_stat_activity h ON h.pid=ANY(pg_blocking_pids(w.pid)) WHERE w.datname=current_database() AND w.application_name='"+waiter+"' AND h.application_name='"+holder+"')")
            if result=='t': return
            time.sleep(.02)
        raise RuntimeError('expected-lock-not-observed')
    def inspecting():
        p=Process(['docker','exec',runner,'/out/native-tests/scientific-guard-probe','inspect',db,'inspected']);children.append(p);return p
    writer=session('fixture_writer',"SET SESSION AUTHORIZATION "+login+"; DELETE FROM public.ducklake_column WHERE false")
    ready_session(writer);inspector=inspecting()
    blocking('bigbrotr-catalog-recovery-probe','fixture_writer')
    assert inspector.process.poll() is None
    release(writer);assert inspector.wait(15)==0 and not inspector.stderr
    record('native-statement-share-lock-before-inspector')
    # Hold metadata so inspector owns the row before a native statement gets its
    # table RowExclusive lock. Its later AccessShare read must remain compatible.
    holder=session('fixture_holder','LOCK TABLE public.ducklake_metadata IN ACCESS EXCLUSIVE MODE')
    ready_session(holder);inspector=inspecting()
    blocking('bigbrotr-catalog-recovery-probe','fixture_holder')
    writer=session('fixture_writer',"SET SESSION AUTHORIZATION "+login+"; DELETE FROM public.ducklake_column WHERE false")
    blocking('fixture_writer','bigbrotr-catalog-recovery-probe')
    release(holder);assert inspector.wait(15)==0 and not inspector.stderr
    ready_session(writer);release(writer)
    record('inspector-owner-lock-before-native-table-lock')
    assert sql(db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name IN ('fixture_holder','fixture_writer','bigbrotr-catalog-recovery-probe')")=='0'
    assert probe('seal',db,'sealed-current')['commit_started']
    assert sql(db,"SELECT ordinal=1 AND NOT enabled FROM bigbrotr_archive.owner WHERE singleton")=='t'
    assert sql(db,"SELECT NOT rolcanlogin FROM pg_roles WHERE rolname='"+login+"'")=='t'
    probe('inspect',db,'inspected')
    assert probe('seal',db,'sealed-history')['commit_started']
    record('seal-and-idempotent-resolution-after-shared-owner-lock')
    report['status']='PASS';save();print(json.dumps({'status':'PASS','cases':len(report['cases'])}))
except BaseException:
    report['status']='FAIL';save();raise
finally:
    for child in children:
        if child.process.poll() is None:
            try:child.send('close');child.wait(5)
            except Exception:child.process.kill();child.process.wait(5)
