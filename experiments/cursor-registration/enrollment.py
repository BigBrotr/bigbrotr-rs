#!/usr/bin/env python3
"""Real additional-key enrollment, immutable initial upgrade and commit recovery."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/control-migrations'))
import lab
from fault_cases import Process


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',required=True,type=Path)
    root=parser.parse_args().root.resolve(strict=True);state=lab.load(root)
    revision=lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD']).stdout.strip()
    assert Path(state['source'])==SOURCE and not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout
    info=json.loads(lab.cli(state,'build-info').stdout)
    assert info==state['build']['build_info']
    if info['source']['revision']!=revision:
        origin=json.loads((root.parent/'artifact-origin.json').read_text())
        assert origin['source']==info['source']['revision'] and origin['fixture_revision']==revision
        # The enclosing exact-Git wrapper verifies all retained build inputs;
        # independently compare the actual current checkout before using them.
        manifest=json.loads((Path(origin['root'])/'out/build/source-manifest.json').read_text())
        assert hashlib.sha256((Path(origin['root'])/'out/build/source-manifest.json').read_bytes()).hexdigest()==info['source']['manifest_sha256']
        current=lab.command(['git','-C',str(SOURCE),'ls-files']).stdout.splitlines()
        names={n for n in current if (not n.startswith('docs/') or n.startswith('docs/reference/')) and n not in {'experiments/cursor-registration/README.md','experiments/cursor-registration/retry.py','experiments/cursor-registration/enrollment.py'}}
        assert names=={n for n in manifest['files_sha256'] if (not n.startswith('docs/') or n.startswith('docs/reference/')) and n not in {'experiments/cursor-registration/README.md','experiments/cursor-registration/retry.py','experiments/cursor-registration/enrollment.py'}}
        assert all(hashlib.sha256((SOURCE/n).read_bytes()).hexdigest()==manifest['files_sha256'][n] for n in names)
    assert lab.docker('exec',state['client'],'sha256sum','/build/bigbrotr').stdout.split()[0]==state['binary_sha256']
    output=root/'cursor-enrollment.json';assert not output.exists()
    report=dict(source=revision,binary_source=info['source']['revision'],binary_sha256=state['binary_sha256'],server=state['server'],status='RUNNING',cases=[],databases=[],fault_containers=[])
    vectors=json.loads((SOURCE/'tests/fixtures/read/keyring-vectors.json').read_text())
    wire=bytes.fromhex(vectors['cases'][0]['hex']);key='55'*16
    def save():
        p=output.with_suffix('.partial');p.write_text(json.dumps(report,indent=2)+'\n');p.replace(output)
    def record(name,ok,**fields):
        assert not any(c['name']==name for c in report['cases']),name
        report['cases'].append(dict(name=name,status='PASS' if ok else 'FAIL',**fields));save()
        if not ok:raise RuntimeError('case failed: '+name)
    def grant(database,canonical=True):
        sql='GRANT INSERT ON control.cursor_keyrings,control.cursor_keys TO bigbrotr_control; GRANT EXECUTE ON FUNCTION control.guard_cursor_registration(),control.require_cursor_registration() TO bigbrotr_control;'
        if canonical:sql+='GRANT SELECT,INSERT ON control.cursor_key_materials TO bigbrotr_control; GRANT EXECUTE ON FUNCTION control.guard_cursor_key_material(),control.capture_initial_cursor_material(),control.require_cursor_enrollment() TO bigbrotr_control;'
        lab.sql(state,database,sql)
    def prepare(name,target=602,data=wire):
        database='bb_enroll_'+name;lab.sql(state,'postgres',f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        report['databases'].append(database);save();path=lab.config(root,state,database);local=root/Path(path).name
        text=lab.change(local.read_text(),'postgres.control','password_ref','file:/lab/secrets/application')
        text=lab.change(text,'topology','dataset_id',vectors['dataset_id'])
        directory=root/('keys-'+name);directory.mkdir(mode=0o700);file=directory/'keyring';file.write_bytes(data);file.chmod(0o600)
        local.write_text(text+f'\n[read.cursor]\nkeyring_ref = "file:/lab/{directory.name}/keyring"\n')
        result=lab.migrate(state,'apply',path,target=target);record('migration-'+name,result.returncode==0,stderr=result.stderr)
        lab.sql(state,database,'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets,control.config_revisions,control.principals,control.management_audit TO bigbrotr_control; GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_control;')
        result=lab.cli(state,'config','initialize','--file',path);record('initialize-'+name,result.returncode==0,stderr=result.stderr)
        grant(database,target==602);return database,path,file
    def call(name,path,operation='enroll-key',selected=key,code=0):
        args=['access','cursor-keyring',operation,'--file',path]
        if operation in ['prepare-key','enroll-key']:args+=['--key-id',selected]
        result=lab.cli(state,*args);value=json.loads(result.stdout) if result.stdout else None
        record(name,result.returncode==code and (value is not None and not result.stderr if code==0 else value is None),exit=result.returncode,output=value,stderr=result.stderr)
        if value:assert value['cursor_authority']=='not-granted'
        return value
    def snapshot(database,initial=False):
        tables=['cursor_keyrings','cursor_keys','management_audit']+([] if initial else ['cursor_key_materials'])
        fields=[]
        for table in tables:
            order={'cursor_keyrings':'dataset_id','cursor_keys':'dataset_id,key_id','management_audit':'audit_id','cursor_key_materials':'dataset_id,key_id'}[table]
            fields += ["'"+table+"'",f"(SELECT coalesce(json_agg(t),'[]') FROM (SELECT *,xmin::text AS row_version FROM control.{table} ORDER BY {order}) t)"]
        return json.loads(lab.sql(state,database,'SELECT json_build_object('+','.join(fields)+');'))
    def binding(data,id):
        records=[data[i:i+48] for i in range(42,len(data)-32,48)]
        row,=[r for r in records if r[:16].hex()==id]
        return hashlib.sha256(b'bigbrotr/cursor-key-material/1\0'+data[8:40]+row).hexdigest()
    def validate(name,database,receipt,file,id=key,created=False,origin="additional"):
        observed=snapshot(database);item,=[r for r in observed['cursor_key_materials'] if r['key_id']=='\\x'+id]
        audit,=[a for a in observed['management_audit'] if a['audit_id']==item['audit_id']]
        actor=lab.sql(state,database,"SELECT encode(principal_id,'hex') FROM control.principals WHERE kind='installation' AND database_role='bigbrotr_control';")
        expected=dict(format='bigbrotr-cursor-key-enrollment/1',dataset_id=vectors['dataset_id'],keyring_id=vectors['keyring_id'],key_id=id,material_fingerprint_sha256=binding(file.read_bytes(),id),cursor_authority='not-granted',state='enrolled' if created else 'already-enrolled',origin=origin,enrolled_at_us=str(item['enrolled_at_us']),actor_id=actor,audit_id=item['audit_id'][2:])
        expected_audit=dict(dataset_id='\\x'+vectors['dataset_id'],actor_id='\\x'+actor,action='cursor-keyring.register' if origin=='initial' else 'cursor-key.enroll',outcome='accepted',subject_id='\\x'+(vectors['keyring_id'] if origin=='initial' else id),created_at_us=item['enrolled_at_us'],config_revision=None,policy_revision=None,reason=None,requester_database_role='bigbrotr_control',requester_os_uid=os.getuid())
        record(name,receipt==expected and item['origin']==origin and item['dataset_id']=='\\x'+vectors['dataset_id'] and item['keyring_id']=='\\x'+vectors['keyring_id'] and item['actor_id']==audit['actor_id']=='\\x'+actor and item['material_digest']=='\\x'+expected['material_fingerprint_sha256'] and all(audit[k]==v for k,v in expected_audit.items()),receipt=receipt,expected=expected,observed=observed)
    def added_only(before,after,id=key):
        if any(after[t]!=before[t] for t in ['cursor_keyrings','cursor_keys']):return False
        old={r['key_id']:r for r in before['cursor_key_materials']};new={r['key_id']:r for r in after['cursor_key_materials']}
        if set(new)-set(old)!={'\\x'+id} or not all(new.get(k)==v for k,v in old.items()):return False
        material=new['\\x'+id]
        old={r['audit_id']:r for r in before['management_audit']};new={r['audit_id']:r for r in after['management_audit']}
        return set(new)-set(old)=={material['audit_id']} and all(new.get(k)==v for k,v in old.items()) and new[material['audit_id']]['action']=='cursor-key.enroll' and new[material['audit_id']]['subject_id']=='\\x'+id
    def register_add(name,database,path,file):
        call(name+'-initial',path,'register');call(name+'-prepare',path,'prepare-key')
        data=file.read_bytes();assert len(data)==218 and data[:40]==wire[:40] and data[42:138]==wire[42:138] and data[138:154].hex()==key
        assert hashlib.sha256(data[:-32]).digest()==data[-32:]
    processes=[];fault=None
    try:
        save();database,path,file=prepare('upgrade',601)
        initial=call('upgrade-initial',path,'register');before=snapshot(database,True)
        call('old-schema-refused',path,selected='33'*16,code=3)
        record('old-schema-retry-no-write',snapshot(database,True)==before)
        result=lab.migrate(state,'apply',path,target=602);record('upgrade-602',result.returncode==0,stderr=result.stderr);grant(database)
        record('initial-history-unchanged',snapshot(database,True)==before,observed=snapshot(database))
        materials=snapshot(database)['cursor_key_materials']
        record('backfill-original-binding',len(materials)==2 and all(m['origin']=='initial' and m['audit_id']=='\\x'+initial['audit_id'] and m['actor_id']=='\\x'+initial['actor_id'] and str(m['enrolled_at_us'])==initial['registered_at_us'] and m['material_digest']=='\\x'+binding(wire,m['key_id'][2:]) for m in materials))
        enrolled_initial=snapshot(database)
        receipt=call('initial-key-resolve',path,selected='33'*16);validate('initial-key-receipt',database,receipt,file,'33'*16,origin='initial')
        record('initial-resolution-no-write',snapshot(database)==enrolled_initial)
        call('upgrade-prepare',path,'prepare-key');first=call('additional-enroll',path);validate('additional-receipt',database,first,file,created=True)
        record('initial-enrollment-only-new-binding-audit',added_only(enrolled_initial,snapshot(database)),before=enrolled_initial,after=snapshot(database))
        before=snapshot(database);call('prepare-another',path,'prepare-key',selected='66'*16);data=file.read_bytes()
        retry=call('retry-after-local-addition',path);validate('retry-added-receipt',database,retry,file)
        record('retry-keeps-original-sql',snapshot(database)==before and {k:v for k,v in first.items() if k!='state'}=={k:v for k,v in retry.items() if k!='state'})
        call('initial-whole-file-conflict',path,'register',code=3);record('initial-conflict-no-write',snapshot(database)==before)
        body=data[:40]+b'\0\1'+data[138:186];reduced=body+hashlib.sha256(body).digest();file.write_bytes(reduced)
        receipt=call('retry-with-only-selected-key',path);validate('reduced-receipt',database,receipt,file);record('reduced-no-write',snapshot(database)==before)
        changed=bytearray(reduced);changed[58]^=1;changed[-32:]=hashlib.sha256(changed[:-32]).digest();file.write_bytes(changed)
        call('permanent-material-conflict',path,code=3);record('conflict-no-write',snapshot(database)==before)
        file.write_bytes(data)
        missing_db,missing_path,_=prepare('missing');baseline=snapshot(missing_db)
        call('missing-initial-ring',missing_path,selected='33'*16,code=3);record('missing-no-implicit-registration',snapshot(missing_db)==baseline)
        denied_db,denied_path,denied_file=prepare('denied');baseline=snapshot(denied_db)
        lab.sql(state,denied_db,'REVOKE INSERT ON control.cursor_key_materials FROM bigbrotr_control;')
        call('initial-mirror-privilege-refusal',denied_path,'register',code=1);record('initial-mirror-atomic-rollback',snapshot(denied_db)==baseline)
        grant(denied_db);register_add('fresh',denied_db,denied_path,denied_file)
        material=snapshot(denied_db)['cursor_key_materials'];record('fresh-initial-mirror',len(material)==2 and all(r['origin']=='initial' for r in material))
        baseline=snapshot(denied_db);lab.sql(state,denied_db,'REVOKE INSERT ON control.cursor_key_materials FROM bigbrotr_control;')
        call('additional-late-permission-refusal',denied_path,code=1);record('additional-audit-rollback',snapshot(denied_db)==baseline)
        grant(denied_db);receipt=call('permission-recovery',denied_path);validate('permission-recovery-receipt',denied_db,receipt,denied_file,created=True)
        record('permission-recovery-only-new-binding-audit',added_only(baseline,snapshot(denied_db)),before=baseline,after=snapshot(denied_db))
        full=bytes.fromhex(next(c['hex'] for c in vectors['cases'] if c['name']=='sixteen-public-keys'))
        full_db,full_path,full_file=prepare('full',data=full);call('sixteen-initial-register',full_path,'register')
        rows=snapshot(full_db)['cursor_key_materials'];record('sixteen-bindings-share-original-audit',len(rows)==16 and len({r['audit_id'] for r in rows})==1 and all(r['material_digest']=='\\x'+binding(full,r['key_id'][2:]) for r in rows))
        schema_db,schema_path,_=prepare('sql');call('sql-initial',schema_path,'register')
        d="decode('"+vectors['dataset_id']+"','hex')";ring="decode('"+vectors['keyring_id']+"','hex')";id="decode(repeat('77',16),'hex')";audit="decode(repeat('78',16),'hex')";actor=f"(SELECT principal_id FROM control.principals WHERE dataset_id={d} AND kind='installation')"
        audit_sql=f"INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,requester_database_role,requester_os_uid) VALUES({audit},{d},{actor},'cursor-key.enroll','accepted',123,{id},SESSION_USER,1000);"
        key_sql=f"INSERT INTO control.cursor_key_materials VALUES({d},{ring},{id},decode(repeat('79',32),'hex'),'additional',123,{actor},{audit});"
        def sql_case(name,statement,expected,role='bigbrotr_control',unchanged=True):
            baseline=lab.schema_digest(state,schema_db)
            result=lab.docker('exec','-i',state['postgres'],'psql','-X','-A','-t','-q','--no-password','-v','ON_ERROR_STOP=1','-v','VERBOSITY=sqlstate','-U',role,'-d',schema_db,data=statement,check=False)
            (root/(name+'.stdout')).write_text(result.stdout);(root/(name+'.stderr')).write_text(result.stderr)
            ok=result.returncode==0 if expected is None else result.returncode!=0 and 'ERROR:  '+expected in result.stderr
            record(name,ok and (not unchanged or lab.schema_digest(state,schema_db)==baseline),exit=result.returncode,expected_sqlstate=expected,stderr=result.stderr)
        for name,body,expected in [
            ('audit-alone',audit_sql,'P0002'),('material-alone',key_sql,'23503'),
            ('forged-initial',audit_sql+key_sql.replace("'additional'","'initial'"),'23514'),
            ('wrong-time',audit_sql+key_sql.replace(",123,",",124,"),'23514'),
            ('wrong-subject',audit_sql.replace(id,"decode(repeat('80',16),'hex')")+key_sql,'P0002'),
            ('early-flush',audit_sql+'SET CONSTRAINTS ALL IMMEDIATE;'+key_sql,'P0002'),
            ('initial-as-additional',audit_sql+key_sql.replace(id,"decode(repeat('33',16),'hex')"),'23514'),
            ('repeatable-read','SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;'+audit_sql+key_sql,'25001'),
            ('rollback',audit_sql+key_sql+'ROLLBACK;BEGIN;',None),
        ]:sql_case('sql-'+name,'BEGIN;'+body+'COMMIT;',expected)
        sql_case('sql-complete','BEGIN;'+audit_sql+key_sql+'SET CONSTRAINTS ALL IMMEDIATE;COMMIT;',None,unchanged=False)
        sql_case('sql-duplicate-material','BEGIN;'+key_sql+'COMMIT;','23505')
        for verb,statement in [('update','UPDATE control.cursor_key_materials SET origin=origin;'),('delete','DELETE FROM control.cursor_key_materials;'),('truncate','TRUNCATE control.cursor_key_materials CASCADE;')]:
            sql_case('sql-immutable-'+verb,statement,'23514','bigbrotr_migrator');sql_case('sql-denied-'+verb,statement,'42501')
        mounts=json.loads(lab.docker('inspect',state['client']).stdout)[0]['Mounts'];build=next(m['Source'] for m in mounts if m['Destination']=='/build' and not m['RW'])
        fault=state['client']+'-cursor-fault';assert lab.docker('container','inspect',fault,check=False).returncode!=0
        entry=dict(name=fault,owner=state['owner'],state='creating');report['fault_containers'].append(entry);save()
        lab.docker('run','-d','--init','--name',fault,'--label','org.bigbrotr.cursor-registration='+state['owner'],'--network','container:'+state['postgres'],'--user',f'{os.getuid()}:{os.getgid()}','--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--cap-add','SYS_PTRACE','--security-opt','seccomp=unconfined','--mount',f'type=bind,src={root},dst=/lab','--mount',f'type=bind,src={build},dst=/build,readonly','--mount','type=bind,src=/home/tux/bigbrotr-implementation-20260917/runs/strace-exit-9b30c35/out,dst=/instrument,readonly','--mount',f'type=bind,src={SOURCE},dst=/source,readonly',state['client_image'],'python3','-c','import time; time.sleep(600)')
        entry.update(id=json.loads(lab.docker('inspect',fault).stdout)[0]['Id'],state='running');save()
        for scenario in ['blocked-output','sync-error']:
            db,config,keys=prepare(scenario.replace('-','_'));register_add(scenario,db,config,keys);baseline=snapshot(db);old=keys.read_bytes()
            result=lab.docker('exec',fault,'python3','-B','/source/experiments/cursor-registration/syscall_cases.py',scenario,config,'/lab/'+scenario+'.json','--key-id',key,check=False)
            observation=json.loads((root/(scenario+'.json')).read_text());record('syscall-'+scenario,result.returncode==0 and observation['status']=='PASS',observation=observation,stderr=result.stderr)
            if scenario=='sync-error':record('sync-refusal-no-sql',snapshot(db)==baseline and keys.read_bytes()==old and len(list(keys.parent.iterdir()))==2)
            else:
                committed=snapshot(db);record('blocked-only-new-binding-audit',added_only(baseline,committed),before=baseline,after=committed);receipt=call('blocked-recovery',config);validate('blocked-recovery-receipt',db,receipt,keys)
                record('blocked-durable-key-and-readonly-retry',keys.read_bytes()==old and snapshot(db)==committed)
        for scenario,mode in [('before','before-forward'),('after','after-complete'),('same','hold'),('different','hold'),('killed','hold')]:
            db,config,keys=prepare(scenario);register_add(scenario,db,config,keys);baseline=snapshot(db);old=keys.read_bytes()
            proxy=Process(['docker','exec','-i',fault,'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--mode',mode,'--deadline-seconds','45'],input_pipe=True);processes.append(proxy)
            ready=proxy.event('ready',10);assert ready['proxy_sha256']==hashlib.sha256((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest()
            original=(root/Path(config).name).read_text();proxyfile=root/(scenario+'-proxy.toml');proxyfile.write_text(lab.change(lab.change(original,'postgres.control','endpoint',f'127.0.0.1:{ready["listen_port"]}'),'postgres.control.tls','local_boundary','loopback'))
            args=['/build/bigbrotr','access','cursor-keyring','enroll-key','--file','/lab/'+proxyfile.name,'--key-id',key]
            app=Process(['docker','exec',fault,*args]);processes.append(app);other=None
            if mode=='hold':
                held=proxy.event('held',15);record('held-'+scenario,held['held'] and held['commit_number']==1 and [held[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete']]==[1,0,0,0] and snapshot(db)==baseline,proxy=held)
                if scenario=='killed':lab.docker('kill','--signal','KILL',fault)
                else:
                    directory=root/(scenario+'-second-keys');directory.mkdir(mode=0o700);data=bytearray(old)
                    if scenario=='different':data[154]^=1;data[-32:]=hashlib.sha256(data[:-32]).digest()
                    second_keys=directory/'keyring';second_keys.write_bytes(data);second_keys.chmod(0o600)
                    second=root/(scenario+'-second.toml');second.write_text(lab.change(original,'read.cursor','keyring_ref','file:/lab/'+directory.name+'/keyring'))
                    other=Process(['docker','exec',state['client'],'/build/bigbrotr','access','cursor-keyring','enroll-key','--file','/lab/'+second.name,'--key-id',key]);processes.append(other)
                    deadline=time.monotonic()+4
                    while True:
                        waits=json.loads(lab.sql(state,db,"SELECT coalesce(json_agg(json_build_object('pid',pid,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock' AND query LIKE '%control.datasets%';"))
                        if any(w['blockers'] for w in waits):break
                        if time.monotonic()>deadline:raise RuntimeError('actual enrollment dataset lock wait absent')
                        time.sleep(.02)
                    record('dataset-lock-'+scenario,other.process.poll() is None,waits=waits);proxy.send('release')
            code=app.wait(15)
            if scenario=='killed':record('killed-before-commit',code==137,exit=code)
            else:
                expected=0 if mode=='hold' else 4
                record('commit-'+scenario,code==expected and (not app.stderr if expected==0 else b'COMMIT outcome unknown' in app.stderr),exit=code,stdout=bytes(app.stdout).decode(),stderr=bytes(app.stderr).decode())
                receipt=proxy.event('receipt',10);expected_counts={'before':[1,0,0,0,1],'after':[1,1,1,0,1],'same':[1,1,1,1,1],'different':[1,1,1,1,1]}[scenario]
                record('proxy-'+scenario,[receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']]==expected_counts and proxy.wait(5)==0 and not proxy.stderr,proxy=receipt)
            if other:
                code=other.wait(15);record('concurrent-'+scenario,code==(0 if scenario=='same' else 3),exit=code,stdout=bytes(other.stdout).decode(),stderr=bytes(other.stderr).decode())
            deadline=time.monotonic()+5
            while lab.sql(state,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';")!='0':
                if time.monotonic()>deadline:raise RuntimeError('enrollment backend remains live')
                time.sleep(.02)
            after=snapshot(db);committed=scenario in ['after','same','different']
            record('outcome-'+scenario,(added_only(baseline,after) if committed else after==baseline),before=baseline,observed=after)
            if mode=='hold' and scenario!='killed':validate('commit-receipt-'+scenario,db,json.loads(app.stdout),keys,created=True)
            if scenario=='same':validate('concurrent-receipt',db,json.loads(other.stdout),keys)
            receipt=call('recover-'+scenario,config);validate('recovery-receipt-'+scenario,db,receipt,keys,created=not committed)
            record('recovery-effects-'+scenario,(snapshot(db)==after if committed else added_only(baseline,snapshot(db))) and keys.read_bytes()==old,before=baseline,observed=snapshot(db))
        baseline=snapshot(database);lab.docker('kill','--signal','KILL',state['postgres']);lab.docker('start',state['postgres']);deadline=time.monotonic()+30
        while lab.docker('exec',state['postgres'],'pg_isready','-h','127.0.0.1','-U','postgres',check=False).returncode:
            if time.monotonic()>deadline:raise RuntimeError('engine did not restart')
            time.sleep(.1)
        receipt=call('restart-retry',path);validate('restart-receipt',database,receipt,file);record('restart-unchanged',snapshot(database)==baseline)
        for db in report['databases']:record('no-backends-'+db,lab.sql(state,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','bigbrotr_migrator');")=='0')
        report['status']='CHECKS_PASSED'
    except BaseException as error:
        report.update(status='FAIL',failure=type(error).__name__);raise
    finally:
        cleanup=[]
        try:
            if fault:
                inspected=lab.docker('inspect',fault,check=False)
                if inspected.returncode==0:
                    c=json.loads(inspected.stdout)[0]
                    assert c['Config']['Labels']['org.bigbrotr.cursor-registration']==state['owner']
                    if 'id' in entry:assert c['Id']==entry['id']
                    if c['State']['Running']:lab.docker('stop','--time','3',fault)
                    c=json.loads(lab.docker('inspect',fault).stdout)[0]
                    assert c['State']['Pid']==0 and not c['State']['Running']
                    entry.update(state='stopped-retained',terminal=c['State'])
                elif 'id' in entry:
                    raise RuntimeError('owned helper observation unavailable')
        except BaseException as error:
            cleanup.append(dict(resource='fault-helper',error=type(error).__name__))
        for index,process in enumerate(processes):
            try:process.stop()
            except BaseException as error:cleanup.append(dict(resource='process-'+str(index),error=type(error).__name__))
        report['cleanup_errors']=cleanup
        if cleanup:report['status']='INCOMPLETE'
        elif report['status']=='CHECKS_PASSED':report['status']='PASS'
        save()
        if cleanup:raise RuntimeError('owned resources require explicit cleanup resolution')


if __name__=='__main__':main()
