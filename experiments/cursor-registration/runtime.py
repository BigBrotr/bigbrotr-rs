#!/usr/bin/env python3
"""Actual serve/key binding, private SQL roles and bounded startup fault cases."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import re
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]


def process_start(pid):
    return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]


def child(args):
    command = [args.binary, 'serve', '--file', args.config, '--roles', 'control']
    with args.record.open('x') as output:
        json.dump(dict(pid=os.getpid(), start=process_start(os.getpid()), command=command), output)
    os.execve(args.binary, command, {})


def stop(args):
    record = json.loads(args.record.read_text()); pid = record['pid']
    assert process_start(pid) == record['start']
    actual = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')[:-1]
    assert actual == [s.encode() for s in record['command']]
    os.kill(pid, getattr(signal, 'SIG'+args.signal))


def main(root):
    sys.path.insert(0, str(SOURCE/'experiments/control-migrations'))
    import lab
    from fault_cases import Process
    state = lab.load(root)
    revision = lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD']).stdout.strip()
    assert not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout
    info = json.loads(lab.cli(state,'build-info').stdout)
    assert info == state['build']['build_info'] and info['source']['revision'] == revision
    assert lab.docker('exec',state['client'],'sha256sum','/build/bigbrotr').stdout.split()[0] == state['binary_sha256']
    output = root/'cursor-runtime.json'; assert not output.exists()
    report = dict(source=revision,binary_sha256=state['binary_sha256'],server=state['server'],status='RUNNING',cases=[],databases=[])
    vectors = json.loads((SOURCE/'tests/fixtures/read/keyring-vectors.json').read_text())
    wire = bytes.fromhex(vectors['cases'][0]['hex'])
    processes = []; fault = state['client']+'-cursor-fault'
    def save():
        temporary=output.with_suffix('.partial');temporary.write_text(json.dumps(report,indent=2)+'\n');temporary.replace(output)
    def record(name, ok, **detail):
        assert not any(c['name']==name for c in report['cases'])
        report['cases'].append(dict(name=name,status='PASS' if ok else 'FAIL',**detail));save()
        if not ok: raise RuntimeError('case failed: '+name)
    def call(path, *args):
        result=lab.cli({**state,'client':fault},*args,'--file',path)
        assert result.returncode==0 and not result.stderr, (args,result.returncode,result.stderr)
        return json.loads(result.stdout)
    def prepare(name, target=602, data=wire, register=True):
        database='bb_keys_'+name
        lab.sql(state,'postgres',f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        report['databases'].append(database);save()
        path=lab.config(root,state,database,endpoint='127.0.0.1:5432',boundary='loopback');local=root/Path(path).name
        text,count=re.subn(r'(?ms)^local_roles = \[.*?\]', 'local_roles = ["control"]', local.read_text());assert count==1
        text=lab.change(text,'topology','dataset_id',vectors['dataset_id'])
        text=lab.change(text,'administration','enabled',False)
        text=lab.change(text,'postgres.control','password_ref','file:/lab/secrets/application')
        local.write_text(text)
        result=lab.migrate({**state,'client':fault},'apply',path,target=target)
        record('migrate-'+name,result.returncode==0,stderr=result.stderr)
        lab.sql(state,database,'GRANT USAGE ON SCHEMA control TO bigbrotr_control,bigbrotr_runtime; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control,bigbrotr_runtime; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_runtime; GRANT INSERT,UPDATE ON control.role_instances,control.owner_epochs TO bigbrotr_runtime; GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_runtime;')
        call(path,'config','initialize')
        call(path,'config','activate-initial','--request-id','71'*16,'--expected-revision','1')
        keys=root/('keys-'+name);keys.mkdir(mode=0o700)
        file=keys/'ring';file.write_bytes(data);file.chmod(0o600)
        local.write_text(text+f'\n[read.cursor]\nkeyring_ref = "file:/lab/{keys.name}/ring"\n')
        if register: call(path,'access','cursor-keyring','register')
        local.write_text(lab.change(local.read_text(),'postgres.control','user','bigbrotr_runtime'))
        return database,path,local,file
    def snapshot(database):
        tables=json.loads(lab.sql(state,database,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control' AND tablename IN ('cursor_keyrings','cursor_keys','cursor_key_materials','management_audit','datasets','config_revisions','policy_revisions');"))
        return {table:json.loads(lab.sql(state,database,f"SELECT coalesce(json_agg(t ORDER BY row_to_json(t)::text),'[]') FROM (SELECT *,xmin::text AS row_version FROM control.{table}) t;")) for table in tables}
    def start(path,name,binary='/build/bigbrotr'):
        process=Process(['docker','exec',fault,'python3','-B','/source/experiments/cursor-registration/runtime.py','--child','--binary',binary,'--config',path,'--record','/lab/'+name+'.process.json'])
        processes.append(process);return process
    def signal_child(name, sig='TERM'):
        lab.docker('exec',fault,'python3','-B','/source/experiments/cursor-registration/runtime.py','--signal',sig,'--record','/lab/'+name+'.process.json')
    def idle(process):
        end=time.monotonic()+8
        while time.monotonic()<end:
            if b'\n' in process.stdout:
                value=json.loads(bytes(process.stdout).splitlines()[0])
                assert value['state']=='idle' and value['listeners']==[] and not process.stderr
                return value
            assert process.process.poll() is None
            time.sleep(.02)
        raise RuntimeError('runtime idle not observed')
    def terminal(process,name,code):
        actual=process.wait(10)
        (root/(name+'.stdout')).write_bytes(process.stdout);(root/(name+'.stderr')).write_bytes(process.stderr)
        record('terminal-'+name,actual==code,exit=actual,stderr=bytes(process.stderr).decode())
    def positive(database,path,file,name,binary='/build/bigbrotr'):
        before=snapshot(database);data=file.read_bytes();inode=file.stat().st_ino
        process=start(path,name,binary);observed=idle(process)
        if binary=='/build/bigbrotr':
            expected=dict(state='enrolled-local-snapshot',dataset_id=data[8:24].hex(),keyring_id=data[24:40].hex(),key_ids=[data[i:i+16].hex() for i in range(42,len(data)-32,48)],cursor_authority='not-granted')
            record('observation-'+name,observed['cursor_keyring']==expected,observed=observed,expected=expected)
        else: record('negative-control-'+name,'cursor_keyring' not in observed,observed=observed)
        owner=json.loads(lab.sql(state,database,"SELECT row_to_json(o) FROM control.owner_epochs o JOIN pg_stat_activity a ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start WHERE o.state='active' AND a.usename='bigbrotr_runtime';"))
        record('owner-'+name,owner['dataset_id']=='\\x'+observed['dataset_id'] and owner['resource_id']=='\\x'+observed['owner_resource_id'] and str(owner['epoch'])==observed['owner_epoch'] and owner['process_incarnation']=='\\x'+observed['process_incarnation'],owner=owner)
        signal_child(name);terminal(process,name,0)
        record('released-'+name,lab.sql(state,database,"SELECT count(*) FROM control.owner_epochs WHERE state<>'released';")=='0')
        record('unchanged-'+name,snapshot(database)==before and file.read_bytes()==data and file.stat().st_ino==inode,before=before,after=snapshot(database))
        return observed
    def key_state(path):
        text=(root/Path(path).name).read_text()
        reference,=re.findall(r'(?m)^keyring_ref = "file:(.+)"$',text)
        relative=Path(reference).relative_to('/lab');directory=(root/relative).parent
        assert directory.is_relative_to(root) and directory.is_dir()
        return {p.name:dict(inode=p.stat().st_ino,mode=p.stat().st_mode,uid=p.stat().st_uid,nlink=p.stat().st_nlink,size=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(directory.iterdir())}
    def refusal(database,path,name,code=3,reason='IncompatibleDeployment'):
        before=snapshot(database);keys=key_state(path);process=start(path,name);terminal(process,name,code)
        record('refusal-'+name,not process.stdout and reason.encode() in process.stderr and snapshot(database)==before and key_state(path)==keys,before=before,after=snapshot(database),keys_before=keys,keys_after=key_state(path))
    def mutate(file,offset):
        data=bytearray(file.read_bytes());data[offset]^=1;data[-32:]=hashlib.sha256(data[:-32]).digest();file.write_bytes(data)
    def blocker(database,name):
        process=Process(['docker','exec','-i',state['postgres'],'psql','-X','-A','-t','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d',database],input_pipe=True)
        processes.append(process)
        process.process.stdin.write(f"BEGIN; SET LOCAL application_name='{name}'; SELECT 1 FROM control.datasets FOR UPDATE;\n".encode());process.process.stdin.flush()
        return process
    def release_blocker(process):
        process.process.stdin.write(b'ROLLBACK;\n');process.process.stdin.close();process.process.stdin=None
        assert process.wait(8)==0 and not process.stderr
    def wait_lock(database,username,query):
        end=time.monotonic()+3
        while time.monotonic()<end:
            rows=json.loads(lab.sql(state,database,f"SELECT coalesce(json_agg(json_build_object('pid',pid,'blockers',pg_blocking_pids(pid),'query',query)),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='{username}' AND wait_event_type='Lock' AND query LIKE '%{query}%';"))
            if rows and all(r['blockers'] for r in rows):return rows
            time.sleep(.01)
        raise RuntimeError('expected dataset lock wait absent')
    save()
    try:
        password=(root/'secrets/application').read_text()
        lab.sql(state,'postgres',f"CREATE ROLE bigbrotr_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}';")
        del password
        mounts=json.loads(lab.docker('inspect',state['client']).stdout)[0]['Mounts']
        build=next(m['Source'] for m in mounts if m['Destination']=='/build' and not m['RW'])
        assert lab.docker('inspect',fault,check=False).returncode!=0
        cid=lab.docker('run','-d','--init','--name',fault,'--label','org.bigbrotr.cursor-registration='+state['owner'],'--network','container:'+state['postgres'],'--user',f'{os.getuid()}:{os.getgid()}','--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--mount',f'type=bind,src={root},dst=/lab','--mount',f'type=bind,src={build},dst=/build,readonly','--mount',f'type=bind,src={SOURCE},dst=/source,readonly',state['client_image'],'python3','-c','import time; time.sleep(1800)').stdout.strip()
        report['fault_container']=dict(id=cid,name=fault,owner=state['owner']);save()
        # Fault container shares the PG namespace; all application endpoints are loopback.
        db,path,local,file=prepare('bindings')
        local.write_text(lab.change(lab.change(local.read_text(),'postgres.control','endpoint','127.0.0.1:5432'),'postgres.control.tls','local_boundary','loopback'))
        positive(db,path,file,'initial')
        assert lab.sql(state,db,"SELECT count(*) FROM control.principals WHERE database_role='bigbrotr_runtime';")=='0'
        record('runtime-has-no-provisioning-actor',lab.sql(state,db,"SELECT has_table_privilege('bigbrotr_runtime','control.cursor_key_materials','INSERT');")=='f')
        positive(db,path,file,'restart')
        mutate(file,58)
        positive(db,path,file,'old-ignores-wrong-material','/build/previous-bigbrotr')
        refusal(db,path,'wrong-material')
        file.write_bytes(wire);mutate(file,24);refusal(db,path,'wrong-ring')
        file.write_bytes(wire);mutate(file,42);refusal(db,path,'missing-id')
        file.write_bytes(wire)
        local.write_text(lab.change(local.read_text(),'postgres.control','user','bigbrotr_control'))
        call(path,'access','cursor-keyring','prepare-key','--key-id','55'*16)
        local.write_text(lab.change(local.read_text(),'postgres.control','user','bigbrotr_runtime'))
        refusal(db,path,'unenrolled-addition')
        local.write_text(lab.change(local.read_text(),'postgres.control','user','bigbrotr_control'))
        call(path,'access','cursor-keyring','enroll-key','--key-id','55'*16)
        local.write_text(lab.change(local.read_text(),'postgres.control','user','bigbrotr_runtime'))
        positive(db,path,file,'additional')
        data=file.read_bytes();body=data[:40]+b'\0\1'+data[138:186];file.write_bytes(body+hashlib.sha256(body).digest())
        positive(db,path,file,'only-additional-subset')
        file.write_bytes(data[:42]+data[138:186]+data[90:138]+data[42:90]+b'\0'*32)
        reordered=file.read_bytes();file.write_bytes(reordered[:-32]+hashlib.sha256(reordered[:-32]).digest())
        positive(db,path,file,'reordered')
        lab.sql(state,db,'REVOKE SELECT ON control.cursor_key_materials FROM bigbrotr_runtime;')
        refusal(db,path,'missing-select',1,'Backend')
        lab.sql(state,db,'GRANT SELECT ON control.cursor_key_materials TO bigbrotr_runtime;')
        positive(db,path,file,'permission-recovery')
        for name,target,register in [('no-ring',602,False),('old-schema',601,True)]:
            other,config,root_file,keys=prepare(name.replace('-','_'),target,register=register)
            root_file.write_text(lab.change(lab.change(root_file.read_text(),'postgres.control','endpoint','127.0.0.1:5432'),'postgres.control.tls','local_boundary','loopback'))
            refusal(other,config,name)
        # The same new binary still serves a pre-keyring Control schema without the root.
        old,config,root_file,keys=prepare('unselected',9,register=False)
        text=root_file.read_text();root_file.write_text(text[:text.index('\n[read.cursor]')])
        root_file.write_text(lab.change(lab.change(root_file.read_text(),'postgres.control','endpoint','127.0.0.1:5432'),'postgres.control.tls','local_boundary','loopback'))
        baseline=snapshot(old);process=start(config,'unselected');observed=idle(process)
        record('unselected-old-schema',observed['cursor_keyring']==dict(state='not-selected',cursor_authority='not-granted'),observed=observed)
        signal_child('unselected');terminal(process,'unselected',0);record('unselected-unchanged',snapshot(old)==baseline)
        full=bytes.fromhex(next(c['hex'] for c in vectors['cases'] if c['name']=='sixteen-public-keys'))
        full_db,full_path,full_config,full_keys=prepare('sixteen',data=full)
        full_config.write_text(lab.change(lab.change(full_config.read_text(),'postgres.control','endpoint','127.0.0.1:5432'),'postgres.control.tls','local_boundary','loopback'))
        positive(full_db,full_path,full_keys,'sixteen')
        # Hold acquisition COMMIT, enqueue a dataset locker, then release COMMIT.
        # The locker wins before the runtime's second transaction, without races.
        for scenario in ['wait-release','wait-expired','wait-cancel','wait-timeout']:
            database,config,root_file,keys=prepare(scenario.replace('-','_'))
            text=root_file.read_text()
            if scenario=='wait-expired':
                text+='\n[runtime]\nlease_ttl = "2500ms"\nheartbeat_interval = "100ms"\nrenewal_timeout = "2s"\nrenewal_safety_margin = "100ms"\n'
            root_file.write_text(text)
            proxy=Process(['docker','exec','-i',fault,'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--mode','hold','--later-commits','16','--deadline-seconds','45'],input_pipe=True);processes.append(proxy)
            ready=proxy.event('ready',8)
            assert ready['proxy_sha256']==hashlib.sha256((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest()
            proxied=root/(scenario+'-proxy.toml');proxied.write_text(lab.change(lab.change(text,'postgres.control','endpoint',f'127.0.0.1:{ready["listen_port"]}'),'postgres.control.tls','local_boundary','loopback'))
            baseline=snapshot(database);process=start('/lab/'+proxied.name,scenario)
            held=proxy.event('held',8);assert held['commits_seen']==1 and held['commits_forwarded']==0
            lock=blocker(database,'cursor_'+scenario.replace('-','_'))
            waits=wait_lock(database,'postgres','control.datasets')
            if scenario=='wait-expired':time.sleep(1.2)
            proxy.send('release')
            waits+=wait_lock(database,'bigbrotr_runtime','control.datasets')
            record('actual-lock-'+scenario,not process.stdout,waits=waits,held=held)
            if scenario=='wait-expired':
                end=time.monotonic()+2
                while True:
                    expiry=json.loads(lab.sql(state,database,"SELECT json_build_object('acquired_at_us',acquired_at_us,'lease_expires_at_us',lease_expires_at_us,'observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) FROM control.owner_epochs WHERE state='active';"))
                    if expiry['observed_at_us']>expiry['lease_expires_at_us']:break
                    assert time.monotonic()<end;time.sleep(.01)
                record('real-owner-lease-expired',process.process.poll() is None,observed=expiry)
            if scenario in ['wait-release','wait-expired']:release_blocker(lock)
            if scenario=='wait-release':
                observed=idle(process);record('wait-release-binding',observed['cursor_keyring']['state']=='enrolled-local-snapshot',observed=observed)
                signal_child(scenario);terminal(process,scenario,0)
            else:
                if scenario=='wait-cancel':signal_child(scenario)
                terminal(process,scenario,4)
                reason={'wait-expired':b'Unavailable','wait-cancel':b'cancelled','wait-timeout':b'deadline exceeded'}[scenario]
                record('bounded-refusal-'+scenario,not process.stdout and reason in process.stderr)
                if scenario!='wait-expired':release_blocker(lock)
            receipt=proxy.event('receipt',8);assert proxy.wait(8)==0 and not proxy.stderr
            counts=[receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete']]
            record('proxy-counts-'+scenario,len(set(counts))==1 and (2<=counts[0]<=17 if scenario=='wait-release' else counts[0]==1) and receipt['faults']==1 and receipt['hold_action']=='release',observed=receipt)
            record('no-key-or-policy-write-'+scenario,snapshot(database)==baseline,before=baseline,after=snapshot(database),proxy=receipt)
            # Direct restart with the same local bytes resolves prior owner custody.
            root_file.write_text(lab.change(lab.change(text,'postgres.control','endpoint','127.0.0.1:5432'),'postgres.control.tls','local_boundary','loopback'))
            positive(database,config,keys,scenario+'-recovery')
        for database in report['databases']:
            record('no-backends-'+database,lab.sql(state,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND (usename IN ('bigbrotr_control','bigbrotr_runtime','bigbrotr_migrator') OR application_name LIKE 'cursor_%');")=='0')
        report['status']='CHECKS_PASSED'
    except BaseException as error:
        report.update(status='INCOMPLETE',failure=str(error));raise
    finally:
        errors=[]
        for index,process in enumerate(processes):
            try:process.stop()
            except BaseException as error:errors.append(dict(process=index,operation='stop',error=type(error).__name__))
            try:
                (root/f'process-{index:02d}.stdout').write_bytes(process.stdout)
                (root/f'process-{index:02d}.stderr').write_bytes(process.stderr)
                report.setdefault('processes',[]).append(dict(index=index,args=process.process.args,exit=process.process.poll(),overflow=process.overflow))
            except BaseException as error:errors.append(dict(process=index,operation='capture',error=type(error).__name__))
        report['process_cleanup_errors']=errors
        report['status']='PASS' if not errors and report['status']=='CHECKS_PASSED' else 'INCOMPLETE'
        save()
        if errors:raise RuntimeError('local process cleanup/capture incomplete; enclosing owner must stop containers')
    print(json.dumps(dict(status=report['status'],checks=len(report['cases']))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path)
    parser.add_argument('--child',action='store_true')
    parser.add_argument('--signal',choices=['TERM','INT'])
    parser.add_argument('--record',type=Path)
    parser.add_argument('--config')
    parser.add_argument('--binary',choices=['/build/bigbrotr','/build/previous-bigbrotr'],default='/build/bigbrotr')
    args=parser.parse_args()
    if args.child:child(args)
    elif args.signal:stop(args)
    else:main(args.root.resolve(strict=True))
