#!/usr/bin/env python3
"""Revoke a live signed export's input grant while its actual native SQL waits."""
import copy
import hashlib
import json
import re
import select
import stat
import subprocess
import time
from signed_publication_cases import Process, command, event, send, sha
from signed_publication_oracle import EVENT


def run(root, state, sql, policy, expected_manifest):
    runner, pg = state['container_id'], state['postgres_id']
    db = 'bbpub_control_normal'
    private = root/'out/private-grant-http'
    result = dict(status='INCOMPLETE', source=state['source'])
    path = root/'revocation-cases.json'
    assert not path.exists()
    proxy = client = blocker = None
    def save(): path.write_text(json.dumps(result, indent=2)+'\n')
    def activate(revision, settings):
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'") == '0'
        selected = copy.deepcopy(policy); selected['settings'].update(settings)
        document = json.dumps(selected, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
        sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT {revision},dataset_id,{revision-1},config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision={revision-1}; UPDATE control.datasets SET policy_revision={revision}; COMMIT;")
    def snapshot():
        return {table:json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY {key}),'[]') FROM control.{table} t"))
            for table,key in [('jobs','job_id'),('export_execution_units','job_id'),('artifacts','artifact_id'),('artifact_files','artifact_id,file_id'),('snapshot_leases','lease_id'),('archive_snapshot_pins','lease_id'),('retention_claims','claim_id')]}
    def target(observation):
        return next(v['row'] for v in observation['jobs'] if v['row']['job_id']=='\\x'+job)
    def authority():
        return json.loads(sql(db,"SELECT jsonb_build_object('account',(SELECT jsonb_build_object('principal',encode(principal_id,'hex'),'grant_revision',grant_revision) FROM control.local_accounts),'grants',(SELECT jsonb_agg(jsonb_build_object('action',action,'enabled',enabled,'generation',generation) ORDER BY action) FROM control.scope_grants))"))
    def waiters():
        return json.loads(sql('bbpub_normal',"SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start,'application_name',application_name,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_receipt_reader' AND wait_event_type='Lock'"))
    def denial_lines():
        logs = subprocess.run(['docker','logs',pg],capture_output=True,text=True,timeout=5)
        assert logs.returncode==0 and len(logs.stdout)+len(logs.stderr)<1024*1024
        # Docker preserves stderr for PostgreSQL diagnostics. The shared command
        # helper returns stdout only; keep just this nonsecret error observation.
        return [line for line in (logs.stdout+logs.stderr).splitlines() if 'export execution requires current input and export grants' in line]
    def process_state(parent, child=None):
        script = """import json,sys
from pathlib import Path
parent=int(sys.argv[1]); children=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  raw=(p/'stat').read_text(); fields=raw[raw.rfind(')')+2:].split()
  if int(fields[1])==parent and (p/'exe').resolve().name=='bigbrotr-archive-worker':
   children.append(dict(pid=int(p.name),ppid=parent,start_ticks=int(fields[19])))
 except (FileNotFoundError,PermissionError,ProcessLookupError):pass
print(json.dumps(dict(parent_present=Path('/proc',str(parent)).exists(),children=children,old_child_present=Path('/proc',sys.argv[2]).exists() if len(sys.argv)==3 else None)))
"""
        return json.loads(command('docker','exec',runner,'python3','-c',script,str(parent),*([str(child)] if child else [])))
    def files():
        destination = root/'out/signed_normal/export-destination'
        mutex = destination/('.artifact-'+artifact+'.lock')
        info = mutex.lstat()
        assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o600
        found = dict(namespace=sorted(p.name for p in destination.iterdir()),
            mutex={key:getattr(info,'st_'+key) for key in ['dev','ino','mode','nlink','size']})
        found['mutex']['sha256'] = sha(mutex)
        found['outputs'] = {}
        for name in ['events.jsonl','manifest.json']:
            item = destination/artifact/name; meta = item.lstat()
            assert stat.S_ISREG(meta.st_mode) and meta.st_nlink==1
            found['outputs'][name] = dict(bytes=meta.st_size, sha256=sha(item), inode=meta.st_ino)
        assert not (destination/('.artifact-'+artifact+'.partial')).exists()
        return found
    try:
        activate(3, {'administration.enabled':True,'export.memory.max_bytes':268435456})
        sql(db,'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
        command('docker','exec',runner,'python3','-B','/out/build/source/experiments/export-execution/recovery_client.py','accept')
        accepted = result['acceptance'] = json.loads((private/'executor-signed-accept.json').read_text())
        assert accepted['status']=='PASS' and accepted['logged_out']
        job = accepted['receipt']['job_id']
        before = result['before'] = snapshot()
        assert len(before['jobs'])==2 and target(before)['state']=='accepted'
        initial_authority = result['authority_before'] = authority()
        assert initial_authority['account']['grant_revision']==4
        assert initial_authority['grants']==[dict(action=action,enabled=True,generation=1) for action in ['export','history.read','private-export']]
        activate(4, {'administration.enabled':True,'export.memory.max_bytes':1073741824})
        # Seed login + three proofs + acceptance exhaust the real account window.
        # Wait with no parent running; never reset counters or increase policy.
        result['rate_before'] = json.loads(sql(db,"SELECT jsonb_object_agg(kind,attempts) FROM control.login_buckets"))
        assert result['rate_before']==dict(account=5,peer=5)
        save(); started = time.monotonic(); time.sleep(60)
        result['rate_wait_seconds'] = time.monotonic()-started
        proxy = Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode','hold-reader-query'],input_pipe=True)
        hello = result['proxy_ready'] = event(proxy,'ready')
        assert hello['source_sha256']==sha(root/'source/experiments/archive-engine/native_commit_proxy.py')
        client = Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/revocation_client.py','attempt',str(hello['port'])],input_pipe=True)
        ready = result['ready'] = event(client,'ready')
        held = result['held'] = event(proxy,'held')
        assert held['stage']=='reader-query'
        result['running'] = snapshot()
        assert target(result['running'])['state']=='running' and target(result['running'])['job_revision']==3
        controls = json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start,'state',state)) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'"))
        assert len(controls)==1 and controls[0]['state']=='idle'
        result['control_backend'] = controls[0]
        denial = 'export execution requires current input and export grants'
        assert not denial_lines()
        parent = ready['parent_pid']
        result['processes_held'] = process_state(parent)
        assert result['processes_held']['parent_present'] and len(result['processes_held']['children'])==1
        child = result['processes_held']['children'][0]['pid']
        blocker = subprocess.Popen(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','bbpub_normal'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        blocker.stdin.write("BEGIN; SET LOCAL lock_timeout='1s'; LOCK TABLE public.ducklake_data_file IN ACCESS EXCLUSIVE MODE; SELECT pg_backend_pid();\n");blocker.stdin.flush()
        assert select.select([blocker.stdout],[],[],2)[0]
        holder = result['holder_pid'] = int(blocker.stdout.readline())
        send(proxy,'release\n'); result['released'] = event(proxy,'released')
        deadline = time.monotonic()+2
        while not (waiting := waiters()):
            assert time.monotonic()<deadline, 'actual native SQL wait absent'
            time.sleep(.02)
        assert len(waiting)==1 and waiting[0]['pid']==held['backend_pid'] and holder in waiting[0]['blockers']
        assert waiting[0]['application_name']=='bigbrotr-archive-event'
        result['waiter_before'] = waiting[0]; save()
        send(client,'revoke\n'); result['revoked'] = event(client,'revoked',timeout=5)
        result['waiter_after'] = waiters()
        assert result['waiter_after']==waiting, 'revocation was not interleaved with actual blocked native SQL'
        result['authority_after'] = authority()
        assert result['authority_after']==dict(account={**initial_authority['account'],'grant_revision':5},grants=[dict(action=action,enabled=action!='history.read',generation=2 if action=='history.read' else 1) for action in ['export','history.read','private-export']])
        result['revoked_while_running'] = snapshot()
        assert result['revoked_while_running']==result['running']
        blocker.stdin.write('ROLLBACK;\n');blocker.stdin.close()
        result['blocker_exit'] = blocker.wait(timeout=5);result['blocker_error'] = blocker.stderr.read()
        assert result['blocker_exit']==0 and not result['blocker_error']
        deadline = time.monotonic()+8
        while True:
            observed = snapshot(); row = target(observed)
            if row['state']=='paused': break
            assert time.monotonic()<deadline, ('export did not park', row)
            time.sleep(.05)
        assert row['job_revision']==4 and row['reason_code']=='execution_refused'
        assert row['checkpoint_revision']==0 and row['result_manifest_digest'] is None and row['completed_at_us'] is None
        # Capture the completed refusal, then sample several queue intervals.
        result['paused'] = observed; time.sleep(1.5)
        assert snapshot()==observed
        # Distinguish actual completion authority refusal from a generic native
        # timeout/capture refusal that could also produce execution_refused.
        errors = denial_lines()
        assert len(errors)==1 and re.search(r'\['+str(controls[0]['pid'])+r'\] ERROR:  '+denial+r'$',errors[0])
        result['completion_authority_error'] = errors[0]
        result['processes_after'] = process_state(parent,child)
        assert result['processes_after']==dict(parent_present=True,children=[],old_child_present=False)
        for table in ['artifacts','artifact_files']:
            assert observed[table]==before[table]
        assert len(observed['jobs'])==len(before['jobs'])
        assert all(row in observed['jobs'] for row in before['jobs'] if row['row']['job_id']!='\\x'+job)
        assert len(observed['export_execution_units'])==len(before['export_execution_units'])+1
        assert all(row in observed['export_execution_units'] for row in before['export_execution_units'])
        for table in ['snapshot_leases','archive_snapshot_pins','retention_claims']:
            assert all(row in observed[table] for row in before[table]), table
            assert len(observed[table])==len(before[table])+1, table
        readers = [v['row'] for v in observed['archive_snapshot_pins'] if v not in before['archive_snapshot_pins']]
        assert len(readers)==1 and readers[0]['purpose']=='reader'
        reader = readers[0]['lease_id']; result['released_reader'] = reader
        assert next(v['row']['state'] for v in observed['snapshot_leases'] if v['row']['lease_id']==reader)=='released'
        claims = [v['row'] for v in observed['retention_claims'] if v not in before['retention_claims']]
        assert len(claims)==1 and claims[0]['lease_id']==reader and claims[0]['state']=='released'
        assert claims[0]['first_sequence']==1 and claims[0]['last_sequence']==10 and claims[0]['release_reason']=='reader_joined'
        unit = next(v for v in observed['export_execution_units'] if v['row']['job_id']=='\\x'+job)
        assert unit in result['running']['export_execution_units']
        assert unit['row']['max_output_bytes']==2097152 and unit['row']['max_temporary_bytes']==4194304
        artifact = unit['row']['artifact_id'][2:]
        directory = root/'out/signed_normal/export-destination'/artifact
        # A previously admitted finite unit may finish private filesystem work;
        # the revoked actor cannot commit a completion or advertise these files.
        signed = (json.dumps({key:EVENT[key] for key in ['id','pubkey','created_at','kind','tags','content','sig']},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        assert len(signed)==399 and (directory/'events.jsonl').read_bytes()==signed
        manifest = copy.deepcopy(expected_manifest)
        manifest.update(artifact_id=artifact,job_id=job)
        manifest['selection']['event_id'] = EVENT['id'];manifest['accepted_revisions']['policy'] = '3'
        manifest['files'][0].update(bytes='399',rows='1',sha256=hashlib.sha256(signed).hexdigest())
        assert json.loads((directory/'manifest.json').read_bytes())==manifest
        result['files'] = files()
        result['output_hashes'] = {str((directory/name).relative_to(root/'out')):sha(directory/name) for name in ['events.jsonl','manifest.json']}
        send(client,'stop\n');assert client.wait(10)==0
        result['client_terminal'] = event(client,'terminal')
        assert not client.stderr and not client.overflow
        client.stop();client=None
        send(proxy,'stop\n');assert proxy.wait(5)==0
        result['proxy_terminal'] = event(proxy,'terminal')
        assert result['proxy_terminal']['unexpected']==0 and not proxy.stderr and not proxy.overflow
        proxy.stop();proxy=None
        assert snapshot()==observed
        client = Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/revocation_client.py','restart','0'],input_pipe=True)
        result['restart_ready'] = event(client,'ready');time.sleep(1.5)
        assert snapshot()==observed and files()==result['files'] and authority()==result['authority_after']
        send(client,'stop\n');assert client.wait(10)==0
        result['restart_terminal'] = event(client,'terminal')
        assert not client.stderr and not client.overflow
        result['terminal'] = snapshot();result['terminal_files'] = files()
        assert result['terminal']==observed and result['terminal_files']==result['files']
        result['backends'] = {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        assert result['backends']=={db:0,'bbpub_normal':0}
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        result['status']='PASS'
        return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        errors=[]
        for label,process in [('client',client),('proxy',proxy)]:
            if process is not None:
                try: process.stop()
                except BaseException as error: errors.append(dict(stage=label,error=type(error).__name__))
                result[label+'_output']=process.stdout.decode();result[label+'_error']=process.stderr.decode()
        if blocker is not None and blocker.poll() is None:
            try:
                blocker.stdin.write('ROLLBACK;\n');blocker.stdin.close();blocker.wait(timeout=5)
            except BaseException as error:
                errors.append(dict(stage='blocker',error=type(error).__name__))
                if blocker.poll() is None: blocker.kill();blocker.wait(timeout=5)
        result['cleanup_errors']=errors
        if errors: result['status']='INCOMPLETE'
        save()
