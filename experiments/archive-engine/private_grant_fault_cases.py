#!/usr/bin/env python3
"""Actual blocked native HTTP query: authority renewal, deadline and signal join.

Run after private_grant_cases.py in the same exclusively owned laboratory. Every
assertion observes a real parent/child/backend; a proxy marker alone is not SQL wait.
"""
import json
from pathlib import Path
import select
import subprocess
import sys
import time
sys.dont_write_bytecode=True
from signed_publication_cases import command, event, send, sha, Process


def main(root):
    resources=json.loads((root/'resources.json').read_text())
    source=root/'source'
    assert Path(__file__).resolve().parents[2]==source
    assert command('git','-C',str(source),'rev-parse','HEAD')==resources['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    runner,pg=resources['container_id'],resources['postgres_id']
    for identity in [runner,pg]:
        c=json.loads(command('docker','inspect',identity))[0]
        assert c['Config']['Labels'][resources['owner_label']]==resources['owner'] and c['State']['Running']
    result=dict(source=resources['source'],tree=resources['tree'],status='INCOMPLETE',cases=[])
    path=root/'private-grant-fault-cases.json'
    assert not path.exists()
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def sql(database,query):return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=query)
    def counts():return json.loads(sql('bbpub_control_normal',"SELECT json_build_object('active_pins',(SELECT count(*) FROM control.snapshot_leases WHERE state='active'),'proofs',(SELECT count(*) FROM control.reauthentication_proofs),'commands',(SELECT count(*) FROM control.management_commands WHERE action='scope-grant.change'),'active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'))"))
    try:
        assert json.loads((root/'private-grant-cases.json').read_text())['status']=='PASS'
        for mode in ['deadline','signal']:
            record=dict(case='http-native-blocked-'+mode,status='INCOMPLETE');result['cases'].append(record);save()
            proxy=client=blocker=None
            try:
                proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode','hold-reader-query'],input_pipe=True)
                hello=record['proxy_ready']=event(proxy,'ready')
                assert hello['source_sha256']==sha(source/'experiments/archive-engine/native_commit_proxy.py')
                client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/private_grant_fault_client.py',mode,str(hello['port'])],input_pipe=True)
                submitted=record['submitted']=event(client,'submitted')
                held=record['reader_authenticated']=event(proxy,'held')
                assert held['stage']=='reader-query'
                record['retained_before_wait']=counts();save()
                assert record['retained_before_wait']['active_pins']==1
                assert sql('bbpub_normal',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='CatalogManager' AND state='idle in transaction'")=='1'
                blocker=subprocess.Popen(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','bbpub_normal'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                blocker.stdin.write("BEGIN; SET LOCAL lock_timeout='1s'; LOCK TABLE public.ducklake_data_file IN ACCESS EXCLUSIVE MODE; SELECT pg_backend_pid();\n");blocker.stdin.flush()
                assert select.select([blocker.stdout],[],[],2)[0]
                holder=int(blocker.stdout.readline())
                send(proxy,'release\n');event(proxy,'released')
                deadline=time.monotonic()+2
                while True:
                    waiting=json.loads(sql('bbpub_normal',"SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'application_name',application_name,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_receipt_reader' AND wait_event_type='Lock'"))
                    if waiting:break
                    assert time.monotonic()<deadline,'actual native SQL wait absent'
                    time.sleep(.02)
                assert len(waiting)==1 and holder in waiting[0]['blockers'] and waiting[0]['application_name']=='bigbrotr-archive-visibility'
                record['sql_waiter']=waiting[0]
                # Actual parent/child relationship and process start time, never
                # a guessed PID or an unrelated shared-host process.
                process_script="""import json,os
from pathlib import Path
parent=int(__import__('sys').argv[1]); rows=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  stat=(p/'stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
  if int(fields[1])==parent and (p/'exe').resolve().name=='bigbrotr-archive-worker':
   rows.append(dict(pid=int(p.name),ppid=parent,start_ticks=int(fields[19])))
 except (FileNotFoundError,PermissionError,ProcessLookupError):pass
print(json.dumps(rows))
"""
                children=json.loads(command('docker','exec',runner,'python3','-c',process_script,str(submitted['parent_pid'])))
                assert len(children)==1
                record['native_child']=children[0];save()
                if mode=='signal':
                    assert sql('bbpub_control_normal',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND state='idle'")=='1'
                    command('docker','exec',runner,'kill','-TERM',str(submitted['parent_pid']))
                started=time.monotonic();send(client,'observe\n')
                record['client_exit']=client.wait(12)
                record['completion_ms']=round((time.monotonic()-started)*1000,3)
                record['client_output']=client.stdout.decode();record['client_error']=client.stderr.decode();save()
                assert record['client_exit']==0 and not client.stderr and not client.overflow
                record['client_terminal']=event(client,'terminal')
                # The parent must positively reap its child, not just close HTTP.
                assert command('docker','exec',runner,'python3','-c',"from pathlib import Path; import sys; print(Path('/proc',sys.argv[1]).exists())",str(children[0]['pid']))=='False'
                record['durable_after']=counts();save()
                assert record['durable_after']==dict(active_pins=0,proofs=1 if mode=='deadline' else 2,commands=2,active_control=0)
                record['status']='PASS'
            finally:
                if blocker is not None:
                    if blocker.poll() is None:
                        blocker.stdin.write('ROLLBACK;\n');blocker.stdin.close()
                    record['blocker_exit']=blocker.wait(timeout=5)
                    record['blocker_error']=blocker.stderr.read()
                if client is not None:
                    record['client_exit']=client.process.poll()
                    record['client_output']=client.stdout.decode();record['client_error']=client.stderr.decode();client.stop()
                if proxy is not None:
                    if proxy.process.poll() is None:send(proxy,'stop\n')
                    record['proxy_exit']=proxy.wait(5);record['proxy_terminal']=event(proxy,'terminal');proxy.stop()
                record['retained_terminal']=counts();save()
        result['status']='PASS'
    except BaseException as error:
        result['failure']=type(error).__name__
        raise
    finally:save()
    print(json.dumps(dict(status=result['status'],cases=len(result['cases']),source=resources['source'])))


if __name__=='__main__':main(Path(sys.argv[1]).resolve())
