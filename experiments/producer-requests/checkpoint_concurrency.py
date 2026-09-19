#!/usr/bin/env python3
"""Independent job/record-closure lock schedules after real signed checkpoints.

The harness may be a later clean Git revision; record both identities and require
identical producer migration bytes. No product source is edited in the laboratory.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/control-migrations'))
from fault_cases import Process


def command(*args, input=None):
    p=subprocess.run(args,input=input,text=True,capture_output=True,timeout=30)
    assert p.returncode==0, p.stderr
    return p.stdout.strip()


def send(p, sql):
    p.process.stdin.write(sql.encode());p.process.stdin.flush()


def marker(p, name, timeout=10):
    deadline=time.monotonic()+timeout
    with p.condition:
        while name.encode() not in bytes(p.stdout).splitlines():
            assert p.process.poll() is None and not p.overflow, 'premature SQL client exit'
            assert time.monotonic()<deadline, 'SQL marker deadline'
            p.condition.wait(.05)


def mark(name): return '\n\\echo '+name+'\n'


def main(root):
    state=json.loads((root/'resources.json').read_text())
    assert state['root']==str(root)
    harness=command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain')
    assert not command('git','-C',str(SOURCE),'diff',state['source'],harness,'--','apps','crates','migrations','Cargo.lock','Cargo.toml')
    product=root/'source'
    assert command('git','-C',str(product),'rev-parse','HEAD')==state['source']
    assert not command('git','-C',str(product),'status','--porcelain')
    assert (SOURCE/'migrations/postgres/0014_import_checkpoints.sql').read_bytes()==(product/'migrations/postgres/0014_import_checkpoints.sql').read_bytes()
    pg=state['postgres_id']
    container=json.loads(command('docker','inspect',pg))[0]
    assert container['Config']['Labels']['org.bigbrotr.publication']==state['owner'] and container['State']['Running']
    prior_path=root/'import-checkpoints-reverse1.json'
    prior=json.loads(prior_path.read_text())
    assert prior['status']=='PASS' and prior['source']==state['source']
    template='bbcheckpoint_reverse1'
    path=root/'import-checkpoint-concurrency-r1.json';assert not path.exists()
    report=dict(status='INCOMPLETE',source=state['source'],harness_revision=harness,
        harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        native_report_sha256=hashlib.sha256(prior_path.read_bytes()).hexdigest(),cases=[],operations=[])
    fixture=(SOURCE/'experiments/producer-requests/binding_schema_cases.sql').read_text()
    claim=fixture[:fixture.index('SELECT pg_temp.job(1001);')]
    # Scope the native prerequisite to its original producer. The race then
    # creates a separate synthetic reservation/outcome, not native attestation.
    claim=claim.replace('count(*)=3 FROM control.producer_requests', "count(*)=3 FROM control.producer_requests WHERE producer_slot_id=decode(repeat('42',16),'hex')")
    claim=claim.replace('count(*)=2 FROM control.producer_input_bindings', "count(*)=2 FROM control.producer_input_bindings WHERE producer_slot_id=decode(repeat('42',16),'hex')")
    claim=claim.replace('reserved_through_request=3 FROM control.producer_epochs', "reserved_through_request=3 FROM control.producer_epochs WHERE producer_slot_id=decode(repeat('42',16),'hex')")
    claim=claim.replace('pg_temp.id(225)','pg_temp.id(230)').replace('pg_temp.id(226)','pg_temp.id(231)')
    closure_fixture=(SOURCE/'experiments/producer-requests/checkpoint_schema_cases.sql').read_text()
    claim += closure_fixture[:closure_fixture.index('SELECT pg_temp.job(2001);')]
    claim += "SELECT pg_temp.job(2001); SELECT pg_temp.shard_record(2001,1,0,1);\n"
    close='SELECT pg_temp.close_record(2001,1);'
    pause="UPDATE control.jobs SET state='paused',job_revision=job_revision+1,reason_code='operator_requested' WHERE job_id=decode(lpad(to_hex(2001),32,'0'),'hex');"
    snapshot="SELECT state FROM control.jobs WHERE job_id=decode(lpad(to_hex(2001),32,'0'),'hex');"
    def save():path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db,text):
        item=dict(database=db,sql_sha256=hashlib.sha256(text.encode()).hexdigest(),exit=None,remote_exclusion='UNKNOWN')
        report['operations'].append(item);save()
        try:
            p=subprocess.run(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input=text,text=True,capture_output=True,timeout=30)
            item.update(exit=p.returncode,stdout=p.stdout,stderr=p.stderr)
            assert p.returncode==0,p.stderr
            return p.stdout.strip()
        except subprocess.TimeoutExpired as error:
            decode=lambda v:v.decode(errors='replace') if isinstance(v,bytes) else v or ''
            item.update(failure='TIMEOUT',stdout=decode(error.stdout),stderr=decode(error.stderr));raise
        finally:save()
    def connect(db, name, clients):
        p=Process(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','postgres','-d',db],input_pipe=True,limit=128*1024)
        # Register before any fallible send/read so partial starts are collected.
        clients.append((name,p))
        send(p,"SET application_name='"+name+"'; SET statement_timeout='15s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='20s';"+mark('CONNECTED'))
        marker(p,'CONNECTED')
        send(p,mark('READY'))
        marker(p,'READY');return p
    def blocking(db, waiter, holder):
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            rows=json.loads(sql(db,"SELECT coalesce(json_agg(t),'[]'::json) FROM (SELECT a.pid,a.backend_start,a.wait_event_type,a.wait_event,pg_blocking_pids(a.pid) AS blockers FROM pg_stat_activity a WHERE a.application_name='"+waiter+"' AND EXISTS (SELECT 1 FROM pg_stat_activity h WHERE h.application_name='"+holder+"' AND h.pid=ANY(pg_blocking_pids(a.pid)))) t"))
            if rows:return rows
            time.sleep(.03)
        raise AssertionError('actual blocking backend not observed')
    def cleanup(db, clients, case):
        started=time.monotonic();errors=[]
        for name,p in clients:
            try:
                if p.process.poll() is None:send(p,'\\q\n')
            except (BrokenPipeError,OSError):pass
        # A queued \q cannot interrupt a running query. Observe and terminate
        # only backends of this newly created, exclusively owned fixture DB.
        deadline=time.monotonic()+.5
        while time.monotonic()<deadline and any(p.process.poll() is None for _,p in clients):time.sleep(.02)
        try:
            rows=json.loads(sql(db,"SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT pid,backend_start,application_name,state FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()) t"))
            case['cleanup_backends']=rows
            for row in rows:
                assert row['application_name'] in {name for name,_ in clients}
                # Both PID and start time protect against a reused backend PID.
                stamp=row['backend_start'].replace("'","''")
                assert sql(db,"SELECT pg_terminate_backend(pid,3000) FROM pg_stat_activity WHERE datname=current_database() AND pid="+str(row['pid'])+" AND backend_start='"+stamp+"'::timestamptz") in ('t','')
        except Exception as error:errors.append(type(error).__name__+': '+str(error))
        for name,p in clients:
            try:p.wait(5)
            except Exception as error:errors.append(type(error).__name__+': '+str(error))
            finally:
                try:p.stop()
                except Exception as error:errors.append(type(error).__name__+': '+str(error))
                case[name]=dict(exit=p.process.returncode,stdout=p.stdout.decode(),stderr=p.stderr.decode(),overflow=p.overflow)
        try:
            case['remaining_backends']=int(sql(db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
            assert case['remaining_backends']==0
            assert all(p.process.poll() is not None and not p.overflow for _,p in clients)
        except Exception as error:errors.append(type(error).__name__+': '+str(error))
        case['cleanup_seconds']=time.monotonic()-started
        case['cleanup_errors']=errors;save()
        assert not errors, 'SQL client cleanup incomplete'
    save()
    for mode in ['closure_commit','closure_rollback','pause_commit','pause_rollback','old_rr','old_rc']:
        case=dict(name=mode,status='INCOMPLETE');report['cases'].append(case);save()
        db='bbcheckpoint_race_r1_'+mode
        sql('postgres','CREATE DATABASE '+db+' TEMPLATE '+template)
        clients=[]
        try:
            producer=connect(db,'producer_'+mode,clients)
            closer=connect(db,'closer_'+mode,clients)
            send(producer,claim+mark('CLAIMED'));marker(producer,'CLAIMED')
            if mode.startswith('closure_'):
                send(producer,'BEGIN;'+close+mark('CLOSED'));marker(producer,'CLOSED')
                send(closer,'BEGIN;'+pause+'COMMIT;'+mark('PAUSED'))
                case['blocking']=blocking(db,'closer_'+mode,'producer_'+mode)
                send(producer,('COMMIT;' if mode.endswith('commit') else 'ROLLBACK;')+mark('RELEASED'))
                marker(producer,'RELEASED');marker(closer,'PAUSED')
                wanted=(1 if mode.endswith('commit') else 0,'paused',5 if mode.endswith('commit') else 4)
            elif mode.startswith('pause_'):
                send(closer,'BEGIN;'+pause+mark('PAUSING'));marker(closer,'PAUSING')
                send(producer,'BEGIN;'+close+'COMMIT;'+mark('CLOSED'))
                case['blocking']=blocking(db,'producer_'+mode,'closer_'+mode)
                send(closer,('COMMIT;' if mode.endswith('commit') else 'ROLLBACK;')+mark('RELEASED'))
                marker(closer,'RELEASED')
                if mode.endswith('commit'):
                    assert producer.wait(5)==3 and b'23514' in producer.stderr
                    wanted=(0,'paused',4)
                else:marker(producer,'CLOSED');wanted=(1,'running',4)
            else:
                isolation='REPEATABLE READ' if mode=='old_rr' else 'READ COMMITTED'
                send(producer,'BEGIN ISOLATION LEVEL '+isolation+';'+snapshot+mark('SNAPSHOT'));marker(producer,'SNAPSHOT')
                assert b'running' in bytes(producer.stdout).splitlines()
                send(closer,pause+mark('PAUSED'));marker(closer,'PAUSED')
                send(producer,close+'COMMIT;'+mark('CLOSED'))
                assert producer.wait(5)==3 and (b'25001' if mode=='old_rr' else b'23514') in producer.stderr
                wanted=(0,'paused',4)
            observed=json.loads(sql(db,"SELECT json_build_object('bindings',(SELECT count(*) FROM control.producer_input_bindings WHERE job_id=j.job_id),'requests',(SELECT count(*) FROM control.producer_requests WHERE producer_slot_id=j.job_id),'outcomes',(SELECT count(*) FROM control.producer_outcomes WHERE producer_slot_id=j.job_id),'closures',(SELECT count(*) FROM control.import_record_closures WHERE job_id=j.job_id),'heads',(SELECT count(*) FROM control.import_checkpoints WHERE job_id=j.job_id),'prefix',(SELECT coalesce(max(complete_through_record),0) FROM control.import_checkpoints WHERE job_id=j.job_id),'shard_revision',(SELECT coalesce(max(revision),0) FROM control.import_checkpoints WHERE job_id=j.job_id),'byte_offset',(SELECT coalesce(max(byte_offset),0) FROM control.import_checkpoints WHERE job_id=j.job_id),'state',j.state,'job_revision',j.job_revision,'job_checkpoint',j.checkpoint_revision,'producer_checkpoints',(SELECT count(*) FROM control.producer_checkpoints)) FROM control.jobs j WHERE job_id=decode(lpad(to_hex(2001),32,'0'),'hex')"))
            case['observed']=observed
            assert observed==dict(bindings=1,requests=1,outcomes=1,closures=wanted[0],heads=wanted[0],prefix=wanted[0],shard_revision=wanted[0],byte_offset=wanted[0]*100,state=wanted[1],job_revision=wanted[2],job_checkpoint=wanted[0],producer_checkpoints=0)
        finally:
            cleanup(db,clients,case)
        case['status']='PASS';save()
    report['status']='PASS';save();print(json.dumps({'status':'PASS','cases':len(report['cases'])}))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    args = parser.parse_args()
    main(args.root.resolve(strict=True))
