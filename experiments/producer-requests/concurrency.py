#!/usr/bin/env python3
"""Independent SQL lock schedules against a verified native producer fixture.

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


class InjectedFailure(Exception):
    """An intentional harness failure; never a product SQL outcome."""

# Literal fixture coordinates, independent of application reservation/transition SQL.
CLAIM="""
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
SELECT control.retain_control_lock();
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
 SELECT decode(repeat('e9',16),'hex'),dataset_id,'control',0 FROM control.datasets;
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
 process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,
 control_backend_pid,control_backend_start)
 SELECT resource_id,epoch+1,dataset_id,'control',decode(repeat('e9',16),'hex'),decode(repeat('ea',16),'hex'),
 (extract(epoch FROM clock_timestamp())*1000000)::bigint,
 (extract(epoch FROM clock_timestamp())*1000000)::bigint+60000000,'active',1,
 pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid())
 FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;
INSERT INTO control.producer_slots(producer_slot_id,dataset_id,registered_by_resource_id,
 registered_by_epoch,registered_by_incarnation,created_at_us)
 SELECT decode(repeat('eb',16),'hex'),dataset_id,resource_id,epoch,process_incarnation,acquired_at_us
 FROM control.owner_epochs WHERE role='control' AND state='active';
INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,
 producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,
 owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
 SELECT decode(repeat('eb',16),'hex'),1,dataset_id,producer_role_instance_id,producer_role,
 producer_incarnation,owner_resource_id,owner_epoch,owner_incarnation,2,opened_at_us
 FROM control.producer_epochs WHERE producer_slot_id=decode(repeat('42',16),'hex');
COMMIT;
"""
RESERVE="""
INSERT INTO control.producer_requests(producer_slot_id,producer_epoch,request_sequence,
 dataset_id,request_digest,input_bytes,input_profile,manifest_sha256,issuer_resource,
 issuer_epoch,issuer_incarnation,reserved_at_us)
 SELECT decode(repeat('eb',16),'hex'),1,1,dataset_id,decode(repeat('02',32),'hex'),300,1,
 decode(repeat('01',32),'hex'),resource_id,epoch,process_incarnation,acquired_at_us
 FROM control.owner_epochs WHERE role='control' AND state='active';
"""
CLOSE="UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=decode(repeat('eb',16),'hex');"
SNAPSHOT="SELECT reserved_through_request FROM control.producer_epochs WHERE producer_slot_id=decode(repeat('eb',16),'hex');"


def main(root):
    state=json.loads((root/'resources.json').read_text())
    assert state['root']==str(root)
    harness=command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain')
    product=root/'source'
    assert command('git','-C',str(product),'rev-parse','HEAD')==state['source']
    assert not command('git','-C',str(product),'status','--porcelain')
    assert (SOURCE/'migrations/postgres/0011_producer_requests.sql').read_bytes()==(product/'migrations/postgres/0011_producer_requests.sql').read_bytes()
    pg=state['postgres_id']
    container=json.loads(command('docker','inspect',pg))[0]
    assert container['Config']['Labels']['org.bigbrotr.publication']==state['owner'] and container['State']['Running']
    prior=json.loads((root/'producer-reservation-cases.json').read_text())
    assert prior['status']=='PASS' and prior['source']==state['source']
    path=root/'producer-concurrency.json';assert not path.exists()
    report=dict(status='INCOMPLETE',source=state['source'],harness_revision=harness,
        harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),cases=[])
    def save():path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db,text):return command('docker','exec','-i',pg,'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def connect(db, name, clients, stall_ready=False):
        p=Process(['docker','exec','-i',pg,'psql','-XqAt','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','postgres','-d',db],input_pipe=True,limit=128*1024)
        # Register before any fallible send/read so partial starts are collected.
        clients.append((name,p))
        send(p,"SET application_name='"+name+"'; SET statement_timeout='15s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='20s';"+mark('CONNECTED'))
        marker(p,'CONNECTED')
        send(p,('SELECT pg_sleep(10);' if stall_ready else '')+mark('READY'))
        if stall_ready:
            try:marker(p,'READY',timeout=.2)
            except AssertionError as error:
                assert str(error)=='SQL marker deadline'
                raise InjectedFailure('second client READY deadline') from error
            raise AssertionError('injected delayed READY unexpectedly completed')
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
    for mode in ['reserve_commit','reserve_rollback','close_commit','close_rollback','old_rr','old_rc','cleanup_ready','cleanup_waiter']:
        case=dict(name=mode,status='INCOMPLETE');report['cases'].append(case);save()
        db='bbproducer_race_'+mode
        sql('postgres','CREATE DATABASE '+db+' TEMPLATE bbproducer_write')
        clients=[]
        try:
            producer=connect(db,'producer_'+mode,clients)
            closer=connect(db,'closer_'+mode,clients,stall_ready=mode=='cleanup_ready')
            send(producer,CLAIM+mark('CLAIMED'));marker(producer,'CLAIMED')
            if mode=='cleanup_waiter':
                send(producer,'BEGIN;'+RESERVE+mark('RESERVED')+'SELECT pg_sleep(10);\n');marker(producer,'RESERVED')
                send(closer,'BEGIN;'+CLOSE+'COMMIT;'+mark('CLOSED'))
                case['blocking']=blocking(db,'closer_'+mode,'producer_'+mode)
                raise InjectedFailure('waiter blocked behind running holder')
            elif mode.startswith('reserve_'):
                send(producer,'BEGIN;'+RESERVE+mark('RESERVED'));marker(producer,'RESERVED')
                send(closer,'BEGIN;'+CLOSE+'COMMIT;'+mark('CLOSED'))
                case['blocking']=blocking(db,'closer_'+mode,'producer_'+mode)
                send(producer,('COMMIT;' if mode.endswith('commit') else 'ROLLBACK;')+mark('RELEASED'))
                marker(producer,'RELEASED');marker(closer,'CLOSED')
                wanted=(1 if mode.endswith('commit') else 0,'closing')
            elif mode.startswith('close_'):
                send(closer,'BEGIN;'+CLOSE+mark('CLOSING'));marker(closer,'CLOSING')
                send(producer,'BEGIN;'+RESERVE+'COMMIT;'+mark('RESERVED'))
                case['blocking']=blocking(db,'producer_'+mode,'closer_'+mode)
                send(closer,('COMMIT;' if mode.endswith('commit') else 'ROLLBACK;')+mark('RELEASED'))
                marker(closer,'RELEASED')
                if mode.endswith('commit'):
                    assert producer.wait(5)==3 and b'23514' in producer.stderr
                    wanted=(0,'closing')
                else:marker(producer,'RESERVED');wanted=(1,'open')
            else:
                isolation='REPEATABLE READ' if mode=='old_rr' else 'READ COMMITTED'
                send(closer,'BEGIN ISOLATION LEVEL '+isolation+';'+SNAPSHOT+mark('SNAPSHOT'));marker(closer,'SNAPSHOT')
                assert b'0' in bytes(closer.stdout).splitlines()
                send(producer,'BEGIN;'+RESERVE+'COMMIT;'+mark('RESERVED'));marker(producer,'RESERVED')
                send(closer,CLOSE+'COMMIT;'+mark('CLOSED'))
                if mode=='old_rr':
                    assert closer.wait(5)==3 and b'40001' in closer.stderr
                    wanted=(1,'open')
                else:marker(closer,'CLOSED');wanted=(1,'closing')
            observed=json.loads(sql(db,"SELECT json_build_object('count',(SELECT count(*) FROM control.producer_requests WHERE producer_slot_id=decode(repeat('eb',16),'hex')),'counter',reserved_through_request,'state',state) FROM control.producer_epochs WHERE producer_slot_id=decode(repeat('eb',16),'hex')"))
            case['observed']=observed
            assert observed==dict(count=wanted[0],counter=wanted[0],state=wanted[1])
        except InjectedFailure as error:
            assert mode.startswith('cleanup_')
            case['injected_failure']=str(error)
        finally:
            cleanup(db,clients,case)
        if mode.startswith('cleanup_'):
            assert case['injected_failure'] and case['cleanup_backends'] and case['cleanup_seconds']<8
        case['status']='PASS';save()
    report['status']='PASS';save();print(json.dumps({'status':'PASS','cases':len(report['cases'])}))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    main(parser.parse_args().root.resolve(strict=True))
