#!/usr/bin/env python3
"""Exact-source isolated Linux reservation boundary; retained raw effects, no ACK claim."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/archive-engine'))
sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
from signed_publication_cases import command, execute, event, proxy_observation
from fault_cases import Process
from oracle import cbor, expected

KILL = '''import json,os,selectors,signal,subprocess,sys,time
p=subprocess.Popen(sys.argv[1:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
s=selectors.DefaultSelector();s.register(p.stdout,selectors.EVENT_READ)
b=b'';deadline=time.monotonic()+25
try:
 while time.monotonic()<deadline:
  assert p.poll() is None
  if s.select(.1):
   part=os.read(p.stdout.fileno(),65536);assert part;b+=part
   assert len(b)<65536
   if any(json.loads(x).get('event')=='reserved-before-native' for x in b.split(b'\\n')[:-1]):break
 else:raise RuntimeError('marker timeout')
 os.killpg(p.pid,signal.SIGKILL);o,e=p.communicate(timeout=5)
 assert p.returncode==-9 and not e
 print(json.dumps({'exit':p.returncode,'stdout':(b+o).decode(),'stderr':e.decode(),'joined':True}))
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)
'''


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    assert SOURCE == root / 'source'
    assert command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == state['source']
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain')
    runner, pg = state['container_id'], state['postgres_id']
    for identity in [runner, pg]:
        record = json.loads(command('docker', 'inspect', identity))[0]
        assert record['State']['Running'] and not record['State']['OOMKilled']
        assert record['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    assert hashlib.sha256((root/'out/native-tests/ingest-bootstrap-probe').read_bytes()).hexdigest() == ready['ingest_probe']
    evidence = root/'producer-reservation-cases.json'
    assert not evidence.exists()
    report = dict(status='INCOMPLETE', source=state['source'], tree=state['tree'], artifacts=ready, cases=[])
    def save(): evidence.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db, text):
        return command('docker','exec','-i',pg,'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def state_rows(db):
        return json.loads(sql(db,"SELECT json_build_object('requests',(SELECT coalesce(json_agg(t ORDER BY request_sequence),'[]'::json) FROM control.producer_requests t),'epochs',(SELECT coalesce(json_agg(t),'[]'::json) FROM control.producer_epochs t))"))
    binary = ['docker','exec',runner,'/out/native-tests/ingest-bootstrap-probe']
    dataset = bytes([1])*16
    ref = lambda n: [dataset,4,bytes([64])*16,n]
    original = b'bigbrotr/producer-request/1\0'+cbor([[bytes([66])*16,1,1],'admit-evidence',1,[1,ref(1),[
        ['dictionary',1,[ref(n),1,'visibility',1,{'class':name}]] for n,name in [(1,'public'),(2,'private')]]]])
    save()
    for label in ['write','reservation-refused','reservation-rollback','reserved','before-forward','after-complete']:
        case = dict(name=label,status='INCOMPLETE',process={});report['cases'].append(case);save()
        db='bbproducer_'+label.replace('-','_')
        sql('postgres',f'CREATE DATABASE {db}')
        setup={};execute(binary+['setup-12',db,'/secrets/postgres','unused'],30,setup)
        case['setup']=setup;save();assert setup['exit']==0 and not setup['stderr']
        if label=='reservation-rollback':
            sql(db,"CREATE FUNCTION public.fixture_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback'; END $$; CREATE TRIGGER zz_fixture_abort AFTER INSERT ON control.producer_requests FOR EACH ROW EXECUTE FUNCTION public.fixture_abort();")
        proxy=None
        mode='reservation-commit-lost' if label in ['before-forward','after-complete'] else label
        home='/out/producer_'+label
        command('docker','exec',runner,'mkdir','-m','700',home)
        try:
            if mode=='reservation-commit-lost':
                proxy=Process(['docker','exec','-i',runner,'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--listen-port','6543','--mode',label,'--commit-number','3','--deadline-seconds','35'],limit=65536)
                event(proxy,'ready')
            args=[mode,db,'/secrets/bigbrotr_control',home+'/db']
            if label=='reserved':
                raw=command('docker','exec',runner,'python3','-c',KILL,'/out/native-tests/ingest-bootstrap-probe',*args,timeout=35)
                case['process']=json.loads(raw)
                assert case['process']['exit']==-9 and case['process']['joined']
            else:
                execute(binary+args,35,case['process'])
                save();assert case['process']['exit']==0 and not case['process']['stderr']
            if proxy:
                assert proxy.wait(5)==0
                case['proxy']=proxy_observation(proxy);save()
                frames=[json.loads(x) for x in case['proxy']['stdout'].splitlines()]
                receipt=frames[-1]
                assert receipt['event']=='receipt' and receipt['commits_seen']==3 and receipt['faults']==1
                assert receipt['server_commit_complete']==(3 if label=='after-complete' else 2)
                assert receipt['client_commit_complete']==2
        finally:
            if proxy:proxy.stop();case['proxy']=proxy_observation(proxy)
            save()
        case['control']=state_rows(db)
        wanted,_,_=expected(dataset,label=='write')
        actual=json.loads(command(*binary,'dump',home+'/db'))
        case['native']=actual
        assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in actual['rows']]==wanted
        retained=list((root/'out'/('producer_'+label)/'db/producer-inputs-v1').iterdir())
        assert len(retained)==1 and retained[0].read_bytes()==original
        case['input']={'name':retained[0].name,'bytes':len(original),'sha256':hashlib.sha256(original).hexdigest()}
        reserved=label not in ['reservation-rollback','before-forward']
        rows=case['control']['requests'];epochs=case['control']['epochs']
        assert len(rows)==int(reserved) and len(epochs)==1
        assert epochs[0]['reserved_through_request']==int(reserved)
        if reserved:
            row=rows[0]
            assert row['request_sequence']==1 and row['producer_epoch']==1
            assert row['producer_slot_id']=='\\x'+(bytes([66])*16).hex()
            assert row['dataset_id']=='\\x'+dataset.hex()
            assert row['request_digest']=='\\x'+hashlib.sha256(original).hexdigest()
            assert row['input_bytes']==len(original) and row['input_profile']==1
            assert row['manifest_sha256']=='\\x'+hashlib.sha256(wanted[0][2]).hexdigest()
        assert sql(db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")=='0'
        inspection={}
        execute(binary+['inspect',db,'/secrets/bigbrotr_control',home+'/db'],30,inspection)
        case['inspection']=inspection;save()
        assert inspection['exit']==0 and not inspection['stderr']
        observed=json.loads(inspection['stdout'])
        assert observed['event']=='reservation-observed' and observed['control_epoch']==2 and observed['present']==reserved
        assert observed['digest']==(hashlib.sha256(original).hexdigest() if reserved else None)
        assert state_rows(db)==case['control']
        case['status']='PASS';save()
    fixture=SOURCE/'experiments/producer-requests/schema_cases.sql'
    completed=subprocess.run(['docker','exec','-i',pg,'psql','-XAt','-U','postgres','-d','bbproducer_write'],
        input=fixture.read_text(),capture_output=True,text=True,timeout=60)
    report['schema']={'exit':completed.returncode,'stdout':completed.stdout,'stderr':completed.stderr,
        'fixture_sha256':hashlib.sha256(fixture.read_bytes()).hexdigest()}
    save()
    assert completed.returncode==0, 'schema fixture failed; inspect retained transcript'
    result, _ = json.JSONDecoder().raw_decode(completed.stdout[completed.stdout.index('{'):])
    assert result['status']=='PASS' and len(result['cases'])==19
    report['status']='PASS';save()
    print(json.dumps({'status':'PASS','cases':len(report['cases'])}))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    main(parser.parse_args().root.resolve(strict=True))
