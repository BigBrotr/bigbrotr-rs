#!/usr/bin/env python3
"""Actual connected AlreadyDurable copy, distinct request and identical retry."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(SOURCE/'experiments/archive-engine'))
sys.path.insert(0,str(SOURCE/'experiments/ingest-bootstrap'))
from signed_publication_cases import execute
from event_oracle import expected_event
from oracle import cbor


def command(*args,input=None):
    p = subprocess.run(args,input=input,text=True,capture_output=True,timeout=30)
    assert p.returncode == 0,p.stderr
    return p.stdout.strip()


def expected(repetitions=1):
    dataset,slot = bytes([1])*16,bytes([66])*16
    be=lambda n,width=8:n.to_bytes(width,'big')
    rows,_,_ = expected_event(dataset,True,repetitions)
    rows={(cf,k):v for cf,k,v in rows}
    source_key=lambda n:dataset+be(0,4)+be(1)+be(n)
    deps=[[[dataset,0,1,n],rows['facts',b'\x01\x20'+source_key(n)][2:34]] for n in [1,3,4,5,6]]
    event=json.loads((SOURCE/'tests/fixtures/ingest/import-event.jsonl').read_text())
    scope=[dataset,4,bytes([64])*16,1]
    lineage=[dataset,9,bytes([69])*16,1]
    signed=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],event['kind'],event['tags'],event['content'],bytes.fromhex(event['sig'])]
    request=[slot,1,3]
    original=b'bigbrotr/producer-request/1\0'+cbor([request,'admit',1,[1,scope,signed,'import',lineage,None]])
    digest=hashlib.sha256(original).digest()
    receipt=b'bigbrotr/receipt/1\0'+cbor([2,request,digest,[dataset,0,1,bytes([67])*16,1],None,deps])
    rows['receipts',b'\x01\x30'+slot+be(1)+be(3)]=receipt
    rows['receipts',b'\x01\x31'+slot+be(1)]=b'\x01\x31'+be(3)+be(0)+be(0)
    assert len(rows)==27
    return sorted((cf,k,v) for (cf,k),v in rows.items()),original,receipt


def check_all_outcome_copies(control, wanted, *, request_count=3, outcome_count=3):
    """Also usable on the hash-bound captured PG observation, without new writes."""
    expected = {(cf, key): value for cf, key, value in wanted}
    slot = bytes([66])*16
    be = lambda n: n.to_bytes(8, 'big')
    manifest = hashlib.sha256(expected['default', b'\x01\x01']).hexdigest()
    assert 1 <= outcome_count <= request_count <= 3
    assert len(control['outcomes']) == outcome_count and len(control['requests']) == request_count
    for sequence, row in enumerate(control['outcomes'], 1):
        receipt = expected['receipts', b'\x01\x30'+slot+be(1)+be(sequence)]
        assert row['request_sequence'] == sequence and row['producer_epoch'] == 1 and row['policy_epoch'] == 1
        assert row['dataset_id'] == '\\x'+(bytes([1])*16).hex() and row['producer_slot_id'] == '\\x'+slot.hex()
        assert row['manifest_sha256'] == '\\x'+manifest
        assert row['outcome_kind'] == (1 if sequence < 3 else 2)
        assert row['outcome_bytes'] == '\\x'+receipt.hex()
        assert row['outcome_sha256'] == '\\x'+hashlib.sha256(receipt).hexdigest()
        request = control['requests'][sequence-1]
        assert row['request_digest'] == request['request_digest']
        assert all(row['recorder_'+field] == request['issuer_'+field] for field in ['resource', 'epoch', 'incarnation'])


def main(root, observe_existing=False, native_build=False):
    state=json.loads((root/'resources.json').read_text())
    harness=command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all')
    ready_path = 'out/native-tests/ready.json' if native_build else 'out/outcome-probe/ready.json'
    ready=json.loads((root/ready_path).read_text())
    assert ready['status']=='READY' and ready['source' if native_build else 'baseline_product']==state['source']
    source_export = root/('out/build/source' if native_build else 'out/outcome-probe/source')
    for name in ['apps/bigbrotr/examples/ingest_bootstrap_probe.rs',
                 'apps/bigbrotr/examples/support/ingest_event.rs',
                 'experiments/ingest-bootstrap/event_oracle.py',
                 'experiments/ingest-bootstrap/oracle.py',
                 'tests/fixtures/ingest/import-event.jsonl']:
        assert (SOURCE/name).read_bytes()==(source_export/name).read_bytes()
    artifact=root/('out/native-tests/ingest-bootstrap-probe' if native_build else 'out/outcome-probe/ingest-bootstrap-probe')
    assert hashlib.sha256(artifact.read_bytes()).hexdigest()==ready['ingest_probe' if native_build else 'sha256']
    for identity in [state['container_id'],state['postgres_id']]:
        c=json.loads(command('docker','inspect',identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication']==state['owner'] and c['State']['Running'] and not c['State']['OOMKilled']
    report=dict(status='INCOMPLETE',source=state['source'],harness_revision=harness,artifacts=ready,processes={})
    path=root/'producer-existing-outcome.json'
    if observe_existing:
        prior=path.read_bytes();report=json.loads(prior)
        assert report['source']==state['source'] and report['artifacts']==ready
        assert report['processes']['admit']['exit']==0
        report.update(status='INCOMPLETE',observer_revision=harness,prior_sha256=hashlib.sha256(prior).hexdigest())
        path=root/'producer-existing-outcome-recheck.json'
    assert not path.exists()
    def save():path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db,text):return command('docker','exec','-i',state['postgres_id'],'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def run(label,args):
        observation=report['processes'][label]={};save()
        try:execute(args,40,observation)
        finally:save()
        assert observation['exit']==0 and not observation['stderr']
        return observation['stdout']
    save();db='bboutcome_existing'
    binary=['docker','exec',state['container_id'],'/'+str(artifact.relative_to(root))]
    if observe_existing:
        stdout=report['processes']['admit']['stdout']
    else:
        sql('postgres','CREATE DATABASE '+db)
        run('setup',binary+['setup-13',db,'/secrets/postgres','unused'])
        (root/'out/outcome-existing').mkdir(mode=0o700)
        stdout=run('admit',binary+['event-outcomes',db,'/secrets/bigbrotr_control','/out/outcome-existing/db'])
    native=json.loads(run('dump',binary+['dump','/out/outcome-existing/db']))
    report['native']=native
    report['control']=json.loads(sql(db,"SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'epochs',(SELECT jsonb_agg(to_jsonb(e)) FROM control.producer_epochs e),'checkpoints',(SELECT count(*) FROM control.producer_checkpoints))"));save()
    wanted,original,receipt=expected()
    assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native['rows']]==wanted
    message=json.loads(stdout.splitlines()[-1])
    assert message==dict(event='existing-receipt',bytes=receipt.hex(),replay_exact=True,row_unchanged=True)
    if native_build:
        messages = [json.loads(line) for line in stdout.splitlines()]
        position = [m for m in messages if m['event']=='import-position']
        source_bytes = (SOURCE/'tests/fixtures/ingest/import-event.jsonl').read_bytes()
        assert position == [dict(event='import-position', line=1, byte_start=0,
            byte_end=len(source_bytes), source_bytes=len(source_bytes),
            source_sha256=hashlib.sha256(source_bytes).hexdigest(),
            record_sha256=hashlib.sha256(source_bytes).hexdigest(),
            completed_import_prefix_before_admission=0)]
        report['source_position'] = position[0]
    rows=report['control'];assert rows['checkpoints']==0 and len(rows['epochs'])==1 and rows['epochs'][0]['reserved_through_request']==3
    check_all_outcome_copies(rows, wanted)
    assert len(rows['requests'])==len(rows['outcomes'])==3
    request,outcome=rows['requests'][2],rows['outcomes'][2]
    manifest=next(v for cf,k,v in wanted if cf=='default' and k==b'\x01\x01')
    for row in [request,outcome]:
        assert row['request_sequence']==3 and row['producer_epoch']==1
        assert row['dataset_id']=='\\x'+(bytes([1])*16).hex()
        assert row['producer_slot_id']=='\\x'+(bytes([66])*16).hex()
        assert row['manifest_sha256']=='\\x'+hashlib.sha256(manifest).hexdigest()
    assert request['input_profile']==1
    assert request['input_bytes']==len(original)
    assert request['request_digest']==outcome['request_digest']=='\\x'+hashlib.sha256(original).hexdigest()
    assert outcome['outcome_kind']==2 and outcome['policy_epoch']==1
    assert outcome['outcome_bytes']=='\\x'+receipt.hex() and outcome['outcome_sha256']=='\\x'+hashlib.sha256(receipt).hexdigest()
    assert outcome['recorder_resource']==request['issuer_resource']
    assert outcome['recorder_epoch']==request['issuer_epoch']
    assert outcome['recorder_incarnation']==request['issuer_incarnation']
    spool=root/'out/outcome-existing/db/producer-inputs-v1'
    assert len(list(spool.iterdir()))==3
    assert (spool/((bytes([66])*16).hex()+'-0000000000000001-0000000000000003.cbor')).read_bytes()==original
    report['remaining_backends']=int(sql(db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    assert report['remaining_backends']==0
    report['status']='PASS';save();print(json.dumps(dict(status='PASS',native_rows=27)))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--observe-existing',action='store_true')
    parser.add_argument('--native-build',action='store_true',help='Use the exact main native build instead of the separate historical fixture build')
    args=parser.parse_args()
    main(args.root.resolve(strict=True),args.observe_existing,args.native_build)
