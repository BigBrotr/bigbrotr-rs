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


def expected():
    dataset,slot = bytes([1])*16,bytes([66])*16
    be=lambda n,width=8:n.to_bytes(width,'big')
    rows,_,_ = expected_event(dataset,True)
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


def main(root):
    state=json.loads((root/'resources.json').read_text())
    harness=command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all')
    ready=json.loads((root/'out/outcome-probe/ready.json').read_text())
    assert ready['status']=='READY' and ready['source']==harness and ready['baseline_product']==state['source']
    artifact=root/'out/outcome-probe/ingest-bootstrap-probe'
    assert hashlib.sha256(artifact.read_bytes()).hexdigest()==ready['sha256']
    for identity in [state['container_id'],state['postgres_id']]:
        c=json.loads(command('docker','inspect',identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication']==state['owner'] and c['State']['Running'] and not c['State']['OOMKilled']
    report=dict(status='INCOMPLETE',source=state['source'],harness_revision=harness,artifacts=ready,processes={})
    path=root/'producer-existing-outcome.json';assert not path.exists()
    def save():path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db,text):return command('docker','exec','-i',state['postgres_id'],'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def run(label,args):
        observation=report['processes'][label]={};save()
        try:execute(args,40,observation)
        finally:save()
        assert observation['exit']==0 and not observation['stderr']
        return observation['stdout']
    save();db='bboutcome_existing'
    sql('postgres','CREATE DATABASE '+db)
    binary=['docker','exec',state['container_id'],'/out/outcome-probe/ingest-bootstrap-probe']
    run('setup',binary+['setup-12',db,'/secrets/postgres','unused'])
    (root/'out/outcome-existing').mkdir(mode=0o700)
    stdout=run('admit',binary+['event-outcomes',db,'/secrets/bigbrotr_control','/out/outcome-existing/db'])
    native=json.loads(run('dump',binary+['dump','/out/outcome-existing/db']))
    report['native']=native
    report['control']=json.loads(sql(db,"SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'epochs',(SELECT jsonb_agg(to_jsonb(e)) FROM control.producer_epochs e),'checkpoints',(SELECT count(*) FROM control.producer_checkpoints))"));save()
    wanted,original,receipt=expected()
    assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native['rows']]==wanted
    message=json.loads(stdout.splitlines()[-1])
    assert message==dict(event='existing-receipt',bytes=receipt.hex(),replay_exact=True,row_unchanged=True)
    rows=report['control'];assert rows['checkpoints']==0 and len(rows['epochs'])==1 and rows['epochs'][0]['reserved_through_request']==3
    assert len(rows['requests'])==len(rows['outcomes'])==3
    request,outcome=rows['requests'][2],rows['outcomes'][2]
    assert request['request_sequence']==outcome['request_sequence']==3
    assert request['input_bytes']==len(original)
    assert request['request_digest']==outcome['request_digest']=='\\x'+hashlib.sha256(original).hexdigest()
    assert outcome['outcome_kind']==2 and outcome['policy_epoch']==1
    assert outcome['outcome_bytes']=='\\x'+receipt.hex() and outcome['outcome_sha256']=='\\x'+hashlib.sha256(receipt).hexdigest()
    spool=root/'out/outcome-existing/db/producer-inputs-v1'
    assert len(list(spool.iterdir()))==3
    assert (spool/((bytes([66])*16).hex()+'-0000000000000001-0000000000000003.cbor')).read_bytes()==original
    report['remaining_backends']=int(sql(db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    assert report['remaining_backends']==0
    report['status']='PASS';save();print(json.dumps(dict(status='PASS',native_rows=27)))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    main(parser.parse_args().root.resolve(strict=True))
