#!/usr/bin/env python3
"""Compare all retained signed-fixture outcomes with an independent byte oracle.

Read-only follow-up to the installed publication schedules. No client is restarted
as a writer; these observations do not establish successor recovery or progress.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/archive-engine'))
from signed_publication_oracle import DATASET, MODES, corpus, producer_input_files, source, cbor


def command(*args, input=None):
    p = subprocess.run(args, input=input, text=True, capture_output=True, timeout=30)
    assert p.returncode == 0, p.stderr
    return p.stdout.strip()


def expected_outcomes(inputs):
    """Literal receipt/1 and rejection/1 recipes; no observed native templates."""
    slot = bytes([66])*16
    context = [DATASET, 0, 1, bytes([67])*16, 1]
    batches, _ = corpus()
    refs = [[source(r['sequence']), bytes.fromhex(r['digest'])]
            for batch in batches for r in batch['facts']]
    result = []
    for sequence, item in enumerate(inputs, 1):
        digest = bytes.fromhex(item['sha256'])
        if sequence in [2, 3]:
            value = (b'bigbrotr/rejection/1\0'+slot+(1).to_bytes(8,'big')
                     +sequence.to_bytes(8,'big')+digest+DATASET+(1).to_bytes(8,'big')
                     +bytes([4 if sequence == 2 else 2]))
            kind = 3
        else:
            batch = refs[:5] if sequence == 1 else refs[5:]
            dependencies = refs[:5] if sequence == 1 else refs
            value = b'bigbrotr/receipt/1\0'+cbor([1,[slot,1,sequence],digest,context,batch,dependencies])
            kind = 1
        result.append((kind, value))
    return result


def main(root):
    state = json.loads((root/'resources.json').read_text())
    assert state['root'] == str(root)
    harness = command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all')
    assert command('git','-C',str(root/'source'),'rev-parse','HEAD') == state['source']
    assert not command('git','-C',str(root/'source'),'status','--porcelain','--untracked-files=all')
    assert (SOURCE/'experiments/archive-engine/signed_publication_oracle.py').read_bytes() == (root/'source/experiments/archive-engine/signed_publication_oracle.py').read_bytes()
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source'] and ready['status'] == 'READY'
    assert hashlib.sha256((root/'out/native-tests/ingest-bootstrap-probe').read_bytes()).hexdigest() == ready['ingest_probe']
    for identity in [state['container_id'],state['postgres_id']]:
        c = json.loads(command('docker','inspect',identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    prior = root/'installed-signed-cases.json'
    publication = json.loads(prior.read_text())
    assert publication['status'] == 'PASS' and publication['source'] == state['source']
    assert [c['mode'] for c in publication['cases']] == list(MODES)
    path = root/'signed-producer-outcomes.json'; assert not path.exists()
    report = dict(status='INCOMPLETE', source=state['source'], harness_revision=harness,
                  harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  publication_sha256=hashlib.sha256(prior.read_bytes()).hexdigest(), cases=[])
    def save(): path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db, query):
        return command('docker','exec','-i',state['postgres_id'],'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=query)
    be = lambda n,width=8: n.to_bytes(width,'big')
    manifest = (bytes.fromhex('01010001000100010001')+DATASET+be(0)+bytes([61])*16
                +be(0,4)+be(1)+bytes([63])*16+bytes([67])*16+be(1)+bytes([1,6])
                +b''.join(be(k,2)+bytes([k])*16 for k in [1,2,3,4,5,8]))
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    save()
    for mode in MODES:
        case = dict(mode=mode,status='INCOMPLETE'); report['cases'].append(case); save()
        home = root/('out/signed_'+mode)
        case['inputs'] = producer_input_files(home)
        db = 'bbpub_control_'+mode
        case['control'] = json.loads(sql(db,"SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'epochs',(SELECT jsonb_agg(to_jsonb(p)) FROM control.producer_epochs p),'checkpoints',(SELECT count(*) FROM control.producer_checkpoints))"))
        case['native'] = json.loads(command('docker','exec',state['container_id'],'/out/native-tests/ingest-bootstrap-probe','dump','/out/signed_'+mode+'/rocks'))
        save()
        native = {(r['cf'],bytes.fromhex(r['key'])):bytes.fromhex(r['value']) for r in case['native']['rows']}
        assert native['default',b'\x01\x01'] == manifest
        rows = case['control']; assert len(rows['requests']) == len(rows['outcomes']) == 4
        assert rows['checkpoints'] == 0 and len(rows['epochs']) == 1
        assert rows['epochs'][0]['reserved_through_request'] == 4
        assert native['receipts',b'\x01\x31'+bytes([66])*16+be(1)] == b'\x01\x31'+be(4)+be(0)+be(0)
        for sequence, (item, (kind, value), request, outcome) in enumerate(zip(case['inputs'],expected_outcomes(case['inputs']),rows['requests'],rows['outcomes'],strict=True),1):
            assert native['receipts',b'\x01\x30'+bytes([66])*16+be(1)+be(sequence)] == value
            for row in [request,outcome]:
                assert row['request_sequence'] == sequence and row['producer_epoch'] == 1
                assert row['producer_slot_id'] == '\\x'+(bytes([66])*16).hex()
                assert row['dataset_id'] == '\\x'+DATASET.hex()
                assert row['request_digest'] == '\\x'+item['sha256']
                assert row['manifest_sha256'] == '\\x'+manifest_sha
            assert request['input_bytes'] == item['bytes'] and request['input_profile'] == 1
            assert outcome['outcome_bytes'] == '\\x'+value.hex()
            assert outcome['outcome_sha256'] == '\\x'+hashlib.sha256(value).hexdigest()
            assert outcome['outcome_kind'] == kind and outcome['policy_epoch'] == 1
            assert outcome['recorder_resource'] == request['issuer_resource']
            assert outcome['recorder_epoch'] == request['issuer_epoch']
            assert outcome['recorder_incarnation'] == request['issuer_incarnation']
        assert sql(db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()') == '0'
        case['status'] = 'PASS'; save()
    report.update(status='PASS', request_count=24, kinds={'NewFacts':12,'Rejected':12,'AlreadyDurable':0}); save()
    print(json.dumps(dict(status='PASS',requests=24)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    main(parser.parse_args().root.resolve(strict=True))
