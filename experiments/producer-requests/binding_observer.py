#!/usr/bin/env python3
"""Check complete outcome copies in a hash-bound capture; no new engine execution."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode=True
from existing_outcome import expected, check_all_outcome_copies
from event_oracle import expected_event


def main(path, digest, output):
    raw=path.read_bytes();assert hashlib.sha256(raw).hexdigest()==digest
    report=json.loads(raw);assert report['status']=='PASS'
    labels=['complete','paused','pause-between','conflict','cancelled','rollback','before','before-forward','after-complete']
    assert [c['name'] for c in report['cases']]==labels
    observations=[]
    for case in report['cases']:
        label=case['name'];assert case['status']=='PASS'
        bound=label in ['complete','paused','pause-between','conflict','before','after-complete']
        wanted=expected(2)[0] if label=='complete' else expected_event(bytes([1])*16,False,2)[0]
        check=lambda control:check_all_outcome_copies(control,wanted,request_count=3 if bound else 1,outcome_count=3 if label=='complete' else 1)
        check(case['control'])
        fields=['request_sequence','producer_epoch','policy_epoch','dataset_id','producer_slot_id',
                'manifest_sha256','outcome_kind','outcome_bytes','outcome_sha256','request_digest',
                'recorder_resource','recorder_epoch','recorder_incarnation']
        for field in fields:
            altered=copy.deepcopy(case['control']);value=altered['outcomes'][0][field]
            altered['outcomes'][0][field]=value+1 if isinstance(value,int) else value+'00'
            try:check(altered)
            except (AssertionError,KeyError):pass
            else:raise AssertionError('altered outcome accepted: '+field)
        observations.append(dict(name=label,copies=len(case['control']['outcomes']),negative_controls=len(fields)))
    result=dict(status='PASS',scope='offline complete captured PG outcome copies; no new engine run',
        source=report['source'],capture_sha256=digest,cases=observations,
        observer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        checker_sha256=hashlib.sha256(Path(__file__).with_name('existing_outcome.py').read_bytes()).hexdigest())
    with output.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(dict(status='PASS',copies=sum(v['copies'] for v in observations),negative_controls=sum(v['negative_controls'] for v in observations))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--expected-sha256',required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();main(args.report,args.expected_sha256,args.output)
