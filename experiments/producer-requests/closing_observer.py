#!/usr/bin/env python3
"""Recheck captured closing evidence and discriminating alterations; no engines."""
import argparse
import copy
import json
from pathlib import Path
import sys
sys.dont_write_bytecode=True
from closing import check_state, compact, expected_native, same_window, sha
from import_originals import originals

TAGS=['normal1','empty1','pgfirst1','oldlease','root1','meta1','policy1','expired1','rollback','latesql1','before2','after1']


def check(report):
    assert report['status']=='PASS' and report['remaining_client_backends']==0
    scenario=report['scenario'];empty=scenario=='empty'
    negative=scenario in ['root','metadata','policy','expired']
    recovered=scenario in ['rollback','late-sql','before-forward','after-complete']
    messages=[json.loads(line) for line in report['processes']['close']['stdout'].splitlines()]
    def one(label):
        found=[m['state'] for m in messages if m['event']==label]
        assert len(found)==1
        return found[0]
    signed=originals(1);slot=(bytes([66])*16).hex()
    expected_originals={slot+'-0000000000000001-'+f'{n:016x}'+'.cbor':dict(bytes=len(signed[n]),sha256=sha(signed[n])) for n in ([] if empty else [1,2])}
    assert report['originals']==expected_originals
    wanted=expected_native(empty,not negative)
    assert report['native']['rows']==[dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in wanted]
    before=one('closing-before');after=one('closing-interrupted' if negative else 'closing-after')
    names=[t[0] for t in before['control_tables']];assert len(names)==37 and names==sorted(set(names))
    bt=check_state(before,expected_native(empty,False),names,expected_originals)
    at=check_state(after,wanted,names,expected_originals)
    same_window(bt,at,not negative and scenario!='pg-first',recovered=recovered)
    assert after['sequence']==before['sequence']+int(not negative)
    if recovered:
        mid=one('closing-interrupted');mt=check_state(mid,wanted,names,expected_originals)
        same_window(bt,mt,scenario=='after-complete')
        assert mid['sequence']==before['sequence']+1
        if scenario=='after-complete': assert mt['producer_epochs']==at['producer_epochs']
    successes=[m for m in messages if m['event']=='producer-closed']
    assert len(successes)==(0 if negative else 1 if recovered else 2)
    for m in successes:
        assert m['issued']==(0 if empty else 2) and m['historical_control_epoch']==1
        assert m['current_control_epoch']==(3 if recovered else 2)
    return True


def alter_state(report, change):
    messages=[json.loads(line) for line in report['processes']['close']['stdout'].splitlines()]
    state=next(m['state'] for m in messages if m['event']=='closing-after')
    tables={n:json.loads(value) for n,value in state['control_tables']}
    change(state,tables)
    state['control_tables']=[[n,json.dumps(rows,sort_keys=True)] for n,rows in sorted(tables.items())]
    state['control_sha256']=sha(compact(state['control_tables']))
    report['processes']['close']['stdout']='\n'.join(json.dumps(m) for m in messages)+'\n'


def main(root,output):
    assert not output.exists()
    reports={tag:json.loads((root/('closing-'+tag+'.json')).read_text()) for tag in TAGS}
    for report in reports.values():assert check(report)
    cases=[]
    def refuse(label,tag,mutate):
        changed=copy.deepcopy(reports[tag]);mutate(changed)
        try:check(changed)
        except (AssertionError,KeyError,StopIteration,ValueError,TypeError):cases.append(label);return
        raise AssertionError('altered evidence accepted: '+label)
    owner=lambda t,n:next(r for r in t['owner_epochs'] if r['role']=='control' and r['epoch']==n)
    role=lambda t,n:next(r for r in t['role_instances'] if r['role_instance_id']=='\\x'+(bytes([n])*16).hex())
    alterations=[
        ('committed-retry-xmin',lambda s,t:t['producer_epochs'][0].update(xmin='999999')),
        ('old-incarnation-reused',lambda s,t:owner(t,3).update(process_incarnation='\\x'+(bytes([41])*16).hex())),
        ('wrong-successor-role',lambda s,t:owner(t,3).update(role_instance_id='\\x'+(bytes([31])*16).hex())),
        ('changed-historical-owner',lambda s,t:owner(t,1).update(lease_expires_at_us=0)),
        ('changed-ingest-target',lambda s,t:next(r for r in t['owner_epochs'] if r['role']=='ingest').update(state='draining')),
        ('wrong-exclusion-digest',lambda s,t:owner(t,2).update(exclusion_digest='\\x'+'00'*32)),
        ('wrong-successor-lease',lambda s,t:owner(t,3).update(lease_expires_at_us=owner(t,3)['acquired_at_us']+1)),
        ('reused-backend',lambda s,t:owner(t,3).update(control_backend_pid=owner(t,2)['control_backend_pid'])),
        ('unretired-old-role',lambda s,t:role(t,33).update(retired_at_us=None)),
        ('wrong-new-role-time',lambda s,t:role(t,34).update(created_at_us=owner(t,3)['acquired_at_us']+1)),
        ('extra-ack',lambda s,t:t['producer_checkpoints'].append({'xmin':'0'})),
        ('omitted-table',lambda s,t:t.pop('jobs')),
        ('extra-native-write',lambda s,t:s.update(sequence=s['sequence']+1)),
    ]
    for label,change in alterations:
        refuse(label,'after1',lambda r,change=change:alter_state(r,change))
    refuse('changed-native-marker','normal1',lambda r:r['native']['rows'][-1].update(value='00'))
    refuse('changed-original','normal1',lambda r:next(iter(r['originals'].values())).update(sha256='00'*32))
    report=dict(status='PASS',scope='offline captured evidence only; no engine execution',
                module_sha256=sha((Path(__file__).parent/'closing.py').read_bytes()),
                observer_sha256=sha(Path(__file__).read_bytes()),
                reports={tag:dict(sha256=sha((root/('closing-'+tag+'.json')).read_bytes()),source=r['source'],harness_revision=r['harness_revision']) for tag,r in reports.items()},
                checked_reports=len(reports),negative_controls=cases)
    output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(status='PASS',reports=len(reports),negative_controls=len(cases))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();main(args.root,args.output)
