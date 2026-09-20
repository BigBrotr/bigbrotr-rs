#!/usr/bin/env python3
"""Recheck a hash-bound recovery capture and reject altered observations offline."""
import argparse
import copy
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from recovery import check_messages, check_inputs, expected_originals, expected_case, sha, PERMANENT
from closing import compact


def check(report):
    assert report['status'] == 'PASS' and report['remaining_backends'] == 0
    result = report['processes']['recover']
    assert result['exit'] == 0 and not result['stderr'] and not result['overflow']
    scenario = report['scenario']
    wanted = expected_case(scenario, scenario != 'pg-only')
    assert report['native']['rows'] == [dict(cf=cf, key=k.hex(), value=v.hex()) for cf, k, v in wanted]
    originals = expected_originals(scenario)
    files = {bytes([66]*16).hex()+'-0000000000000001-'+f'{n:016x}'+'.cbor':originals[n] for n in originals}
    assert report['originals'] == {name:dict(bytes=len(value), sha256=sha(value)) for name, value in files.items()}
    check_inputs(report['control'], files, wanted, scenario)
    messages = [json.loads(line) for line in result['stdout'].splitlines()]
    return check_messages(messages, scenario, report['table_names'], report['originals'],
                          report['control'], report['seed_failure'])


def main(path, digest, output):
    raw = path.read_bytes()
    assert sha(raw) == digest
    report = json.loads(raw)
    checked = check(report)
    cases = []
    def reject(name, change):
        altered = copy.deepcopy(report)
        change(altered)
        try: check(altered)
        except (AssertionError, KeyError): pass
        else: raise AssertionError('altered capture accepted: '+name)
        cases.append(name)
    def messages(report, change):
        result = report['processes']['recover']
        values = [json.loads(line) for line in result['stdout'].splitlines()]
        change(values)
        result['stdout'] = ''.join(json.dumps(v)+'\n' for v in values)
    def tables(values, events, table, change):
        for message in values:
            if message['event'] not in events: continue
            state = message['state']
            row = next(row for row in state['control_tables'] if row[0] == table)
            rows = json.loads(row[1]); change(rows)
            row[1] = json.dumps(rows, sort_keys=True)
            state['control_sha256'] = sha(compact(state['control_tables']))
    # JSONB text order includes xmin and need not follow numeric request order.
    reordered = copy.deepcopy(report)
    for table in ['producer_requests', 'producer_outcomes']:
        messages(reordered, lambda ms,t=table:tables(ms,
            ['closing-before', 'recovery-before', 'recovery-interrupted', 'recovery-after', 'recovery-observed', 'recovery-corruption-before', 'recovery-corruption-after'],
            t, lambda rows:rows.reverse()))
    assert check(reordered) == checked
    reject('missing-seed-engine-error', lambda r:r['seed_failure'].__setitem__('lines', []))
    reject('wrong-seed-backend', lambda r:r['seed_failure'].__setitem__('control_backend_pid', 1))
    reject('wrong-original-bytes', lambda r:next(iter(r['originals'].values())).__setitem__('sha256', '00'*32))
    reject('wrong-reservation-input-length', lambda r:r['control']['requests'][1].__setitem__('input_bytes', r['control']['requests'][1]['input_bytes']+1))
    reject('remaining-client', lambda r:r.__setitem__('remaining_backends', 1))
    scenario = report['scenario']
    if scenario not in PERMANENT:
        final = ['recovery-after', 'recovery-observed']
        reject('wrong-resolved-bytes', lambda r:messages(r, lambda ms:next(m for m in ms if m['event']=='producer-resolved').__setitem__('bytes', '00')))
        if scenario in ['already', 'rejected']:
            reject('wrong-outcome-kind', lambda r:r['control']['outcomes'][-1].__setitem__('outcome_kind', 1))
        if scenario == 'rejected':
            reject('wrong-rejection-reason', lambda r:messages(r, lambda ms:next(m for m in ms if m['event']=='producer-resolved').__setitem__('rejection', 'EventId')))
        reject('missing-intermediate-snapshot', lambda r:messages(r, lambda ms:ms.pop(next(i for i,m in enumerate(ms) if m['event']=='recovery-observed'))))
        for table, n in [('producer_outcomes', 1), ('producer_requests', 2)]:
            def change_rows(rows, n=n):
                next(row for row in rows if row['request_sequence']==n)['xmin'] = '999999'
            reject('rewritten-'+table+'-xmin', lambda r,t=table,f=change_rows:messages(r, lambda ms:tables(ms, final, t, f)))
        reject('changed-native-sequence', lambda r:messages(r, lambda ms:[m['state'].__setitem__('sequence', m['state']['sequence']+1) for m in ms if m['event'] in final]))
        if scenario != 'normal':
            for field in ['xmin', 'lease_expires_at_us']:
                def change_owner(rows, field=field):
                    row = next(row for row in rows if row['role']=='ingest')
                    row[field] = '999999' if field == 'xmin' else row[field]+1
                reject('changed-historical-ingest-'+field, lambda r,f=change_owner:messages(r, lambda ms:tables(ms, final, 'owner_epochs', f)))
        if scenario == 'after-complete':
            def changed_copy(rows):
                next(row for row in rows if row['request_sequence']==2)['xmin'] = '999999'
            reject('rewritten-committed-copy-xmin', lambda r:messages(r, lambda ms:tables(ms, final, 'producer_outcomes', changed_copy)))
    else:
        if scenario == 'missing-detail':
            reject('missing-corruption-snapshot', lambda r:messages(r, lambda ms:ms.pop(next(i for i,m in enumerate(ms) if m['event']=='recovery-corruption-before'))))
        if scenario == 'missing-reservation':
            reject('wrong-unreserved-request', lambda r:messages(r, lambda ms:next(m for m in ms if m['event']=='recovery-unreserved-request').__setitem__('request', 2)))
        reject('wrong-refusal', lambda r:messages(r, lambda ms:next(m for m in ms if m['event']=='recovery-refused').__setitem__('error', 'Control(Backend)')))
    result = dict(status='PASS', scope='offline captured observations; no new engine run',
        source=report['source'], harness=report['harness_revision'], scenario=scenario,
        capture_sha256=digest, observed=checked, negative_controls=cases,
        positive_controls=['snapshot-request-row-order-independent'],
        observer_sha256=sha(Path(__file__).read_bytes()),
        checker_sha256=sha(Path(__file__).with_name('recovery.py').read_bytes()))
    with output.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(dict(status='PASS', scenario=scenario, negative_controls=len(cases))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    main(args.report, args.expected_sha256, args.output)
