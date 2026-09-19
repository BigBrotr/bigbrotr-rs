#!/usr/bin/env python3
"""Discriminating checks on a hash-bound capture; no new engine execution."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from certification import check_messages, expected_event


def main(path, digest, output):
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == digest
    report = json.loads(raw)
    assert report['status'] == 'PASS' and report['scenario'] == 'normal'
    messages = [json.loads(line) for line in report['processes']['evaluate']['stdout'].splitlines()]
    # Independent catalog enumeration was captured by the host-side SQL client.
    table_names = [json.loads(v['stdout']) for v in report['sql']
                   if v.get('stdout', '').startswith('["allocation_')]
    assert len(table_names) == 1
    wanted = expected_event(bytes([1])*16, True, 1)[0]
    check = lambda values: check_messages(values, 'normal', wanted, report['originals'], table_names[0])
    checked = check(messages)
    cases = []
    existing = next(i for i, v in enumerate(messages) if v['event'] == 'certification')
    after = next(i for i, v in enumerate(messages) if v['event'] == 'certification-after')

    def reject(name, change):
        altered = copy.deepcopy(messages)
        change(altered)
        try: check(altered)
        except (AssertionError, KeyError): pass
        else: raise AssertionError('altered capture accepted: '+name)
        cases.append(name)

    for field in ['intent_digest', 'proof_digest']:
        reject(field, lambda v, f=field: v[existing]['output'].__setitem__(f, '00'*32))
    for field in ['dataset_id', 'topology_generation', 'protection_class', 'recovery_generation', 'policy_epoch']:
        reject('context-'+field, lambda v, f=field: v[existing]['output']['context'].__setitem__(f, 'wrong'))
    for field in ['dataset_id', 'shard_id', 'owner_epoch', 'sequence']:
        reject('source-'+field, lambda v, f=field: v[existing]['output']['dependencies'][0]['source_record_id'].__setitem__(f, 'wrong'))
    reject('dependency-digest', lambda v: v[existing]['output']['dependencies'][0].__setitem__('payload_digest', '00'*32))
    reject('missing-dependency', lambda v: v[existing]['output']['dependencies'].pop())
    reject('dependency-order', lambda v: v[existing]['output']['dependencies'].reverse())
    reject('false-signature-rejection', lambda v: v[existing].__setitem__('output', dict(result='rejected', reason='Signature')))
    for field, value in [('sequence', 30), ('native_sha256', 'wrong'), ('control_sha256', 'wrong'),
                         ('control_tables', []), ('originals', [])]:
        reject('changed-'+field, lambda v, f=field, x=value: v[after]['state'].__setitem__(f, x))
    result = dict(status='PASS', scope='offline captured proof and nonmutation counterexamples; no new engine run',
        source=report['source'], harness=report['harness_revision'], capture_sha256=digest,
        observed=checked, negative_controls=cases,
        observer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        checker_sha256=hashlib.sha256(Path(__file__).with_name('certification.py').read_bytes()).hexdigest())
    with output.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(dict(status='PASS', negative_controls=len(cases))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    main(args.report, args.expected_sha256, args.output)
