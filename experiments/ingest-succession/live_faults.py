#!/usr/bin/env python3
"""Actual activation/outcome COMMIT loss with explicit fresh-process disposition.

Four fresh activated-terminal2 copies; no original native store is opened.
Only the unactivated copy can currently retire. Activated-family recovery is a
separate missing responsibility, so the other fresh processes must refuse.
"""
import copy
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
import transition
import transition_faults

history, schema, lab = transition.history, transition.schema, transition.lab
read, digest, normalized = transition.read, transition.digest, transition.normalized
CLONES = ('activation-before', 'activation-after', 'outcome-before', 'outcome-after')
CASES = tuple(clone + '-' + step for clone in CLONES for step in ('loss', 'fresh'))
SUITE = transition.Suite(
    directory='live-faults', label='org.bigbrotr.succession-live-fault',
    status='PASS_SUCCESSOR_LIVE_COMMIT_LOSS',
    client_prefix='bigbrotr-successor-live-fault-', database_prefix='bb_live_fault_',
    clones=CLONES, cases=CASES,
    scope='Actual activation and first dictionary outcome COMMIT reply loss before forwarding and after completion; fresh empty retirement or explicit activated-history refusal',
    not_run=('Positive activated-family recovery and outcome repair',
             'Same-session reuse after failure, cancellation/expiry and process kill',
             'Activated retirement, installed repeated lifetimes, capacity/RSS qualification'))


def execute(pg, client, output, case, directory, expected, proxy_source, fault):
    from live_fault_replay import verify, negative_controls
    name, database = case['name'], case['database']
    before = schema.rows(pg, database)
    lab.write_json(output / (name + '-before.json'), before)
    inputs = history.inputs_at(directory / 'store')
    lab.write_json(output / (name + '-inputs.json'), inputs)
    native = history.dump(client, output, name + '-before', directory)
    assert normalized(before) == normalized(expected[0])
    assert native == expected[1] and inputs == expected[2]
    lab.write_json(output / (name + '-fault.json'), fault)
    response = dict(returncode=None, stdout='', stderr='', outcome='UNKNOWN')
    window, capture_errors = {}, []
    try:
        response, window = transition_faults.invoke(pg, client, output, case,
            proxy_source, fault, commit_number=fault['commit_number'] if fault else 11)
    except BaseException as error:
        capture_errors.append(dict(stage='invoke', error=repr(error)))
    finally:
        # invoke's nested finally collects the proxy even when decoding fails.
        # Retain malformed response bytes and forbid native reopen if custody is
        # unknown. SQL/input observations remain available as provisional data.
        path = output / (name + '-response.json')
        if path.exists():
            try:
                recorded = read(path)
                if not isinstance(recorded, dict):
                    raise ValueError('response is not an object')
                response = recorded
            except (ValueError, TypeError) as error:
                capture_errors.append(dict(stage='response', error=repr(error)))
        lab.write_json(output / (name + '-capture-errors.json'), capture_errors)
        case['returncode'] = response.get('returncode')
        if case['operation'] == 'lineage-history':
            assert fault is None
            events, after, final_native, final_inputs = history.external(
                pg, client, output, case, directory, response)
        else:
            events, after, final_native, final_inputs = transition.external(
                pg, client, output, case, directory, response,
                control_failed=fault is not None,
                proxy_closed=transition_faults.proxy_closed(fault),
                completion_event='lineage-live-complete' if fault else 'lineage-transition-complete')
    assert not capture_errors and response['returncode'] == 0 and not response['stderr']
    assert 'orchestration_error' not in response and 'clock_error' not in response
    checked = verify(events, before, native, inputs, case, window, fault=fault)
    controls = negative_controls(events, before, native, inputs, case, window, fault=fault)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == dict(final['state']['originals'])
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window, fault=copy.deepcopy(fault))
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    name = clone['name']
    assert name in CLONES and clone['origin'] == 'activated'
    source = history.SOURCE / 'experiments/control-migrations/fault_proxy.py'
    target = output / 'successor-live-fault-proxy.py'
    if not target.exists():
        shutil.copy2(source, target)
    assert digest(target) == digest(source)
    point, boundary = name.split('-')
    predecessor = None
    for step in ('loss', 'fresh'):
        operation = 'lineage-live-new' if step == 'loss' else (
            'empty' if name == 'activation-before' else 'lineage-history')
        case = dict(name=name + '-' + step, clone=name, origin='activated', operation=operation,
            predecessor=predecessor, database=clone['database'], config=clone['config'],
            metadata_bytes=262144, proxy_port=None, status='RUNNING')
        fault = None if step == 'fresh' else dict(point=point,
            mode='before-forward' if boundary == 'before' else 'after-complete',
            commit_number=12 if point == 'activation' else 16,
            proxy_port=None, wire=dict(status='NOT_RUN'))
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        expected = execute(pg, client, output, case, directory, expected, target, fault)
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = case['name']
        lab.write_json(output / 'result.json', report)
    return predecessor


def run(root, build):
    transition.run(root, build, suite=SUITE, sequence=run_sequence)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
