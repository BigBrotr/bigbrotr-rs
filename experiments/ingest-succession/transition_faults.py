#!/usr/bin/env python3
"""Real prepare COMMIT loss, fresh gap completion and native-first empty recovery.

One activated-terminal2 clone, five finite processes. The unchanged wire proxy
creates the two interrupted states; no SQL or native fixture constructs them.
"""
import copy
import json
from pathlib import Path
import shutil
import sys
import time

sys.dont_write_bytecode = True
import transition
from activated_recovery_faults import finish_proxy
from fault_cases import Process

history, schema, lab = transition.history, transition.schema, transition.lab
read, digest, normalized = transition.read, transition.digest, transition.normalized
CASES = ('activated-prepare-after', 'activated-gap', 'activated-empty-before',
         'activated-empty-complete', 'activated-empty-retry')
SUITE = transition.Suite(
    directory='transition-faults', label='org.bigbrotr.succession-transition-fault',
    status='PASS_SUCCESSOR_TRANSITION_COMMIT_LOSS',
    client_prefix='bigbrotr-successor-transition-fault-', database_prefix='bb_transition_fault_',
    clones=('activated',), cases=CASES,
    scope='Actual prepare COMMIT reply loss, fresh pending gap, native CLOSING before SQL COMMIT and fresh terminal recovery/retry',
    not_run=('Empty legacy genesis COMMIT-loss variant',
             'Activation, writer delivery, installed succession and admitted new requests',
             'Process kill, cancellation/expiry, other COMMIT boundaries, 255-epoch capacity and RSS qualification'))


def proxy_closed(fault):
    if fault is None:
        return True
    wire = fault['wire']
    return (wire.get('status') == 'CAPTURED' and wire.get('exit_code') == 0
        and wire.get('wrapper_exit') == 0 and wire.get('forced_wrapper_stop') is False
        and wire.get('stderr_nonempty') is False and wire.get('output_overflow') is False)


def invoke(pg, client, output, case, proxy_source, fault, *, commit_number=11):
    assert type(commit_number) is int and 1 <= commit_number <= 64
    name, database = case['name'], case['database']
    response = dict(returncode=None, stdout='', stderr='', outcome='UNKNOWN')
    proxy = None
    window = {}
    started = time.monotonic()
    try:
        extra = []
        if fault is not None:
            wire = fault['wire']
            proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
                '/lab/' + proxy_source.name, '--upstream-port', '5432', '--mode', fault['mode'],
                '--commit-number', str(commit_number), '--deadline-seconds', '30'], input_pipe=True)
            ready = proxy.event('ready', 5)
            wire.update(status='RUNNING', ready=ready, proxy_sha256=digest(proxy_source))
            fault['proxy_port'] = case['proxy_port'] = ready['listen_port']
            lab.write_json(output / (name + '-fault.json'), fault)
            assert ready['proxy_sha256'] == digest(proxy_source)
            assert ready['listen_host'] == '127.0.0.1' and ready['upstream_port'] == 5432
            assert ready['mode'] == fault['mode'] and ready['commit_number'] == commit_number
            extra = [str(ready['listen_port'])]
        window['before_us'] = history.server_clock(pg, database)
        lab.write_json(output / (name + '-clock.json'), window)
        history.capture(client, output, name, 'successor-' + case['operation'], database,
            '/run/secrets/application', case['config'], str(case['metadata_bytes']), *extra)
    except BaseException as error:
        response['orchestration_error'] = repr(error)
    finally:
        try:
            path = output / (name + '-response.json')
            if path.exists():
                recorded = read(path)
                if not isinstance(recorded, dict):
                    raise ValueError('response is not an object')
                if 'orchestration_error' in response:
                    recorded['orchestration_error'] = response['orchestration_error']
                response = recorded
            response['seconds'] = time.monotonic() - started
            try:
                window['after_us'] = history.server_clock(pg, database)
            except BaseException as error:
                response['clock_error'] = repr(error)
            # Preserve the actual response before collecting proxy checks.
            lab.write_json(path, response)
            lab.write_json(output / (name + '-clock.json'), window)
        finally:
            # Malformed/truncated output or a failed capture write must still
            # reap the proxy. Leave its original response bytes untouched on a
            # decode failure; execute() records the unknown external outcome.
            try:
                if proxy is not None:
                    finish_proxy(proxy, fault['wire'])
            finally:
                lab.write_json(output / (name + '-fault.json'), fault)
                if fault is not None:
                    lab.write_json(output / (name + '-wire.json'), fault['wire'])
    return response, window


def execute(pg, client, output, case, directory, expected, proxy_source, fault):
    from transition_fault_replay import verify, negative_controls
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
    window = {}
    try:
        response, window = invoke(pg, client, output, case, proxy_source, fault)
    finally:
        # Even unexpected proxy-collection failures preserve available external
        # state. A retained response can establish native custody independently;
        # missing/malformed completion or an unreaped proxy still forbids reopen.
        path = output / (name + '-response.json')
        if path.exists():
            try:
                recorded = read(path)
                if not isinstance(recorded, dict):
                    raise ValueError('response is not an object')
                response = recorded
            except (ValueError, TypeError) as error:
                response['response_error'] = repr(error)
        case['returncode'] = response.get('returncode')
        events, after, final_native, final_inputs = transition.external(
            pg, client, output, case, directory, response,
            control_failed=fault is not None, proxy_closed=proxy_closed(fault))
    assert response['returncode'] == 0 and not response['stderr']
    assert 'orchestration_error' not in response and 'clock_error' not in response
    checked = verify(events, before, native, inputs, case, window, fault=fault)
    controls = negative_controls(events, before, native, inputs, case, window, fault=fault)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == inputs
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window, fault=copy.deepcopy(fault))
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    assert clone['name'] == clone['origin'] == 'activated'
    source = history.SOURCE / 'experiments/control-migrations/fault_proxy.py'
    target = output / 'successor-transition-fault-proxy.py'
    assert not target.exists()
    shutil.copy2(source, target)
    assert digest(target) == digest(source)
    predecessor = None
    for name, operation, point, wire_mode in (
        (CASES[0], 'prepare', 'prepare', 'after-complete'),
        (CASES[1], 'gap', None, None),
        (CASES[2], 'empty', 'empty', 'before-forward'),
        (CASES[3], 'empty', None, None),
        (CASES[4], 'empty-retry', None, None),
    ):
        case = dict(name=name, clone='activated', origin='activated', operation=operation,
            predecessor=predecessor, database=clone['database'], config=clone['config'],
            metadata_bytes=262144, proxy_port=None, status='RUNNING')
        fault = None if point is None else dict(point=point, mode=wire_mode, commit_number=11,
            proxy_port=None, wire=dict(status='NOT_RUN'))
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        expected = execute(pg, client, output, case, directory, expected, target, fault)
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = name
        lab.write_json(output / 'result.json', report)
    return predecessor


def run(root, build):
    transition.run(root, build, suite=SUITE, sequence=run_sequence)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
