#!/usr/bin/env python3
"""Six real retirement fault observations on four fresh genuine-gap copies.

Only the immutable qualified acquisition endpoint supplies the original state.
Losses retain the actual terminal receipt or its absence; a fresh process owns
each retry. The independent reader decides all scientific success and refusal.
"""
import copy
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
import retirement
import transition_faults
import retirement_wait

transition = retirement.transition
history, schema, lab = transition.history, transition.schema, transition.lab
read, digest, normalized = transition.read, transition.digest, transition.normalized
ORIGINS = (copy.deepcopy(retirement.ORIGINS[0]),)
CLONES = ('before', 'after', 'cancel', 'expiry')
STEPS = {
    'before': ('before-forward', 'before-forward-fresh'),
    'after': ('after-complete', 'after-complete-fresh'),
    'cancel': ('final-job-cancel',),
    'expiry': ('final-job-expiry',),
}
CASES = tuple(name for clone in CLONES for name in STEPS[clone])
SUITE = transition.Suite(
    directory='retirement-faults', label='org.bigbrotr.succession-retirement-fault',
    status='PASS_SUCCESSOR_RETIREMENT_FAULTS',
    client_prefix='bigbrotr-successor-retirement-fault-', database_prefix='bb_successor_retirement_fault_',
    clones=CLONES, cases=CASES,
    scope='Actual final retirement COMMIT10 loss in both directions and fresh retry, final-job-wait cancellation and real Control UTC expiry',
    not_run=('Installed supervision, process kill and remote cancellation',
             'Epoch4 admission, additional corruption or raw SQL guard cases',
             'Unresolved zero-request claim clone remains untouched; no workflow repair or import closure'))


def control_failed(response, fault):
    """Observe a proxy driver's actual result; the reader binds the wire cause."""
    if fault is None:
        return False
    events = [json.loads(line) for line in response['stdout'].splitlines()]
    assert all(isinstance(event, dict) for event in events)
    driver, = [e for e in events if e.get('event') == 'lineage-history-driver'
        and e.get('driver') == 'control']
    assert type(driver['failed']) is bool and driver['joined'] is True
    return driver['failed']


def execute(pg, client, output, case, directory, expected, proxy_source, fault, wait_mode):
    from retirement_fault_replay import verify, negative_controls
    name, database = case['name'], case['database']
    before = schema.rows(pg, database)
    lab.write_json(output / (name + '-before.json'), before)
    inputs = history.inputs_at(directory / 'store')
    lab.write_json(output / (name + '-inputs.json'), inputs)
    native = history.dump(client, output, name + '-before', directory)
    assert normalized(before) == normalized(expected[0])
    assert native == expected[1] and inputs == expected[2]
    response = dict(returncode=None, stdout='', stderr='', outcome='UNKNOWN')
    window, schedule, diagnostic, errors = {}, None, None, []
    lab.write_json(output / (name + '-fault.json'), fault)
    try:
        if wait_mode is None:
            response, window = transition_faults.invoke(pg, client, output, case,
                proxy_source, fault, commit_number=10)
        else:
            assert fault is None
            response, window, schedule = retirement_wait.invoke(
                pg, client, output, case, wait_mode)
    except BaseException as error:
        errors.append(dict(stage='invoke', error=repr(error)))
    finally:
        # A malformed wrapper reply stays on disk. It cannot establish remote
        # native exclusion, even if the PostgreSQL backend has disappeared.
        path = output / (name + '-response.json')
        if path.exists():
            try:
                recorded = read(path)
                if not isinstance(recorded, dict):
                    raise ValueError('response is not an object')
                response = recorded
            except (ValueError, TypeError, OSError) as error:
                errors.append(dict(stage='response', error=repr(error)))
                response = dict(returncode=None, stdout='', stderr='', outcome='UNKNOWN_RESPONSE')
        else:
            lab.write_json(path, response)
        if wait_mode is not None and schedule is None:
            path = output / (name + '-schedule.json')
            try:
                schedule = read(path) if path.exists() else None
            except (ValueError, TypeError, OSError) as error:
                errors.append(dict(stage='schedule', error=repr(error)))
        lab.write_json(output / (name + '-capture-errors.json'), errors)
        case['returncode'] = response.get('returncode')
        auxiliary_closed = False
        failed = False
        try:
            auxiliary_closed = (transition_faults.proxy_closed(fault) if wait_mode is None
                else retirement_wait.closed_and_joined(response, schedule))
            failed = control_failed(response, fault)
        except (ValueError, TypeError, KeyError, AttributeError, AssertionError) as error:
            errors.append(dict(stage='driver-result', error=repr(error)))
            auxiliary_closed = False
            lab.write_json(output / (name + '-capture-errors.json'), errors)
        events, after, final_native, final_inputs = transition.external(
            pg, client, output, case, directory, response,
            control_failed=failed, proxy_closed=auxiliary_closed,
            completion_event='lineage-retirement-complete')
    # Capture first; semantic or orchestration rejection never discards a known
    # external final state. Unknown custody above prohibits native opening.
    if wait_mode is not None:
        diagnostic = retirement_wait.diagnostic(pg, output, name, schedule, window, wait_mode)
    assert not errors and response['returncode'] == 0 and not response['stderr']
    assert not any(k in response for k in ('orchestration_error', 'clock_error'))
    if wait_mode is not None:
        assert schedule['status'] == 'CAPTURED' and not schedule['cleanup_errors']
        assert 'schedule_error' not in schedule
    checked = verify(events, before, native, inputs, case, window,
        wire=fault, schedule=schedule, diagnostic=diagnostic)
    controls = negative_controls(events, before, native, inputs, case, window,
        wire=fault, schedule=schedule, diagnostic=diagnostic)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_native == native and final_inputs == inputs == dict(final['state']['originals'])
    assert checked['database'] == database and checked['status'] == controls['status'] == 'PASS'
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window, fault=copy.deepcopy(fault))
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    mode = clone['name']
    assert mode in CLONES and clone['origin'] == 'genuine-gap'
    source = history.SOURCE / 'experiments/control-migrations/fault_proxy.py'
    target = output / 'successor-retirement-fault-proxy.py'
    if not target.exists():
        shutil.copy2(source, target)
    assert digest(target) == digest(source)
    predecessor = None
    for name in STEPS[mode]:
        case = dict(name=name, clone=mode, origin='genuine-gap', operation='retire',
            predecessor=predecessor, database=clone['database'], config=clone['config'],
            metadata_bytes=262144, proxy_port=None, status='RUNNING')
        wait_mode = mode if mode in ('cancel', 'expiry') else None
        fault = (dict(point='retirement', mode=name, commit_number=10,
            proxy_port=None, wire=dict(status='NOT_RUN'))
            if name in ('before-forward', 'after-complete') else None)
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        expected = execute(pg, client, output, case, directory, expected, target, fault, wait_mode)
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = name
        lab.write_json(output / 'result.json', report)
    return predecessor


def run(root, build):
    transition.run(root, build, suite=SUITE, sequence=run_sequence,
        origin_specs=ORIGINS, origin_for_clone={name: 'genuine-gap' for name in CLONES},
        extra_grants=retirement.GRANTS)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
