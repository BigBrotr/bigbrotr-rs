#!/usr/bin/env python3
"""One genuine activated acquisition claim, source closure and abandonment.

Only the preserved empty-terminal2 origin is cloned. Its fresh candidate
activates epoch3 and issues a real authenticated queued claim plus an accepted,
unclaimed intent. Three later processes close the source, abandon the claim and
verify exact retry. No historical authority, job, gap or native input is forged.
"""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import transition

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized

STEPS = (
    ('empty-claims3', 'acquisition-claims', 'successor-lineage-acquisition-claims',
     'lineage-acquisition-complete'),
    ('empty-source3', 'source', 'successor-lineage-source', 'lineage-source-complete'),
    ('empty-abandon3', 'abandon', 'successor-lineage-abandon', 'lineage-acquisition-complete'),
    ('empty-abandon-retry3', 'abandon', 'successor-lineage-abandon', 'lineage-acquisition-complete'),
)
CASES = tuple(step[0] for step in STEPS)
GRANTS = (
    'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
    'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;',
)
SUITE = transition.Suite(
    directory='acquisition', label='org.bigbrotr.succession-acquisition',
    status='PASS_SUCCESSOR_ACQUISITION_FIRST_WAVE',
    client_prefix='bigbrotr-successor-acquisition-', database_prefix='bb_successor_acquisition_',
    clones=('empty',), cases=CASES,
    scope='Genuine activated3 zero-request acquisition claim and unclaimed intent, fresh source closure, whole-intent abandonment and exact retry',
    not_run=('Installed acquisition/retirement, socket execution and coverage completion',
             '64/65 claim bounds, unsupported batches, COMMIT loss and cancellation/expiry schedules',
             'Import closure, retirement, ACK/GC and PostgreSQL 255-epoch capacity'))


def invocation(case):
    """The finite command set cannot silently select a different operation."""
    index = CASES.index(case['name'])
    _, operation, command, completion = STEPS[index]
    assert case['clone'] == case['origin'] == 'empty'
    assert case['operation'] == operation
    assert case['predecessor'] == (CASES[index - 1] if index else None)
    assert type(case['metadata_bytes']) is int and case['metadata_bytes'] == 262144
    assert case['proxy_port'] is None
    return command, completion


def execute(pg, client, output, case, directory, expected):
    from acquisition_replay import verify, negative_controls
    command, completion = invocation(case)
    name, database = case['name'], case['database']
    before = schema.rows(pg, database)
    lab.write_json(output / (name + '-before.json'), before)
    inputs = history.inputs_at(directory / 'store')
    lab.write_json(output / (name + '-inputs.json'), inputs)
    native = history.dump(client, output, name + '-before', directory)
    assert normalized(before) == normalized(expected[0])
    assert native == expected[1] and inputs == expected[2]
    window = dict(before_us=history.server_clock(pg, database))
    lab.write_json(output / (name + '-clock.json'), window)
    response = dict(returncode=None, stdout='', stderr='', outcome='UNKNOWN')
    capture_errors = []
    try:
        history.capture(client, output, name, command, database,
            '/run/secrets/application', case['config'], str(case['metadata_bytes']))
    except BaseException as error:
        capture_errors.append(dict(stage='capture', error=repr(error)))
    finally:
        path = output / (name + '-response.json')
        if path.exists():
            try:
                recorded = read(path)
                if not isinstance(recorded, dict):
                    raise ValueError('response is not an object')
                response = recorded
            except (ValueError, TypeError, OSError) as error:
                # Preserve malformed bytes. A returned wrapper or backend
                # absence alone never establishes native process custody.
                capture_errors.append(dict(stage='response', error=repr(error)))
        else:
            lab.write_json(path, response)
        try:
            window['after_us'] = history.server_clock(pg, database)
        except BaseException as error:
            capture_errors.append(dict(stage='clock', error=repr(error)))
        lab.write_json(output / (name + '-capture-errors.json'), capture_errors)
        lab.write_json(output / (name + '-clock.json'), window)
        case['returncode'] = response.get('returncode')
        events, after, final_native, final_inputs = transition.external(
            pg, client, output, case, directory, response, completion_event=completion)
    assert not capture_errors and response['returncode'] == 0 and not response['stderr']
    assert not any(key in response for key in ('orchestration_error', 'clock_error'))
    # The independent reader is mandatory in real execution. It checks the
    # complete dynamic manifests/roles and job history, not a projected seed.
    checked = verify(events, before, native, inputs, case, window)
    controls = negative_controls(events, before, native, inputs, case, window)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == inputs == dict(final['state']['originals'])
    if case['operation'] == 'abandon':
        assert final_native == native, 'abandonment must preserve the already-CLOSING native store'
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window)
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    assert clone['name'] == clone['origin'] == 'empty'
    predecessor = None
    for name, operation, _, _ in STEPS:
        case = dict(name=name, clone='empty', origin='empty', operation=operation,
            predecessor=predecessor, database=clone['database'], config=clone['config'],
            metadata_bytes=262144, proxy_port=None, status='RUNNING')
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        expected = execute(pg, client, output, case, directory, expected)
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = name
        lab.write_json(output / 'result.json', report)
    return predecessor


def run(root, build):
    # The unchanged implicit-origin preflight verifies/preserves both original
    # stores, but this suite creates only the empty-origin copy. Existing auth,
    # jobs and gap privileges are retained; no policy/scope or authority repair.
    transition.run(root, build, suite=SUITE, sequence=run_sequence, extra_grants=GRANTS)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
