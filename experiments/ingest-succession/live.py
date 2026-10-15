#!/usr/bin/env python3
"""Finite continuously owned successor writers on three fresh terminal2 copies.

The third copy first retires an unactivated epoch3, then creates a new local
candidate for epoch4. No closed candidate or recorded activation is adopted.
"""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import transition

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized
CASES = ('activated-live3', 'empty-live3', 'activated-later-prepare3',
         'activated-later-empty3', 'activated-later-live4')
SUITE = transition.Suite(
    directory='live', label='org.bigbrotr.succession-live',
    status='PASS_SUCCESSOR_LIVE_FIRST_WAVE',
    client_prefix='bigbrotr-successor-live-', database_prefix='bb_successor_live_',
    clones=('activated', 'empty', 'activated-later'), cases=CASES,
    scope='Continuous local preparation/activation and live dictionary/event/retry/read at epoch3 from both terminal2 origins, and epoch4 after a genuine unactivated epoch3 retirement',
    not_run=('Activated successor retirement and installed repeated lifetimes',
             'Activation/admission COMMIT loss, cancellation/expiry and process kill',
             'Real source workflow/import closure, 255-epoch capacity and RSS qualification'))


def execute(pg, client, output, case, directory, expected):
    from live_replay import verify, negative_controls
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
        history.capture(client, output, name, 'successor-' + case['operation'], database,
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
            except (ValueError, TypeError) as error:
                # Preserve the original malformed bytes. Unknown process custody
                # below permits SQL/input capture but forbids native reopen.
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
            pg, client, output, case, directory, response,
            completion_event='lineage-live-complete' if case['operation'].startswith('lineage-live')
            else 'lineage-transition-complete')
    assert not capture_errors and response['returncode'] == 0 and not response['stderr']
    checked = verify(events, before, native, inputs, case, window)
    controls = negative_controls(events, before, native, inputs, case, window)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == dict(final['state']['originals'])
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window)
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    mode, predecessor = clone['name'], None
    operations = (('prepare3', 'prepare'), ('empty3', 'empty'), ('live4', 'lineage-live-new')) if mode == 'activated-later' else (('live3', 'lineage-live-new'),)
    for suffix, operation in operations:
        case = dict(name=mode + '-' + suffix, clone=mode, origin=clone['origin'],
            operation=operation, predecessor=predecessor, database=clone['database'],
            config=clone['config'], metadata_bytes=262144, proxy_port=None, status='RUNNING')
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        expected = execute(pg, client, output, case, directory, expected)
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
