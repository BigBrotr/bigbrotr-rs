#!/usr/bin/env python3
"""Retirement, exact retry, fresh live4 and import-Event refusal on real copies.

Three explicit final origins retain their actual SQL/native/input histories.
The separate retirement_claim_veto suite starts from the legacy empty origin;
it never reconstructs the unresolved state from the completed acquisition run.
"""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import transition

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized

# Literal final-endpoint identities, independently checked against retained files.
ORIGINS = ({'name': 'genuine-gap',
  'run': 'ingest-succession-acquisition-engine-f169ad7',
  'subdirectory': 'acquisition',
  'database': 'bb_successor_acquisition_f169ad7_empty',
  'status': 'PASS_SUCCESSOR_ACQUISITION_FIRST_WAVE',
  'source': 'f169ad73c0b2349032afc183b6b5b82182661eae',
  'owner': '1c1f76d5bb56a7b669db61e634040a92',
  'label': 'org.bigbrotr.succession-acquisition',
  'client_id': 'b5e6f664d585b9bc19a0e0a9f0e659cc4aa9bcf7cc8979ac59c0402592dbf1ff',
  'source_clone': 'empty',
  'final_case': 'empty-abandon-retry3',
  'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
  'result_sha256': '2eee461317257f1b105a1b498043cd885c109c6dfcb9153038d2c4a29f3ba146',
  'snapshot': 'empty-abandon-retry3-after.json',
  'snapshot_sha256': '9b3a1e6a2f7ab530f04efb669fc5f8a1075d04b48ea3e8493efe51a552b851da',
  'native_file': 'empty-abandon-retry3-after-native.json',
  'native_sha256': '269bf396c8cfc09588405495364359df1fab33737036d30990469530aa5722bc',
  'inputs_file': 'empty-abandon-retry3-after-inputs.json',
  'inputs_sha256': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
  'store': 'empty/store'},
 {'name': 'dictionary',
  'run': 'ingest-succession-source-engine-e6adc19',
  'subdirectory': 'source-close',
  'database': 'bb_successor_source_e6adc19_outcome_before_loss',
  'status': 'PASS_SUCCESSOR_SOURCE_FIRST_WAVE',
  'source': 'e6adc1902edd62b0490895c668c92f2eb21c3c35',
  'owner': '35bfc18e53fb981d70cd6b9a7ae5c7c4',
  'label': 'org.bigbrotr.succession-source',
  'client_id': '0f19cd029e76b27ce278303854c4b6132e7493c035b8df41059d5d9336a44d4d',
  'source_clone': 'outcome-before-loss',
  'final_case': 'outcome-before-loss-retry',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '79ea3633475146c908edd058ff20ef1a1b2895aae81601eb525df67c9c2189ac',
  'snapshot': 'outcome-before-loss-retry-after.json',
  'snapshot_sha256': '2c26b49cb3cab2599df2dbf819419c55627d28210f72ac8439ab972ed8677485',
  'native_file': 'outcome-before-loss-retry-after-native.json',
  'native_sha256': 'a618e931f2bf82445986ebaaf5d602de69f4b1d58c1e034c2c4af0c025a3e087',
  'inputs_file': 'outcome-before-loss-retry-after-inputs.json',
  'inputs_sha256': 'a83eecfc89111b99217d5934d6a85ab54a8e3eec9e117664638e4dbceed47db7',
  'store': 'outcome-before-loss/store'},
 {'name': 'import-event',
  'run': 'ingest-succession-source-engine-e6adc19',
  'subdirectory': 'source-close',
  'database': 'bb_successor_source_e6adc19_activated_live3',
  'status': 'PASS_SUCCESSOR_SOURCE_FIRST_WAVE',
  'source': 'e6adc1902edd62b0490895c668c92f2eb21c3c35',
  'owner': '35bfc18e53fb981d70cd6b9a7ae5c7c4',
  'label': 'org.bigbrotr.succession-source',
  'client_id': '0f19cd029e76b27ce278303854c4b6132e7493c035b8df41059d5d9336a44d4d',
  'source_clone': 'activated-live3',
  'final_case': 'activated-live3-retry',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '79ea3633475146c908edd058ff20ef1a1b2895aae81601eb525df67c9c2189ac',
  'snapshot': 'activated-live3-retry-after.json',
  'snapshot_sha256': '5cc5bd8b6c93893d9348640a4b2fb1efa9b04532e4d092537cf003cf2909adde',
  'native_file': 'activated-live3-retry-after-native.json',
  'native_sha256': '28e68d5e6ea304ef44e7968b7d341b8b4e70b56aa11c1200c260c6696727a4c8',
  'inputs_file': 'activated-live3-retry-after-inputs.json',
  'inputs_sha256': 'e87b39837e29fed0422de73faf08a1b622e1d2e04f3922889a2be320e4b0cd42',
  'store': 'activated-live3/store'})

# name, observed operation, finite command, completion event, reader status
STEPS = {
    'genuine-gap': (
        ('genuine-gap-retire3', 'retire', 'successor-lineage-retire', 'lineage-retirement-complete', 'PASS'),
        ('genuine-gap-retry3', 'retire', 'successor-lineage-retire', 'lineage-retirement-complete', 'PASS'),
        ('genuine-gap-live4', 'live-new', 'successor-lineage-live-new', 'lineage-live-complete', 'PASS'),
    ),
    'dictionary': (
        ('dictionary-retire3', 'retire', 'successor-lineage-retire', 'lineage-retirement-complete', 'PASS'),
        ('dictionary-live4', 'live-new', 'successor-lineage-live-new', 'lineage-live-complete', 'PASS'),
    ),
    'import-event': (
        ('import-event-refusal3', 'retire', 'successor-lineage-retire',
         'lineage-retirement-complete', 'PASS_EXPECTED_REFUSAL'),
    ),
}
CLONES = tuple(origin['name'] for origin in ORIGINS)
CASES = tuple(step[0] for mode in CLONES for step in STEPS[mode])
GRANTS = (
    'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
    'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;',
)
SUITE = transition.Suite(
    directory='retirement', label='org.bigbrotr.succession-retirement',
    status='PASS_SUCCESSOR_RETIREMENT_FIRST_WAVE',
    client_prefix='bigbrotr-successor-retirement-', database_prefix='bb_successor_retirement_',
    clones=CLONES, cases=CASES,
    scope='Actual activated3 retirement/retry and fresh live4 from genuine-gap and dictionary-only predecessors, plus unbound import Event refusal',
    not_run=('Retirement COMMIT loss, cancellation/expiry and missing/conflicting inputs or SQL copies',
             'Queued zero-request claim refusal belongs to the separate retirement-claim-veto root',
             'Installed repeated lifetimes, import closure, ACK/GC, Archive and PostgreSQL 255-epoch capacity'))


def _execute(pg, client, output, case, directory, expected, step):
    # Mandatory independent science for both suites; no fallback in real runs.
    from retirement_replay import verify, negative_controls
    name, operation, command, completion, expected_status = step
    assert case['name'] == name and case['operation'] == operation
    assert type(case['metadata_bytes']) is int and case['metadata_bytes'] == 262144
    assert case['proxy_port'] is None
    database = case['database']
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
                # Retain malformed raw bytes. Neither a wrapper return nor PG
                # absence alone proves that the native process has stopped.
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
    checked = verify(events, before, native, inputs, case, window)
    controls = negative_controls(events, before, native, inputs, case, window)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == dict(final['state']['originals'])
    if operation != 'live-new':
        assert final_inputs == inputs
    if operation == 'retire':
        assert final_native == native, 'retirement and refusal must preserve native rows and sequence'
    assert checked['database'] == database and checked['status'] == expected_status
    assert controls['status'] == 'PASS'
    # A known business refusal is not a process/transport failure. Its exact
    # stage and whole-state semantics are decided only by the independent reader.
    case.update(status=checked['status'], oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window)
    return after, final_native, final_inputs


def _sequence(pg, client, output, clone, directory, expected, report, steps):
    predecessor = None
    for step in steps:
        name, operation, _, _, _ = step
        case = dict(name=name, clone=clone['name'], origin=clone['origin'], operation=operation,
            predecessor=predecessor, database=clone['database'], config=clone['config'],
            metadata_bytes=262144, proxy_port=None, status='RUNNING')
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        expected = _execute(pg, client, output, case, directory, expected, step)
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = name
        lab.write_json(output / 'result.json', report)
    return predecessor


def run_sequence(pg, client, output, clone, directory, expected, report):
    assert clone['name'] == clone['origin'] and clone['name'] in CLONES
    return _sequence(pg, client, output, clone, directory, expected, report, STEPS[clone['name']])


def run(root, build):
    transition.run(root, build, suite=SUITE, sequence=run_sequence,
        origin_specs=ORIGINS, origin_for_clone={name: name for name in CLONES}, extra_grants=GRANTS)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
