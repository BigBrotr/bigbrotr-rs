#!/usr/bin/env python3
"""Fresh activated successor recovery and exact retry on six preserved origins.

Fault origins include their already-qualified fresh history refusal: the actual
final database is copied, never projected back to its earlier loss snapshot.
Original native stores are read only as files and never opened by an engine.
"""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import transition

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized

# Literal pins from the six stopped, qualified full-state origins.
ORIGINS = ({'name': 'activation-after-loss',
  'run': 'ingest-succession-live-faults-engine-e3806a9',
  'subdirectory': 'live-faults',
  'database': 'bb_live_fault_e3806a9_activation_after',
  'status': 'PASS_SUCCESSOR_LIVE_COMMIT_LOSS',
  'source': 'e3806a9f48cdeb4b5c7350f4e4f44ae18bb25819',
  'owner': '1d25e8dbff2141300432875866555117',
  'label': 'org.bigbrotr.succession-live-fault',
  'client_id': '05c859a84ae1ea4d2317b4c84f39e057b88a166275f5f15e9e26a099714aa0d3',
  'source_clone': 'activation-after',
  'final_case': 'activation-after-fresh',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '424255457b839d34bfa2db42a70ca81cfcb081050bc6f4de457f648fda17ffd1',
  'snapshot': 'activation-after-fresh-after.json',
  'snapshot_sha256': 'ca873ae8a78b917c74f2f6ad9dab9214674181cfe80cb60e0d42701666ceb0b2',
  'native_file': 'activation-after-fresh-after-native.json',
  'native_sha256': '3e104cc276e9b22192405252b936cb54b235f18aba56c9f19c29f90e3a6d6b6b',
  'inputs_file': 'activation-after-fresh-after-inputs.json',
  'inputs_sha256': '2a60c0a8f80eab6c2875f5546ab47df9053e2fac9f69b799fc7e1d276560af25',
  'store': 'activation-after/store'},
 {'name': 'outcome-before-loss',
  'run': 'ingest-succession-live-faults-engine-e3806a9',
  'subdirectory': 'live-faults',
  'database': 'bb_live_fault_e3806a9_outcome_before',
  'status': 'PASS_SUCCESSOR_LIVE_COMMIT_LOSS',
  'source': 'e3806a9f48cdeb4b5c7350f4e4f44ae18bb25819',
  'owner': '1d25e8dbff2141300432875866555117',
  'label': 'org.bigbrotr.succession-live-fault',
  'client_id': '05c859a84ae1ea4d2317b4c84f39e057b88a166275f5f15e9e26a099714aa0d3',
  'source_clone': 'outcome-before',
  'final_case': 'outcome-before-fresh',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '424255457b839d34bfa2db42a70ca81cfcb081050bc6f4de457f648fda17ffd1',
  'snapshot': 'outcome-before-fresh-after.json',
  'snapshot_sha256': 'b33ee8dbc35f193ceac83bb4621b5899d89c0442b5c43ba9537200f2eb977092',
  'native_file': 'outcome-before-fresh-after-native.json',
  'native_sha256': '70cc31c03be545dbfbc4c3c5293b2617e413bf45d5b0af8a39fab41a4e9c7ed7',
  'inputs_file': 'outcome-before-fresh-after-inputs.json',
  'inputs_sha256': 'a83eecfc89111b99217d5934d6a85ab54a8e3eec9e117664638e4dbceed47db7',
  'store': 'outcome-before/store'},
 {'name': 'outcome-after-loss',
  'run': 'ingest-succession-live-faults-engine-e3806a9',
  'subdirectory': 'live-faults',
  'database': 'bb_live_fault_e3806a9_outcome_after',
  'status': 'PASS_SUCCESSOR_LIVE_COMMIT_LOSS',
  'source': 'e3806a9f48cdeb4b5c7350f4e4f44ae18bb25819',
  'owner': '1d25e8dbff2141300432875866555117',
  'label': 'org.bigbrotr.succession-live-fault',
  'client_id': '05c859a84ae1ea4d2317b4c84f39e057b88a166275f5f15e9e26a099714aa0d3',
  'source_clone': 'outcome-after',
  'final_case': 'outcome-after-fresh',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '424255457b839d34bfa2db42a70ca81cfcb081050bc6f4de457f648fda17ffd1',
  'snapshot': 'outcome-after-fresh-after.json',
  'snapshot_sha256': '88620fc0f1520fdb0f861ef5ba09b703e021cd980746568769462df43c3c8b95',
  'native_file': 'outcome-after-fresh-after-native.json',
  'native_sha256': '8dc9f9dd47bc03a550c26307f007e111c24cd958655a7e5fbd97095f5112f637',
  'inputs_file': 'outcome-after-fresh-after-inputs.json',
  'inputs_sha256': 'f5dfa70f26aad25dc36e0676cab9e698373262080130c2715d69696495ccc80b',
  'store': 'outcome-after/store'},
 {'name': 'activated-live3',
  'run': 'ingest-succession-live-engine-006fe3d',
  'subdirectory': 'live',
  'database': 'bb_successor_live_006fe3d_activated',
  'status': 'PASS_SUCCESSOR_LIVE_FIRST_WAVE',
  'source': '006fe3d9e6f5cd4cef3eca7b4ea2fb08ca4d8063',
  'owner': '63d2ef283499bd9354aa47ad668ab38a',
  'label': 'org.bigbrotr.succession-live',
  'client_id': '733815b607de6afd630ddfc3a9a582e9befded51e4a124da4adec6f592e57ba0',
  'source_clone': 'activated',
  'final_case': 'activated-live3',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '1c921c35572e651ac0df45ead7a1e510181326dea99a6b6ab99fbf820c76b5ed',
  'snapshot': 'activated-live3-after.json',
  'snapshot_sha256': '72f166eb1deb56129e792a9d426cdf2e54924c6b33872c6cdf4278a94cca45e1',
  'native_file': 'activated-live3-after-native.json',
  'native_sha256': 'd14338f256988eeed3bd1e913f56e55eff7614dc50b24ea2500b8aaa5b8ee189',
  'inputs_file': 'activated-live3-after-inputs.json',
  'inputs_sha256': 'e87b39837e29fed0422de73faf08a1b622e1d2e04f3922889a2be320e4b0cd42',
  'store': 'activated/store'},
 {'name': 'empty-live3',
  'run': 'ingest-succession-live-engine-006fe3d',
  'subdirectory': 'live',
  'database': 'bb_successor_live_006fe3d_empty',
  'status': 'PASS_SUCCESSOR_LIVE_FIRST_WAVE',
  'source': '006fe3d9e6f5cd4cef3eca7b4ea2fb08ca4d8063',
  'owner': '63d2ef283499bd9354aa47ad668ab38a',
  'label': 'org.bigbrotr.succession-live',
  'client_id': '733815b607de6afd630ddfc3a9a582e9befded51e4a124da4adec6f592e57ba0',
  'source_clone': 'empty',
  'final_case': 'empty-live3',
  'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
  'result_sha256': '1c921c35572e651ac0df45ead7a1e510181326dea99a6b6ab99fbf820c76b5ed',
  'snapshot': 'empty-live3-after.json',
  'snapshot_sha256': '1c9a755a2c3bdb3d0b87cf1feba032c98920dd29a921b1e747b53e7c3d0d6df9',
  'native_file': 'empty-live3-after-native.json',
  'native_sha256': 'ef0bc5537f80cf3bcc7eacc9ee3b669e5d49df37fa283af667271fbb2390074c',
  'inputs_file': 'empty-live3-after-inputs.json',
  'inputs_sha256': 'be0224ca229820c162970bba46e8e9459d5c0aa2bbbd16b0fdd1c284fad5db18',
  'store': 'empty/store'},
 {'name': 'activated-later-live4',
  'run': 'ingest-succession-live-engine-006fe3d',
  'subdirectory': 'live',
  'database': 'bb_successor_live_006fe3d_activated_later',
  'status': 'PASS_SUCCESSOR_LIVE_FIRST_WAVE',
  'source': '006fe3d9e6f5cd4cef3eca7b4ea2fb08ca4d8063',
  'owner': '63d2ef283499bd9354aa47ad668ab38a',
  'label': 'org.bigbrotr.succession-live',
  'client_id': '733815b607de6afd630ddfc3a9a582e9befded51e4a124da4adec6f592e57ba0',
  'source_clone': 'activated-later',
  'final_case': 'activated-later-live4',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '1c921c35572e651ac0df45ead7a1e510181326dea99a6b6ab99fbf820c76b5ed',
  'snapshot': 'activated-later-live4-after.json',
  'snapshot_sha256': 'bc4355fdef03f0633a7407687cec92d33c2ddf2682a573faabd453d0ce9fa4fd',
  'native_file': 'activated-later-live4-after-native.json',
  'native_sha256': '5a96206f1e8bd78decc280a5765e51bc407b12e778381bccf0b1d167b462c275',
  'inputs_file': 'activated-later-live4-after-inputs.json',
  'inputs_sha256': '90127efe7298437df3866ac4c44e83779fb325de5624a9a2b81f3c102f183afd',
  'store': 'activated-later/store'})

CLONES = tuple(origin['name'] for origin in ORIGINS)
CASES = tuple(name + '-' + step for name in CLONES for step in ('recover', 'retry'))
GRANTS = (
    'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
    'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;',
)
SUITE = transition.Suite(
    directory='recovery', label='org.bigbrotr.succession-recovery',
    status='PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
    client_prefix='bigbrotr-successor-recovery-', database_prefix='bb_successor_recover_',
    clones=CLONES, cases=CASES,
    scope='Fresh activated-family native/SQL closing, positive original-input replay, missing current SQL copy repair and exact retry on six genuine stopped origins',
    not_run=('Installed recovery, source closure, workflow abandonment and retirement',
             'Recovery COMMIT loss, cancellation/expiry, unresolved staged/reserved tails and corruption refusals',
             'PostgreSQL 255-epoch capacity, performance and RSS qualification'))


def execute(pg, client, output, case, directory, expected):
    from recovery_replay import verify, negative_controls
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
        history.capture(client, output, name, 'successor-lineage-recover', database,
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
                # Keep malformed raw bytes; custody is positively established
                # from exact terminal telemetry, never wrapper exit alone.
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
        # Known close/join plus actual backend exclusion precede fresh native
        # inspection. Unknown custody preserves provisional SQL/input captures.
        events, after, final_native, final_inputs = transition.external(
            pg, client, output, case, directory, response,
            completion_event='lineage-recovery-complete')
    assert not capture_errors and response['returncode'] == 0 and not response['stderr']
    assert 'orchestration_error' not in response and 'clock_error' not in response
    checked = verify(events, before, native, inputs, case, window)
    controls = negative_controls(events, before, native, inputs, case, window)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == inputs == dict(final['state']['originals'])
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window)
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    name = clone['name']
    assert name in CLONES and clone['origin'] == name
    predecessor = None
    for step in ('recover', 'retry'):
        case = dict(name=name + '-' + step, clone=name, origin=name, operation='recover',
            predecessor=predecessor, database=clone['database'], config=clone['config'],
            metadata_bytes=262144, proxy_port=None, status='RUNNING')
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        expected = execute(pg, client, output, case, directory, expected)
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = case['name']
        lab.write_json(output / 'result.json', report)
    return predecessor


def run(root, build):
    transition.run(root, build, suite=SUITE, sequence=run_sequence,
        origin_specs=ORIGINS, origin_for_clone={name: name for name in CLONES}, extra_grants=GRANTS)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
