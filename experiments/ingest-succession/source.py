#!/usr/bin/env python3
"""Source closure and exact retry on six genuine final recovery copies.

All seeds are already natively CLOSING. Both invocations must preserve every
native row, sequence and original input. Source publication changes SQL only;
this fixture does not retire owners, complete workflows or adopt a writer.
Original native stores are read only as files and never opened by an engine.
"""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import transition

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized

# Literal pins of the six actual e2eb140 recovery/retry endpoints.
ORIGINS = ({'name': 'activation-after-loss',
  'run': 'ingest-succession-recovery-engine-e2eb140',
  'subdirectory': 'recovery',
  'database': 'bb_successor_recover_e2eb140_activation_after_loss',
  'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
  'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
  'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
  'label': 'org.bigbrotr.succession-recovery',
  'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
  'source_clone': 'activation-after-loss',
  'final_case': 'activation-after-loss-retry',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
  'snapshot': 'activation-after-loss-retry-after.json',
  'snapshot_sha256': 'c1c76f83af2b47c96552eb9fd9a317acb3177b459b70a312d588b2a11a51617a',
  'native_file': 'activation-after-loss-retry-after-native.json',
  'native_sha256': '78b4a460f7e3abdbf513fb5d5625e7a2cf481445e23bf40437109370ac7c10b4',
  'inputs_file': 'activation-after-loss-retry-after-inputs.json',
  'inputs_sha256': '2a60c0a8f80eab6c2875f5546ab47df9053e2fac9f69b799fc7e1d276560af25',
  'store': 'activation-after-loss/store'},
 {'name': 'outcome-before-loss',
  'run': 'ingest-succession-recovery-engine-e2eb140',
  'subdirectory': 'recovery',
  'database': 'bb_successor_recover_e2eb140_outcome_before_loss',
  'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
  'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
  'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
  'label': 'org.bigbrotr.succession-recovery',
  'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
  'source_clone': 'outcome-before-loss',
  'final_case': 'outcome-before-loss-retry',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
  'snapshot': 'outcome-before-loss-retry-after.json',
  'snapshot_sha256': '5293a2ee51c91a6685fbbf41c57e35c3366bbcfc92ee2d27c5984680580853c7',
  'native_file': 'outcome-before-loss-retry-after-native.json',
  'native_sha256': 'a618e931f2bf82445986ebaaf5d602de69f4b1d58c1e034c2c4af0c025a3e087',
  'inputs_file': 'outcome-before-loss-retry-after-inputs.json',
  'inputs_sha256': 'a83eecfc89111b99217d5934d6a85ab54a8e3eec9e117664638e4dbceed47db7',
  'store': 'outcome-before-loss/store'},
 {'name': 'outcome-after-loss',
  'run': 'ingest-succession-recovery-engine-e2eb140',
  'subdirectory': 'recovery',
  'database': 'bb_successor_recover_e2eb140_outcome_after_loss',
  'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
  'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
  'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
  'label': 'org.bigbrotr.succession-recovery',
  'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
  'source_clone': 'outcome-after-loss',
  'final_case': 'outcome-after-loss-retry',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
  'snapshot': 'outcome-after-loss-retry-after.json',
  'snapshot_sha256': '90c29db8e66616ba26efdc46e11d4eff706f3310068a43a9af32c8ac4871e35a',
  'native_file': 'outcome-after-loss-retry-after-native.json',
  'native_sha256': '22312f5cc31c739c43ec9b8ff8d78a940a9e87494682a847f214099a47379239',
  'inputs_file': 'outcome-after-loss-retry-after-inputs.json',
  'inputs_sha256': 'f5dfa70f26aad25dc36e0676cab9e698373262080130c2715d69696495ccc80b',
  'store': 'outcome-after-loss/store'},
 {'name': 'activated-live3',
  'run': 'ingest-succession-recovery-engine-e2eb140',
  'subdirectory': 'recovery',
  'database': 'bb_successor_recover_e2eb140_activated_live3',
  'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
  'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
  'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
  'label': 'org.bigbrotr.succession-recovery',
  'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
  'source_clone': 'activated-live3',
  'final_case': 'activated-live3-retry',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
  'snapshot': 'activated-live3-retry-after.json',
  'snapshot_sha256': '1239a1e4d7615ae4b641ee070f6576b4ab0f08ed66ebe8c8311b15ca4bf30334',
  'native_file': 'activated-live3-retry-after-native.json',
  'native_sha256': '28e68d5e6ea304ef44e7968b7d341b8b4e70b56aa11c1200c260c6696727a4c8',
  'inputs_file': 'activated-live3-retry-after-inputs.json',
  'inputs_sha256': 'e87b39837e29fed0422de73faf08a1b622e1d2e04f3922889a2be320e4b0cd42',
  'store': 'activated-live3/store'},
 {'name': 'empty-live3',
  'run': 'ingest-succession-recovery-engine-e2eb140',
  'subdirectory': 'recovery',
  'database': 'bb_successor_recover_e2eb140_empty_live3',
  'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
  'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
  'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
  'label': 'org.bigbrotr.succession-recovery',
  'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
  'source_clone': 'empty-live3',
  'final_case': 'empty-live3-retry',
  'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
  'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
  'snapshot': 'empty-live3-retry-after.json',
  'snapshot_sha256': '1b3ddf145507be385c685cb308746d73202f2d6d2831bb8b1e04b829fd5bbb85',
  'native_file': 'empty-live3-retry-after-native.json',
  'native_sha256': 'a90e114d12d15a7c96c41a5e87ea8c4a999edd78c0126d610277be3cac7738cd',
  'inputs_file': 'empty-live3-retry-after-inputs.json',
  'inputs_sha256': 'be0224ca229820c162970bba46e8e9459d5c0aa2bbbd16b0fdd1c284fad5db18',
  'store': 'empty-live3/store'},
 {'name': 'activated-later-live4',
  'run': 'ingest-succession-recovery-engine-e2eb140',
  'subdirectory': 'recovery',
  'database': 'bb_successor_recover_e2eb140_activated_later_live4',
  'status': 'PASS_SUCCESSOR_RECOVERY_FIRST_WAVE',
  'source': 'e2eb14064900ec2ba97fa3b551a686c1195244b6',
  'owner': 'b48752e102ce8fbbb769afdcb3ec150e',
  'label': 'org.bigbrotr.succession-recovery',
  'client_id': '38e9d281b74e98ab6af9bf993e2d40f7ebd796e01ad8dc2ecfcdf99e867ea3cf',
  'source_clone': 'activated-later-live4',
  'final_case': 'activated-later-live4-retry',
  'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
  'result_sha256': '618a1e7ec2a100e26074285edda72a27f275d9953ea8e1771afb2d85ea5a7e56',
  'snapshot': 'activated-later-live4-retry-after.json',
  'snapshot_sha256': 'de9ef1f396bf7979664860e4b28c0e75b097c3d105f6d81847fa0fc4a068419b',
  'native_file': 'activated-later-live4-retry-after-native.json',
  'native_sha256': 'ebe8cbcf3d9fc04bc7f4fd585326875dc160235a8bedb4131a543c00c51fa29e',
  'inputs_file': 'activated-later-live4-retry-after-inputs.json',
  'inputs_sha256': '90127efe7298437df3866ac4c44e83779fb325de5624a9a2b81f3c102f183afd',
  'store': 'activated-later-live4/store'})

CLONES = tuple(origin['name'] for origin in ORIGINS)
CASES = tuple(name + '-' + step for name in CLONES for step in ('source', 'retry'))
GRANTS = (
    'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
    'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;',
)
SUITE = transition.Suite(
    directory='source-close', label='org.bigbrotr.succession-source',
    status='PASS_SUCCESSOR_SOURCE_FIRST_WAVE',
    client_prefix='bigbrotr-successor-source-', database_prefix='bb_successor_source_',
    clones=CLONES, cases=CASES,
    scope='Positive later-epoch source closure and existing-copy exact retry on six genuinely recovered origins, with zero native writes',
    not_run=('Installed source closure, workflow abandonment and retirement',
             'Source-open missing-copy repair, corrupt closed-source copies, source COMMIT loss, cancellation/expiry and unresolved tails',
             'PostgreSQL 255-epoch capacity, performance and RSS qualification'))


def execute(pg, client, output, case, directory, expected):
    from source_replay import verify, negative_controls
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
        history.capture(client, output, name, 'successor-lineage-source', database,
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
            completion_event='lineage-source-complete')
    assert not capture_errors and response['returncode'] == 0 and not response['stderr']
    assert 'orchestration_error' not in response and 'clock_error' not in response
    checked = verify(events, before, native, inputs, case, window)
    controls = negative_controls(events, before, native, inputs, case, window)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == inputs == dict(final['state']['originals'])
    assert final_native == native, 'already-CLOSING source closure must not write native state'
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window)
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    name = clone['name']
    assert name in CLONES and clone['origin'] == name
    predecessor = None
    for step in ('source', 'retry'):
        case = dict(name=name + '-' + step, clone=name, origin=name, operation='source',
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
