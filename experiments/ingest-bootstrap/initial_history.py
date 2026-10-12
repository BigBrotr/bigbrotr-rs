"""Connected read-only history reconstruction from a completed prepared seed.

Each case starts a genuinely new probe process with only configuration and
retained stores. Corruption is explicit fixture setup on new copies. No original
seed is opened natively, repaired, removed or adopted as a writer.
"""
import copy
import json
from pathlib import Path
import shutil
import subprocess
import time

import lab
from installed import set_value
from initial_succession import census, digest
from initial_history_oracle import verify, negative_controls

SEED = 'ingest-succession-ac644ef'
DATABASE = 'bb_scope_bind_ac644ef_successor_normal'
SOURCE = 'ac644ef566a0f61a6315a480e196e189bc359977'
OWNER = 'cdfb0f1b755b29ccedc057b786efa116'
FROZEN = {
    'result.json': '769db38827d8efaab5cfde11af91ef7a82a6414889772db0ab6bb2f99dbc778a',
    'successor-normal-after.json': '216d261d06b3533220c5b87217d3e4d2a9c3e27aa895654da47da4aedf0c3899',
    'successor-normal-after-native.json': '011e08b3a4efde2379b143b23e539a93ba912b4ef0bb72062a2bcaf9188be4d8',
    'successor-normal-response.json': '0b1f07dbbfff71fe700a0ce55ae3390646529acf1bacff811a9b01921c1711ce',
    'successor-source.json': 'f61d7f727de7b2ff1ef4143e7a480b9b1df40140e7586808bd098fd9903160a1',
    'successor-source-preserved.json': '899e16242dbc7adb2a69fbabd4a95d2fcda73576bcb3e347a544c3a46be6f884',
}
CASES = ('history-normal', 'history-missing-intent', 'history-corrupt-intent',
    'history-missing-handoff', 'history-nonempty-target', 'history-missing-input',
    'history-missing-outcome')
NATIVE_FAULTS = {'history-missing-handoff': 'missing-handoff',
    'history-nonempty-target': 'target-nonzero'}


def inputs_at(store):
    return {p.name: dict(bytes=p.stat().st_size, sha256=digest(p))
        for p in (store / 'producer-inputs-v1').iterdir()}


def capture(client, output, label, *arguments):
    started = time.monotonic()
    try:
        response = lab.docker('exec', client, '/build/ingest_bootstrap_probe',
            *arguments, check=False, timeout=90)
    except subprocess.TimeoutExpired as error:
        def partial(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
        lab.write_json(output / (label + '-response.json'), dict(returncode=None,
            stdout=partial(error.stdout), stderr=partial(error.stderr),
            seconds=time.monotonic() - started, outcome='UNKNOWN_TIMEOUT',
            cleanup='outer owner-checked client stop; no inferred SQL/native rollback'))
        raise
    elapsed = time.monotonic() - started
    lab.write_json(output / (label + '-response.json'), dict(returncode=response.returncode,
        stdout=response.stdout, stderr=response.stderr, seconds=elapsed))
    return response, elapsed


def expected_native(mode, seed):
    wanted = copy.deepcopy(seed['native'])
    intent, = seed['control']['initial_ingest_successions']
    dataset, resource = intent['dataset_id'][2:], intent['owner_resource_id'][2:]
    if mode == 'history-missing-handoff':
        key = '010a' + dataset + resource + (1).to_bytes(8, 'big').hex()
        found, = [r for r in wanted['rows'] if r['cf'] == 'default' and r['key'] == key]
        assert found['value'] == intent['handoff'][2:]
        wanted['rows'].remove(found)
    elif mode == 'history-nonempty-target':
        key = '0102' + dataset + intent['shard_id'].to_bytes(4, 'big').hex() + (2).to_bytes(8, 'big').hex()
        found, = [r for r in wanted['rows'] if r['cf'] == 'default' and r['key'] == key]
        assert found['value'] == '01020000000000000000'
        found['value'] = '01020000000000000001'
    return wanted


def require_setup(mode, control, native, inputs, seed):
    """Prove exactly the selected setup delta before any fresh Control claim."""
    wanted = copy.deepcopy(seed['control'])
    if mode == 'history-missing-intent':
        wanted['initial_ingest_successions'] = []
    elif mode == 'history-corrupt-intent':
        previous, = wanted['initial_ingest_successions']
        changed, = control['initial_ingest_successions']
        assert changed['xmin'] != previous['xmin']
        previous.update(closure_digest='\\x' + 'ab' * 32, xmin=changed['xmin'])
    elif mode == 'history-missing-outcome':
        found, = [r for r in wanted['producer_outcomes']
            if r['producer_epoch'] == 1 and r['request_sequence'] == 3]
        wanted['producer_outcomes'].remove(found)
    assert control == wanted, (mode, 'unexpected SQL fixture change')
    assert native == expected_native(mode, seed), (mode, 'unexpected native fixture change')
    wanted_inputs = dict(seed['inputs'])
    if mode == 'history-missing-input':
        name, = [n for n in wanted_inputs if n.endswith('-0000000000000004.cbor')]
        del wanted_inputs[name]
    assert inputs == wanted_inputs, (mode, 'unexpected original input fixture change')


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / SEED / 'bindings'
    store = basis / 'successor-normal/store'
    assert all(digest(basis / name) == value for name, value in FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INITIAL_SUCCESSOR_PREPARATION'
    assert retained['source'] == SOURCE and retained['owner'] == OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed']
    assert not retained['missing_owned']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(DATABASE)
    assert original == json.loads((basis / 'successor-normal-after.json').read_text())
    baseline = json.loads((basis / 'successor-normal-after-native.json').read_text())
    files = census(store)
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    inputs = inputs_at(store)
    assert len(original) == 71 and len(baseline['rows']) == 52 and len(inputs) == 4
    assert inputs == json.loads((basis / 'successor-source.json').read_text())['inputs']
    assert all(json.loads((basis / 'successor-source-preserved.json').read_text()).values())
    response = json.loads((basis / 'successor-normal-response.json').read_text())
    assert response['returncode'] == 0 and not response['stderr']
    events = [json.loads(line) for line in response['stdout'].splitlines()]
    final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == 'final']
    assert {table: json.loads(value) for table, value in final['state']['control_tables']} == original
    assert final['rows'] == baseline['rows'] and dict(final['state']['originals']) == inputs
    seed_sequence = final['state']['sequence']
    assert seed_sequence == 62
    assert all(r['state'] == 'released' for r in original['owner_epochs'] if r['role'] == 'control')
    assert lab.sql(pg, DATABASE, "SELECT count(*) FROM pg_stat_activity WHERE "
        "datname=current_database() AND usename='bigbrotr_control'") == '0'
    seed = dict(control=original, native=baseline, inputs=inputs)
    lab.write_json(output / 'history-source.json', dict(**seed, files=files,
        sequence=seed_sequence, frozen=FROZEN, previous_client=retained['client'],
        previous_process='completed and owned source client stopped; no original Control backend'))
    cases = []

    def dump(label, directory):
        captured, _ = capture(client, output, label + '-dump', 'dump',
            '/lab/' + directory.name + '/store')
        assert captured.returncode == 0 and not captured.stderr
        (output / (label + '-native.json')).write_text(captured.stdout)
        return json.loads(captured.stdout)

    try:
        for mode in CASES:
            database = clone(mode, DATABASE)
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == baseline
            assert inputs_at(directory / 'store') == inputs
            config = lab.config(output, dict(pg, client=client), database,
                endpoint='127.0.0.1:5432', boundary='loopback')
            path = output / Path(config).name
            text = path.read_text()
            for section, key, value in [
                ('topology', 'dataset_id', original['datasets'][0]['dataset_id'][2:]),
                ('topology', 'instance_id', original['datasets'][0]['instance_id'][2:]),
                ('topology', 'local_roles', ['control', 'ingest', 'archive']),
                ('postgres.control', 'password_ref', 'file:/run/secrets/application'),
                ('storage', 'ingest_root', '/lab/' + mode + '/store'),
                ('runtime', 'lease_ttl', '20s'), ('runtime', 'renewal_timeout', '1s'),
                ('runtime', 'heartbeat_interval', '1s'), ('runtime', 'renewal_safety_margin', '1s'),
                ('runtime', 'authorization_ttl', '5s'), ('runtime', 'graceful_shutdown_timeout', '2s'),
                ('runtime.processes.application', 'memory_max_bytes', '1536MiB'),
                ('runtime.processes.application', 'baseline_bytes', '128MiB'),
                ('runtime.processes.application', 'payload_bytes', '1GiB'),
                ('runtime.processes.application', 'native_bytes', '256MiB'),
                ('runtime.processes.application', 'completion_reserve_bytes', '32MiB'),
            ]:
                text = set_value(text, section, key, value)
            path.write_text(text)
            valid = lab.cli(dict(pg, client=client), 'config', 'validate', '--file', config)
            assert valid.returncode == 0, valid.stderr
            statement = {
                'history-missing-intent': 'DELETE FROM control.initial_ingest_successions',
                'history-corrupt-intent': "UPDATE control.initial_ingest_successions SET closure_digest=decode(repeat('ab',32),'hex')",
                'history-missing-outcome': 'DELETE FROM control.producer_outcomes WHERE producer_epoch=1 AND request_sequence=3',
            }.get(mode)
            if statement:
                # An explicit operator corruption fixture, never an application
                # mutation. Only the newly named clone is selected here.
                lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; ' + statement + '; COMMIT;')
            native_corruption = None
            if mode in NATIVE_FAULTS:
                mutation = NATIVE_FAULTS[mode]
                captured, _ = capture(client, output, mode + '-corruption', 'history-corrupt',
                    '/lab/' + mode + '/store', mutation)
                (output / (mode + '-native-corruption.jsonl')).write_text(captured.stdout)
                assert captured.returncode == 0 and not captured.stderr
                native_corruption = [json.loads(line) for line in captured.stdout.splitlines()]
                before_mutation, after_mutation = native_corruption
                assert all(e['event'] == 'successor-history-corruption' and e['mutation'] == mutation
                    and e['cf'] == 'default' for e in native_corruption)
                assert before_mutation['phase'] == 'before' and after_mutation['phase'] == 'after'
                assert before_mutation['rows'] == baseline['rows']
                assert after_mutation['rows'] == expected_native(mode, seed)['rows']
                assert before_mutation['sequence'] == seed_sequence
                assert after_mutation['sequence'] == seed_sequence + 1
                assert before_mutation['key'] == after_mutation['key']
                before_values = {(r['cf'], r['key']): r['value'] for r in before_mutation['rows']}
                after_values = {(r['cf'], r['key']): r['value'] for r in after_mutation['rows']}
                assert {key for key in before_values.keys() | after_values.keys()
                    if before_values.get(key) != after_values.get(key)} == {
                        (before_mutation['cf'], before_mutation['key'])}
                for event in native_corruption:
                    found = [r['value'] for r in event['rows']
                        if r['cf'] == event['cf'] and r['key'] == event['key']]
                    assert found == ([] if event['value'] is None else [event['value']])
            removed = None
            if mode == 'history-missing-input':
                name, = [n for n in inputs if n.endswith('-0000000000000004.cbor')]
                removed = directory / 'removed-input.cbor'
                assert not removed.exists()
                (directory / 'store/producer-inputs-v1' / name).rename(removed)
                assert dict(bytes=removed.stat().st_size, sha256=digest(removed)) == inputs[name]
            before, native, present_inputs = rows(database), dump(mode + '-before', directory), inputs_at(directory / 'store')
            require_setup(mode, before, native, present_inputs, seed)
            lab.write_json(output / (mode + '-before.json'), before)
            lab.write_json(output / (mode + '-inputs.json'), present_inputs)
            captured, elapsed = capture(client, output, mode, 'successor-history', database,
                '/run/secrets/application', config)
            after = rows(database)
            final_native = dump(mode + '-after', directory)
            lab.write_json(output / (mode + '-after.json'), after)
            assert captured.returncode == 0 and not captured.stderr, (mode, captured.stderr)
            observed = [json.loads(line) for line in captured.stdout.splitlines()]
            evidence = verify(observed, before, native, present_inputs, mode, seed=seed)
            mutations = negative_controls(observed, before, native, present_inputs, mode, seed=seed)
            snapshots = [e for e in observed if e['event'] == 'successor-state']
            assert snapshots[0]['state']['sequence'] == seed_sequence + int(mode in NATIVE_FAULTS)
            final, = [e for e in snapshots if e['stage'] == 'history-final']
            assert after == {table: json.loads(value) for table, value in final['state']['control_tables']}
            assert final_native == dict(native, rows=final['rows']) == native
            assert inputs_at(directory / 'store') == present_inputs
            if removed:
                assert dict(bytes=removed.stat().st_size, sha256=digest(removed)) == inputs[name]
            assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE "
                "datname=current_database() AND usename='bigbrotr_control'") == '0'
            result, = [e for e in observed if e['event'] == 'successor-history-result']
            cases.append(dict(name=mode, status='PASS', seconds=elapsed, result=result,
                oracle=evidence, negative_controls=mutations, native_corruption=native_corruption,
                removed_input_retained=removed is not None))
            lab.write_json(output / 'history-cases.json', cases)
    finally:
        preserved = dict(control=rows(DATABASE) == original, files=census(store) == files,
            artifacts=all(digest(basis / name) == value for name, value in FROZEN.items()))
        lab.write_json(output / 'history-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='prepared-initial-history', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=SOURCE, source_owner=OWNER,
        source_preserved=preserved, source_artifacts=FROZEN,
        scope='separate-process positive predecessor reconstruction under fresh actual Control; read-only stores',
        excluded='PG/native gap completion, abandoned empty-attempt retirement, writer activation, ACK/GC, installed restart/resend and full M1')
