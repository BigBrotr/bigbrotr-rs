"""Complete genuine PG/native gaps in distinct finite replacement processes.

The seed is created by the actual preparation adapter and a post-COMMIT authority
refusal. The preparing process exits and its actual backends disappear before
copies are given to config-only recovery processes. Original labs stay immutable.
"""
import json
from pathlib import Path
import shutil

import lab
from installed import set_value
import initial_succession as preparing
from initial_succession_oracle import verify as verify_preparation, negative_controls as perturb_preparation
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_gap_oracle import verify, negative_controls, known_gap, preparation

CASES = ('gap-normal', 'gap-missing-intent', 'gap-corrupt-intent', 'gap-target-input',
    'gap-missing-input', 'gap-missing-outcome', 'gap-cancelled')


def configuration(pg, client, output, database, directory, original):
    config = lab.config(output, dict(pg, client=client), database,
        endpoint='127.0.0.1:5432', boundary='loopback')
    path = output / Path(config).name
    text = path.read_text()
    for section, key, value in [
        ('topology', 'dataset_id', original['datasets'][0]['dataset_id'][2:]),
        ('topology', 'instance_id', original['datasets'][0]['instance_id'][2:]),
        ('topology', 'local_roles', ['control', 'ingest', 'archive']),
        ('postgres.control', 'password_ref', 'file:/run/secrets/application'),
        ('storage', 'ingest_root', '/lab/' + directory.name + '/store'),
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
    return config


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    assert all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert retained['source'] == preparing.SOURCE and retained['owner'] == preparing.OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == preparing.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    baseline = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    files = preparing.census(store)
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    inputs = inputs_at(store)
    assert len(original) == 70 and len(baseline['rows']) == 49 and len(inputs) == 4
    retired = dict(control=original, native=baseline, inputs=inputs)
    original_absence = observe_backend_absence(pg, preparing.DATABASE, output, 'gap-original')
    lab.write_json(output / 'gap-original.json', dict(**retired, files=files,
        frozen=preparing.FROZEN, backend_absence=original_absence))
    cases = []
    seed = seed_files = seed_database = seed_directory = None

    def dump(label, directory):
        response, _ = capture(client, output, label + '-dump', 'dump', '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        (output / (label + '-native.json')).write_text(response.stdout)
        return json.loads(response.stdout)

    def execute(label, database, config, command):
        window = dict(before_us=server_clock(pg, database))
        lab.write_json(output / (label + '-clock.json'), window)
        response, elapsed = capture(client, output, label, command, database,
            '/run/secrets/application', config)
        window['after_us'] = server_clock(pg, database)
        lab.write_json(output / (label + '-clock.json'), window)
        assert 0 < window['before_us'] <= window['after_us'] <= window['before_us'] + 120_000_000
        absence = observe_backend_absence(pg, database, output, label)
        assert response.returncode == 0 and not response.stderr, (label, response.stderr)
        events = [json.loads(line) for line in response.stdout.splitlines()]
        return events, elapsed, window, absence

    try:
        seed_database = clone('gap-seed', preparing.DATABASE)
        seed_directory = output / 'gap-seed'
        seed_directory.mkdir(mode=0o700)
        shutil.copytree(store, seed_directory / 'store')
        assert dump('gap-seed-before', seed_directory) == baseline
        config = configuration(pg, client, output, seed_database, seed_directory, original)
        preparing.upgrade(pg, client, output, seed_database, config, 'gap-seed', rows)
        before = rows(seed_database)
        lab.write_json(output / 'gap-seed-before.json', before)
        events, seconds, clock, absence = execute('gap-seed', seed_database, config, 'successor-gap-only')
        qualified = verify_preparation(events, before, baseline, inputs, 'successor-gap-only')
        mutations = perturb_preparation(events, before, baseline, inputs, 'successor-gap-only')
        final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == 'final']
        seed_control = rows(seed_database)
        seed_native = dump('gap-seed-after', seed_directory)
        assert seed_control == {t: json.loads(v) for t, v in final['state']['control_tables']}
        assert seed_native == dict(baseline, rows=final['rows']) == baseline
        assert inputs_at(seed_directory / 'store') == inputs
        seed = dict(control=seed_control, native=seed_native, inputs=inputs, sequence=final['state']['sequence'])
        seed_files = preparing.census(seed_directory / 'store')
        known, _, _ = known_gap(seed, retired)
        lab.write_json(output / 'gap-seed-after.json', seed_control)
        lab.write_json(output / 'gap-seed.json', dict(**seed, files=seed_files, oracle=qualified,
            negative_controls=mutations, seconds=seconds, server_window=clock, backend_absence=absence,
            process_boundary='preparing command exited 0 with native closed and driver joined; actual backend absence observed; no plan transfer'))
        for mode in CASES:
            database = clone(mode, seed_database)
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(seed_directory / 'store', directory / 'store')
            assert dump(mode + '-seed', directory) == baseline
            assert inputs_at(directory / 'store') == inputs
            config = configuration(pg, client, output, database, directory, original)
            statement = {
                'gap-missing-intent': 'DELETE FROM control.initial_ingest_successions',
                'gap-corrupt-intent': "UPDATE control.initial_ingest_successions SET closure_digest=decode(repeat('ab',32),'hex')",
                'gap-missing-outcome': 'DELETE FROM control.producer_outcomes WHERE producer_epoch=1 AND request_sequence=3',
            }.get(mode)
            if statement:
                # Explicit isolated corruption; never a product mutation.
                lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; ' + statement + '; COMMIT;')
            removed = None
            if mode == 'gap-missing-input':
                name, = [n for n in inputs if n.endswith('-0000000000000004.cbor')]
                removed = directory / 'removed-input.cbor'
                (directory / 'store/producer-inputs-v1' / name).rename(removed)
                assert dict(bytes=removed.stat().st_size, sha256=preparing.digest(removed)) == inputs[name]
            if mode == 'gap-target-input':
                slot = known['registration']['producer_slot']
                folder = directory / 'store/producer-inputs-v1'
                previous = folder / (slot + '-0000000000000001-0000000000000001.cbor')
                target = folder / (slot + '-0000000000000002-0000000000000001.cbor')
                assert not target.exists()
                shutil.copy2(previous, target)
            before, native, present = rows(database), dump(mode + '-before', directory), inputs_at(directory / 'store')
            preparation(before, native, present, mode, seed, known)
            lab.write_json(output / (mode + '-before.json'), before)
            lab.write_json(output / (mode + '-inputs.json'), present)
            command = 'successor-gap-cancelled' if mode == 'gap-cancelled' else 'successor-gap-recover'
            observed, elapsed, window, absence = execute(mode, database, config, command)
            context = dict(seed=seed, retired=retired, server_window=window)
            checked = verify(observed, before, native, present, mode, **context)
            perturbations = negative_controls(observed, before, native, present, mode, **context)
            after, final_native = rows(database), dump(mode + '-after', directory)
            lab.write_json(output / (mode + '-after.json'), after)
            final, = [e for e in observed if e['event'] == 'successor-state' and e['stage'] == 'gap-final']
            assert after == {t: json.loads(v) for t, v in final['state']['control_tables']}
            assert final_native == dict(native, rows=final['rows'])
            assert inputs_at(directory / 'store') == present
            if removed:
                assert dict(bytes=removed.stat().st_size, sha256=preparing.digest(removed)) == inputs[name]
            cases.append(dict(name=mode, status='PASS', seconds=elapsed, oracle=checked,
                negative_controls=perturbations, server_window=window, backend_absence=absence,
                removed_input_retained=removed is not None))
            lab.write_json(output / 'gap-cases.json', cases)
    finally:
        preserved = dict(control=rows(preparing.DATABASE) == original, files=preparing.census(store) == files,
            artifacts=all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items()))
        if seed is not None:
            preserved.update(prepared_control=rows(seed_database) == seed['control'],
                prepared_files=preparing.census(seed_directory / 'store') == seed_files)
        lab.write_json(output / 'gap-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='prepared-initial-gap-completion', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=preparing.SOURCE, source_owner=preparing.OWNER,
        source_preserved=preserved, source_artifacts=preparing.FROZEN,
        preparation=qualified, preparation_negative_controls=mutations,
        scope='actual post-COMMIT gap followed by config-only process reconstruction and existing-intent native completion',
        excluded='writer activation, empty-attempt retirement, installed restart/resend, native syscall uncertainty/power loss, current policy advancement and full M1')
