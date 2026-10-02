"""Recover retained signed acquisition in isolated copies, without rerunning export.

The original laboratory is never mounted in the client. Its catalog, Control
tables and native/archive files are checked before and after the experiment.
Expected signed bytes come from the independent installed Sync oracle.
"""
import json
from pathlib import Path
import shutil
import stat
import time

import lab
from installed import set_value
from installed_archive import catalog_rows, digest
from installed_archive_native import native

SEED = 'installed-archive-export-b7b60f9'
DATABASE = 'bb_scope_bind_b7b60f9_archive_export'
SOURCE = 'b7b60f9283e3d3c72421c4df665dcd0b42c41798'
OWNER = 'df63ed47a165a536aa0937277bfb4caa'
RESULT_SHA256 = '08ec9000df8e5cf9facfd20d67cbf379f14a4c492fe36dedebb1b35e44ca63d0'


def census(home):
    """Only scientific/native/export trees; credentials are outside these paths."""
    result = {}
    for directory in ('store', 'data', 'state', 'destination'):
        root = home / directory
        for path in [root, *sorted(root.rglob('*'))]:
            metadata = path.lstat()
            assert stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)
            result[str(path.relative_to(home))] = dict(mode=metadata.st_mode,
                inode=metadata.st_ino, device=metadata.st_dev, links=metadata.st_nlink,
                bytes=metadata.st_size, sha256=digest(path) if path.is_file() else None)
    return result


def run(pg, client, output, rows, clone):
    basis = output.parent.parent / SEED / 'bindings'
    home = basis / 'archive-export'
    assert digest(basis / 'result.json') == RESULT_SHA256
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INSTALLED_ARCHIVE_EXPORT'
    assert retained['source'] == SOURCE and retained['owner'] == OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    original_client, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert original_client['Config']['Labels'][retained['label']] == OWNER
    assert not original_client['State']['Running'] and not original_client['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(DATABASE)
    assert len(original) == 69
    assert original == json.loads((basis / 'archive-export-ordinary-after.json').read_text())
    catalog = catalog_rows(pg, DATABASE + '_cat')
    assert len(catalog) == 32
    assert catalog == json.loads((basis / 'archive-export-ordinary-catalog.json').read_text())
    files = census(home)
    assert sum(v['bytes'] for k, v in files.items() if k.startswith('store/')) < 1024**2
    lab.write_json(output / 'signed-source-files.json', files)
    lab.write_json(output / 'signed-source-control.json', original)
    lab.write_json(output / 'signed-source-catalog.json', catalog)
    seed = output / 'signed-seed'
    seed.mkdir(mode=0o700)
    shutil.copytree(home / 'store', seed / 'store')
    shutil.copy2(home / 'event.jsonl', seed / 'event.jsonl')
    initialized = json.loads((basis / 'archive-export.stdout').read_text().splitlines()[0])
    transcript = [json.loads((home / 'relay-closed-001.json').read_text())]
    # The old issuer is selected from immutable reservations, never from the
    # current maximum Control epoch (ordinary read/export already claimed later).
    issuer_tuples = {(r['issuer_resource'], r['issuer_epoch'], r['issuer_incarnation'])
        for r in original['producer_requests']}
    issuer_tuple, = issuer_tuples
    issuer, = [r for r in original['owner_epochs'] if r['role'] == 'control'
        and (r['resource_id'], r['epoch'], r['process_incarnation']) == issuer_tuple]
    oracle_state = dict(original, owner_epochs=[r for r in original['owner_epochs']
        if r['role'] != 'control' or r == issuer])
    assert issuer['epoch'] < max(r['epoch'] for r in original['owner_epochs'] if r['role'] == 'control')
    evidence, _ = native(client, output, seed.name, oracle_state, initialized,
        transcript, issuer['released_at_us'])
    assert evidence['native_rows'] == 49 and evidence['original_requests'] == 4
    lab.write_json(output / 'signed-seed-oracle.json', dict(initialized=initialized,
        transcript=transcript, completed_us=issuer['released_at_us'], evidence=evidence))
    baseline = json.loads((output / 'signed-seed-native.json').read_text())
    assert baseline == json.loads((basis / 'archive-export-native.json').read_text())
    wanted = json.loads(json.dumps(baseline))
    marker, = [r for r in wanted['rows'] if r['cf'] == 'receipts' and r['key'].startswith('0131')]
    assert bytes.fromhex(marker['value']) == b'\x01\x31' + (4).to_bytes(8, 'big') + bytes(16)
    marker['value'] = (b'\x02\x31' + (4).to_bytes(8, 'big') + bytes(16) + b'\x01').hex()
    inputs = {p.name: p.read_bytes() for p in (seed / 'store/producer-inputs-v1').iterdir()}
    cases = []

    def prepare(name):
        database = clone(name, DATABASE)
        directory = output / name
        directory.mkdir(mode=0o700)
        shutil.copytree(seed / 'store', directory / 'store')
        config = lab.config(output, dict(pg, client=client), database,
            endpoint='127.0.0.1:5432', boundary='loopback')
        path = output / Path(config).name
        text = path.read_text()
        for section, key, value in [
            ('topology', 'dataset_id', initialized['dataset_id']),
            ('topology', 'instance_id', original['datasets'][0]['instance_id'][2:]),
            ('topology', 'local_roles', ['control', 'ingest', 'archive']),
            ('postgres.control', 'password_ref', 'file:/run/secrets/application'),
            ('storage', 'ingest_root', '/lab/' + name + '/store'),
            ('runtime', 'lease_ttl', '6s'), ('runtime', 'renewal_timeout', '1s'),
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
        return database, config, directory

    def invoke(database, config, directory, name, copy=False):
        before = rows(database)
        lab.write_json(output / (name + '-before.json'), before)
        started = time.monotonic()
        response = lab.cli(dict(pg, client=client), 'ingest', 'recover', '--file', config)
        elapsed = time.monotonic() - started
        lab.write_json(output / (name + '-response.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr, elapsed_seconds=elapsed))
        after = rows(database)
        lab.write_json(output / (name + '-after.json'), after)
        dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump',
            '/lab/' + directory.name + '/store')
        assert dumped.returncode == 0 and not dumped.stderr
        (output / (name + '-native.json')).write_text(dumped.stdout)
        assert json.loads(dumped.stdout) == wanted, (name, 'complete native effects')
        assert inputs == {p.name: p.read_bytes() for p in (directory / 'store/producer-inputs-v1').iterdir()}
        assert response.returncode == 0 and not response.stderr, (name, response.stderr)
        result, = [json.loads(line) for line in response.stdout.splitlines()]
        assert result == dict(format='bigbrotr-ingest-recovery/1', state='positive-outcomes-resolved',
            dataset_id=initialized['dataset_id'], owner_resource_id=initialized['owner_resource_id'],
            producer_slot_id=initialized['producer_slot_id'], producer_epoch='1', issued_through='4',
            reserved_through='4', resolved_requests='4', native='closed', owner_release='committed', ready=False)
        changed = {'owner_epochs', 'role_instances', 'producer_epochs'}
        if copy:
            changed.add('producer_outcomes')
        for table in before.keys() - changed:
            assert before[table] == after[table], (name, table)
        old, new = dict(before['producer_epochs'][0]), dict(after['producer_epochs'][0])
        assert new.pop('state') == 'closing'
        if old.pop('state') == 'closing':
            assert old['xmin'] == new['xmin']
        old.pop('xmin'); new.pop('xmin')
        assert old == new
        for table in ('owner_epochs', 'role_instances'):
            assert all(r in after[table] for r in before[table])
            assert len(after[table]) == len(before[table]) + 1
        current, = [r for r in after['owner_epochs'] if r not in before['owner_epochs']]
        assert current['role'] == 'control' and current['state'] == 'released'
        assert current['epoch'] == max(r['epoch'] for r in before['owner_epochs'] if r['role'] == 'control') + 1
        assert current['released_at_us'] is not None
        role, = [r for r in after['role_instances'] if r not in before['role_instances']]
        assert role['role'] == 'control' and role['retired_at_us'] is not None
        if copy:
            assert all(r in after['producer_outcomes'] for r in before['producer_outcomes'])
            added, = [r for r in after['producer_outcomes'] if r not in before['producer_outcomes']]
            expected = dict(next(r for r in original['producer_outcomes'] if r['request_sequence'] == 4))
            for key in ('recorded_at_us', 'recorder_resource', 'recorder_epoch', 'recorder_incarnation', 'xmin'):
                expected[key] = added[key]
            assert added == expected
            assert (added['recorder_resource'], added['recorder_epoch'], added['recorder_incarnation']) == (
                current['resource_id'], current['epoch'], current['process_incarnation'])
            assert current['acquired_at_us'] <= added['recorded_at_us'] <= current['released_at_us']
        assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        cases.append(dict(name=name, status='PASS', native_rows=49, original_requests=4,
            control_tables=len(after), seconds=elapsed, new_event_outcome_copy=copy))
        lab.write_json(output / 'signed-recovery-cases.json', cases)

    try:
        database, config, directory = prepare('signed_recovery')
        invoke(database, config, directory, 'signed-recovery')
        invoke(database, config, directory, 'signed-recovery-retry')
        database, config, directory = prepare('signed_copy')
        lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
            'DELETE FROM control.producer_outcomes WHERE request_sequence=4; COMMIT;')
        assert len(rows(database)['producer_outcomes']) == 3
        invoke(database, config, directory, 'signed-recovery-copy', copy=True)
    finally:
        preserved = dict(control=rows(DATABASE) == original,
            catalog=catalog_rows(pg, DATABASE + '_cat') == catalog,
            files=census(home) == files, report=digest(basis / 'result.json') == RESULT_SHA256)
        lab.write_json(output / 'signed-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='positive-recovery-signed', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=SOURCE, source_owner=OWNER,
        source_result_sha256=RESULT_SHA256, source_preserved=preserved, oracle=evidence,
        scope='installed signed positive recovery; successor and M1 restart/resend remain separate')
