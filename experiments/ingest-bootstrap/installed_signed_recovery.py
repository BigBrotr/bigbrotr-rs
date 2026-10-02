"""Recover retained signed acquisition in isolated copies, without rerunning export.

The original laboratory is never mounted in the client. Its catalog, Control
tables and native/archive files are checked before and after the experiment.
Expected signed bytes come from the independent installed Sync oracle.
"""
import hashlib
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


def run(pg, client, output, rows, clone, close_source=False, schedules=None):
    assert schedules in (None, 'faults', 'locks', 'abandon', 'abandon-guards', 'abandon-limits', 'abandon-faults') and (not schedules or close_source)
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
    evidence, expected_source = native(client, output, seed.name, oracle_state, initialized,
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
    # Independent fixed-width closure preimage; no product closure/digest is an
    # expected input. The preceding oracle checked these full native/input bytes.
    manifest, = [v for cf, k, v in expected_source['native'] if cf == 'default' and k == b'\x01\x01']
    closure = hashlib.sha256(b'bigbrotr/initial-source-closure/1\0')
    closure.update(hashlib.sha256(manifest).digest())
    closure.update(bytes.fromhex(initialized['producer_slot_id']))
    closure.update((1).to_bytes(8, 'big'))
    closure.update((4).to_bytes(8, 'big'))
    for sequence, (original_input, receipt) in enumerate(zip(expected_source['inputs'], expected_source['receipts']), 1):
        closure.update(sequence.to_bytes(8, 'big'))
        closure.update(hashlib.sha256(original_input).digest())
        closure.update(hashlib.sha256(receipt).digest())
    closure.update((12).to_bytes(8, 'big'))
    closure_digest = closure.hexdigest()
    cases = []

    def prepare(name, template=DATABASE):
        database = clone(name, template)
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

    def check_native(directory, name):
        dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump',
            '/lab/' + directory.name + '/store')
        assert dumped.returncode == 0 and not dumped.stderr
        (output / (name + '-native.json')).write_text(dumped.stdout)
        assert json.loads(dumped.stdout) == wanted, (name, 'complete native effects')
        assert inputs == {p.name: p.read_bytes() for p in (directory / 'store/producer-inputs-v1').iterdir()}

    def invoke(database, config, directory, name, copy=False, error=None, reclaims_control=False):
        before = rows(database)
        lab.write_json(output / (name + '-before.json'), before)
        started = time.monotonic()
        flags = ['--close-source'] if close_source else []
        response = lab.cli(dict(pg, client=client), 'ingest', 'recover', '--file', config, *flags)
        elapsed = time.monotonic() - started
        lab.write_json(output / (name + '-response.json'), dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr, elapsed_seconds=elapsed))
        after = rows(database)
        lab.write_json(output / (name + '-after.json'), after)
        check_native(directory, name)
        result = dict(format='bigbrotr-ingest-recovery/1', state='source-closed' if close_source else 'positive-outcomes-resolved',
            dataset_id=initialized['dataset_id'], owner_resource_id=initialized['owner_resource_id'],
            producer_slot_id=initialized['producer_slot_id'], producer_epoch='1', issued_through='4',
            reserved_through='4', resolved_requests='4', native='closed', owner_release='committed', ready=False)
        if close_source:
            result['source_closure'] = dict(last_complete_sequence='12', sha256=closure_digest)
        if error:
            assert response.returncode == 4 and not response.stdout and error in response.stderr, response
        else:
            assert response.returncode == 0 and not response.stderr, (name, response.stderr)
            actual, = [json.loads(line) for line in response.stdout.splitlines()]
            assert actual == result, actual
        changed = {'owner_epochs', 'role_instances', 'producer_epochs'}
        if copy:
            changed.add('producer_outcomes')
        if close_source and not error:
            changed.add('source_streams')
        for table in before.keys() - changed:
            assert before[table] == after[table], (name, table)
        old, new = dict(before['producer_epochs'][0]), dict(after['producer_epochs'][0])
        assert new.pop('state') == 'closing'
        if old.pop('state') == 'closing':
            assert old['xmin'] == new['xmin']
        old.pop('xmin'); new.pop('xmin')
        assert old == new
        for table in ('owner_epochs', 'role_instances'):
            assert len(after[table]) == len(before[table]) + 1
            if not reclaims_control:
                assert all(r in after[table] for r in before[table])
        previous_epoch = max(r['epoch'] for r in before['owner_epochs'] if r['role'] == 'control')
        current, = [r for r in after['owner_epochs'] if r['role'] == 'control' and r['epoch'] == previous_epoch + 1]
        assert current['role'] == 'control' and current['state'] == ('active' if error else 'released')
        assert (current['released_at_us'] is None) == bool(error)
        role, = [r for r in after['role_instances'] if r['role_instance_id'] == current['role_instance_id']]
        assert role['role'] == 'control' and (role['retired_at_us'] is None) == bool(error)
        if reclaims_control:
            from source_closure_faults import assert_reclaimed
            assert_reclaimed(before, after, current)
        if close_source and not error:
            old, new = dict(before['source_streams'][0]), dict(after['source_streams'][0])
            if old['state'] == 'closed':
                assert old == new, 'closed retry changed retained attribution or xmin'
            else:
                assert old.pop('state') == 'open' and new.pop('state') == 'closed'
                assert old.pop('closed_at_us') is None
                assert current['acquired_at_us'] <= new.pop('closed_at_us') <= current['released_at_us']
                assert old.pop('last_complete_sequence') is None and new.pop('last_complete_sequence') == 12
                assert old.pop('closure_digest') is None and new.pop('closure_digest') == '\\x' + closure_digest
                old.pop('xmin'); new.pop('xmin')
                assert old == new
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
            control_tables=len(after), seconds=elapsed, new_event_outcome_copy=copy,
            source_closed=close_source and not error, expected_refusal=error))
        lab.write_json(output / 'signed-recovery-cases.json', cases)

    try:
        if schedules in ('abandon', 'abandon-guards', 'abandon-limits', 'abandon-faults'):
            from acquisition_abandonment import run as run_abandonment
            run_abandonment(pg, client, output, rows, prepare, check_native, cases, closure_digest, initialized,
                wave={'abandon': 'first', 'abandon-guards': 'guards', 'abandon-limits': 'limits', 'abandon-faults': 'faults'}[schedules])
        elif schedules == 'locks':
            from source_closure_locks import run as run_locks
            run_locks(pg, client, output, rows, prepare, check_native, cases, closure_digest)
        elif schedules == 'faults':
            from source_closure_faults import run as run_faults
            run_faults(pg, client, output, rows, prepare, check_native, invoke, cases, closure_digest)
        else:
            database, config, directory = prepare('signed_recovery')
            invoke(database, config, directory, 'signed-recovery')
            invoke(database, config, directory, 'signed-recovery-retry')
            database, config, directory = prepare('signed_copy')
            lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
                'DELETE FROM control.producer_outcomes WHERE request_sequence=4; COMMIT;')
            assert len(rows(database)['producer_outcomes']) == 3
            invoke(database, config, directory, 'signed-recovery-copy', copy=True)
        if close_source and not schedules:
            database, config, directory = prepare('source_cut_conflict')
            # Only this isolated clone has an existing job cut above the final
            # native bound. Source closure must roll back, preserving that cut.
            lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
                'UPDATE control.job_input_bounds SET last_complete_sequence=13 '
                'WHERE last_complete_sequence=12; COMMIT;')
            assert any(r['last_complete_sequence'] == 13 for r in rows(database)['job_input_bounds'])
            invoke(database, config, directory, 'source-cut-conflict', error='Backend')
    finally:
        preserved = dict(control=rows(DATABASE) == original,
            catalog=catalog_rows(pg, DATABASE + '_cat') == catalog,
            files=census(home) == files, report=digest(basis / 'result.json') == RESULT_SHA256)
        lab.write_json(output / 'signed-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='acquisition-abandonment-signed' if schedules in ('abandon', 'abandon-guards', 'abandon-limits', 'abandon-faults') else 'source-closure-signed' if close_source else 'positive-recovery-signed', status='PASS', cases=cases,
        source_lab=str(basis.parent), source_revision=SOURCE, source_owner=OWNER,
        source_result_sha256=RESULT_SHA256, source_preserved=preserved, oracle=evidence,
        scope='installed initial acquisition abandonment' if schedules in ('abandon', 'abandon-guards', 'abandon-limits', 'abandon-faults') else 'installed initial source closure' if close_source else 'installed signed positive recovery',
        excluded='producer/domain retirement, successor and M1 restart/resend')
