#!/usr/bin/env python3
"""Trusted exact-source product receipt reader, SELECT-only PG and readonly data.

Run only after signed_publication_cases in the separately inventoried lab. Each
finite reader gets a new owned container, readonly source/artifacts/data, bounded
resources and one private job root. No host ports or credentials in diagnostics.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
import time

from signed_publication_cases import SOURCE, command, event, send, sha
from signed_publication_oracle import corpus
from fault_cases import Incomplete, Process


def main(root):
    resources = json.loads((root / 'resources.json').read_text())
    assert SOURCE == root / 'source'
    assert command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    assert json.loads((root / 'signed-cases.json').read_text())['status'] == 'PASS'
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    assert ready['source'] == resources['source']
    for key, name in [('receipt_reader', 'native-tests/archive-receipt-probe'),
                      ('worker', 'archive-worker/bigbrotr-archive-worker')]:
        assert ready[key] == sha(root / 'out' / name)
    pg = resources['postgres_id']
    state = json.loads(command('docker', 'inspect', pg))[0]
    assert state['State']['Running'] and state['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
    home = root / 'receipt-cases'
    home.mkdir(mode=0o700)
    result = dict(source=resources['source'], tree=resources['tree'], artifacts=ready,
                  harness_sha256=sha(Path(__file__)), status='INCOMPLETE', cases=[])
    evidence = root / 'receipt-cases.json'

    def save():
        evidence.write_text(json.dumps(result, indent=2) + '\n')

    def sql(query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', 'bbpub_normal', input=query)

    def catalog():
        return json.loads(sql("SELECT json_build_object('snapshots',(SELECT count(*) FROM ducklake_snapshot),'files',(SELECT json_agg(row_to_json(f) ORDER BY data_file_id) FROM ducklake_data_file f),'owner',(SELECT row_to_json(o) FROM bigbrotr_archive.owner o));"))

    batch = corpus()[0][1]
    base = dict(expected=dict(dataset_id='01' * 16, schema_version=1, topology_generation='0',
        batch_digest=batch['archive_digest'], record_count=5,
        input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=6, last_sequence=10)],
        attempt_id=[183] * 16, publication_ordinal=3), catalog_id='5a' * 16,
        data_root='/out/signed_normal/kind=7/', database='bbpub_normal', port=5432,
        role='bigbrotr_receipt_reader', max_receipts=16, reserve_bytes=384 << 20,
        deadline_ms=15000, cancel_when_blocked=False)
    before = result['catalog_before'] = catalog()
    privileges = json.loads(sql("SELECT json_build_object('superuser',rolsuper,'createdb',rolcreatedb,'createrole',rolcreaterole,'replication',rolreplication,'bypassrls',rolbypassrls,'memberships',(SELECT count(*) FROM pg_auth_members WHERE member=r.oid),'connect',has_database_privilege(r.oid,current_database(),'CONNECT'),'create',has_database_privilege(r.oid,current_database(),'CREATE'),'temp',has_database_privilege(r.oid,current_database(),'TEMP'),'native_tables',(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r'),'native_select',(SELECT bool_and(has_table_privilege(r.oid,c.oid,'SELECT')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r'),'native_write',(SELECT bool_or(has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r'),'owner_select',has_table_privilege(r.oid,'bigbrotr_archive.owner','SELECT')) FROM pg_roles r WHERE rolname='bigbrotr_receipt_reader';"))
    result['reader_privileges'] = privileges
    save()
    assert privileges == dict(superuser=False, createdb=False, createrole=False, replication=False,
        bypassrls=False, memberships=0, connect=True, create=False, temp=False, native_tables=28,
        native_select=True, native_write=False, owner_select=False)

    def run(name, value, wanted, block=None):
        case = dict(name=name, status='INCOMPLETE', input=value)
        result['cases'].append(case)
        directory = home / name
        directory.mkdir(mode=0o700)
        job = directory / 'job'
        job.mkdir(mode=0o700)
        fixture = directory / 'fixture'
        fixture.mkdir(mode=0o700)
        (fixture / 'other').mkdir(mode=0o700)
        (fixture / 'input.json').write_text(json.dumps(value) + '\n')
        save()
        holder = None
        identity = None
        started = time.monotonic()

        def logs():
            capture = Process(['docker', 'logs', identity], limit=65536)
            try:
                code = capture.wait(5)
            finally:
                capture.stop()
                case.update(stdout=capture.stdout.decode(errors='replace'),
                            stderr=capture.stderr.decode(errors='replace'), logs_overflow=capture.overflow)
                save()
            assert code == 0 and not capture.overflow

        try:
            if block:
                holder = Process(['docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
                    '-U', 'postgres', '-d', 'bbpub_normal'], input_pipe=True)
                send(holder, "BEGIN; LOCK TABLE bigbrotr_archive.installation IN ACCESS EXCLUSIVE MODE; SELECT json_build_object('event','held','pid',pg_backend_pid());\n")
                held = event(holder, 'held')
            identity = command('docker', 'create', '--name', resources['container'] + '-receipt-' + name,
                '--label', 'org.bigbrotr.publication=' + resources['owner'],
                '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '1', '--memory', '768m',
                '--memory-swap', '768m', '--pids-limit', '256', '--network', 'container:' + pg,
                '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                '--mount', f'type=bind,src={root / "out"},dst=/out,readonly',
                '--mount', f'type=bind,src={root / "secrets" / value["role"]},dst=/secrets/{value["role"]},readonly',
                '--mount', f'type=bind,src={job},dst=/job',
                '--mount', f'type=bind,src={fixture},dst=/fixture,readonly',
                resources['image'], '/out/native-tests/archive-receipt-probe')
            case['container_id'] = identity
            save()
            command('docker', 'start', identity)
            if block:
                until = time.monotonic() + 4
                while time.monotonic() < until:
                    waiting = json.loads(sql(f"SELECT coalesce(json_agg(row_to_json(a)),'[]'::json) FROM (SELECT pid,backend_start,wait_event_type,wait_event FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_receipt_reader' AND application_name='bigbrotr-archive-receipt' AND {held['pid']}=ANY(pg_blocking_pids(pid)))a;"))
                    if waiting:
                        break
                    time.sleep(.02)
                assert len(waiting) == 1 and waiting[0]['wait_event_type'] == 'Lock', 'real reader must block'
                case['blocked_backend'] = waiting[0]
                save()
                if block == 'cancel':
                    cancellation_started = time.monotonic()
                    (fixture / 'cancel').touch(exist_ok=False)
            exit_code = int(command('docker', 'wait', identity, timeout=25))
            if block == 'cancel':
                case['cancel_to_exit_ms'] = (time.monotonic() - cancellation_started) * 1000
            case['exit'] = exit_code
            logs()
            save()
            assert exit_code == 0 and not case['stderr']
            observed = json.loads(case['stdout'])
            assert observed['outcome'] == wanted and observed['active_permits'] == 0, observed
            assert observed['cancelled'] == (block == 'cancel')
            if block == 'deadline':
                assert 5000 <= observed['elapsed_ms'] < 7000
            if block == 'cancel':
                # The 15s deadline cannot satisfy this independent cancellation
                # window. Keep the actual PG blocker until after reader exit.
                assert case['cancel_to_exit_ms'] < 2000
            assert catalog() == before, 'receipt read changed durable native state'
        except BaseException as error:
            case['failure'] = type(error).__name__
            case['status'] = 'INCOMPLETE' if isinstance(error, (Incomplete, subprocess.TimeoutExpired, KeyboardInterrupt)) else 'FAIL'
            raise
        finally:
            try:
                if holder:
                    send(holder, 'ROLLBACK;\n\\q\n')
                    holder.wait(5)
                    holder.stop()
            finally:
                if identity:
                    actual = json.loads(command('docker', 'inspect', identity))[0]
                    assert actual['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
                    if actual['State']['Running']:
                        command('docker', 'kill', identity)
                        command('docker', 'wait', identity)
                        actual = json.loads(command('docker', 'inspect', identity))[0]
                    case['terminal'] = actual['State']
                    case['data_mount_readonly'] = any(m['Destination'] == '/out' and not m['RW'] for m in actual['Mounts'])
                    if 'stdout' not in case:
                        logs()
                    assert not actual['State']['Running'] and not actual['State']['OOMKilled'] and case['data_mount_readonly']
                case['wall_seconds'] = time.monotonic() - started
                save()
        case['status'] = 'PASS'
        save()

    try:
        run('present', base, 'Ok(Present)')
        absent = copy.deepcopy(base)
        absent['expected'].update(batch_digest='f1' * 32, attempt_id=[242] * 16, publication_ordinal=50)
        run('absent', absent, 'Ok(Absent)')
        for name, changes in [
            ('digest', dict(batch_digest='f1' * 32)), ('attempt', dict(attempt_id=[242] * 16)),
            ('ordinal', dict(publication_ordinal=50)), ('dataset', dict(dataset_id='02' * 16)),
            ('topology', dict(topology_generation='1')),
        ]:
            value = copy.deepcopy(base)
            value['expected'].update(changes)
            run(name, value, 'Err(ObservationUnknown)')
        for name, changes, wanted in [
            ('catalog', dict(catalog_id='5b' * 16), 'Err(ObservationUnknown)'),
            ('root', dict(data_root='/fixture/other/'), 'Err(ObservationUnknown)'),
            ('denied', dict(role='bigbrotr_receipt_denied'), 'Err(ObservationUnknown)'),
            ('unavailable', dict(port=9), 'Err(ObservationUnknown)'),
            ('scan-limit', dict(max_receipts=1), 'Err(ObservationUnknown)'),
            ('allocation', dict(reserve_bytes=1), 'Err(ResourceLimit)'),
        ]:
            run(name, dict(base, **changes), wanted)
        for block in ['deadline', 'cancel']:
            run(block, dict(base, deadline_ms=5000 if block == 'deadline' else 15000,
                           cancel_when_blocked=block == 'cancel'),
                'Err(ObservationUnknown)', block=block)
        files = json.loads(sql("SELECT json_agg(s.path||t.path||f.path ORDER BY data_file_id) FROM ducklake_data_file f JOIN ducklake_table t USING(table_id) JOIN ducklake_schema s USING(schema_id) WHERE t.table_name='archive_commits';"))
        assert len(files) == 2
        path = root / 'out/signed_normal/kind=7' / files[-1]
        assert path.resolve().is_relative_to(root / 'out/signed_normal/kind=7')
        original = path.read_bytes()
        original_mode = path.stat().st_mode & 0o777
        result['fault_object'] = dict(path=str(path.relative_to(root)), sha256=sha(path), bytes=len(original))
        backup = home / 'retained-receipt.parquet'
        backup.write_bytes(original)
        save()
        try:
            path.unlink()
            run('missing-unrelated', absent, 'Err(ObservationUnknown)')
            path.write_bytes(b'corrupt receipt fixture')
            run('corrupt-unrelated', absent, 'Err(ObservationUnknown)')
        finally:
            path.write_bytes(original)
            path.chmod(original_mode)
            assert sha(path) == result['fault_object']['sha256']
        run('restored', base, 'Ok(Present)')
        result['catalog_after'] = catalog()
        assert result['catalog_after'] == before
        assert sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_receipt_reader','bigbrotr_receipt_denied');") == '0'
        result['status'] = 'PASS'
    except BaseException as error:
        result['status'] = 'INCOMPLETE' if isinstance(error, (Incomplete, subprocess.TimeoutExpired, KeyboardInterrupt)) else 'FAIL'
        raise
    finally:
        save()
    print(json.dumps(dict(status=result['status'], cases=len(result['cases']))), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    main(parser.parse_args().root.resolve())
