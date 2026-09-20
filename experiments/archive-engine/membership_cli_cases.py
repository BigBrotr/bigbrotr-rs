#!/usr/bin/env python3
"""Actual batch-inspect over signed publication in one inventoried exact-Git lab.

The fixture owns retained files throughout; no installed frontier/pin is implied.
An owner-verified terminal audit must exclude remote processes even on failure.
"""
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from signed_publication_cases import Process, command, event, execute, send, sha
from signed_publication_oracle import check, corpus, input_expectations, input_files, producer_input_files
from coordinator_cli_cases import SIGNAL, change
from receipt_cli_cases import WATCH


def main(root):
    source = Path(__file__).resolve().parents[2]
    resources = json.loads((root/'resources.json').read_text())
    assert source == root/'source'
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == resources['source']
    for label, path in [('control', 'native-tests/bigbrotr'), ('worker', 'archive-worker/bigbrotr-archive-worker'),
                        ('publisher', 'native-tests/archive-publication-probe'), ('reader', 'native-tests/archive-read-probe')]:
        assert sha(root/'out'/path) == ready[label]
    path = root/'membership-cli-cases.json'
    assert not path.exists()
    report = dict(source=resources['source'], tree=resources['tree'], status='INCOMPLETE', artifacts=ready,
                  harness_sha256=sha(Path(__file__)), oracle_sha256=sha(source/'experiments/archive-engine/signed_publication_oracle.py'),
                  cases=[], fixtures={}, blocked=[])
    binary = '/out/native-tests/bigbrotr'
    native = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib', runner]

    def save():
        path.write_text(json.dumps(report, indent=2)+'\n')

    def sql(db, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=query)

    def drain():
        until = time.monotonic()+8
        while sql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('bigbrotr_control','CatalogManager','bigbrotr_receipt_reader')") != '0':
            assert time.monotonic() < until, 'application backends did not drain'
            time.sleep(.02)

    def metadata(mode):
        db = 'bbpub_'+mode
        names = json.loads(sql(db, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='public'"))
        assert names and all(n.startswith('ducklake_') and n.replace('_', '').isalnum() for n in names)
        return {name: json.loads(sql(db, f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM public.{name} t")) for name in names}

    def preserved(mode):
        home = root/f'out/signed_{mode}'
        return dict(files={str(p.relative_to(home)): dict(bytes=p.stat().st_size, sha256=sha(p))
                           for folder in ['kind=7', 'archive-state'] for p in sorted((home/folder).rglob('*')) if p.is_file()},
                    inputs=input_files(home, mode), producer_inputs=producer_input_files(home, history=mode == 'history'),
                    catalog=metadata(mode), control=json.loads(sql('bbpub_control_'+mode,
                    "SELECT json_build_object('bindings',(SELECT json_agg(t) FROM (SELECT c.*,xmin::text AS xmin FROM control.archive_coordinators c)t),'attempts',(SELECT json_agg(t ORDER BY ordinal) FROM (SELECT a.*,xmin::text AS xmin FROM control.archive_attempts a)t))")))

    def args(mode, attempt, extras=()):
        return [binary, 'archive', 'batch-inspect', '--file', f'/out/signed_{mode}/receipt.toml',
                '--attempt-id', attempt, '--worker-root', '/out/archive-worker', *extras]

    def expected(mode, ordinal, snapshot):
        wanted = next(v for v in input_expectations(mode) if v['publication_ordinal'] == ordinal)
        counts = [5, 0, 0, 0, 0] if ordinal == 2 else [0, 0, 0, 1, 1] if ordinal == 5 else [1, 1, 1, 1, 1]
        return dict(format='bigbrotr-archive-batch-membership/1', state='present', dataset_id='01'*16,
                    catalog_id='5a'*16, attempt_id=bytes(wanted['attempt_id']).hex(),
                    publication_ordinal=str(ordinal), batch_digest=wanted['batch_digest'], max_receipts=128,
                    catalog_inspection='verified', reader_privileges='select-only', writer_exclusion='not-performed',
                    replay_authority='not-granted', source_protection='not-granted', ready=False,
                    snapshot_id=str(snapshot), family_counts=counts, files=1+sum(n != 0 for n in counts),
                    max_facts=20000, payload_recomputation='not-performed', reference_closure='not-certified',
                    owner_release='committed', policy_revision='1')

    def cli(label, mode, ordinal, snapshot, *, extras=(), code=0, contains=None):
        wanted = expected(mode, ordinal, snapshot)
        record = dict(name=label, status='INCOMPLETE', args=args(mode, wanted['attempt_id'], extras), expected_exit=code)
        report['cases'].append(record)
        started = time.monotonic()
        try:
            execute(['docker', 'exec', runner, *record['args']], 135, record)
        finally:
            record['elapsed_ms'] = round((time.monotonic()-started)*1000)
            save()
        assert record['exit'] == code and not record['overflow'], record
        if code:
            assert not record['stdout'] and contains in record['stderr'], record
        else:
            assert not record['stderr'] and json.loads(record['stdout']) == wanted, record
        drain()
        record['status'] = 'PASS'; save()

    def blocked(cancel):
        """Real SELECT lock wait after manager inspection, with pidfd child reap."""
        label = 'blocked-signal' if cancel else 'blocked-deadline'
        observation = dict(name=label, status='INCOMPLETE')
        report['blocked'].append(observation); save()
        config = root/'out/signed_normal/receipt.toml'
        original = config.read_text()
        proxy = holder = watcher = client = None
        try:
            proxy = Process(['docker', 'exec', '-i', runner, 'python3', '-B',
                '/source/experiments/archive-engine/native_commit_proxy.py', '--mode', 'hold-reader-query',
                '--lifetime-seconds', '180'], input_pipe=True)
            hello = event(proxy, 'ready')
            assert hello['source_sha256'] == sha(source/'experiments/archive-engine/native_commit_proxy.py')
            observation['proxy_ready'] = hello
            config.write_text(change(original, 'postgres.catalog', 'endpoint', f'127.0.0.1:{hello["port"]}'))
            argv = args('normal', expected('normal', 3, 3)['attempt_id'])
            client = Process(['docker', 'exec', runner, *argv])
            started = time.monotonic()
            observation['held'] = event(proxy, 'held')
            until = time.monotonic()+8
            while sql('bbpub_normal', "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='CatalogManager'") != '0':
                assert time.monotonic() < until
                time.sleep(.02)
            holder = Process(['docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bbpub_normal'], input_pipe=True)
            send(holder, "BEGIN; LOCK TABLE public.ducklake_snapshot IN ACCESS EXCLUSIVE MODE; SELECT json_build_object('event','locked','pid',pg_backend_pid());\n")
            blocker = event(holder, 'locked')['pid']
            send(proxy, 'release\n'); event(proxy, 'released')
            until = time.monotonic()+10
            while True:
                rows = json.loads(sql('bbpub_normal', f"SELECT coalesce(json_agg(json_build_object('pid',pid,'application',application_name,'blockers',pg_blocking_pids(pid))),'[]'::json) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_receipt_reader' AND wait_event_type='Lock' AND {blocker}=ANY(pg_blocking_pids(pid))"))
                if rows:
                    assert len(rows) == 1 and rows[0]['application'] == 'bigbrotr-archive-membership'
                    observation['blocking'] = rows; save(); break
                assert time.monotonic() < until, 'no real native reader lock wait'
                time.sleep(.02)
            watch = WATCH.replace("'inspect-receipt' in r['argv']", "'inspect-membership' in r['argv']")
            watcher = Process(['docker', 'exec', '-i', runner, 'python3', '-c', watch, *argv], input_pipe=True)
            observation['watch_ready'] = event(watcher, 'watch-ready')
            marked = time.monotonic()
            if cancel:
                observation['signal'] = json.loads(command('docker', 'exec', runner, 'python3', '-c', SIGNAL, 'TERM', *argv))
            observation['exit'] = client.wait(12 if cancel else 130)
            observation['elapsed_ms'] = round((time.monotonic()-started)*1000)
            observation['after_marker_ms'] = round((time.monotonic()-marked)*1000)
            observation.update(stdout=client.stdout.decode(), stderr=client.stderr.decode(), overflow=client.overflow)
            save()
            assert observation['exit'] == 4 and not observation['stdout'] and not observation['overflow']
            if cancel:
                assert 'cancelled' in observation['stderr']
            else:
                # The supervised task and its parent start separate120s timers.
                # Either can report first; the real lock, elapsed lower bound,
                # pidfd reap and unchanged durable state discriminate expiry.
                assert ('deadline' in observation['stderr'] or
                        'receipt observation unresolved: ObservationUnknown' in observation['stderr'])
                observation['deadline_path'] = ('supervisor' if 'deadline' in observation['stderr'] else 'native-observation')
            assert observation['after_marker_ms'] < (10000 if cancel else 125000)
            if not cancel: assert observation['elapsed_ms'] >= 119000
            send(watcher, 'finish\n')
            observation['watch_terminal'] = event(watcher, 'watch-terminal')
            assert watcher.wait(5) == 0 and not watcher.stderr and not watcher.overflow
            send(holder, 'ROLLBACK;\n\\q\n'); assert holder.wait(5) == 0 and not holder.stderr
            send(proxy, 'stop\n'); assert proxy.wait(5) == 0 and not proxy.stderr
            observation['proxy_terminal'] = event(proxy, 'terminal')
            assert observation['proxy_terminal']['unexpected'] == 0
            observation['status'] = 'PASS'
        finally:
            cleanup_errors = []
            try: config.write_text(original)
            except OSError as error: cleanup_errors.append(dict(action='restore-config', error=type(error).__name__))
            observation['processes'] = {}
            for process_label, process in [('client', client), ('watcher', watcher), ('holder', holder), ('proxy', proxy)]:
                if process:
                    try: process.stop()
                    except BaseException as error: cleanup_errors.append(dict(action='stop-'+process_label, error=type(error).__name__))
                    observation['processes'][process_label] = dict(exit=process.process.poll(),
                        stdout=process.stdout.decode(errors='replace'), stderr=process.stderr.decode(errors='replace'),
                        overflow=process.overflow, remote_exclusion='UNKNOWN; requires owned terminal lab audit')
            observation['cleanup_errors'] = cleanup_errors
            save()
            if cleanup_errors and sys.exc_info()[0] is None:
                raise AssertionError('fixture cleanup incomplete; see retained observations')
        drain()
        cli(label+'-retry', 'normal', 3, 3)

    save()
    try:
        for mode in ['normal', 'history']:
            db = 'bbpub_'+mode
            sql('postgres', f'CREATE DATABASE {db}; CREATE DATABASE bbpub_control_{mode};')
            sql(db, f'REVOKE ALL ON DATABASE {db} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
            fixture = report['fixtures'][mode] = dict(status='INCOMPLETE', publisher={})
            try:
                execute(native+['/out/native-tests/archive-publication-probe', mode, '5432', '--event-read'], 180, fixture['publisher'])
            finally: save()
            parent = fixture['publisher']
            assert parent['exit'] == 0 and not parent['stderr'] and not parent['overflow']
            lines = [json.loads(v) for v in parent['stdout'].splitlines()]
            assert next(v['batches'] for v in lines if v['event'] == 'retained-inputs') == corpus(history=mode == 'history')[0]
            assert [v['expected'] for v in lines if v['event'] == 'inputs-reopened'] == input_expectations(mode)
            fixture['independent_readback'] = {}
            try: execute(native+['/out/native-tests/archive-read-probe', mode], 45, fixture['independent_readback'])
            finally: save()
            readback = fixture['independent_readback']
            assert readback['exit'] == 0 and not readback['stderr'] and not readback['overflow']
            check(mode, [v for v in lines if v['event'] != 'inputs-reopened'], json.loads(readback['stdout']))
            home, chome = root/f'out/signed_{mode}', f'/out/signed_{mode}'
            for name, role in [('control', 'bigbrotr_control'), ('reader', 'bigbrotr_receipt_reader')]:
                f = home/name; f.write_bytes((root/'secrets'/role).read_bytes()); f.chmod(0o600)
            text = (source/'config/recipes/archive-only.toml').read_text()
            for section, key, value in [
                ('administration', 'enabled', False), ('topology', 'dataset_id', '01'*16),
                ('topology', 'instance_id', '02'*16), ('archive', 'catalog_id', '5a'*16),
                ('storage', 'archive_root', chome+'/kind=7'), ('storage', 'archive_state_root', chome+'/archive-state'),
                ('storage', 'scratch_root', chome+'/initialize-scratch'),
                ('postgres.control', 'database', 'bbpub_control_'+mode), ('postgres.control', 'password_ref', 'file:'+chome+'/control'),
                ('postgres.catalog', 'database', db), ('postgres.catalog', 'user', 'CatalogManager'),
                ('postgres.catalog', 'password_ref', 'file:'+chome+'/manager'),
            ]: text = change(text, section, key, value)
            text += '\n[runtime]\nheartbeat_interval="1s"\n'
            text = text.replace('[postgres.catalog]\n', '[postgres.catalog]\nread_user="bigbrotr_receipt_reader"\nread_password_ref="file:'+chome+'/reader"\n')
            (home/'receipt.toml').write_text(text)
            before = fixture['before'] = preserved(mode)
            current = 4 if mode == 'history' else 3
            rocks = home/'rocks'; permission = rocks.stat().st_mode & 0o777; rocks.chmod(0)
            try:
                assert subprocess.run(['docker', 'exec', runner, 'test', '-r', chome+'/rocks'], capture_output=True).returncode == 1
                cli(mode+'-bootstrap', mode, 2, current)
                cli(mode+'-all-families', mode, 3, current)
                cli(mode+'-exact-3', mode, 3, 3, extras=['--snapshot-id', '3'])
                if mode == 'history':
                    cli('history-corrections', mode, 5, 4)
                    cli('history-corrections-not-yet-visible', mode, 5, 3, extras=['--snapshot-id', '3'], code=4, contains='ObservationUnknown')
                else:
                    for snapshot in [0, 1, 2, 4]:
                        cli('unavailable-snapshot-'+str(snapshot), mode, 3, snapshot, extras=['--snapshot-id', str(snapshot)], code=4, contains='ObservationUnknown')
                    cli('fact-limit', mode, 3, 3, extras=['--max-facts', '4'], code=3, contains='ResourceLimit')
                    cli('receipt-limit', mode, 3, 3, extras=['--max-receipts', '1'], code=4, contains='ObservationUnknown')
                    cli('allocation-limit', mode, 3, 3, extras=['--max-receipts', '1000'], code=3, contains='ResourceLimit')
                    file = next((home/'kind=7/main/events').rglob('*.parquet'))
                    original = file.read_bytes()
                    for fault in ['missing', 'corrupt']:
                        retained = file.with_suffix('.retained'); file.rename(retained)
                        try:
                            if fault == 'corrupt': file.write_bytes(original[:-4]+b'FAIL')
                            cli(fault+'-file', mode, 3, 3, code=4, contains='ObservationUnknown')
                        finally:
                            if file.exists(): file.unlink()
                            retained.rename(file)
                        assert file.read_bytes() == original
                        cli(fault+'-file-restored', mode, 3, 3)
                    blocked(True); blocked(False)
            finally: rocks.chmod(permission)
            drain()
            fixture['after'] = preserved(mode)
            assert fixture['after'] == before
            old_start = sql(db, 'SELECT pg_postmaster_start_time()::text;')
            command('docker', 'exec', pg, '/bin/bash', '-c', 'kill -QUIT "$(head -n1 /var/lib/postgresql/data/postmaster.pid)"')
            until = time.monotonic()+10
            while True:
                restart = subprocess.run(['docker', 'exec', pg, 'psql', '-XAtq', '-U', 'postgres', '-d', db,
                    '-c', 'SELECT pg_postmaster_start_time()::text;'], capture_output=True, text=True, timeout=2)
                if restart.returncode == 0 and restart.stdout.strip() != old_start:
                    fixture['restart'] = dict(before=old_start, after=restart.stdout.strip(), signal='SIGQUIT'); break
                assert time.monotonic() < until
                time.sleep(.05)
            cli(mode+'-after-pg-crash', mode, 3, 3, extras=['--snapshot-id', '3'])
            fixture['restarted'] = preserved(mode)
            assert fixture['restarted'] == before
            fixture['status'] = 'PASS'; save()
        report['status'] = 'PASS'
    except BaseException as error:
        report['failure'] = str(error) if isinstance(error, AssertionError) else type(error).__name__
        raise
    finally: save()
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']), blocked=len(report['blocked']), source=resources['source'])))


if __name__ == '__main__':
    assert len(sys.argv) == 2
    main(Path(sys.argv[1]).resolve())
