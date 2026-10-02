"""Stop the actual source UPDATE/COMMIT, then recover identical signed input.

Triggers belong only to freshly cloned fixture databases. Independent sessions
observe the blocked statement, client lifetime and durable effects. No product
code or persistent migration is instrumented.
"""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

import lab
from installed import terminal, wait_for

LOCK = (180023441, 20261002)


def install_barrier(pg, database, deferred, *, abandon_acquisition=False):
    event = 'INSERT' if abandon_acquisition else 'UPDATE'
    table = 'acquisition_gaps' if abandon_acquisition else 'source_streams'
    condition = 'true' if abandon_acquisition else "NEW.state = 'closed'"
    commit_trigger = 'recovery_commit_barrier' if abandon_acquisition else 'source_commit_barrier'
    write_trigger = 'recovery_write_barrier' if abandon_acquisition else 'source_update_barrier'
    timing = (f'CONSTRAINT TRIGGER {commit_trigger} AFTER {event} '
        f'ON control.{table} DEFERRABLE INITIALLY DEFERRED' if deferred else
        f'TRIGGER {write_trigger} BEFORE {event} ON control.{table}')
    statement = f"""
        CREATE SCHEMA source_closure_fixture;
        CREATE FUNCTION source_closure_fixture.wait_for_observer() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_advisory_xact_lock({LOCK[0]}, {LOCK[1]});
            RETURN NEW;
        END $$;
        CREATE {timing} FOR EACH ROW WHEN ({condition})
            EXECUTE FUNCTION source_closure_fixture.wait_for_observer();
    """
    lab.sql(pg, database, statement)
    return statement


def signal(client, prefix, *, abandon_acquisition=False):
    identity = json.loads(Path(str(prefix) + '.pid.json').read_text())
    flag = '--abandon-acquisition' if abandon_acquisition else '--close-source'
    script = '''import json,os,signal
from pathlib import Path
r=json.loads(%r)
p=Path('/proc')/str(r['pid'])
assert p.joinpath('stat').read_text().split()[21]==r['start_ticks']
assert p.joinpath('cmdline').read_bytes().split(b'\\0')[:-1]==[
    b'/build/bigbrotr',b'ingest',b'recover',b'--file',r['config'].encode(),%r]
os.kill(r['pid'],signal.SIGTERM)
''' % (json.dumps(identity), flag.encode())
    lab.docker('exec', client, 'python3', '-c', script)


def assert_effects(before, after, closed, digest, released=False):
    changed = {'owner_epochs', 'role_instances', 'producer_epochs'}
    if closed:
        changed.add('source_streams')
    assert before.keys() == after.keys()
    for table in before.keys() - changed:
        assert before[table] == after[table], table
    for table in ('owner_epochs', 'role_instances'):
        assert len(after[table]) == len(before[table]) + 1
        assert all(row in after[table] for row in before[table])
    owner, = [r for r in after['owner_epochs'] if r not in before['owner_epochs']]
    role, = [r for r in after['role_instances'] if r not in before['role_instances']]
    assert owner['role'] == role['role'] == 'control'
    assert owner['state'] == ('released' if released else 'active')
    assert (owner['released_at_us'] is not None) == released
    assert (role['retired_at_us'] is not None) == released
    assert owner['epoch'] == max(r['epoch'] for r in before['owner_epochs'] if r['role'] == 'control') + 1
    old, new = dict(before['producer_epochs'][0]), dict(after['producer_epochs'][0])
    if old['state'] == 'closing':
        assert old == new, 'identical producer close rewrote state'
    assert old.pop('state') in ('open', 'closing') and new.pop('state') == 'closing'
    old.pop('xmin'); new.pop('xmin')
    assert old == new
    if closed:
        old, new = dict(before['source_streams'][0]), dict(after['source_streams'][0])
        if old['state'] == 'closed':
            assert old == new, 'identical source close rewrote state'
            assert new['last_complete_sequence'] == 12 and new['closure_digest'] == '\\x' + digest
        else:
            assert old.pop('state') == 'open' and new.pop('state') == 'closed'
            assert old.pop('closed_at_us') is None
            assert owner['acquired_at_us'] <= new.pop('closed_at_us') < owner['lease_expires_at_us']
            assert old.pop('last_complete_sequence') is None and new.pop('last_complete_sequence') == 12
            assert old.pop('closure_digest') is None and new.pop('closure_digest') == '\\x' + digest
            old.pop('xmin'); new.pop('xmin')
            assert old == new


def assert_reclaimed(before, after, current):
    """The fresh Control fences only its immediately preceding lost owner."""
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    assert previous['state'] == 'active' and previous['released_at_us'] is None
    released, = [r for r in after['owner_epochs'] if r['resource_id'] == previous['resource_id'] and r['epoch'] == previous['epoch']]
    preimage = b'bigbrotr/control-exclusion/1\0'
    preimage += bytes.fromhex(previous['dataset_id'][2:] + previous['resource_id'][2:])
    preimage += previous['epoch'].to_bytes(8, 'big')
    preimage += bytes.fromhex(previous['process_incarnation'][2:] + current['process_incarnation'][2:])
    assert released['state'] == 'released' and released['exclusion_method'] == 'storage_fenced'
    assert released['exclusion_digest'] == '\\x' + hashlib.sha256(preimage).hexdigest()
    assert previous['acquired_at_us'] < released['released_at_us'] <= current['acquired_at_us']
    changed = {'state', 'released_at_us', 'exclusion_method', 'exclusion_digest', 'xmin'}
    assert {k: v for k, v in previous.items() if k not in changed} == {k: v for k, v in released.items() if k not in changed}
    old_role, = [r for r in before['role_instances'] if r['role_instance_id'] == previous['role_instance_id']]
    new_role, = [r for r in after['role_instances'] if r['role_instance_id'] == previous['role_instance_id']]
    assert old_role['retired_at_us'] is None
    assert released['released_at_us'] <= new_role['retired_at_us'] <= current['acquired_at_us']
    assert {k: v for k, v in old_role.items() if k not in ('xmin', 'retired_at_us')} == {
        k: v for k, v in new_role.items() if k not in ('xmin', 'retired_at_us')}
    assert all(r in after['owner_epochs'] for r in before['owner_epochs'] if r != previous)
    assert all(r in after['role_instances'] for r in before['role_instances'] if r != old_role)


def run(pg, client, output, rows, prepare, check_native, retry, cases, digest, *, abandon_acquisition=False):
    # Both consumers share process ownership, the independent SQL barrier and
    # retry timing. Their durable effects remain separate explicit oracles.
    def effects(before, after, committed):
        if not abandon_acquisition:
            assert_effects(before, after, committed, digest)
            return
        from acquisition_abandonment import assert_abandoned
        if committed:
            assert assert_abandoned(before, after, digest) == 1
        else:
            assert before['jobs'] == after['jobs']
            assert before['acquisition_gaps'] == after['acquisition_gaps']
        isolated = dict(before, jobs=after['jobs'], acquisition_gaps=after['acquisition_gaps'])
        assert_effects(isolated, after, True, digest)

    shutil.copy2(Path(__file__).with_name('installed_client.py'), output / 'installed_client.py')
    schedules = ([('abandon_insert_signal', False), ('abandon_commit_deadline', True)] if abandon_acquisition
        else [('source_update_signal', False), ('source_commit_deadline', True)])
    for name, deferred in schedules:
        database, config, directory = prepare(name)
        before = rows(database)
        lab.write_json(output / (name + '-before.json'), before)
        ddl = install_barrier(pg, database, deferred, abandon_acquisition=abandon_acquisition)
        (output / (name + '-barrier.sql')).write_text(ddl)
        # The advisory key is database-local, absent before this isolated owner.
        assert lab.sql(pg, database, f"SELECT count(*) FROM pg_locks WHERE database = "
            f"(SELECT oid FROM pg_database WHERE datname=current_database()) AND locktype='advisory' "
            f"AND classid={LOCK[0]}::oid AND objid={LOCK[1]}::oid AND objsubid=2") == '0'
        lock_out, lock_err = output / (name + '-lock.stdout'), output / (name + '-lock.stderr')
        with lock_out.open('x') as out, lock_err.open('x') as err:
            locker = subprocess.Popen(['docker', 'exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
                '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database],
                stdin=subprocess.PIPE, stdout=out, stderr=err, text=True)
            try:
                locker.stdin.write(f'SELECT pg_advisory_lock({LOCK[0]}, {LOCK[1]}); SELECT pg_backend_pid();\n')
                locker.stdin.flush()
                holder = int(wait_for(lambda: lock_out.read_text().strip(), 'source-barrier-owned', 5))
                prefix = output / name
                lab.docker('exec', '-d', client, 'python3', '/lab/installed_client.py', config,
                    '/lab/' + name, 'abandonment' if abandon_acquisition else 'source-closure')
                wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'source-CLI-pid', 5)

                def blocked():
                    return json.loads(lab.sql(pg, database, f"""
                        SELECT coalesce(json_agg(json_build_object('pid', pid, 'backend_start', backend_start,
                            'query', query, 'wait_event', wait_event)), '[]') FROM pg_stat_activity
                        WHERE datname=current_database() AND usename='bigbrotr_control'
                          AND wait_event_type='Lock' AND {holder}=ANY(pg_blocking_pids(pid))
                    """)) or None

                observed = wait_for(blocked, 'actual-source-statement-blocked', 3)
                assert len(observed) == 1
                query = observed[0]['query'].strip()
                if deferred:
                    assert query == 'COMMIT', query
                elif abandon_acquisition:
                    assert 'INSERT INTO control.acquisition_gaps' in query, query
                else:
                    assert 'UPDATE control.source_streams' in query and "state = 'closed'" in query, query
                lab.write_json(output / (name + '-blocked.json'), observed)
                measured = time.monotonic()
                if not deferred:
                    signal(client, prefix, abandon_acquisition=abandon_acquisition)
                ended = terminal(prefix)
                elapsed = time.monotonic() - measured
                assert ended['exit_code'] == 4 and not ended['forced'] and elapsed < 6, ended
                assert not Path(str(prefix) + '.stdout').read_bytes()
                error = Path(str(prefix) + '.stderr').read_text()
                assert any(word in error for word in ('cancelled', 'deadline', 'expired')), error
                if deferred:
                    assert ended['elapsed_seconds'] >= 1
                remaining = blocked()
                # A joined Rust process does not assert server-side cancellation.
                held = rows(database)
                lab.write_json(output / (name + '-after-client.json'), dict(rows=held, backend=remaining))
                effects(before, held, False)
                check_native(directory, name + '-after-client')
            finally:
                if locker.poll() is None:
                    locker.stdin.write(f'SELECT pg_advisory_unlock({LOCK[0]}, {LOCK[1]});\n\\q\n')
                    locker.stdin.flush()
                locker.communicate(timeout=5)
                assert locker.returncode == 0 and not lock_err.read_text()
        wait_for(lambda: lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'source-backend-ended', 5)
        after = rows(database)
        lab.write_json(output / (name + '-after.json'), after)
        # UPDATE without a sent COMMIT rolls back; COMMIT already executing may
        # finish after client death. This case qualifies the observed durable path.
        effects(before, after, deferred)
        check_native(directory, name + '-after')
        result = dict(name=name, status='PASS', terminal=ended, native_rows=49,
            original_requests=4, control_tables=len(after), source_closed=abandon_acquisition or deferred,
            seconds_after_observed_block=elapsed, backends_after_client=len(remaining or []),
            schedule='deadline during COMMIT; durable after client death' if deferred else
                'SIGTERM during INSERT; rollback' if abandon_acquisition else 'SIGTERM during UPDATE; rollback')
        if abandon_acquisition:
            result['acquisition_abandoned'] = deferred
        cases.append(result)
        lab.write_json(output / (name + '-result.json'), result)
        # Preserve lease semantics. Retry uses a fresh real claim only after the
        # original lease naturally expires; no fixture clock or authority edits.
        wait_for(lambda: lab.sql(pg, database, "SELECT bool_and(lease_expires_at_us < "
            "(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) FROM control.owner_epochs "
            "WHERE role='control'") == 't', 'prior-Control-lease-expired', 8)
        retry(database, config, directory, name + '-retry', reclaims_control=True)
