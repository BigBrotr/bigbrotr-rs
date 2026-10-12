"""Actual retirement authority expiry and Ingest draining in both lock orders.

Privileged fixture writers retain every product trigger. Only new cloned
databases receive observation barriers or local lifetime settings.
"""
import json
from pathlib import Path
import shutil
import time

import lab
from concurrency_cases import Session
from installed import set_value, terminal, wait_for
from source_closure_faults import LOCK, install_barrier
from source_closure_locks import blocked

CLAIM_LOCK = (LOCK[0], LOCK[1] + 1)


def claim_barrier(pg, database):
    statement = f"""
        CREATE SCHEMA retirement_claim_fixture;
        CREATE FUNCTION retirement_claim_fixture.wait_for_observer() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_advisory_xact_lock({CLAIM_LOCK[0]},{CLAIM_LOCK[1]});
            RETURN NEW;
        END $$;
        CREATE CONSTRAINT TRIGGER retirement_claim_barrier AFTER INSERT
            ON control.owner_epochs DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW WHEN (NEW.role = 'control')
            EXECUTE FUNCTION retirement_claim_fixture.wait_for_observer();
    """
    lab.sql(pg, database, statement)
    return statement


def draining_row(before, answer):
    """Check the writer's one row before allowing it into retirement's baseline."""
    old, = [r for r in before['owner_epochs'] if r['role'] == 'ingest']
    assert old['state'] == 'active'
    assert answer['sqlstate'] == '00000' and answer['rows'] == 1
    row, = [json.loads(line) for line in answer['lines']]
    assert row == dict(old, state='draining', xmin=row['xmin'])
    assert row['xmin'] != old['xmin']
    return dict(before, owner_epochs=[row if r == old else r for r in before['owner_epochs']])


def run(pg, client, output, rows, ready, check_native, retry, cases, digest, initialized):
    from initial_retirement import assert_retired, expected_output
    shutil.copy2(Path(__file__).with_name('installed_client.py'), output / 'installed_client.py')
    pg_id = json.loads(lab.docker('inspect', pg['postgres']).stdout)[0]['Id']

    def owned():
        current = json.loads(lab.docker('inspect', pg_id).stdout)[0]
        assert current['State']['Running'] and current['Config']['Labels'][lab.LABEL] == pg['owner']

    def start(config, name):
        lab.docker('exec', '-d', client, 'python3', '/lab/installed_client.py', config,
            '/lab/' + name, 'retirement')
        prefix = output / name
        wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'retirement-CLI-pid', 2)
        return prefix

    for schedule in ('policy_expiry', 'drain_first', 'retirement_first'):
        name = 'retire_' + schedule
        database, config, directory, _ = ready(name)
        if schedule == 'policy_expiry':
            path = output / Path(config).name
            # Retain accepted policy and its five-second authorization ceiling.
            # A fixture SQL execution delay expires it before the longer local
            # operation timeout, without running into SQL's five-second lock cap.
            text = set_value(path.read_text(), 'runtime', 'lease_ttl', '20s')
            text = set_value(text, 'runtime', 'renewal_timeout', '8s')
            path.write_text(text)
            valid = lab.cli(dict(pg, client=client), 'config', 'validate', '--file', config)
            lab.write_json(output / (name + '-config-validation.json'), dict(
                returncode=valid.returncode, stdout=valid.stdout, stderr=valid.stderr))
            assert valid.returncode == 0, valid.stderr
        before = rows(database)
        assert len(before) == 70
        lab.write_json(output / (name + '-before.json'), before)
        triggers = json.loads(lab.sql(pg, database, "SELECT json_agg(json_build_object("
            "'name',tgname,'enabled',tgenabled) ORDER BY tgname) FROM pg_trigger "
            "WHERE tgrelid='control.owner_epochs'::regclass AND NOT tgisinternal"))
        assert triggers and all(t['enabled'] in ('O', 'A') for t in triggers)
        assert {'archive_owner_protocol_guard', 'control_backend_guard',
            'owner_epoch_guard', 'owner_ingest_dependents_guard'} <= {t['name'] for t in triggers}
        ddl = (claim_barrier(pg, database) if schedule == 'drain_first' else
            install_barrier(pg, database, schedule == 'retirement_first', retire_initial=True))
        if schedule == 'policy_expiry':
            delay = f"""
                CREATE OR REPLACE FUNCTION source_closure_fixture.wait_for_observer()
                RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                    PERFORM pg_advisory_xact_lock({LOCK[0]},{LOCK[1]});
                    PERFORM pg_sleep(5.5);
                    RETURN NEW;
                END $$;
            """
            lab.sql(pg, database, delay)
            ddl += delay
        (output / (name + '-barrier.sql')).write_text(ddl)
        key = CLAIM_LOCK if schedule == 'drain_first' else LOCK
        sessions, observations = [], {}
        try:
            # Session creation precedes the CLI's one-second operation phases.
            barrier = Session(pg, database, name + '_barrier', sessions, role='postgres')
            writer = None if schedule == 'policy_expiry' else Session(
                pg, database, name + '_writer', sessions, role='postgres')
            for session in sessions:
                assert session.execute('SHOW session_replication_role;')['lines'] == ['origin']
            barrier.execute(f'SELECT pg_advisory_lock({key[0]},{key[1]});')
            if writer:
                writer.execute('BEGIN;')
            prefix = start(config, name)
            observed = wait_for(lambda: blocked(pg, database, barrier.backend['pid']),
                'retirement-observation-barrier', 2)
            assert len(observed) == 1
            query, owner_pid = observed[0]['query'].strip(), observed[0]['pid']
            observations['barrier'] = observed
            baseline = before
            if schedule == 'policy_expiry':
                assert 'UPDATE control.producer_epochs' in query and "state='retired'" in query
                released_at = time.monotonic()
                barrier.execute(f'SELECT pg_advisory_unlock({key[0]},{key[1]});')
                delayed = wait_for(lambda: json.loads(lab.sql(pg, database, f"""
                    SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,
                        'query',query,'wait_event',wait_event)), '[]') FROM pg_stat_activity
                    WHERE datname=current_database() AND usename='bigbrotr_control'
                        AND pid={owner_pid} AND wait_event='PgSleep'
                """)) or None, 'actual-retirement-SQL-execution-delay', 2)
                assert len(delayed) == 1 and delayed[0]['backend_start'] == observed[0]['backend_start']
                assert delayed[0]['query'].strip() == query
                observations['execution_delay'] = delayed
            else:
                assert query == 'COMMIT', query
                old, = [r for r in before['owner_epochs'] if r['role'] == 'ingest']
                resource = bytes.fromhex(old['resource_id'][2:]).hex()
                update = ("UPDATE control.owner_epochs o SET state='draining' "
                    f"WHERE resource_id=decode('{resource}','hex') AND epoch=1 "
                    "RETURNING to_jsonb(o)||jsonb_build_object('xmin',xmin::text);")
                if schedule == 'drain_first':
                    # The claim COMMIT gate is after historical lookup. A lock
                    # taken before launch would only qualify that earlier reader.
                    answer = writer.execute(update, rows=1)
                    baseline = draining_row(before, answer)
                    observations['writer'] = answer
                    barrier.execute(f'SELECT pg_advisory_unlock({key[0]},{key[1]});')
                    waited = wait_for(lambda: blocked(pg, database, writer.backend['pid'], owner_pid),
                        'draining-blocks-current-retirement', 2)
                    assert len(waited) == 1
                    query = waited[0]['query']
                    assert 'SELECT epoch FROM control.owner_epochs' in query and 'FOR UPDATE' in query, query
                    observations['retirement_wait'] = waited
                    writer.execute('COMMIT;')
                else:
                    writer.start(update)
                    waited = wait_for(lambda: blocked(pg, database, owner_pid, writer.backend['pid']),
                        'retirement-blocks-draining', 2)
                    assert len(waited) == 1 and "SET state='draining'" in waited[0]['query']
                    observations['writer_wait'] = waited
                    barrier.execute(f'SELECT pg_advisory_unlock({key[0]},{key[1]});')
                    observations['writer'] = writer.expect(writer.finish(), expected='23514')
                    writer.execute('ROLLBACK;')
            ended = terminal(prefix)
            stdout = Path(str(prefix) + '.stdout').read_text()
            stderr = Path(str(prefix) + '.stderr').read_text()
            assert ended['elapsed_seconds'] < (12 if schedule == 'policy_expiry' else 6)
            if schedule == 'policy_expiry':
                elapsed = time.monotonic() - released_at
                assert 5 <= elapsed < 8, elapsed
                observations['unlock_to_terminal_seconds'] = elapsed
                assert ended['exit_code'] == 4 and not stdout
                assert 'DeliveryUnknown' in stderr and not any(word in stderr for word in
                    ('Backend', 'cancelled', 'deadline', 'expired')), stderr
            else:
                assert ended['exit_code'] == 0 and not stderr, (ended, stderr)
                actual, = [json.loads(line) for line in stdout.splitlines()]
                assert actual == expected_output(initialized, len(before['allocation_domains']), digest)
            wait_for(lambda: lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname=current_database() AND usename='bigbrotr_control'") == '0',
                'retirement-backend-joined', 5)
            after = rows(database)
            lab.write_json(output / (name + '-after.json'), after)
            assert_retired(baseline, after, digest,
                committed=schedule != 'policy_expiry', released=schedule != 'policy_expiry')
            check_native(directory, name)
            result = dict(name=name, status='PASS', terminal=ended, observations=observations,
                control_tables=70, native_rows=49, original_requests=4,
                expected_refusal='DeliveryUnknown' if schedule == 'policy_expiry' else None,
                qualification='Current policy expiry' if schedule == 'policy_expiry' else
                    'Actual retirement versus privileged relational Ingest draining; product triggers active')
            lab.write_json(output / (name + '-result.json'), result)
            cases.append(result)
        finally:
            errors = []
            for session in reversed(sessions):
                try:
                    session.close(owned)
                except BaseException as error:
                    errors.append(type(error).__name__)
            lab.write_json(output / (name + '-sessions.json'), dict(
                identities=[s.backend for s in sessions], cleanup_errors=errors,
                product_owner_triggers=triggers, observations=observations))
            assert not errors, errors
        if schedule == 'policy_expiry':
            # Remove only this case's execution delay. This permits a real retry
            # under the same unchanged accepted policy and native root.
            remove = ('DROP TRIGGER source_update_barrier ON control.producer_epochs; '
                'DROP FUNCTION source_closure_fixture.wait_for_observer(); '
                'DROP SCHEMA source_closure_fixture;')
            (output / (name + '-remove-barrier.sql')).write_text(remove)
            lab.sql(pg, database, remove)
            assert rows(database) == after
            wait_for(lambda: lab.sql(pg, database, "SELECT bool_and(lease_expires_at_us < "
                "(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) FROM control.owner_epochs "
                "WHERE role='control'") == 't', 'prior-Control-lease-expired', 22)
            retry(database, config, directory, name + '-retry', reclaims_control=True)
        lab.write_json(output / 'signed-recovery-cases.json', cases)
