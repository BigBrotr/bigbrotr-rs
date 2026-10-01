"""Clone-only relational checks inside the real authenticated Control operation."""
import hashlib
import json
from pathlib import Path
import lab
from registration_faults import (
    URL, effects, event, finish, go, prepare, receipt, retire, start_probe,
)

CASES = {'register-relations'}
CHECKS = [
    ('valid-before', 'ZX001'), ('missing-detail', '23514'),
    ('missing-command', '23503'), ('missing-audit', '23503'),
    ('wrong-endpoint', '23514'), ('wrong-audit-time', '23514'),
    ('extra-audit', '23514'), ('old-owner', '23514'), ('absent-session', '23514'),
    ('revoked-session', 'BB001'), ('credential-revision', 'BB001'),
    ('authentication-revision', 'BB001'), ('grant-revision', 'BB001'),
    ('mutate-detail', '23514'), ('delete-detail', '23514'),
    ('valid-after', 'ZX001'),
]


TRUNCATE = """
DO $$ DECLARE observed_code text; observed_message text;
BEGIN
    IF session_user <> 'bigbrotr_control' OR current_user <> 'bigbrotr_control' THEN
        RAISE EXCEPTION 'fixture must use actual Control login';
    END IF;
    BEGIN
        TRUNCATE control.relay_registration_commands;
        RAISE EXCEPTION 'fixture accepted truncate' USING ERRCODE='ZX001';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS observed_code=RETURNED_SQLSTATE,observed_message=MESSAGE_TEXT;
    END;
    IF observed_code <> '23514' OR observed_message <> 'immutable control history cannot be changed' THEN
        RAISE EXCEPTION 'wrong truncate refusal: % / %',observed_code,observed_message;
    END IF;
END $$;
SELECT json_build_object('name','truncate-detail','sqlstate','23514','backend_pid',pg_backend_pid());
"""


def run(pg, client, output, database, name, rows, *, scope=False):
    if scope:
        from scope_command_relations import CASES as cases, CHECKS as checks_expected, render
        from scope_command_faults import prepare as prepare_case, effects as effects_case, receipt as receipt_case
        detail_table, table_count = 'acquisition_scope_commands', 69
    else:
        cases, checks_expected = CASES, CHECKS
        prepare_case, effects_case, receipt_case = prepare, effects, receipt
        detail_table, table_count = 'relay_registration_commands', 68
    assert name in cases
    directory = output / name
    directory.mkdir(mode=0o700)
    original = rows(database)
    identity = prepare_case(directory, original)
    if scope:
        rendered, digest = render(original)
    else:
        dataset = bytes.fromhex(original['datasets'][0]['dataset_id'][2:])
        # Literal command framing, independent of both Rust and SQL digest helpers.
        digest = hashlib.sha256(b'bigbrotr/relay-registration/1\0' + dataset
            + bytes.fromhex('10' * 16 + '88' * 16) + (1).to_bytes(8, 'big') * 2
            + (1).to_bytes(2, 'big') + len(URL).to_bytes(4, 'big') + URL).hexdigest()
        fixture = Path(__file__).with_suffix('.sql').read_text()
        assert fixture.count('@fresh_digest@') == 1
        rendered = fixture.replace('@fresh_digest@', digest)
    (directory / 'fixture.sql').write_text(rendered)
    lab.sql(pg, database, rendered)
    assert rows(database) == original, 'fixture installation changed Control data'
    processes = []
    result = dict(name=name, status='RUNNING',
        fixture_sha256=hashlib.sha256(rendered.encode()).hexdigest(), expected_digest=digest)
    result['preexisting_scope' if scope else 'preexisting_endpoint'] = identity
    try:
        process, token = start_probe(client, database, directory, 'accept', 11, processes, scope=scope)
        ready = event(process, 'ready')
        assert ready['epoch'] == 2 and ready['config'] == ready['policy'] == 1
        before = rows(database)
        lab.write_json(directory / 'before.json', before)
        # Real login prunes the old revoked session. Its durable registration
        # history must survive; no synthetic session or altered lifetime is used.
        pruned = {r['session_id'] for r in original['browser_sessions'] if r['revoked_at_us'] is not None}
        assert pruned and pruned.isdisjoint(r['session_id'] for r in before['browser_sessions'])
        assert {r['session_id'] for r in original[detail_table]} == pruned
        assert before[detail_table] == original[detail_table]
        go(token)
        outcome = finish(process)
        after = rows(database)
        lab.write_json(directory / 'after.json', after)
        effects_case(before, after, True)
        accepted = receipt_case(after, ready)
        assert outcome == dict(receipt=dict(accepted, created=True), commit_started=True)
        checks = json.loads(lab.sql(pg, database,
            'SELECT json_agg(r ORDER BY position) FROM registration_fixture.results r'))
        lab.write_json(directory / 'relational-checks.json', checks)
        assert [(r['name'], r['sqlstate']) for r in checks] == checks_expected
        assert [r['position'] for r in checks] == list(range(1, len(checks_expected) + 1))
        assert all(r['unchanged'] and r['backend_pid'] == ready['pid'] and r['owner_epoch'] == 2 for r in checks)
        assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        # TRUNCATE must run outside the INSERT's active query, otherwise
        # PostgreSQL's table-in-use check could mask the actual immutable guard.
        truncated = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-U', 'bigbrotr_control', '-d', database, '-v', 'ON_ERROR_STOP=1',
            data=TRUNCATE.replace('control.relay_registration_commands', 'control.' + detail_table), check=False)
        (directory / 'truncate.stdout').write_text(truncated.stdout)
        (directory / 'truncate.stderr').write_text(truncated.stderr)
        assert truncated.returncode == 0 and not truncated.stderr
        truncate = json.loads(truncated.stdout)
        assert truncate['name'] == 'truncate-detail' and truncate['sqlstate'] == '23514'
        assert rows(database) == after, 'truncate refusal changed Control data'
        result.update(status='PASS', ready=ready, outcome=outcome, checks=checks, truncate=truncate,
            complete_control_tables=table_count, pruned_historical_sessions=len(pruned))
    except BaseException as error:
        result.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
        raise
    finally:
        retire(directory, processes, result)
    assert result['status'] == 'PASS'
    return result
