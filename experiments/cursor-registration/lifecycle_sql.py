"""Independent literal lifecycle SQL, including a caller-created trigger attack."""
import json
from pathlib import Path
import lab


def cases(f):
    db, _, _, _ = f.prepare('sql')
    actor = lab.sql(f.state, db, "SELECT encode(principal_id,'hex') FROM control.principals WHERE kind='installation';")
    q = lambda value: "decode('" + value + "','hex')"
    dataset, ring = q(f.vectors['dataset_id']), q(f.vectors['keyring_id'])

    def pair(action, key, request, expected, who=actor):
        audit = q(request)
        digest = q(f.digest(who, action, key, request, expected))
        audit_sql = f"""INSERT INTO control.management_audit
            (audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,
             requester_database_role,requester_os_uid)
            VALUES({audit},{dataset},{q(who)},'cursor-key.{action}','accepted',123,
                   {q(key)},SESSION_USER,1000);"""
        values = f"({dataset},{q(request)},{ring},{q(who)},'{action}',{q(key)},{expected},{digest},123,{audit})"
        return audit_sql, values

    insert = '''INSERT INTO control.cursor_key_transitions
        (dataset_id,request_id,keyring_id,actor_id,action,key_id,expected_revision,
         request_digest,accepted_at_us,audit_id) VALUES '''

    def sql_case(name, statement, expected=None, unchanged=True, role='bigbrotr_control', reason=None):
        before = f.snapshot(db)
        result = lab.docker('exec', '-i', f.state['postgres'], 'psql', '-X', '-A', '-t', '-q',
                            '--no-password', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=default',
                            '-U', role, '-d', db, data='\\set VERBOSITY verbose\n' + statement, check=False)
        (f.root / (name + '.stdout')).write_text(result.stdout)
        (f.root / (name + '.stderr')).write_text(result.stderr)
        passed = result.returncode == 0 if expected is None else result.returncode != 0 and 'ERROR:  ' + expected in result.stderr
        if unchanged:
            passed = passed and before == f.snapshot(db)
        if reason:
            passed = passed and reason in result.stderr
        f.record(name, passed, exit=result.returncode, expected_sqlstate=expected, stderr=result.stderr)

    audit, value = pair('activate', f.a, '31'*16, 0)
    for name, body, state in [
        ('audit-alone', audit, 'P0002'),
        ('no-audit', insert + value + ';', '23503'),
        ('wrong-audit-time', audit.replace("'accepted',123", "'accepted',124") + insert + value + ';', '23514'),
        ('wrong-digest', audit + insert + value.replace(q(f.digest(actor, 'activate', f.a, '31'*16, 0)), q('01'*32)) + ';', '23514'),
        ('no-predecessor', audit + insert + pair('activate', f.a, '31'*16, 1)[1] + ';', '23514'),
    ]:
        sql_case('sql-' + name, 'BEGIN;' + body + 'COMMIT;', state)
    sql_case('sql-repeatable-read', 'BEGIN ISOLATION LEVEL REPEATABLE READ;' + audit + insert + value + ';COMMIT;', '25001')

    # Genuine temporary-table privilege does not identify the trigger caller.
    # All receipts are visible in the outer DML before queued AFTER captures.
    sequence = [pair('activate', f.a, '31'*16, 0), pair('activate', f.b, '32'*16, 1), pair('activate', f.a, '33'*16, 2)]
    attack = '''CREATE TEMP TABLE attack (LIKE control.cursor_key_transitions);
        CREATE FUNCTION pg_temp.inject_projection() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO control.cursor_key_lifecycle(dataset_id,key_id,first_activation_revision)
                VALUES(NEW.dataset_id,NEW.key_id,NEW.revision) ON CONFLICT DO NOTHING;
            RETURN NEW;
        END $$;
        CREATE TRIGGER inject BEFORE INSERT ON attack FOR EACH ROW EXECUTE FUNCTION pg_temp.inject_projection();'''
    attack += ''.join(a for a, _ in sequence)
    attack += 'WITH added AS (' + insert + ','.join(v for _, v in sequence) + ' RETURNING *) INSERT INTO attack SELECT * FROM added ORDER BY revision DESC;'
    sql_case('sql-temporary-trigger-cannot-forge-first-activation', 'BEGIN;' + attack + 'COMMIT;', '23514',
             reason='cursor projection differs from first transition')

    # Multiple receipts in one statement, then immediate checks. The earlier
    # receipt must not be compared against the final mutable projection.
    sequence.append(pair('revoke', f.b, '34'*16, 3))
    sql_case('sql-multirow-chain', 'BEGIN;' + ''.join(a for a, _ in sequence)
             + insert + ','.join(v for _, v in sequence) + ';SET CONSTRAINTS ALL IMMEDIATE;COMMIT;', unchanged=False)
    observed = f.snapshot(db)
    states = {r['key_id'][2:]: r for r in observed['cursor_key_lifecycle']}
    transitions = sorted(observed['cursor_key_transitions'], key=lambda r: r['revision'])
    f.record('sql-chain-independent-result',
        [(r['revision'], r['resulting_active_key_id'][2:] if r['resulting_active_key_id'] else None) for r in transitions]
            == [(1, f.a), (2, f.b), (3, f.a), (4, f.a)]
        and states[f.a]['first_activation_revision'] == 1 and states[f.a]['revoked_revision'] is None
        and states[f.b]['first_activation_revision'] == 2 and states[f.b]['revoked_revision'] == 4
        and all(r['decrypt_until_us'] is None for r in states.values()), observed=observed)
    audit, value = pair('revoke', f.a, '35'*16, 4)
    sql_case('sql-explicit-rollback', 'BEGIN;' + audit + insert + value + ';SET CONSTRAINTS ALL IMMEDIATE;ROLLBACK;')
    audit, value = pair('activate', f.b, '35'*16, 4)
    sql_case('sql-revocation-permanent', 'BEGIN;' + audit + insert + value + ';COMMIT;', '23514')
    for table in ['cursor_key_transitions', 'cursor_key_lifecycle']:
        for action in [f'UPDATE control.{table} SET dataset_id=dataset_id;', f'DELETE FROM control.{table};', f'TRUNCATE control.{table} CASCADE;']:
            # Migrator permits DML/TRUNCATE, so refusal exercises the actual guard.
            sql_case('sql-guard-' + table + '-' + action.split()[0].lower(), action, '23514', role='bigbrotr_migrator')
    sql_case('sql-retention-writer-not-yet-enabled', 'UPDATE control.cursor_key_lifecycle SET decrypt_until_us=9999999999999999;', '23514')
    sql_case('sql-projection-cannot-bypass-receipt', f"INSERT INTO control.cursor_key_lifecycle(dataset_id,key_id,first_activation_revision) VALUES({dataset},{q(f.a)},1);", '23514')
