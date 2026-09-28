"""Isolate the scope helper's post-wait semantics from the Rust revision guard."""
import lab
from concurrency_cases import Session
from locks import blocked


ADVANCE_POLICY = """
    SELECT dataset_id FROM control.datasets FOR UPDATE;
    INSERT INTO control.config_revisions
        SELECT 2,dataset_id,1,2,authority_mode,document,document_digest,accepted_source_digest,
            accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1;
    INSERT INTO control.policy_revisions
        SELECT 2,dataset_id,1,2,2,document,document_digest,accepted_at_us+1,actor_id
        FROM control.policy_revisions WHERE revision=1;
    UPDATE control.datasets SET config_revision=2,policy_revision=2;
"""
CHECK_SCOPE = """
    SELECT control.require_acquisition_scope(dataset_id,scope_id,endpoint_id,access_scope_id)
    FROM control.acquisition_scopes ORDER BY scope_id LIMIT 1;
"""


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def run_policy_race(pg, db, name, rows, output, owned):
    """Readers use the real application login; only the writer injects policy."""
    before = rows(db)
    require(len(before) == 66 and len(before['acquisition_scopes']) == 2, 'race-basis')
    sessions = []
    result = dict(name=name, database=db, status='RUNNING', barriers=[], responses=[], sessions=[])
    path = output / (name + '-race.json')
    lab.write_json(output / (name + '-before.json'), before)
    lab.write_json(path, result)

    def execute(session, sql, expected='00000'):
        session.start(sql)
        return finish(session, expected, sql)

    def finish(session, expected='00000', sql=None):
        answer = session.finish()
        result['responses'].append(dict(user=session.role, sql=sql, answer=answer))
        session.expect(answer, expected)
        return answer

    try:
        reader = Session(pg, db, 'scope-guard-reader', sessions, role='bigbrotr_control')
        result['sessions'].append(reader.backend)
        execute(reader, 'BEGIN ISOLATION LEVEL REPEATABLE READ;' if name == 'repeatable' else 'BEGIN;')
        if name == 'repeatable':
            execute(reader, CHECK_SCOPE, '25001')
            execute(reader, 'ROLLBACK;')
        else:
            writer = Session(pg, db, 'scope-guard-writer', sessions, role='postgres')
            result['sessions'].append(writer.backend)
            execute(writer, 'BEGIN; SET LOCAL session_replication_role=replica;')
            commit = name.endswith('commit')
            if name.startswith('writer_'):
                execute(writer, ADVANCE_POLICY)
                reader.start(CHECK_SCOPE)
                result['barriers'].append(blocked(pg, db, reader.backend, writer.backend))
                execute(writer, 'COMMIT;' if commit else 'ROLLBACK;')
                finish(reader, '23514' if commit else '00000')
                execute(reader, 'ROLLBACK;')
            else:
                execute(reader, CHECK_SCOPE)
                writer.start(ADVANCE_POLICY)
                result['barriers'].append(blocked(pg, db, writer.backend, reader.backend))
                execute(reader, 'ROLLBACK;')
                finish(writer)
                execute(writer, 'COMMIT;' if commit else 'ROLLBACK;')
                # A new invocation must observe the now committed epoch.
                execute(reader, 'BEGIN;')
                execute(reader, CHECK_SCOPE, '23514' if commit else '00000')
                execute(reader, 'ROLLBACK;')
        after = rows(db)
        if name in ('writer_commit', 'scope_commit'):
            expected = dict(before)
            expected['datasets'] = [dict(before['datasets'][0], config_revision=2,
                policy_revision=2, xmin=after['datasets'][0]['xmin'])]
            for table in ('config_revisions', 'policy_revisions'):
                require(len(before[table]) == 1 and len(after[table]) == 2, 'policy-row-count')
                added = next(row for row in after[table] if row['revision'] == 2)
                new = dict(before[table][0], revision=2, previous_revision=1,
                    accepted_at_us=before[table][0]['accepted_at_us']+1, xmin=added['xmin'])
                if table == 'policy_revisions':
                    new['config_revision'] = 2
                expected[table] = sorted([before[table][0], new], key=lambda r: r['revision'])
                require(sorted(after[table], key=lambda r: r['revision']) == expected[table], 'exact-policy-copy')
                expected[table] = after[table]  # SQL JSON text ordering differs from revision order.
            require(after == expected, 'unexpected-race-effects')
        else:
            require(after == before, 'complete-race-rollback')
        lab.write_json(output / (name + '-after.json'), after)
        result['status'] = 'PASS'
    except BaseException as error:
        result.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
        raise
    finally:
        errors = []
        for session in sessions:
            try:
                session.close(owned)
            except BaseException as error:
                errors.append(type(error).__name__)
        result['cleanup_errors'] = errors
        if errors:
            result['status'] = 'INCOMPLETE_SESSION_CLEANUP'
        elif result['status'] == 'FAIL':
            # Preserve durable state for negative controls and unexpected faults.
            lab.write_json(output / (name + '-after.json'), rows(db))
        lab.write_json(path, result)
    require(result['status'] == 'PASS', 'incomplete-race-cleanup')
    return dict(name=name, status='PASS', actual_pg=True, observed_lock_barriers=len(result['barriers']))
