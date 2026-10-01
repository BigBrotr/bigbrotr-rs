"""Current authority writers versus actual registration; isolated privileged faults."""
import copy
import json
import time
import lab
from concurrency_cases import Session
from registration_faults import (
    ACTOR, DOMAIN_TABLES, effects, event, finish, go, observe, prepare, receipt,
    retire, start_probe, start_proxy,
)

KINDS = ('policy', 'permission', 'revoked', 'inactive', 'authentication')
CASES = {f'register-{order}-{kind}': (order, kind) for order in ('writer', 'accept') for kind in KINDS}
CASES['register-rollback'] = ('rollback', 'policy')


def mutation(kind, session):
    if kind == 'policy':
        return ('SELECT dataset_id FROM control.datasets FOR UPDATE;', '''
            INSERT INTO control.config_revisions
                SELECT revision+1,dataset_id,revision,schema_version,authority_mode,
                    document,document_digest,accepted_source_digest,accepted_at_us+1,actor_id
                FROM control.config_revisions WHERE revision=1;
            INSERT INTO control.policy_revisions
                SELECT revision+1,dataset_id,revision,2,schema_version,document,
                    document_digest,accepted_at_us+1,actor_id
                FROM control.policy_revisions WHERE revision=1;
            UPDATE control.datasets SET config_revision=2,policy_revision=2;
        ''', {'datasets', 'config_revisions', 'policy_revisions'})
    if kind == 'revoked':
        return (f"SELECT session_id FROM control.browser_sessions WHERE session_id=decode('{session}','hex') FOR UPDATE;",
            f"UPDATE control.browser_sessions SET revoked_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint WHERE session_id=decode('{session}','hex');",
            {'browser_sessions'})
    lock = "SELECT principal_id FROM control.local_accounts WHERE username='alice' FOR UPDATE;"
    if kind == 'permission':
        return (lock, "DELETE FROM control.principal_grants WHERE action='policy.change'; UPDATE control.local_accounts SET grant_revision=grant_revision+1 WHERE username='alice';",
            {'local_accounts', 'principal_grants'})
    change = "UPDATE control.local_accounts SET active=false WHERE username='alice';"
    if kind == 'authentication':
        change += "UPDATE control.local_accounts SET active=true WHERE username='alice';"
    return lock, change, {'local_accounts', 'browser_sessions'}


def blocked(pg, database, waiter, holder):
    end = time.monotonic() + 5
    while True:
        observed = json.loads(lab.sql(pg, database, f"""
            SELECT json_build_object('pid',a.pid,'start',a.backend_start::text,
                'wait',a.wait_event_type,'blockers',pg_blocking_pids(a.pid),
                'holder_start',(SELECT backend_start::text FROM pg_stat_activity
                    WHERE pid={holder['pid']} AND datname=current_database()))
            FROM pg_stat_activity a WHERE a.datname=current_database() AND a.pid={waiter['pid']}
        """))
        if observed and observed['wait'] == 'Lock' and holder['pid'] in observed['blockers']:
            assert observed['start'] == waiter['start'] and observed['holder_start'] == holder['start']
            return observed
        assert time.monotonic() < end, 'required lock barrier not observed'
        time.sleep(.02)


def writer_effects(before, after, kind, session):
    """Literal changes, including automatic account-authentication revocation."""
    expected = copy.deepcopy(before)
    if kind == 'policy':
        expected['datasets'][0].update(config_revision=2, policy_revision=2)
        expected['datasets'][0]['xmin'] = after['datasets'][0]['xmin']
        for table in ('config_revisions', 'policy_revisions'):
            old, = before[table]
            new = dict(old, revision=2, previous_revision=1, accepted_at_us=old['accepted_at_us']+1)
            if table == 'policy_revisions':
                new['config_revision'] = 2
            actual, = [r for r in after[table] if r['revision'] == 2]
            new['xmin'] = actual['xmin']
            expected[table].append(new)
    elif kind == 'permission':
        expected['principal_grants'] = [r for r in before['principal_grants'] if r['action'] != 'policy.change']
        assert len(expected['principal_grants']) == len(before['principal_grants']) - 1
        expected['local_accounts'][0]['grant_revision'] += 1
        expected['local_accounts'][0]['xmin'] = after['local_accounts'][0]['xmin']
    else:
        if kind in ('inactive', 'authentication'):
            expected['local_accounts'][0]['active'] = kind == 'authentication'
            expected['local_accounts'][0]['authentication_revision'] += 2 if kind == 'authentication' else 1
            expected['local_accounts'][0]['xmin'] = after['local_accounts'][0]['xmin']
        for row in expected['browser_sessions']:
            affected = row['session_id'] == '\\x' + session if kind == 'revoked' else row['revoked_at_us'] is None
            if affected:
                actual, = [r for r in after['browser_sessions'] if r['session_id'] == row['session_id']]
                assert actual['revoked_at_us'] is not None and actual['revoked_at_us'] >= row['issued_at_us']
                row.update(revoked_at_us=actual['revoked_at_us'], xmin=actual['xmin'])
    ordered = lambda state: {t: sorted(v, key=lambda r: json.dumps(r, sort_keys=True)) for t, v in state.items()}
    assert ordered(after) == ordered(expected), 'writer effects differ from literal mutation'


def run(pg, client, output, database, name, rows):
    order, kind = CASES[name]
    directory = output / name
    directory.mkdir(mode=0o700)
    endpoint = prepare(directory, rows(database))
    processes, writers = [], []
    result = dict(name=name, status='RUNNING', order=order, writer_kind=kind, preexisting_endpoint=endpoint)

    def snapshot(label):
        state = rows(database)
        lab.write_json(directory / (label + '.json'), state)
        return state

    def owned():
        item, = json.loads(lab.docker('inspect', pg['postgres']).stdout)
        assert item['Config']['Labels'][lab.LABEL] == pg['owner'] and item['State']['Running']

    try:
        proxy = None
        port = 5432
        if order == 'accept':
            proxy, proxy_ready = start_proxy(client, 'hold', processes)
            port = proxy_ready['listen_port']
        mode = kind if order == 'writer' else 'accept'
        process, token = start_probe(client, database, directory, mode, 11, processes, port)
        ready = event(process, 'ready')
        assert ready['epoch'] == 2 and ready['config'] == ready['policy'] == 1
        initial = snapshot('before')
        observed = observe(pg, database, ready)
        assert not observed['owner_expired'] and not observed['session_expired']
        actor, = [r for r in initial['local_accounts'] if r['principal_id'] == ACTOR]
        lock, change, allowed = mutation(kind, observed['session'])
        if order == 'accept':
            go(token)
            proxy.event('held', 10)
            assert rows(database) == initial, 'held registration visible'
        writer = Session(pg, database, name, writers)
        writer.execute('BEGIN;')
        writer_identity = dict(pid=writer.backend['pid'], start=writer.backend['backend_start'])
        if order == 'accept':
            writer.start(lock + change + 'COMMIT;')
            barrier = blocked(pg, database, writer_identity, observed)
        else:
            writer.execute(lock)
            writer.execute(change)
            go(token)
            barrier = blocked(pg, database, observed, writer_identity)
        assert rows(database) == initial, 'provisional changes visible'
        if order == 'accept':
            proxy.send('release')
        else:
            writer.execute('ROLLBACK;' if order == 'rollback' else 'COMMIT;')
        outcome = finish(process)
        if order == 'accept':
            writer.expect(writer.finish())
            wire = proxy.event('receipt', 10)
            assert proxy.wait(5) == 0 and not proxy.stderr
            assert [wire[k] for k in ('commits_seen','commits_forwarded','server_commit_complete',
                'client_commit_complete','faults')] == [4,4,4,4,1]
            result['wire'] = wire
        writer.close(owned)
        writers.remove(writer)
        after = snapshot('after')
        committed = order != 'writer'
        accepted = receipt(after, ready, actor)
        assert (accepted is not None) == committed
        if committed:
            assert outcome == dict(receipt=dict(accepted, created=True), commit_started=True)
        if order == 'rollback':
            effects(initial, after, True)
        else:
            # Separate immutable command additions from the explicit writer's
            # literal effects; every remaining relation and old row stays exact.
            command_only = {t: after[t] if t in DOMAIN_TABLES else initial[t] for t in initial}
            effects(initial, command_only, committed)
            writer_only = {t: after[t] if t in allowed else initial[t] for t in initial}
            assert {t for t in initial if initial[t] != after[t]} == allowed | (DOMAIN_TABLES if committed else set())
            writer_effects(initial, writer_only, kind, observed['session'])
        result.update(ready=ready, observation=observed, barrier=barrier, outcome=outcome,
            committed=committed, exact_writer_effects=True)
        if order == 'accept' and kind in ('policy', 'permission'):
            recovery, recovery_token = start_probe(client, database, directory, 'accept', 13, processes)
            recovered_owner = event(recovery, 'ready')
            assert recovered_owner['epoch'] == 3
            assert recovered_owner['config'] == recovered_owner['policy'] == (2 if kind == 'policy' else 1)
            before_retry = snapshot('recovery-before')
            go(recovery_token)
            recovered = finish(recovery)
            after_retry = snapshot('recovery-after')
            assert before_retry == after_retry, 'historical recovery changed data'
            assert receipt(after_retry, ready, actor) == accepted
            assert recovered == dict(receipt=dict(accepted, created=False), commit_started=False)
            result.update(recovery_owner=recovered_owner, recovered=recovered)
        assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
        result['status'] = 'PASS'
    except BaseException as error:
        result.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
        raise
    finally:
        writer_errors = []
        for writer in writers:
            try:
                writer.close(owned)
            except BaseException as error:
                writer_errors.append(type(error).__name__)
        result['writer_cleanup_errors'] = writer_errors
        if writer_errors:
            result['status'] = 'INCOMPLETE_CLEANUP'
        retire(directory, processes, result)
    assert result['status'] == 'PASS'
    return result
