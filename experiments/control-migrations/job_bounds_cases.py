#!/usr/bin/env python3
"""MON-009 actual PostgreSQL cases; synthetic cuts, not admission receipts."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import stat

import lab
from concurrency_cases import Session, identity, require, sha, wait_blocked


def fixture(s):
    s.execute("BEGIN;")
    for statement in (
        f"INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES({identity(1)},{identity(2)},0);",
        f"INSERT INTO control.config_revisions VALUES(1,{identity(1)},NULL,1,'managed','{{}}',{identity(1,32)},{identity(2,32)},0,{identity(3)});",
        f"INSERT INTO control.policy_revisions VALUES(1,{identity(1)},NULL,1,1,'{{}}',{identity(3,32)},0,{identity(3)});",
        f"INSERT INTO control.role_instances VALUES({identity(11)},{identity(1)},'ingest',0,NULL);",
        f"INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES({identity(30)},1,{identity(1)},'ingest',{identity(11)},{identity(31)},0,-1,'active');",
        f"INSERT INTO control.topology_generations VALUES({identity(1)},0,NULL,0,false);",
    ):
        s.execute(statement)
    for shard in (1, 2):
        s.execute(f"INSERT INTO control.topology_streams VALUES({identity(1)},0,{shard},1,NULL);")
        s.execute(f"INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,owner_incarnation,opened_at_us) VALUES({identity(1)},{shard},1,{identity(30)},{identity(31)},0);")
    s.execute("UPDATE control.topology_generations SET sealed=true WHERE generation=0;")
    s.execute("UPDATE control.datasets SET config_revision=1,policy_revision=1,topology_generation=0;")
    s.execute("COMMIT;")
    s.execute("UPDATE control.source_streams SET state='closing';")


def job(s, sequences=(5, 0), number=40):
    s.execute(f"INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_topology_generation) VALUES({identity(number)},{identity(1)},{identity(3)},{identity(number)},{identity(40,32)},'export',1,1,0,0);")
    for shard, sequence in enumerate(sequences, 1):
        s.execute(f"INSERT INTO control.job_input_bounds VALUES({identity(number)},{identity(1)},0,{shard},1,{sequence});")
    s.execute(f"UPDATE control.jobs SET input_complete=true WHERE job_id={identity(number)};")


def close_sql(shard=1, cap=5):
    return f"UPDATE control.source_streams SET state='closed',last_complete_sequence={cap},closed_at_us=1,closure_digest={identity(55,32)} WHERE shard_id={shard};"


def observe(state, database):
    return json.loads(lab.sql(state, database, """SELECT json_build_object(
      'streams',(SELECT json_agg(json_build_array(shard_id,state,last_complete_sequence) ORDER BY shard_id) FROM control.source_streams),
      'topology',(SELECT json_agg(json_build_array(shard_id,handoff_last_complete_sequence) ORDER BY shard_id) FROM control.topology_streams),
      'jobs',(SELECT coalesce(json_agg(json_build_array(encode(job_id,'hex'),state,input_complete) ORDER BY job_id),'[]') FROM control.jobs),
      'bounds',(SELECT coalesce(json_agg(json_build_array(encode(job_id,'hex'),shard_id,last_complete_sequence) ORDER BY job_id,shard_id),'[]') FROM control.job_input_bounds));"""))


def expected(cap=None, sequences=None):
    key = format(40, '032x')
    return dict(streams=[[1, 'closing' if cap is None else 'closed', cap], [2, 'closing', None]],
                topology=[[1, None], [2, None]],
                jobs=[] if sequences is None else [[key, 'accepted', True]],
                bounds=[] if sequences is None else [[key, i, v] for i, v in enumerate(sequences, 1)])


def run(root, name):
    state = lab.load(root)
    source = Path(__file__).resolve().parents[2]
    require(not lab.command(['git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    out = root / (name + '-job-bounds')
    out.mkdir(mode=0o700)
    report = dict(status='RUNNING', scope='MON-009 negative controls on migration 2', cases=[], cleanup='NOT_RUN',
                  revision=lab.command(['git', '-C', str(source), 'rev-parse', 'HEAD']).stdout.strip(),
                  binary_sha256=state['binary_sha256'], application_source=state['build']['build_info']['source'],
                  harness_sha256=sha(Path(__file__)), server=state['server'], sessions=[])
    sessions = []
    owned_ids = {key: json.loads(lab.docker('inspect', state[key]).stdout)[0]['Id'] for key in ('postgres', 'client')}

    def owned():
        lab.load(root)
        for key, value in owned_ids.items():
            info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
            require(info['Id'] == value and info['State']['Running'], 'lab-incarnation-changed')

    def save():
        lab.write_json(out / 'result.json', report)

    try:
        owned()
        for case in ('closure_then_job', 'job_then_closure'):
            database = 'bb_' + name + '_' + case
            lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
            config = lab.config(root, state, database)
            result = lab.migrate(state, 'apply', config, target=2)
            require(result.returncode == 0 and json.loads(result.stdout)['current_version'] == '2', 'migration-2-failed')
            receipts = json.loads(lab.sql(state, database, "SELECT json_agg(json_build_array(migration,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions;"))
            require(receipts == [[n, sha(source / 'migrations/postgres' / filename)] for n, filename in ((1, '0001_control.sql'), (2, '0002_ingest.sql'))], 'receipt-mismatch')
            s = Session(state, database, name + '_' + case, sessions)
            report['sessions'].append(s.backend)
            fixture(s)
            require(observe(state, database) == expected(), 'bootstrap-rows')
            if case == 'closure_then_job':
                s.execute(close_sql())
            s.execute('BEGIN;')
            job(s, (6, 0))
            s.execute('COMMIT;')
            if case == 'job_then_closure':
                s.execute(close_sql())
            actual = observe(state, database)
            require(actual == expected(5, (6, 0)), 'counterexample-not-reproduced')
            report['cases'].append(dict(name=case, status='COUNTEREXAMPLE_REPRODUCED', rows=actual, receipts=receipts))
            save()
        report['status'] = 'COUNTEREXAMPLES_REPRODUCED'
    finally:
        try:
            for s in sessions:
                s.close(owned)
            report['cleanup'] = 'PASS'
        finally:
            save()
    print(json.dumps(dict(status=report['status'], output=str(out))))



def qualify(root, name):
    state = lab.load(root)
    source = Path(__file__).resolve().parents[2]
    require(not lab.command(['git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    out = root / (name + '-job-bounds')
    out.mkdir(mode=0o700)
    sessions = []
    report = dict(status='RUNNING', cleanup='NOT_RUN', cases=[], sessions=[],
                  revision=lab.command(['git', '-C', str(source), 'rev-parse', 'HEAD']).stdout.strip(),
                  application_source=state['build']['build_info']['source'],
                  binary_sha256=state['binary_sha256'], server=state['server'],
                  harness_sha256=sha(Path(__file__)))
    owned_ids = {key: json.loads(lab.docker('inspect', state[key]).stdout)[0]['Id'] for key in ('postgres', 'client')}
    entries = json.loads((source / 'migrations/manifest.json').read_text())['application_order']
    exact_receipts = [[e['version'], sha(source / 'migrations/postgres' / e['name'])] for e in entries]

    def owned():
        lab.load(root)
        for key, value in owned_ids.items():
            info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
            require(info['Id'] == value and info['State']['Running'], 'lab-incarnation-changed')

    def save():
        lab.write_json(out / 'result.json', report)

    def record(case, **observed):
        while sessions:
            sessions[-1].close(owned)
            sessions.pop()
        report['cases'].append(dict(name=case, status='PASS', **observed))
        save()

    def new_session(database, suffix):
        s = Session(state, database, name + '_' + suffix, sessions)
        report['sessions'].append(s.backend)
        return s

    def fresh(case, target=8):
        # No database reuse or fixture reset; a failed attempt stays inspectable.
        database = 'bb_' + name + '_' + case
        require(len(database) <= 63, 'database-name-limit')
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        config = lab.config(root, state, database)
        result = lab.migrate(state, 'apply', config, target=target)
        require(result.returncode == 0 and not result.stderr and json.loads(result.stdout)['current_version'] == str(target), 'cli-apply')
        receipt_check(database, target)
        s = new_session(database, case)
        fixture(s)
        require(observe(state, database) == expected(), 'initial-rows')
        return database, config, s

    def receipt_check(database, target):
        actual = json.loads(lab.sql(state, database, "SELECT json_agg(json_build_array(migration,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions;"))
        wanted = exact_receipts[:next(i + 1 for i, e in enumerate(entries) if e['version'] == target)]
        require(actual == sorted(wanted), 'exact-receipts')
        return actual

    def rows_check(database, wanted):
        actual = observe(state, database)
        require(actual == wanted, 'external-durable-rows')
        return actual

    def seal(s, sequences, expected_state='00000', commit=True):
        s.execute('BEGIN;')
        job(s, sequences)
        # COMMIT exercises real deferred validation; failed COMMIT rolls back.
        s.execute('COMMIT;' if commit else 'SET CONSTRAINTS ALL IMMEDIATE;', expected_state)

    try:
        owned()
        # Qualify explicit eligible upgrade and incompatible history. Both use
        # the actual CLI; pg_dump covers every control row, function and index.
        for invalid in (False, True):
            case = 'upgrade_invalid' if invalid else 'upgrade_valid'
            db, config, s = fresh(case, target=2)
            s.execute(close_sql())
            seal(s, (6 if invalid else 5, 0))
            before = lab.schema_digest(state, db)
            before_rows = observe(state, db)
            result = lab.migrate(state, 'apply', config, target=8)
            if invalid:
                require(result.returncode == 1 and 'migration 8 refuses job cuts beyond authoritative source closure' in result.stderr, 'specific-upgrade-refusal')
                require(lab.schema_digest(state, db) == before, 'failed-upgrade-mutated-schema-or-data')
                receipts = receipt_check(db, 2)
            else:
                require(result.returncode == 0 and json.loads(result.stdout)['current_version'] == '8', 'upgrade-failed')
                receipts = receipt_check(db, 8)
                applied = lab.schema_digest(state, db)
                repeated = lab.migrate(state, 'apply', config, target=8)
                require(repeated.returncode == 0 and json.loads(repeated.stdout)['status'] == 'current', 'no-op-reapply')
                require(lab.schema_digest(state, db) == applied, 'reapply-mutated-schema-or-data')
            rows_check(db, before_rows)
            record(case, rows=before_rows, receipts=receipts, before_sha256=before, after_sha256=lab.schema_digest(state, db))

        for seq in (0, 5, 6):
            case = 'closed_then_job_' + str(seq)
            db, _, s = fresh(case)
            s.execute(close_sql())
            before = lab.schema_digest(state, db)
            seal(s, (seq, 0), '23514' if seq > 5 else '00000')
            if seq > 5:
                require(before == lab.schema_digest(state, db), 'failed-job-mutated-state')
            record(case, rows=rows_check(db, expected(5, None if seq > 5 else (seq, 0))))

        for cap in (4, 5, 6):
            case = 'job_then_close_' + str(cap)
            db, _, s = fresh(case)
            seal(s, (5, 0))
            before = lab.schema_digest(state, db)
            s.execute(close_sql(cap=cap), '23514' if cap < 5 else '00000')
            if cap < 5:
                require(before == lab.schema_digest(state, db), 'failed-close-mutated-state')
            record(case, rows=rows_check(db, expected(None if cap < 5 else cap, (5, 0))))

        # Every schedule observes a real server lock and both backend incarnations.
        # A sleep or transaction exit code is never the barrier/commit oracle.
        for first in ('job', 'close'):
            for rollback in (False, True):
                for seq in (5, 6):
                    case = first + '_first_' + ('rollback_' if rollback else 'commit_') + str(seq)
                    db, _, a = fresh(case)
                    b = new_session(db, case + '_waiter')
                    if first == 'job':
                        seal(a, (seq, 0), commit=False)
                        b.execute('BEGIN;')
                        b.start(close_sql())
                    else:
                        a.execute('BEGIN;')
                        a.execute(close_sql())
                        b.execute('BEGIN;')
                        job(b, (seq, 0))
                        b.start('SET CONSTRAINTS ALL IMMEDIATE;')
                    barrier = wait_blocked(state, db, b, a)
                    rows_check(db, expected())
                    a.execute('ROLLBACK;' if rollback else 'COMMIT;')
                    refused = seq > 5 and not rollback
                    answer = b.expect(b.finish(), '23514' if refused else '00000')
                    b.execute('ROLLBACK;' if refused else 'COMMIT;')
                    final_cap = (None if rollback else 5) if first == 'close' else (None if refused else 5)
                    final_job = (None if rollback else (seq, 0)) if first == 'job' else (None if refused else (seq, 0))
                    record(case, lock=barrier, waiter=answer, rows=rows_check(db, expected(final_cap, final_job)))

        # Distinct nonzero streams, one zero closure, omitted stream, full rollback.
        db, _, s = fresh('multiple_streams')
        s.execute(close_sql(shard=1, cap=5))
        s.execute(close_sql(shard=2, cap=0))
        before = lab.schema_digest(state, db)
        for sequences in ((5,), (5, 1), (6, 0)):
            seal(s, sequences, '23514')
            require(before == lab.schema_digest(state, db), 'multi-stream-refusal-mutated-state')
        seal(s, (5, 0))
        wanted = expected(5, (5, 0)); wanted['streams'][1] = [2, 'closed', 0]
        record('multiple_streams', rows=rows_check(db, wanted), refused_cuts=[[5], [5, 1], [6, 0]])

        db, _, s = fresh('rollback_both')
        before = lab.schema_digest(state, db)
        s.execute('BEGIN;')
        job(s, (5, 0))
        s.execute(close_sql())
        s.execute('SET CONSTRAINTS ALL IMMEDIATE;')
        s.execute('ROLLBACK;')
        require(before == lab.schema_digest(state, db), 'explicit-rollback-mutated-state')
        record('rollback_both', rows=rows_check(db, expected()), schema_sha256=before)

        for isolation in ('REPEATABLE READ', 'SERIALIZABLE', 'READ UNCOMMITTED'):
            for action in ('job', 'close'):
                case = isolation.lower().replace(' ', '_') + '_' + action
                db, _, s = fresh(case)
                before = lab.schema_digest(state, db)
                s.execute('BEGIN ISOLATION LEVEL ' + isolation + ';')
                if action == 'job':
                    job(s)
                    s.execute('SET CONSTRAINTS ALL IMMEDIATE;', '25001')
                else:
                    s.execute(close_sql(), '25001')
                s.execute('ROLLBACK;')
                require(before == lab.schema_digest(state, db), 'unsupported-isolation-mutated-state')
                record(case, rows=rows_check(db, expected()))
        report['status'] = 'PASS'
    except Exception as error:
        report['status'] = 'FAIL' if isinstance(error, (AssertionError, ValueError)) or type(error).__name__ == 'Mismatch' else 'UNKNOWN'
        report['failure_type'] = type(error).__name__
        raise
    finally:
        try:
            for s in sessions:
                s.close(owned)
            report['cleanup'] = 'PASS'
        finally:
            save()
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']), output=str(out))))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-name', required=True)
    parser.add_argument('--mode', choices=('negative', 'qualify'), default='negative')
    args = parser.parse_args()
    require(args.root.is_absolute() and args.root.resolve() == args.root, 'root-must-be-canonical')
    info = args.root.stat()
    require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700, 'root-ownership')
    require(re.fullmatch('[a-z_]{1,16}', args.run_name), 'run-name')
    with (args.root / 'job-bounds.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (run if args.mode == 'negative' else qualify)(args.root, args.run_name)
