#!/usr/bin/env python3
"""Actual CLI/PG admission order; synthetic metadata, not export execution."""
import argparse
import fcntl
import json
from pathlib import Path
import re
import sys
import time

sys.dont_write_bytecode = True
import lab
import concurrency_cases as concurrency
from concurrency_cases import Session, identity, require, sha, wait_blocked
from fault_cases import Process

SOURCE = Path(__file__).resolve().parents[2]
concurrency.ROLE = 'bigbrotr_control'


class RecordedSession(Session):
    def __init__(self, *args):
        self.commands = []
        super().__init__(*args)

    def start(self, sql):
        self.commands.append(dict(sql=sql, answer=None))
        super().start(sql)

    def finish(self, timeout=12):
        answer = super().finish(timeout)
        self.commands[-1]['answer'] = answer
        return answer


def insert(number, created=100):
    return f"""INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_complete)
        SELECT {identity(number)},d.dataset_id,p.principal_id,{identity(number)},{identity(1,32)},'backup',d.config_revision,d.policy_revision,{created},true
        FROM control.datasets d JOIN control.principals p USING(dataset_id) WHERE p.kind='installation'"""


def main(root):
    state = lab.load(root)
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    require(Path(state['source']) == SOURCE, 'fixture-source-identity')
    info = json.loads(lab.cli(state, 'build-info').stdout)
    require(info == state['build']['build_info'] and info['source']['revision'] == revision, 'source-identity')
    require(lab.docker('exec', state['client'], 'sha256sum', '/build/bigbrotr').stdout.split()[0] == state['binary_sha256'], 'binary-identity')
    output = root/'job-admission.json'
    require(not output.exists(), 'existing-result')
    report = dict(format='bigbrotr-job-admission-native/1', source=revision,
                  binary_sha256=state['binary_sha256'], server=state['server'],
                  status='RUNNING', cases=[], databases=[], sessions=[], cleanup='NOT_RUN')
    sessions, processes = [], []
    owned_ids = {key: json.loads(lab.docker('inspect', state[key]).stdout)[0]['Id'] for key in ('postgres', 'client')}
    entries = json.loads((SOURCE/'migrations/manifest.json').read_text())['application_order']

    def save():
        staging = output.with_suffix('.partial')
        staging.write_text(json.dumps(report, indent=2)+'\n'); staging.replace(output)

    def owned():
        lab.load(root)
        for key, identifier in owned_ids.items():
            current = json.loads(lab.docker('inspect', state[key]).stdout)[0]
            require(current['Id'] == identifier and current['State']['Running'], 'lab-incarnation')

    def record(name, ok=True, **detail):
        require(not any(c['name'] == name for c in report['cases']), 'duplicate-case')
        report['cases'].append(dict(name=name, status='PASS' if ok else 'FAIL', **detail)); save()
        require(ok, name)

    def session(db, name):
        s = RecordedSession(state, db, 'admission_'+name, sessions)
        report['sessions'].append(dict(backend=s.backend, commands=s.commands)); save()
        return s

    def close_sessions():
        errors = []
        for s in list(sessions):
            try:
                s.close(owned); sessions.remove(s)
            except BaseException as error:
                errors.append(type(error).__name__)
        require(not errors, 'session-cleanup-'+','.join(errors))

    def receipts(db, target):
        actual = json.loads(lab.sql(state, db, "SELECT json_agg(json_build_array(migration,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions;"))
        end = next(i+1 for i, e in enumerate(entries) if e['version'] == target)
        expected = sorted([[e['version'], sha(SOURCE/'migrations/postgres'/e['name'])] for e in entries[:end]])
        require(actual == expected, 'migration-checksums')
        return actual

    def migrate(db, path, target):
        result = lab.migrate(state, 'apply', path, target=target)
        record('migrate-'+db+'-'+str(target), result.returncode == 0 and not result.stderr,
               exit=result.returncode, stdout=result.stdout, stderr=result.stderr)
        receipt = receipts(db, target)
        lab.sql(state, db, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        return receipt

    def fresh(name, target=815, management=False):
        db = 'bb_admission_'+name
        lab.sql(state, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator;')
        report['databases'].append(db); save()
        path = lab.config(root, state, db); local = root/Path(path).name
        text, count = re.subn(r'(?ms)^local_roles = \[.*?\]', 'local_roles = ["control"]', local.read_text())
        require(count == 1, 'roles-config')
        text = lab.change(text, 'postgres.control', 'password_ref', 'file:/lab/secrets/application')
        text = lab.change(text, 'administration', 'enabled', management)
        if management:
            require('[administration.listen]' not in text, 'listen-already-configured')
            text += '\n[administration.listen]\nhost = "127.0.0.1"\nport = 14281\n'
        local.write_text(text)
        migrate(db, path, target)
        if management:
            # The real listener expires session/proof rows. Its unrelated
            # startup prerequisite must hold before testing the optional815gate.
            lab.sql(state, db, 'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
        for args in [('config', 'initialize'), ('config', 'activate-initial', '--request-id', '71'*16, '--expected-revision', '1')]:
            result = lab.cli(state, *args, '--file', path)
            record('-'.join(args[:2])+'-'+name, result.returncode == 0 and not result.stderr,
                   exit=result.returncode, stdout=result.stdout, stderr=result.stderr)
        return db, path, session(db, name)

    def rows(db):
        return json.loads(lab.sql(state, db, "SELECT coalesce(json_agg(json_build_array(encode(j.job_id,'hex'),a.admission_ordinal,j.created_at_us) ORDER BY a.admission_ordinal),'[]') FROM control.job_admissions a JOIN control.jobs j USING(job_id);"))

    def want(db, expected):
        actual = rows(db)
        require(actual == [[format(n, '032x'), ordinal, created] for n, ordinal, created in expected], 'independent-admission-rows')
        require(lab.sql(state, db, 'SELECT count(*) FROM control.jobs j FULL JOIN control.job_admissions a USING(job_id) WHERE j.job_id IS NULL OR a.job_id IS NULL;') == '0', 'mapping-completeness')
        return actual

    def snapshot(db):
        tables = json.loads(lab.sql(state, db, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        parts = [f"SELECT '{t}' AS name,coalesce(json_agg(t ORDER BY row_to_json(t)::text),'[]') AS rows FROM (SELECT *,xmin::text AS row_version FROM control.{t}) t" for t in tables]
        return json.loads(lab.sql(state, db, 'SELECT json_object_agg(name,rows) FROM ('+' UNION ALL '.join(parts)+') all_rows;'))

    def cut(s):
        s.execute('SELECT dataset_id FROM control.datasets FOR SHARE;')
        return int(s.execute('SELECT coalesce(max(admission_ordinal),0) FROM control.job_admissions;')['lines'][0])

    def page(db, upper=None, after=None, limit=20):
        predicates = []
        if upper is not None: predicates.append(f'a.admission_ordinal<={upper}')
        if after is not None: predicates.append(f'(j.created_at_us,j.job_id)<({after[0]},{identity(after[1])})')
        where = ' WHERE '+' AND '.join(predicates) if predicates else ''
        return json.loads(lab.sql(state, db, "SELECT coalesce(json_agg(encode(job_id,'hex') ORDER BY created_at_us DESC,job_id DESC),'[]') FROM (SELECT j.job_id,j.created_at_us FROM control.jobs j JOIN control.job_admissions a USING(job_id)"+where+f' ORDER BY j.created_at_us DESC,j.job_id DESC LIMIT {limit}) p;'))

    try:
        owned()
        db, path, s = fresh('upgrade', target=602)
        for n, created in [(10, 200), (11, 100), (12, 200)]: s.execute(insert(n, created)+';')
        before = snapshot(db); original = receipts(db, 602)
        new_receipts = migrate(db, path, 815)
        after = snapshot(db)
        record('upgrade-original-rows-and-xmin', all(after[t] == v for t, v in before.items() if t != 'schema_versions')
               and all(row in after['schema_versions'] for row in before['schema_versions']), before=before, after=after,
               original_receipts=original, new_receipts=new_receipts)
        record('deterministic-backfill', rows=want(db, [(11, 1, 100), (10, 2, 200), (12, 3, 200)]))
        digest = lab.schema_digest(state, db)
        result = lab.migrate(state, 'apply', path, target=815)
        record('no-op-upgrade', result.returncode == 0 and json.loads(result.stdout)['status'] == 'current' and lab.schema_digest(state, db) == digest)
        s.execute('BEGIN;'); upper = cut(s); first = page(db, upper, limit=2); s.execute('COMMIT;')
        record('first-page-cut', upper == 3 and first == [format(n,'032x') for n in [12,10]], upper=upper, page=first)
        for n, created in [(20, 150), (9, 200), (22, 50)]: s.execute(insert(n, created)+';')
        later = page(db, upper, (200,10)); negative = page(db, None, (200,10)); refreshed = page(db)
        record('backdated-and-equal-time-excluded', later == [format(11,'032x')]
               and negative == [format(n,'032x') for n in [9,20,11,22]]
               and refreshed == [format(n,'032x') for n in [12,10,9,20,11,22]],
               upper=upper, continued=later, time_only_negative=negative, refreshed=refreshed,
               rows=want(db, [(11,1,100),(10,2,200),(12,3,200),(20,4,150),(9,5,200),(22,6,50)]))
        baseline = snapshot(db)
        s.execute(insert(20,150)+' ON CONFLICT DO NOTHING;')
        record('same-job-retry-no-new-admission', snapshot(db) == baseline)
        s.execute('BEGIN;'); s.execute(insert(23)+';'); s.execute('ROLLBACK;')
        record('explicit-rollback-no-admission', snapshot(db) == baseline)
        for isolation in ['REPEATABLE READ', 'SERIALIZABLE', 'READ UNCOMMITTED']:
            s.execute('BEGIN ISOLATION LEVEL '+isolation+';'); s.execute(insert(23)+';', '25001'); s.execute('ROLLBACK;')
            record('isolation-'+isolation, snapshot(db) == baseline)
        for privilege in ['SELECT','INSERT']:
            lab.sql(state, db, 'REVOKE '+privilege+' ON control.job_admissions FROM bigbrotr_control;')
            answer = s.execute(insert(23)+';', '42501')
            record('missing-'+privilege, snapshot(db) == baseline, answer=answer)
            lab.sql(state, db, 'GRANT '+privilege+' ON control.job_admissions TO bigbrotr_control;')
        s.execute(insert(23)+';')
        record('same-job-after-grant-recovery', rows(db)[-1] == [format(23,'032x'),7,100], rows=rows(db))
        # Owner-level attempts reach immutable guards, rather than permission checks.
        for name, sql, expected in [
            ('update', 'UPDATE control.job_admissions SET admission_ordinal=admission_ordinal;', '23514'),
            ('delete', 'DELETE FROM control.job_admissions;', '23514'),
            ('truncate', 'TRUNCATE control.job_admissions;', '23514'),
            ('caller-ordinal', f'INSERT INTO control.job_admissions VALUES({identity(10)},99);', '23514'),
            ('duplicate', f'INSERT INTO control.job_admissions(job_id) VALUES({identity(10)});', '23505'),
            ('orphan', f'INSERT INTO control.job_admissions(job_id) VALUES({identity(99)});', '23514')]:
            before = snapshot(db)
            result = lab.docker('exec','-i',state['postgres'],'psql','-X','-A','-t','-q','--no-password','-U','bigbrotr_migrator','-d',db,'-v','ON_ERROR_STOP=1','-v','VERBOSITY=sqlstate',data=sql,check=False)
            record('raw-'+name, result.returncode != 0 and 'ERROR:  '+expected in result.stderr and snapshot(db) == before,
                   sql=sql, exit=result.returncode, stdout=result.stdout, stderr=result.stderr, expected_sqlstate=expected)
        close_sessions()
        db, _, s = fresh('multirow')
        s.execute('BEGIN;'); record('empty-cut-zero', cut(s) == 0); s.execute('COMMIT;')
        for offset, constraint in [(0,'control.job_admission_complete'),(2,'ALL')]:
            s.execute('BEGIN;'); s.execute('SET CONSTRAINTS '+constraint+' IMMEDIATE;')
            sql = insert(30+offset).replace(identity(30+offset), "decode(lpad(to_hex(n),32,'0'),'hex')")
            sql = sql.replace('FROM control.datasets d', f'FROM generate_series({30+offset},{31+offset}) n CROSS JOIN control.datasets d')
            s.execute(sql+';'); s.execute('COMMIT;')
            record('multirow-'+constraint, rows=want(db, [(n,n-29,100) for n in range(30,32+offset)]))
        close_sessions()
        for mode in ['commit','rollback','backend-death']:
            db, _, a = fresh('writers_'+mode.replace('-','_')); b = session(db, 'waiter_'+mode)
            a.execute('BEGIN;'); a.execute(insert(40)+';'); b.execute('BEGIN;'); b.start(insert(41)+';')
            barrier = wait_blocked(state,db,b,a)
            record('uncommitted-hidden-'+mode, rows(db) == [], barrier=barrier, rows=rows(db))
            if mode == 'backend-death': a.close(owned); sessions.remove(a)
            else: a.execute('COMMIT;' if mode == 'commit' else 'ROLLBACK;')
            answer = b.expect(b.finish()); b.execute('COMMIT;')
            expected = [(40,1,100),(41,2,100)] if mode == 'commit' else [(41,1,100)]
            record('writer-order-'+mode, rows=want(db, expected), waiter=answer)
            close_sessions()
        for first in ['writer','cut']:
            for rollback in [False, True]:
                name = first+('_rollback' if rollback else '_commit')
                db, _, a = fresh(name); b = session(db, name+'_waiter')
                a.execute('BEGIN;'); b.execute('BEGIN;')
                if first == 'writer':
                    a.execute(insert(50)+';'); b.start('SELECT dataset_id FROM control.datasets FOR SHARE;')
                else:
                    observed = cut(a); require(observed == 0,'initial-cut'); b.start(insert(50)+';')
                barrier = wait_blocked(state,db,b,a)
                a.execute('ROLLBACK;' if rollback else 'COMMIT;'); answer = b.expect(b.finish())
                if first == 'writer': observed = int(b.execute('SELECT coalesce(max(admission_ordinal),0) FROM control.job_admissions;')['lines'][0])
                b.execute('COMMIT;')
                expected = [] if first == 'writer' and rollback else [(50,1,100)]
                record('cut-lock-order-'+name, observed == (1 if first == 'writer' and not rollback else 0),
                       cut=observed, barrier=barrier, waiter=answer, rows=want(db,expected))
                close_sessions()
        # Small, explicit index observation: not mixed/aged throughput qualification.
        db, _, s = fresh('index')
        sql = insert(1000).replace(identity(1000), "decode(lpad(to_hex(n),32,'0'),'hex')")
        s.execute(sql.replace('FROM control.datasets d', 'FROM generate_series(1000,1999) n CROSS JOIN control.datasets d')+';')
        lab.sql(state,db,'ANALYZE control.job_admissions;')
        plan = json.loads(lab.sql(state,db,'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT admission_ordinal FROM control.job_admissions ORDER BY admission_ordinal DESC LIMIT 1;'))
        scan = plan[0]['Plan']['Plans'][0]
        record('bounded-backward-index', scan['Node Type'] in ['Index Scan','Index Only Scan'] and scan['Scan Direction'] == 'Backward' and scan['Actual Rows'] == 1,
               plan=plan, rows=int(lab.sql(state,db,'SELECT count(*) FROM control.job_admissions;')),
               relation_bytes=int(lab.sql(state,db,"SELECT pg_total_relation_size('control.job_admissions');")))
        close_sessions()
        # Actual Control executable observes the optional815 permission gate.
        db, path, _ = fresh('runtime', target=602, management=True); close_sessions()
        for name, privilege, expected_status in [('old',None,401),('new',None,401),('no_select','SELECT',503),('no_insert','INSERT',503),('restored',None,401)]:
            if name == 'new': migrate(db,path,815)
            if name in ['no_select','no_insert','restored']:
                lab.sql(state,db,'GRANT SELECT,INSERT ON control.job_admissions TO bigbrotr_control;')
                if privilege: lab.sql(state,db,'REVOKE '+privilege+' ON control.job_admissions FROM bigbrotr_control;')
            process = Process(['docker','exec',state['client'],'python3','-B','/source/experiments/cursor-registration/runtime.py','--child','--config',path,'--record','/lab/admission-'+name+'.process.json'])
            processes.append((name,process))
            end = time.monotonic()+10
            while b'\n' not in process.stdout:
                require(process.process.poll() is None and time.monotonic()<end, 'serve-startup-'+name); time.sleep(.02)
            observed = json.loads(bytes(process.stdout).splitlines()[0])
            # Literal nonexistent session:401 shows the handler reached auth;
            # 503 shows its optional prerequisite refused before auth. No login claim.
            probe = "import http.client,json; c=http.client.HTTPConnection('127.0.0.1',14281,timeout=5); c.request('GET','/management/v1/jobs/"+'51'*16+"',headers={'Cookie':'BigBrotrLocalSession="+'52'*32+"','x-csrf-token':'"+'53'*32+"'}); r=c.getresponse(); print(json.dumps(dict(status=r.status,body=r.read(4096).decode(),headers=r.getheaders()))); c.close()"
            response = lab.docker('exec',state['client'],'python3','-c',probe)
            actual = json.loads(response.stdout)
            record('runtime-optional-'+name, actual['status'] == expected_status, observed=observed, response=actual)
            lab.docker('exec',state['client'],'python3','-B','/source/experiments/cursor-registration/runtime.py','--signal','TERM','--record','/lab/admission-'+name+'.process.json')
            record('runtime-terminal-'+name, process.wait(10) == 0 and not process.stderr)
            record('runtime-owner-released-'+name, lab.sql(state,db,"SELECT count(*) FROM control.owner_epochs WHERE state<>'released';") == '0')
        # Restart all retained data; independent rows/receipts are compared again.
        prior = {db:snapshot(db) for db in report['databases']}
        owned(); lab.docker('restart','--time','10',owned_ids['postgres'])
        end = time.monotonic()+30
        while lab.docker('exec',state['postgres'],'pg_isready','-U','postgres',check=False).returncode != 0:
            require(time.monotonic()<end,'postgres-restart-deadline'); time.sleep(.1)
        owned()
        for db, before in prior.items():
            after = snapshot(db)
            record('restart-'+db, after == before, before=before, after=after)
        db = 'bb_admission_upgrade'; s = session(db,'after_restart'); s.execute(insert(24)+';')
        record('post-restart-next-order', rows(db)[-1] == [format(24,'032x'),8,100], rows=rows(db))
        report['status'] = 'CHECKS_PASSED'
    except BaseException as error:
        report.update(status='INCOMPLETE', failure_type=type(error).__name__)
        raise
    finally:
        errors = []
        try: close_sessions()
        except BaseException as error: errors.append(dict(resource='sessions',type=type(error).__name__))
        for name, process in processes:
            try:
                process.stop()
            except BaseException as error: errors.append(dict(resource=name+'-stop',type=type(error).__name__))
            try:
                (root/('runtime-'+name+'.stdout')).write_bytes(process.stdout)
                (root/('runtime-'+name+'.stderr')).write_bytes(process.stderr)
                lab.write_json(root/('runtime-'+name+'.terminal.json'),dict(
                    argv=process.process.args, exit=process.process.poll(), overflow=process.overflow,
                    stdout_bytes=len(process.stdout), stderr_bytes=len(process.stderr),
                    reader_threads_joined=all(not thread.is_alive() for thread in process.threads)))
            except BaseException as error: errors.append(dict(resource=name+'-capture',type=type(error).__name__))
        report['cleanup'] = 'UNRESOLVED' if errors else 'PASS'
        report['cleanup_errors'] = errors
        report['status'] = 'PASS' if report['status'] == 'CHECKS_PASSED' and not errors else 'INCOMPLETE'
        save()
    require(report['status'] == 'PASS','qualification-incomplete')
    print(json.dumps(dict(status='PASS',cases=len(report['cases']),databases=len(report['databases']))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    require(args.root.is_absolute() and args.root.resolve(strict=True) == args.root,'canonical-root')
    with (args.root/'job-admission.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        main(args.root)
