#!/usr/bin/env python3
"""Real product catalog faults/races in one already-inventoried exact-source lab.

Run on Tux with the owned root. Uses docker exec, no socket in the runner. All
scientific input is synthetic; observed activation never launches a native client.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
from fault_cases import Process

def require(value, reason):
    if not value:
        raise RuntimeError(reason)

def command(*args, input=None):
    result = subprocess.run(args, input=input, text=True, capture_output=True, timeout=30)
    require(result.returncode == 0, 'command-refused')
    require(len(result.stdout) <= 1024*1024, 'command-output-bound')
    return result.stdout.strip()

def main():
    root = Path(sys.argv[1]).resolve()
    state = json.loads((root / 'resources.json').read_text())
    runner, pg = state['container_id'], state['postgres_id']
    require(SOURCE == root / 'source', 'wrong-source-root')
    require(command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == state['source'], 'source-revision')
    require(not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all'), 'dirty-source')
    for identifier in [runner, pg]:
        info = json.loads(command('docker', 'inspect', identifier))[0]
        require(info['Config']['Labels']['org.bigbrotr.catalog-recovery'] == state['owner'] and info['State']['Running'], 'resource-owner-or-state')
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    require(ready['source'] == state['source'], 'artifact-source')
    report = {'source': state['source'], 'artifacts': ready, 'status': 'RUNNING', 'cases': [], 'scope': 'synthetic retained Control intents and actual catalog activation; no scientific publication'}
    path = root / 'recovery.json'
    require(not path.exists(), 'existing-report')
    def save():
        temp = path.with_suffix('.new')
        temp.write_text(json.dumps(report, indent=2) + '\n')
        temp.replace(path)
    def sql(db, text):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, input=text)
    def snapshot(db):
        present = sql(db, "SELECT to_regnamespace('bigbrotr_archive') IS NOT NULL;") == 't'
        item = {'guard': present}
        item['native_tables'] = int(sql(db, "SELECT count(*) FROM pg_tables WHERE schemaname='public';"))
        item['bootstrap_create'] = sql(db, "SELECT has_schema_privilege('BootstrapMixed','public','CREATE');") == 't'
        item['guard_triggers'] = int(sql(db, "SELECT count(*) FROM pg_trigger WHERE tgname='bigbrotr_writer_guard' AND tgenabled='O';"))
        item['writer_privileges'] = [int(sql(db, "SELECT count(*) FROM pg_tables t CROSS JOIN (VALUES ('SELECT'),('INSERT'),('UPDATE'),('DELETE')) p(privilege) WHERE t.schemaname='public' AND has_table_privilege('bb_archive_" + byte*16 + "',quote_ident(t.schemaname)||'.'||quote_ident(t.tablename),p.privilege);")) for byte in ['14', '16']]
        item['bootstrap_privileges'] = int(sql(db, "SELECT count(*) FROM pg_tables t CROSS JOIN (VALUES ('SELECT'),('INSERT'),('UPDATE'),('DELETE'),('TRUNCATE'),('REFERENCES'),('TRIGGER')) p(privilege) WHERE t.schemaname='public' AND has_table_privilege('BootstrapMixed',quote_ident(t.schemaname)||'.'||quote_ident(t.tablename),p.privilege);"))
        item['writer_forbidden'] = [int(sql(db, "SELECT count(*) FROM pg_tables t CROSS JOIN (VALUES ('TRUNCATE'),('REFERENCES'),('TRIGGER')) p(privilege) WHERE t.schemaname='public' AND has_table_privilege('bb_archive_" + byte*16 + "',quote_ident(t.schemaname)||'.'||quote_ident(t.tablename),p.privilege);")) for byte in ['14', '16']]
        item['writer_schema'] = [json.loads(sql(db, "SELECT json_build_object('usage',has_schema_privilege('bb_archive_" + byte*16 + "','public','USAGE'),'create',has_schema_privilege('bb_archive_" + byte*16 + "','public','CREATE'));")) for byte in ['14', '16']]
        item['native_manager_owned'] = int(sql(db, "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner='postgres';"))
        if present:
            for name in ['installation', 'owner', 'attempts']:
                item[name] = json.loads(sql(db, 'SELECT coalesce(json_agg(t),\'[]\') FROM (SELECT * FROM bigbrotr_archive.' + name + (' ORDER BY ordinal' if name == 'attempts' else '') + ')t;'))
        return item
    children = []
    def start(args, pipe=False):
        process = Process(['docker', 'exec'] + (['-i'] if pipe else []) + [runner] + args, input_pipe=pipe)
        children.append(process)
        return process
    def event(app, index, timeout=10):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            with app.condition:
                lines = bytes(app.stdout).splitlines()
            require(not app.overflow, 'app-output-bound')
            if len(lines) > index:
                value = json.loads(lines[index])
                require(set(value) in [{'event','pid','start'}, {'event','outcome','commit_started'}, {'event','aborted'}], 'app-envelope')
                return value
            require(app.process.poll() is None, 'app-ended-before-event')
            time.sleep(.01)
        raise RuntimeError('app-event-timeout')
    def probe(db, mode, ordinal=0, port=5432):
        app = start(['/out/native-tests/catalog-recovery-probe', mode, db, str(ordinal), str(port)])
        backend = event(app, 0)
        require(backend['event'] == 'connected', 'backend-envelope')
        return app, backend
    def finish(app, outcome, committed):
        require(app.wait(15) == 0 and not app.stderr, 'probe-terminal')
        require(event(app, 1) == {'event':'outcome','outcome':outcome,'commit_started':committed}, 'actual-guard-outcome')
        require(event(app, 2) == {'event':'joined','aborted':False}, 'probe-driver-not-joined')
    def direct(db, mode, ordinal, outcome, committed):
        app, _ = probe(db, mode, ordinal)
        finish(app, outcome, committed)
    def proxy(mode):
        p = start(['python3', '-B', '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432', '--mode', mode, '--deadline-seconds', '60'], True)
        r = p.event('ready', 10)
        require(r['proxy_sha256'] == hashlib.sha256((SOURCE / 'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest(), 'proxy-source')
        return p, r['listen_port']
    def receipt(p, counts):
        value = p.event('receipt', 10)
        require(p.wait(5) == 0 and not p.stderr, 'proxy-terminal')
        keys = ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']
        require([value[k] for k in keys] == counts, 'actual-COMMIT-boundary')
        return value
    def prepare(name, install=True):
        db = 'bbcat_' + name
        sql('postgres', 'CREATE DATABASE ' + db + ';')
        sql(db, 'GRANT CREATE ON SCHEMA public TO "BootstrapMixed";')
        result = command('docker', 'exec', runner, 'python3', '-B', '/source/experiments/archive-engine/native_catalog_recovery.py', 'bootstrap', db)
        require(json.loads(result)['status'] == 'initialized', 'actual-native-bootstrap')
        if install:
            direct(db, 'install', 0, 'installed', True)
        return db
    def expected(db, ordinals, head):
        item = snapshot(db)
        require(item['guard'] and item['native_tables'] == item['guard_triggers'] == 28 and not item['bootstrap_create'], 'complete-installed-guard')
        require([a['ordinal'] for a in item['attempts']] == ordinals, 'immutable-attempt-count-and-order')
        require(item['owner'] == [{'singleton':True,'ordinal':head,'enabled':head != 0}], 'durable-owner')
        require(item['writer_privileges'] == [112 if n in ordinals else 0 for n in [1,2]], 'atomic-all-native-DML-grants')
        require(item['bootstrap_privileges'] == 0 and item['writer_forbidden'] == [0,0] and item['native_manager_owned'] == 28, 'atomic-ownership-and-revocation')
        require(item['writer_schema'] == [{'usage':True,'create':False}]*2, 'native-schema-authority')
        require(len(item['installation']) == 1, 'installation-singleton')
        header = item['installation'][0]
        require(header['dataset_id'] == '\\x' + '01'*16 and header['catalog_id'] == '\\x' + '5a'*16 and header['data_root'] == '/out/archive_' + db + '/' and header['protocol'] == 1, 'independent-installation-identity')
        require(header['sql_checksum'] == '\\x' + hashlib.sha256((SOURCE / 'migrations/catalog/0001_writer_guard.sql').read_bytes()).hexdigest(), 'independent-installation-checksum')
        for a in item['attempts']:
            n = 20 if a['ordinal'] == 1 else 22
            require(a['attempt_id'] == '\\x' + bytes([n]*16).hex() and a['worker_incarnation'] == '\\x' + bytes([n+1]*16).hex(), 'immutable-identity')
            require(a['topology_generation'] == 0 and a['login_name'] == 'bb_archive_' + bytes([n]*16).hex(), 'immutable-topology-login')
            require(int(a['login_oid']) == int(sql(db, "SELECT oid FROM pg_roles WHERE rolname='" + a['login_name'] + "';")), 'actual-role-identity')
            require(a['coordinator_incarnation'] == '\\x' + '50'*16 and a['batch_digest'] == '\\x6617e55295d367ffc9453a93ceb054623a952d289dcbf8e6ad5f5332db9134d0', 'independent-input-digest')
        return item
    def blocked(db, first, second):
        end = time.monotonic() + 3
        while time.monotonic() < end:
            observation = json.loads(sql(db, "SELECT coalesce(json_agg(t),'[]') FROM (SELECT a.pid,a.backend_start::text,a.state,a.wait_event_type,a.wait_event,pg_blocking_pids(a.pid) AS blockers FROM pg_stat_activity a WHERE a.datname=current_database() AND a.pid IN (" + str(first['pid']) + ',' + str(second['pid']) + ') ORDER BY a.pid)t;'))
            indexed = {a['pid']: a for a in observation}
            if second['pid'] in indexed and first['pid'] in indexed[second['pid']]['blockers']:
                require(indexed[first['pid']]['backend_start'] == first['start'] and indexed[second['pid']]['backend_start'] == second['start'], 'backend-incarnation-changed')
                require(indexed[first['pid']]['state'] == 'idle in transaction' and indexed[second['pid']]['wait_event_type'] == 'Lock', 'actual-lock-wait')
                locks = json.loads(sql(db, "SELECT coalesce(json_agg(t),'[]') FROM (SELECT pid,locktype,mode,granted,transactionid::text FROM pg_locks WHERE locktype='transactionid' AND pid IN (" + str(first['pid']) + ',' + str(second['pid']) + ') ORDER BY pid,granted)t;'))
                held = {l['transactionid'] for l in locks if l['pid'] == first['pid'] and l['granted'] and l['mode'] == 'ExclusiveLock'}
                require(any(l['pid'] == second['pid'] and not l['granted'] and l['mode'] == 'ShareLock' and l['transactionid'] in held for l in locks), 'owner-row-transaction-lock')
                return {'activity':observation,'locks':locks}
            time.sleep(.02)
        raise RuntimeError('no-observed-lock-wait')
    def record(item):
        report['cases'].append(item)
        save()
    save()
    try:
        for operation in ['install','activate']:
            for fault, counts in [('pass',[1,1,1,1,0]),('before-forward',[1,0,0,0,1]),('after-complete',[1,1,1,0,1])]:
                name = operation + '_' + fault.split('-')[0]
                report['active'] = name; save()
                db = prepare(name, operation != 'install')
                before = snapshot(db)
                p, port = proxy(fault)
                app, backend = probe(db, operation, int(operation == 'activate'), port)
                finish(app, ('installed' if operation == 'install' else 'activated') if fault == 'pass' else 'unknown', True)
                wire = receipt(p, counts)
                observed = snapshot(db)
                if fault == 'before-forward':
                    require(observed == before, 'unforwarded-transaction-changed-state')
                direct(db, operation, int(operation == 'activate'), ('installed' if operation == 'install' else 'activated') if fault == 'before-forward' else ('existing' if operation == 'install' else 'current'), fault == 'before-forward')
                final = expected(db, [] if operation == 'install' else [1], 0 if operation == 'install' else 1)
                if fault != 'before-forward': require(final == observed, 'retry-rewrote-committed-state')
                record({'name':name,'status':'PASS','backend':backend,'proxy':wire,'observed':observed,'resolved':final})
        for first, second, action in [(1,2,'release'),(1,2,'close'),(2,1,'release'),(2,1,'close'),(1,1,'release'),(1,1,'close')]:
            name = f'race_{first}_{second}_{action}'
            report['active'] = name; save()
            db = prepare(name)
            p, port = proxy('hold')
            app, holder = probe(db, 'activate', first, port)
            p.event('held', 10)
            waiter, contender = probe(db, 'activate', second)
            observation = blocked(db, holder, contender)
            expected(db, [], 0)
            p.send(action)
            finish(app, 'activated' if action == 'release' else 'unknown', True)
            outcome = 'activated' if action == 'close' or second > first else ('current' if first == second else 'conflict')
            finish(waiter, outcome, outcome == 'activated')
            wire = receipt(p, [1,1,1,1,1] if action == 'release' else [1,0,0,0,1])
            ordinals = sorted(set([first,second])) if action == 'release' and first <= second else [first if action == 'release' else second]
            final = expected(db, ordinals, max(ordinals))
            if first <= max(ordinals):
                direct(db, 'activate', first, 'current' if first == max(ordinals) else ('historical' if first in ordinals else 'conflict'), False)
            record({'name':name,'status':'PASS','blocking':observation,'proxy':wire,'contender_outcome':outcome,'resolved':final})
        for action in ['release','close']:
            name = 'cancel_' + action
            report['active'] = name; save()
            db = prepare(name)
            p, port = proxy('hold')
            app, holder = probe(db, 'cancel', 1, port)
            p.event('held', 10)
            require(event(app,1,5) == {'event':'outcome','outcome':'cancelled','commit_started':True}, 'not-cancelled-at-COMMIT')
            waiter, contender = probe(db, 'activate', 1)
            observation = blocked(db, holder, contender)
            expected(db, [], 0)
            p.send(action)
            finish(app, 'cancelled', True)
            finish(waiter, 'current' if action == 'release' else 'activated', action == 'close')
            wire = receipt(p, [1,1,1,1,1] if action == 'release' else [1,0,0,0,1])
            record({'name':name,'status':'PASS','blocking_after_cancellation':observation,'proxy':wire,'resolved':expected(db,[1],1)})
        require(sql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE application_name='bigbrotr-catalog-recovery-probe';") == '0', 'remaining-probe-backends')
        report.update(status='PASS', active=None, remaining_probe_backends=0)
        save()
    except BaseException:
        report['status'] = 'FAIL_OR_UNKNOWN'
        save()
        raise
    finally:
        for child in children: child.stop()
        # Host owner must positively stop the inventoried runner/PG, even after
        # a failed docker-exec wrapper. This file never retires their resources.

if __name__ == '__main__':
    main()
