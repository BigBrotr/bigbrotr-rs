#!/usr/bin/env python3
"""Exact-source registry fault cases using the retained PG and new cloned DBs.

Only the verified stopped original PostgreSQL is restarted; a new bounded client
shares its network namespace. Loopback authentication is an explicit fixture
boundary, not new SCRAM/TLS qualification. Original databases/results stay intact.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
from fault_cases import Process
from concurrency_cases import Session
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
build = root / 'out/artifacts'
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert SOURCE == root / 'source'
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
assert not lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', '03b0197', revision,
    '--', 'apps/bigbrotr/src', 'crates', 'migrations', 'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']).stdout
receipt = json.loads((build / 'build-result.json').read_text())
assert receipt['source'] == revision and receipt['probe_name'] == 'relay_registry_fault_probe'
assert hashlib.sha256((build / receipt['probe_name']).read_bytes()).hexdigest() == receipt['probe_sha256']
original_root = root.parent / 'relay-registry-03b0197'
pg = lab.load(original_root / 'pg')
original = json.loads((original_root / 'pg/registry-result.json').read_text())
assert original['source'] == '03b019794a7b79f9f8a0daaa6a1b6f902c783a3a'
assert pg['state'] == 'stopped-retained' and pg['owner'] == 'b49addf3ec7daaef890692e501309e01'
output = root / 'faults'
output.mkdir(mode=0o700)
owner = secrets.token_hex(16)
label = 'org.bigbrotr.relay-registry-faults'
client = 'bigbrotr-relay-registry-faults-' + revision[:7]
report = dict(format='bigbrotr-relay-registry-faults/1', source=revision, build=receipt,
    status='RUNNING', owner=owner, label=label, cases=[], databases=[], client=None,
    postgres=pg['postgres'], postgres_owner=pg['owner'], original_source=original['source'],
    original_report_sha256=hashlib.sha256((original_root / 'pg/registry-result.json').read_bytes()).hexdigest(),
    not_run=['enrollment', 'network action grants', 'socket acquisition/M1', 'load', 'arm64', 'release'])
processes, sessions = [], []


def save():
    lab.write_json(output / 'result.json', report)


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def record(name, **details):
    report['cases'].append(dict(name=name, status='PASS', **details))
    save()


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def tables(db):
    names = json.loads(lab.sql(pg, db,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    parts = []
    for name in names:
        require(re.fullmatch('[a-z_]+', name), 'table-name')
        parts.append(f"""SELECT '{name}' AS name, jsonb_build_object(
            'rows',count(*),'sha256',encode(sha256(convert_to(
            coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]')::text,'UTF8')),'hex')) AS value
            FROM control.{name} t""")
    return json.loads(lab.sql(pg, db, 'SELECT jsonb_object_agg(name,value) FROM (' + ' UNION ALL '.join(parts) + ') s'))


def unchanged(before, after, allowed):
    changed = {name for name in before if before[name] != after[name]}
    require(before.keys() == after.keys() and changed <= set(allowed), 'unrelated-table-change')
    return sorted(changed)


def endpoint(db, path='fault'):
    require(path in ('fault', 'a/~?b=2&a=1'), 'fixture-endpoint')
    return json.loads(lab.sql(pg, db, f"""SELECT coalesce(json_agg(json_build_object(
        'id',encode(e.endpoint_id,'hex'),'time',e.registered_at_us,
        'config',e.config_revision,'policy',e.policy_revision,'epoch',e.owner_epoch,
        'resource',encode(e.owner_resource_id,'hex'),'incarnation',encode(e.owner_incarnation,'hex'),
        'role_instance',(SELECT json_agg(get_byte(o.role_instance_id,n) ORDER BY n) FROM generate_series(0,15) n))), '[]')
        FROM control.relay_endpoints e JOIN control.owner_epochs o
            ON o.resource_id=e.owner_resource_id AND o.epoch=e.owner_epoch
        WHERE e.url_bytes=convert_to('wss://fixture.example/{path}','UTF8')"""))


def prepare(name, template='basic'):
    db = 'bb_relay_faults_' + revision[:7] + '_' + name
    require(re.fullmatch('[a-z0-9_]{1,63}', db), 'database-name')
    require(lab.sql(pg, 'postgres', f"SELECT count(*) FROM pg_database WHERE datname='{db}'") == '0', 'existing-database')
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE bb_registry_{template}')
    report['databases'].append(db)
    expected = next(c['after'] for c in original['cases'] if c['name'] == 'independent-effects-' + template)
    require(tables(db) == expected, 'clone-changed-original-table-content')
    save()
    return db


def app(db, mode, nonce, port=5432):
    token = db + '-' + str(nonce)
    process = Process(['docker', 'exec', client, '/build/relay_registry_fault_probe', db,
        '/run/secrets/application', '/lab/policy.json', mode, str(port), str(nonce), '/lab/' + token])
    processes.append((token, process))
    return process, token


def event(process, wanted, seconds=10):
    deadline = time.monotonic() + seconds
    with process.condition:
        while True:
            require(not process.overflow, 'probe-output-overflow')
            end = process.stdout.find(b'\n', process.offset)
            if end >= 0:
                value = json.loads(bytes(process.stdout[process.offset:end]))
                process.offset = end + 1
                require(value.get('event') == wanted, 'unexpected-probe-event')
                return value
            require(time.monotonic() < deadline and
                (process.process.poll() is None or process.threads[0].is_alive()), 'missing-probe-event-' + wanted)
            process.condition.wait(0.02)


def finish(process):
    value = event(process, 'done')
    require(process.wait(10) == 0 and not process.stderr and value['driver_joined'], 'probe-terminal')
    return value['outcome']


def direct(db, mode, nonce):
    process, _ = app(db, mode, nonce)
    if mode == 'busy':
        value = event(process, 'busy')
        require(process.wait(10) == 0 and not process.stderr and not value['commit_started'], 'busy-probe')
        return value
    ready = event(process, 'ready')
    return dict(ready=ready, outcome=finish(process))


def observe_owner(db, pid):
    value = json.loads(lab.sql(pg, db, f"""SELECT json_build_object(
        'pid',a.pid,'start',a.backend_start::text,'state',a.state,'wait',a.wait_event,
        'expired',o.lease_expires_at_us < (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
        'epoch',o.epoch,'fenced',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid
            AND locktype='advisory' AND classid=1111651442::oid AND objid=1129599564::oid
            AND objsubid=2 AND mode='ExclusiveLock' AND granted))
        FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid
            AND o.control_backend_start=a.backend_start
        WHERE a.pid={int(pid)} AND a.datname='{db}' AND o.state='active'"""))
    require(value and value['fenced'] and value['pid'] == pid, 'actual-owner-fence')
    return value


def verify_receipt(value, rows, created):
    require(len(rows) == 1 and value == dict(rows[0], created=created), 'complete-original-receipt')


def wait_expired(db, pid, observations):
    deadline = time.monotonic() + 5
    while True:
        current = observe_owner(db, pid)
        require(not observations or current['start'] == observations[0]['start'], 'owner-incarnation-changed')
        observations.append(current)
        if current['expired']:
            return
        require(time.monotonic() < deadline, 'owner-not-expired')
        time.sleep(0.05)


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner']
assert target['State']['Status'] == 'exited' and target['State']['Pid'] == 0
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert shutil.disk_usage(root).free > 2 * 1024**3
assert lab.docker('inspect', client, check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    free_bytes_before=shutil.disk_usage(root).free, postgres_id=target['Id'])
lab.write_json(output / 'preflight.json', [projection(c) for c in prior])
save()
try:
    lab.docker('start', target['Id'])
    deadline = time.monotonic() + 30
    while lab.docker('exec', target['Id'], 'pg_isready', '-U', 'postgres', check=False).returncode:
        require(time.monotonic() < deadline, 'postgres-readiness')
        time.sleep(0.1)
    for case in ['basic', 'collision', 'expired']:
        expected = next(c['after'] for c in original['cases'] if c['name'] == 'independent-effects-' + case)
        require(tables('bb_registry_' + case) == expected, 'original-state-before')
    shutil.copyfile(original_root / 'pg/basic-policy.json', output / 'policy.json')
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128',
        '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={original_root}/pg/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly',
        '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    for name, proxy_mode in [('pass', 'pass'), ('before', 'before-forward'), ('after', 'after-complete'),
                             ('cancel_close', 'hold'), ('cancel_release', 'hold'), ('expiry', 'hold')]:
        db = prepare(name)
        before = tables(db)
        proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
            '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
            '--mode', proxy_mode, '--commit-number', '2', '--deadline-seconds', '35'], input_pipe=True)
        processes.append((db + '-proxy', proxy))
        ready = proxy.event('ready', 10)
        require(ready['proxy_sha256'] == hashlib.sha256((SOURCE / 'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest(), 'proxy-source')
        mode = 'unknown' if name in ('before', 'after') else 'cancel' if name.startswith('cancel_') else 'held-expired' if name == 'expiry' else 'insert'
        process, token = app(db, mode, 11, ready['listen_port'])
        acquired = event(process, 'ready')
        require(acquired['epoch'] == 3, 'first-new-owner-epoch')
        observations = []
        if proxy_mode == 'hold':
            proxy.event('held', 10)
            observations.append(observe_owner(db, acquired['pid']))
            if name == 'expiry':
                require(not observations[0]['expired'], 'owner-already-expired-before-hold-observation')
            require(endpoint(db) == [], 'uncommitted-endpoint-visible')
            if name.startswith('cancel_'):
                require(event(process, 'cancelled', 5)['commit_started'], 'registration-not-cancelled')
            else:
                wait_expired(db, acquired['pid'], observations)
            observations.append(observe_owner(db, acquired['pid']))
            owner_state = tables(db)
            direct(db, 'busy', 12)
            require(tables(db) == owner_state, 'busy-owner-created-rows')
            proxy.send('close' if name == 'cancel_close' else 'release')
            if name.startswith('cancel_'):
                (output / (token + '.finish')).touch(exist_ok=False)
        result = finish(process)
        wire = proxy.event('receipt', 10)
        require(proxy.wait(5) == 0 and not proxy.stderr, 'proxy-terminal')
        counts = [wire[k] for k in ['commits_seen', 'commits_forwarded', 'server_commit_complete', 'client_commit_complete', 'faults']]
        expected = {'pass': [2,2,2,2,0], 'before': [2,1,1,1,1], 'after': [2,2,2,1,1],
                    'cancel_close': [2,1,1,1,1], 'cancel_release': [2,2,2,2,1], 'expiry': [2,2,1,1,1]}[name]
        require(counts == expected, 'exact-selected-COMMIT-counts')
        observed = endpoint(db)
        committed = name in ('pass', 'after', 'cancel_release')
        require(len(observed) == int(committed), 'independent-endpoint-outcome')
        if committed:
            require(observed[0]['epoch'] == 3 and observed[0]['config'] == observed[0]['policy'] == 1, 'original-attribution')
        if name == 'pass':
            verify_receipt(result['receipt'], observed, True)
        recovered = direct(db, 'insert', 13)
        require(recovered['ready']['epoch'] == 4, 'recovery-owner-epoch')
        durable = endpoint(db)
        verify_receipt(recovered['outcome']['receipt'], durable, not committed)
        require(durable == observed if committed else durable[0]['epoch'] == 4, 'recovery-original-attribution')
        after = tables(db)
        changed = unchanged(before, after, ['owner_epochs', 'role_instances', 'relay_endpoints'])
        record('COMMIT-' + name, outcome=result, proxy=wire, observations=observations,
               before_endpoint=observed, recovery=recovered, after_endpoint=durable, changed=changed,
               before=before, after=after)
    db = prepare('policy')
    initial = tables(db)
    historic = endpoint(db, 'a/~?b=2&a=1')
    process, token = app(db, 'policy', 11)
    acquired = event(process, 'ready')
    writer = Session(pg, db, 'relay-policy-writer', sessions)
    writer.execute('BEGIN; SELECT dataset_id FROM control.datasets FOR UPDATE;')
    writer.execute("""INSERT INTO control.config_revisions
        SELECT revision+1,dataset_id,revision,schema_version,authority_mode,document,
            document_digest,accepted_source_digest,accepted_at_us+1,actor_id
        FROM control.config_revisions WHERE revision=1;
        INSERT INTO control.policy_revisions
        SELECT revision+1,dataset_id,revision,2,schema_version,document,document_digest,accepted_at_us+1,actor_id
        FROM control.policy_revisions WHERE revision=1;
        UPDATE control.datasets SET config_revision=2,policy_revision=2;""")
    (output / (token + '.go')).touch(exist_ok=False)
    deadline = time.monotonic() + 3
    while True:
        barrier = json.loads(lab.sql(pg, db, f"""SELECT json_build_object(
            'pid',pid,'start',backend_start::text,'wait_type',wait_event_type,
            'wait',wait_event,'blockers',pg_blocking_pids(pid))
            FROM pg_stat_activity WHERE pid={acquired['pid']}"""))
        if barrier and barrier['wait_type'] == 'Lock' and writer.backend['pid'] in barrier['blockers']:
            break
        require(time.monotonic() < deadline, 'dataset-lock-wait-not-observed')
        time.sleep(0.02)
    writer.execute('COMMIT;')
    result = finish(process)
    require(result == dict(result='relay registration failed: PolicyChanged', commit_started=False), 'policy-race-result')
    require(endpoint(db) == [], 'stale-policy-inserted')
    existing = direct(db, 'existing', 12)
    verify_receipt(existing['outcome']['receipt'], historic, False)
    require(existing['ready']['config'] == existing['ready']['policy'] == 2, 'fresh-policy-validation')
    inserted = direct(db, 'insert', 13)
    durable = endpoint(db)
    verify_receipt(inserted['outcome']['receipt'], durable, True)
    require(durable[0]['config'] == durable[0]['policy'] == 2 and durable[0]['epoch'] == 5, 'new-policy-attribution')
    after = tables(db)
    changed = unchanged(initial, after, ['datasets', 'config_revisions', 'policy_revisions', 'owner_epochs', 'role_instances', 'relay_endpoints'])
    record('policy-after-lock-wait-and-fresh-successors', barrier=barrier, result=result,
           existing=existing, inserted=inserted, historic=historic, after_endpoint=durable,
           changed=changed, before=initial, after=after)
    writer.close(lambda: lab.load(original_root / 'pg'))
    sessions.remove(writer)
    db = prepare('overfull', 'collision')
    lab.sql(pg, db, """BEGIN; ALTER TABLE control.relay_endpoints DISABLE TRIGGER USER;
        INSERT INTO control.relay_endpoints(endpoint_id,dataset_id,normalization_version,url_bytes,
            registered_at_us,config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
        SELECT decode(repeat('ee',16),'hex'),dataset_id,1,convert_to('wss://fixture.example/corrupted','UTF8'),
            registered_at_us,config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation
        FROM control.relay_endpoints LIMIT 1;
        ALTER TABLE control.relay_endpoints ENABLE TRIGGER USER; COMMIT;""")
    require(lab.sql(pg, db, 'SELECT count(*) FROM control.relay_endpoints') == '65', 'overfull-fixture-count')
    require(lab.sql(pg, db, "SELECT count(*) FROM pg_trigger WHERE tgrelid='control.relay_endpoints'::regclass AND tgenabled<>'O'") == '0', 'guards-not-restored')
    initial = tables(db)
    results = [direct(db, mode, nonce) for mode, nonce in [('overfull',11), ('overfull-new',12)]]
    after = tables(db)
    changed = unchanged(initial, after, ['owner_epochs', 'role_instances'])
    record('corrupt-overfull-existing-and-absent', outcomes=results, before=initial, after=after, changed=changed)
    db = prepare('lookup_expired')
    initial = tables(db)
    historic = endpoint(db, 'a/~?b=2&a=1')
    process, token = app(db, 'existing-expired', 11)
    acquired = event(process, 'ready')
    observations = []
    wait_expired(db, acquired['pid'], observations)
    (output / (token + '.go')).touch(exist_ok=False)
    result = dict(ready=acquired, outcome=finish(process))
    verify_receipt(result['outcome']['receipt'], historic, False)
    require(lab.sql(pg, db, 'SELECT bool_and(lease_expires_at_us < (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) FROM control.owner_epochs WHERE state=\'active\'') == 't', 'lookup-owner-not-expired')
    after = tables(db)
    changed = unchanged(initial, after, ['owner_epochs', 'role_instances'])
    record('expired-scheduling-lease-identity-only', outcome=result, historic=historic, observations=observations, changed=changed, before=initial, after=after)
    for case in ['basic', 'collision', 'expired']:
        expected = next(c['after'] for c in original['cases'] if c['name'] == 'independent-effects-' + case)
        require(tables('bb_registry_' + case) == expected, 'original-state-after')
    require(hashlib.sha256((original_root / 'pg/registry-result.json').read_bytes()).hexdigest() == report['original_report_sha256'], 'original-report-changed')
    require(lab.sql(pg, 'postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename='bigbrotr_control'") == '0', 'application-backends-remain')
    report['status'] = 'PASS_SCOPED_REGISTRY_FAULTS'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    cleanup_errors = []
    for session in sessions:
        try:
            session.close(lambda: lab.load(original_root / 'pg'))
        except BaseException as error:
            cleanup_errors.append('sql-session-' + type(error).__name__)
    if report['client']:
        try:
            item = json.loads(lab.docker('inspect', report['client']['id']).stdout)[0]
            require(item['Config']['Labels'][label] == owner, 'client-ownership')
            if item['State']['Running']:
                lab.docker('stop', '--time', '3', item['Id'])
        except BaseException as error:
            cleanup_errors.append('client-' + type(error).__name__)
    for name, process in processes:
        try:
            process.stop()
        except BaseException as error:
            cleanup_errors.append('process-' + type(error).__name__)
        (output / (name + '.stdout')).write_bytes(process.stdout)
        (output / (name + '.stderr')).write_bytes(process.stderr)
    try:
        item = json.loads(lab.docker('inspect', target['Id']).stdout)[0]
        require(item['Config']['Labels'][lab.LABEL] == pg['owner'], 'postgres-ownership')
        if item['State']['Running']:
            lab.docker('stop', '--time', '10', target['Id'])
    except BaseException as error:
        cleanup_errors.append('postgres-' + type(error).__name__)
    after = {c['Id']: c for c in containers()}
    report['unrelated_changed'] = [c['Id'] for c in prior if c['Id'] != target['Id']
        and projection(c) != projection(after[c['Id']])]
    report['terminal'] = [projection(after[ident]) for ident in [target['Id']] +
        ([report['client']['id']] if report['client'] else [])]
    report['cleanup_errors'] = cleanup_errors
    if cleanup_errors or report['unrelated_changed'] or any(c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    report.update(finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), free_bytes_after=shutil.disk_usage(root).free)
    save()
print(json.dumps(dict(status=report['status'], cases=len(report['cases']), output=str(output))))
require(report['status'] == 'PASS_SCOPED_REGISTRY_FAULTS', 'incomplete-fault-profile')
