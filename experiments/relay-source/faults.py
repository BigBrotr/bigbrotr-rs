#!/usr/bin/env python3
"""Real source-binding lock/COMMIT schedules and revoked historical recovery.

External writers are privileged, clone-only state injections. The probe's later
withdrawal uses the actual authenticated command. Neither is a socket grant.
"""
import copy
import datetime
import hashlib
import importlib.util
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
import concurrency_cases
concurrency_cases.ROLE = 'postgres'
from concurrency_cases import Session
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection
spec = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
m = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = m
spec.loader.exec_module(m)
from oracle import complete_source
from locks import blocked

root = Path(sys.argv[1]).resolve(strict=True)
assert len(sys.argv) in (2, 3) and SOURCE == root / 'source'
scoped = len(sys.argv) == 3
assert not scoped or sys.argv[2] == '--scope'
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = root / 'out/artifacts' if scoped else root.parent / 'relay-source-faults-87ad138/out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
sha = lambda value: hashlib.sha256(value).hexdigest()
if scoped:
    assert built['source'] == revision
else:
    assert built['source'] == '87ad138926d164e47a8cd9b653b8bca19759c0a7'
    changed = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert changed and all(n.startswith(('docs/', 'experiments/')) for n in changed), changed
for name, key in [('bigbrotr', 'binary_sha256'), ('ingest_bootstrap_probe', 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
original = root.parent / 'relay-source-recheck-e06f580'
original_report = (original / 'pg/source-result.json').read_bytes()
assert json.loads(original_report)['status'] == 'PASS_SCOPED_RELAY_SOURCE_FIRST_WAVE'
pg = lab.load(original / 'pg')
assert pg['state'] == 'stopped-retained' and pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
output = root / 'faults'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.relay-source-faults'
client = 'bigbrotr-relay-source-faults-' + revision[:7]
report = dict(format='bigbrotr-relay-source-faults/1', source=revision, build=built,
    application_source=built['source'], scoped=scoped, owner=owner, label=label, status='RUNNING',
    client=None, postgres=pg['postgres'], postgres_owner=pg['owner'], cases=[], databases=[],
    original_report_sha256=sha(original_report), not_run=['source grant and socket attribution',
    'private routes and bans', 'large ledger upgrade', 'load/aged state', 'arm64/release'])
processes, sessions = [], []


def save():
    lab.write_json(output / 'result.json', report)


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def rows(db):
    names = json.loads(lab.sql(pg, db, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    assert len(names) in ((64, 66) if scoped else (64,)) and all(re.fullmatch('[a-z_]+', n) for n in names)
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
        f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, db, 'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def state_rows(state):
    return {name: json.loads(value) for name, value in state['control_tables']}


def unchanged(before, after, allowed=()):
    changes = sorted(n for n in before if before[n] != after[n])
    require(before.keys() == after.keys() and set(changes) <= set(allowed), 'unexpected-ledger-effects:' + ','.join(changes))
    return changes


def event(process, wanted, seconds=15):
    deadline = time.monotonic() + seconds
    with process.condition:
        while True:
            require(not process.overflow, 'probe-output-overflow')
            end = process.stdout.find(b'\n', process.offset)
            if end >= 0:
                value = json.loads(bytes(process.stdout[process.offset:end]))
                process.offset = end + 1
                require(value['event'] == wanted, 'unexpected-event:' + value['event'])
                return value
            require(time.monotonic() < deadline and
                (process.process.poll() is None or process.threads[0].is_alive()), 'missing-event:' + wanted)
            process.condition.wait(.02)


def observe(db, ready):
    value = json.loads(lab.sql(pg, db, f"""SELECT json_build_object(
        'pid',a.pid,'backend_start',a.backend_start::text,'epoch',o.epoch,
        'live',o.lease_expires_at_us>(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
        'fenced',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid AND locktype='advisory'
            AND classid=1111651442::oid AND objid=1129599564::oid AND objsubid=2
            AND mode='ExclusiveLock' AND granted))
        FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid
            AND o.control_backend_start=a.backend_start
        WHERE a.pid={ready['pid']} AND a.datname='{db}' AND o.state='active'"""))
    require(value and value['backend_start'] == ready['backend_start'] and
        value['epoch'] == 1 and value['live'] and value['fenced'], 'actual-Control-custody')
    return value


def writer_for(db, ready):
    writer = Session(pg, db, 'source-writer-' + db, sessions)
    writer.execute('BEGIN; SET LOCAL session_replication_role=replica;')
    endpoint = ready['binding']['endpoint']
    require(re.fullmatch('[0-9a-f]{32}', endpoint), 'endpoint-shape')
    predicate = f"endpoint_id=decode('{endpoint}','hex')"
    lock = 'SELECT dataset_id FROM control.datasets FOR UPDATE;'
    if scoped:
        return writer, lock, ADVANCE_POLICY, ''
    withdrawal = f'UPDATE control.relay_enrollments SET enabled=false WHERE {predicate};'
    # Exact current/historical generation3 exists and is enabled; only old source
    # generation1 is stale. Replica injection bypasses command/audit authorization
    # deliberately, preserving production guards on the actual admission backend.
    reenroll = f"""INSERT INTO control.relay_enrollment_changes
        SELECT (jsonb_populate_record(NULL::control.relay_enrollment_changes,
            to_jsonb(h)||jsonb_build_object('request_key',encode(decode(repeat('98',16),'hex'),'hex'),
            'expected_generation',2,'generation',3,'acceptance_transaction',pg_current_xact_id()::text))).*
        FROM control.relay_enrollment_changes h WHERE {predicate} AND generation=1;"""
    # PostgreSQL jsonb bytea input requires its hex marker.
    reenroll = reenroll.replace("encode(decode(repeat('98',16),'hex'),'hex')", "'\\x' || repeat('98',16)")
    reenroll += f"UPDATE control.relay_enrollments SET enabled=true,generation=3,request_key=decode(repeat('98',16),'hex') WHERE {predicate};"
    return writer, lock, withdrawal, reenroll


ADVANCE_POLICY = """
    INSERT INTO control.config_revisions
        SELECT 2,dataset_id,1,2,authority_mode,document,document_digest,accepted_source_digest,
            accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1;
    INSERT INTO control.policy_revisions
        SELECT 2,dataset_id,1,2,2,document,document_digest,accepted_at_us+1,actor_id
        FROM control.policy_revisions WHERE revision=1;
    UPDATE control.datasets SET config_revision=2,policy_revision=2;
"""


def verify_policy(before, after):
    require(len(before['datasets']) == len(after['datasets']) == 1, 'single-policy-dataset')
    require(after['datasets'] == [dict(before['datasets'][0], config_revision=2,
        policy_revision=2, xmin=after['datasets'][0]['xmin'])], 'exact-policy-advance')
    for table in ('config_revisions', 'policy_revisions'):
        require(len(before[table]) == 1 and all(r in after[table] for r in before[table]), 'retained-policy-document')
        added = [r for r in after[table] if r not in before[table]]
        require(len(added) == 1, 'policy-row-count')
        expected = dict(before[table][0], revision=2, previous_revision=1,
            accepted_at_us=before[table][0]['accepted_at_us']+1, xmin=added[0]['xmin'])
        if table == 'policy_revisions': expected['config_revision'] = 2
        require(added == [expected], 'exact-copied-policy')
    require(before['acquisition_scopes'] == after['acquisition_scopes']
        and before['acquisition_selectors'] == after['acquisition_selectors'], 'scope-rebound-after-policy')


def verify_injection(before, final, binding, schedule):
    if scoped:
        policy_tables = ['datasets', 'config_revisions', 'policy_revisions']
        if schedule == 'writer_rollback':
            require(all(before[t] == final[t] for t in policy_tables), 'policy-rollback')
        else:
            verify_policy(before, final)
        unchanged(before, final, policy_tables + ['producer_epochs', 'producer_requests', 'producer_outcomes'])
        return
    endpoint = '\\x' + binding['endpoint']
    selected = lambda table: next(r for r in table if r['endpoint_id'] == endpoint)
    expected = copy.deepcopy(selected(before['relay_enrollments']))
    actual = selected(final['relay_enrollments'])
    if schedule == 'writer_rollback':
        require(final['relay_enrollments'] == before['relay_enrollments'] and
            final['relay_enrollment_changes'] == before['relay_enrollment_changes'], 'writer-rollback-state')
        return
    expected.update(enabled=schedule == 'reservation_first', xmin=actual['xmin'])
    require(actual['xmin'] != selected(before['relay_enrollments'])['xmin'], 'writer-not-committed')
    if schedule == 'reservation_first':
        expected.update(generation=3, request_key='\\x' + '98'*16)
        original_history = selected(before['relay_enrollment_changes'])
        new_history = [r for r in final['relay_enrollment_changes'] if r not in before['relay_enrollment_changes']]
        require(len(new_history) == 1, 'injected-history-count')
        expected_history = dict(original_history, generation=3, expected_generation=2,
            request_key='\\x' + '98'*16, xmin=actual['xmin'],
            acceptance_transaction=new_history[0]['acceptance_transaction'])
        require(new_history == [expected_history] and all(
            r in final['relay_enrollment_changes'] for r in before['relay_enrollment_changes']),
            'enabled-generation3-history')
    else:
        require(final['relay_enrollment_changes'] == before['relay_enrollment_changes'], 'withdrawal-history-injection')
    require(actual == expected and all(r in final['relay_enrollments']
        for r in before['relay_enrollments'] if r['endpoint_id'] != endpoint), 'exact-injected-enrollment')
    unchanged(before, final, ['relay_enrollments', 'relay_enrollment_changes',
        'producer_epochs', 'producer_requests', 'producer_outcomes'])


def verify_native(name, dataset, committed, closing, staged):
    result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/' + name)
    (output / (name + '-native.json')).write_text(result.stdout)
    actual = json.loads(result.stdout)['rows']
    expected, inputs, receipts = complete_source(dataset, committed, closing)
    require([(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in actual] == expected,
        'independent-complete-native-bytes')
    # A reservation can retain original input without having admitted any facts.
    _, all_inputs, _ = complete_source(dataset, True)
    files = sorted((output / name / 'producer-inputs-v1').glob('*.cbor'))
    require(len(files) == (2 if staged else 1), 'original-input-count')
    require(all(p.read_bytes() == all_inputs[int(p.stem[-16:], 16)] for p in files), 'original-input-bytes')
    return receipts


def run_case(name, app_mode, wire_mode='pass', commit=64, schedule=None, baseline=None):
    db = ('bb_scope_recovery_' if scoped else 'bb_source_fault_') + revision[:7] + '_' + name
    require(re.fullmatch('[a-z0-9_]{1,63}', db), 'database-name')
    report['active_case'] = name
    save()
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE bb_source_template')
    report['databases'].append(db)
    require(rows(db) == template_before, 'clone-state')
    if scoped:
        config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
        migrated = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '21')
        (output / (name + '-migration.stdout')).write_text(migrated.stdout)
        (output / (name + '-migration.stderr')).write_text(migrated.stderr)
        require(migrated.returncode == 0, 'scope-migration')
        lab.sql(pg, db, 'GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes TO bigbrotr_control; '
            'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
        '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
        '--listen-port', '6543', '--mode', wire_mode, '--commit-number', str(commit),
        '--later-commits', str(64-commit),
        '--deadline-seconds', '60'], input_pipe=True)
    processes.append((name + '-proxy', proxy))
    proxy_ready = proxy.event('ready', 10)
    require(proxy_ready['proxy_sha256'] == sha((SOURCE / 'experiments/control-migrations/fault_proxy.py').read_bytes()), 'proxy-source')
    process = Process(['docker', 'exec', client, '/build/ingest_bootstrap_probe',
        ('source-fault-scope-' if scoped else 'source-fault-') + app_mode,
        db, '/run/secrets/application', '/lab/' + name], limit=8*1024**2)
    processes.append((name + '-probe', process))
    initialized = event(process, 'initialized')
    ready = event(process, 'source-ready')
    observation = observe(db, ready)
    before = rows(db)
    require(before == state_rows(ready['state']), 'ready-state-mismatch')
    proxy.send('status')
    initial_wire = proxy.event('status', 5)
    count = initial_wire['commits_seen']
    require(count == initial_wire['commits_forwarded'] == initial_wire['server_commit_complete'] == initial_wire['client_commit_complete'], 'setup-COMMITs-incomplete')
    if baseline is not None:
        require(count == baseline, 'changed-setup-COMMIT-ordinal')
    barriers, writer = [], None
    if schedule:
        writer, lock, withdrawal, reenroll = writer_for(db, ready)
    if schedule in ('writer_commit', 'writer_rollback'):
        writer.execute(lock + withdrawal)
    (output / (name + '.go')).touch(exist_ok=False)
    if schedule in ('writer_commit', 'writer_rollback'):
        barriers.append(blocked(pg, db, ready, writer.backend))
        writer.execute('ROLLBACK;' if schedule == 'writer_rollback' else 'COMMIT;')
    elif schedule in ('reservation_first', 'dispatch_first'):
        proxy.event('held', 5)
        held = rows(db)
        wanted_requests = 1 if schedule == 'reservation_first' else 2
        require(len(held['producer_requests']) == wanted_requests and len(held['producer_outcomes']) == 1, 'wrong-held-COMMIT-phase')
        writer.start(lock + (withdrawal + reenroll if schedule == 'reservation_first' else withdrawal))
        barriers.append(blocked(pg, db, writer.backend, ready))
        proxy.send('release')
        writer.expect(writer.finish())
        if schedule == 'reservation_first':
            barriers.append(blocked(pg, db, ready, writer.backend))
        writer.execute('COMMIT;')
    if writer:
        writer.close(lambda: lab.load(original / 'pg'))
        sessions.remove(writer)
    admitted = event(process, 'source-admission-finished')
    recovery = app_mode in ('pass', 'reservation-lost', 'outcome-lost')
    revoked = closed = policy_advanced = None
    resolved = []
    if recovery:
        if scoped:
            released = event(process, 'scope-recovery-ready')
            require(released['driver_joined'], 'old-driver-custody')
            lab.sql(pg, db, 'BEGIN; SELECT dataset_id FROM control.datasets FOR UPDATE;' + ADVANCE_POLICY + 'COMMIT;')
            policy_advanced = rows(db)
            admitted_rows = state_rows(admitted['state'])
            verify_policy(admitted_rows, policy_advanced)
            unchanged(admitted_rows, policy_advanced, ['datasets', 'config_revisions', 'policy_revisions'])
            (output / (name + '.recover')).touch(exist_ok=False)
        revoked = event(process, 'source-revoked')
        closed = event(process, 'source-closed')
        for _ in range(1 if app_mode == 'reservation-lost' else 2):
            resolved.append(event(process, 'source-resolved'))
    done = event(process, 'source-fault-done')
    require(done['driver_joined'] and process.wait(10) == 0 and not process.stderr, 'probe-terminal')
    wire = proxy.event('receipt', 10)
    require(proxy.wait(5) == 0 and not proxy.stderr, 'proxy-terminal')
    final = rows(db)  # Writer joined: never assert an atomic cross-table live snapshot.
    lab.write_json(output / (name + '-final.json'), final)
    dataset = bytes.fromhex(final['datasets'][0]['dataset_id'][2:])
    require(initialized['dataset'] == dataset.hex(), 'initialized-dataset')
    committed = app_mode not in ('source-refused', 'reservation-lost')
    staged = committed or app_mode == 'reservation-lost' or schedule == 'reservation_first'
    require(len(final['producer_requests']) == (2 if staged else 1) and
        len(final['producer_outcomes']) == (2 if committed else 1), 'durable-row-counts')
    receipts = verify_native(name, dataset, committed, recovery, staged)
    first = before['producer_requests'][0]
    require(first in final['producer_requests'] and before['producer_outcomes'][0] in final['producer_outcomes'], 'bootstrap-history-changed')
    binding = ready['binding']
    if scoped:
        require(re.fullmatch('[0-9a-f]{32}', binding['scope']), 'scope-identity')
        require(len(before['acquisition_scopes']) == len(before['acquisition_selectors']) == 1, 'one-retained-scope')
        original_scope = before['acquisition_scopes'][0]
        require(original_scope['scope_id'] == '\\x' + binding['scope'] and original_scope['policy_revision'] == 1
            and original_scope['endpoint_id'] == '\\x' + binding['endpoint']
            and original_scope['access_scope_id'] == '\\x' + binding['access'], 'original-scope-source')
        require(before['acquisition_scopes'] == final['acquisition_scopes']
            and before['acquisition_selectors'] == final['acquisition_selectors'], 'immutable-scope-history')
        document = b'{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}'
        require(before['acquisition_selectors'][0]['document'] == '\\x' + document.hex(), 'literal-scope-document')
    if schedule:
        verify_injection(before, final, binding, schedule)
    if staged:
        request = next(r for r in final['producer_requests'] if r['request_sequence'] == 2)
        require(request['relay_endpoint_id'] == '\\x' + binding['endpoint'] and
            request['relay_access_scope_id'] == '\\x' + binding['access'] and
            request['relay_route_id'] == '\\x' + binding['route'] and
            request['relay_enrollment_generation'] == 1 and request['issuer_epoch'] == 1, 'original-source-reservation')
        require(request in state_rows(admitted['state'])['producer_requests'], 'recovery-rebound-reservation')
        if scoped:
            require(request['acquisition_scope_id'] == '\\x' + binding['scope'], 'original-scope-reservation')
        _, expected_inputs, _ = complete_source(dataset, True)
        require(request['request_digest'] == '\\x' + sha(expected_inputs[2]) and
            request['input_bytes'] == len(expected_inputs[2]) and
            request['manifest_sha256'] == '\\x' + sha(bytes.fromhex(initialized['manifest'])),
            'original-reservation-bytes')
    recorder = None
    if committed:
        outcome = next(r for r in final['producer_outcomes'] if r['request_sequence'] == 2)
        require(outcome['outcome_bytes'] == '\\x' + receipts[2].hex() and
            outcome['outcome_sha256'] == '\\x' + sha(receipts[2]), 'independent-PG-receipt')
        recorder = 2 if wire_mode == 'before-forward' else 1
        require(outcome['recorder_epoch'] == recorder and
            outcome['recorder_incarnation'] == '\\x' + bytes([43 if recorder == 2 else 41]).hex()*16 and
            outcome['request_digest'] == request['request_digest'] and
            outcome['manifest_sha256'] == request['manifest_sha256'] and outcome['policy_epoch'] == 1,
            'original-recorder')
        if recorder == 1:
            require(outcome in state_rows(admitted['state'])['producer_outcomes'], 'original-outcome-rewritten')
        if admitted['receipt'] is not None:
            require(admitted['receipt'] == receipts[2].hex(), 'returned-receipt')
    if recovery:
        admitted_rows = state_rows(admitted['state'])
        unchanged(before, admitted_rows, ['producer_epochs', 'producer_requests', 'producer_outcomes'])
        revoked_rows = state_rows(revoked['state'])
        enrollment = next(r for r in revoked_rows['relay_enrollments'] if r['endpoint_id'] == '\\x' + binding['endpoint'])
        require(not enrollment['enabled'] and enrollment['generation'] == 2 and
            len(revoked_rows['relay_enrollment_changes']) == len(before['relay_enrollment_changes']) + 1,
            'actual-authenticated-withdrawal')
        unchanged(policy_advanced if scoped else admitted_rows, revoked_rows, ['role_instances', 'owner_epochs', 'login_buckets',
            'browser_sessions', 'management_audit', 'management_commands',
            'relay_enrollment_changes', 'relay_enrollments'])
        close_rows = state_rows(closed['state'])
        unchanged(revoked_rows, close_rows, ['producer_epochs'])
        require(len(close_rows['producer_epochs']) == 1 and
            close_rows['producer_epochs'][0] == dict(revoked_rows['producer_epochs'][0],
                state='closing', xmin=close_rows['producer_epochs'][0]['xmin']), 'exact-producer-closing')
        for value in resolved:
            require(value['unresolved'] == (not committed), 'resolution-status')
            require(value['receipt'] == (receipts[2].hex() if committed else None), 'historical-resolution-bytes')
            unchanged(close_rows, state_rows(value['state']), ['producer_outcomes'] if committed and recorder == 2 else [])
            for field in ('native_sha256', 'native_rows', 'sequence', 'originals'):
                require(value['state'][field] == closed['state'][field], 'resolution-rewrote-native')
        if committed:
            require(resolved[0]['state'] == resolved[1]['state'], 'retry-changed-state')
        last_rows = state_rows(resolved[-1]['state'])
        unchanged(last_rows, final, ['owner_epochs', 'role_instances'] if committed else [])
        if committed:
            current_role = '\\x' + '21'*16
            prior_role = next(r for r in last_rows['role_instances'] if r['role_instance_id'] == current_role)
            retired = next(r for r in final['role_instances'] if r['role_instance_id'] == current_role)
            require(prior_role['retired_at_us'] is None and isinstance(retired['retired_at_us'], int) and
                retired == dict(prior_role, xmin=retired['xmin'], retired_at_us=retired['retired_at_us']) and
                all(r in final['role_instances'] for r in last_rows['role_instances'] if r != prior_role),
                'exact-current-role-retirement')
    else:
        allowed = ['producer_epochs', 'producer_requests', 'producer_outcomes'] if staged else []
        if scoped:
            if schedule != 'writer_rollback': allowed += ['datasets', 'config_revisions', 'policy_revisions']
        else:
            if schedule != 'writer_rollback': allowed += ['relay_enrollments']
            if schedule == 'reservation_first': allowed += ['relay_enrollment_changes']
        unchanged(before, final, allowed)
    expected_count = count + (1 if app_mode == 'reservation-lost' or schedule == 'reservation_first' else 2 if committed else 0)
    counters = [wire[k] for k in ('commits_seen', 'commits_forwarded', 'server_commit_complete', 'client_commit_complete', 'faults')]
    wanted = [expected_count] * 4 + [int(wire_mode != 'pass')]
    if wire_mode == 'before-forward':
        wanted[1:4] = [expected_count-1]*3
    elif wire_mode == 'after-complete':
        wanted[3] -= 1
    require(counters == wanted, 'exact-wire-COMMIT-boundary:' + str(counters) + ':' + str(wanted))
    report['cases'].append(dict(name=name, status='PASS', db=db, baseline_commits=count,
        observation=observation, initial_wire=initial_wire, wire=wire, barriers=barriers,
        admission=admitted, policy_advanced=policy_advanced, revoked=revoked, closed=closed, resolved=resolved,
        before=before, final=final, complete_native_bytes=True))
    save()
    return count


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect', client, check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    free_bytes_before=shutil.disk_usage(root).free, postgres_id=target['Id'])
assert report['free_bytes_before'] > 2 * 1024**3
lab.write_json(output / 'preflight.json', dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps', '-eo', 'pid,ppid,stat,comm']).stdout,
    ports=lab.command(['ss', '-lntup']).stdout, volumes=lab.docker('volume', 'ls').stdout,
    directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    lab.docker('start', target['Id'])
    deadline = time.monotonic() + 30
    while lab.docker('exec', target['Id'], 'pg_isready', '-U', 'postgres', check=False).returncode:
        require(time.monotonic() < deadline, 'postgres-readiness')
        time.sleep(.1)
    template_before = rows('bb_source_template')
    original_before = rows('bb_source_pass')
    # Normalize only the fixture's earlier name for the same physical xmin.
    expected_original = json.loads((original / 'pg/source-pass-after.json').read_text())
    for values in expected_original.values():
        for value in values:
            value['xmin'] = value.pop('lab_xmin')
    canonical = lambda tables: {n: sorted(values, key=lambda r: json.dumps(r, sort_keys=True)) for n, values in tables.items()}
    require(canonical(original_before) == canonical(expected_original), 'original-state')
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={original}/pg/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly',
        '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    baseline = run_case('pass', 'pass')
    run_case('writer_commit', 'source-refused', schedule='writer_commit', baseline=baseline)
    run_case('writer_rollback', 'admission', schedule='writer_rollback', baseline=baseline)
    run_case('reservation_first', 'source-refused', 'hold', baseline+1, 'reservation_first', baseline)
    run_case('dispatch_first', 'admission', 'hold', baseline+2, 'dispatch_first', baseline)
    run_case('reservation_lost', 'reservation-lost', 'after-complete', baseline+1, baseline=baseline)
    run_case('outcome_before', 'outcome-lost', 'before-forward', baseline+2, baseline=baseline)
    run_case('outcome_after', 'outcome-lost', 'after-complete', baseline+2, baseline=baseline)
    require(rows('bb_source_pass') == original_before and rows('bb_source_template') == template_before and
        (original / 'pg/source-result.json').read_bytes() == original_report, 'original-input-changed')
    require(lab.sql(pg, 'postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename='bigbrotr_control'") == '0', 'remaining-Control-backends')
    report.update(status='PASS_SCOPED_SOURCE_FAULTS', active_case=None, original_inputs_unchanged=True)
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    errors = []
    for session in sessions:
        try:
            session.close(lambda: lab.load(original / 'pg'))
        except BaseException as error:
            errors.append('session-' + type(error).__name__)
    if report['client']:
        try:
            item = json.loads(lab.docker('inspect', report['client']['id']).stdout)[0]
            require(item['Config']['Labels'][label] == owner, 'client-owner')
            if item['State']['Running']:
                lab.docker('stop', '--time', '3', item['Id'])
        except BaseException as error:
            errors.append('client-' + type(error).__name__)
    for name, process in processes:
        try:
            process.stop()
        except BaseException as error:
            errors.append('process-' + type(error).__name__)
        for suffix, transcript in [('stdout', process.stdout), ('stderr', process.stderr)]:
            try:
                (output / (name + '.' + suffix)).write_bytes(transcript)
            except BaseException as error:
                errors.append('transcript-' + type(error).__name__)
    try:
        item = json.loads(lab.docker('inspect', target['Id']).stdout)[0]
        require(item['Config']['Labels'][lab.LABEL] == pg['owner'], 'postgres-owner')
        if item['State']['Running']:
            lab.docker('stop', '--time', '10', target['Id'])
        logs = lab.docker('logs', '--since', report['started_at'], target['Id'])
        (output / 'postgres.stdout').write_text(logs.stdout)
        (output / 'postgres.stderr').write_text(logs.stderr)
    except BaseException as error:
        errors.append('postgres-' + type(error).__name__)
    try:
        current = {c['Id']: projection(c) for c in containers()}
    except BaseException as error:
        current = {}
        errors.append('inventory-' + type(error).__name__)
    owned = [target['Id']] + ([report['client']['id']] if report['client'] else [])
    report.update(cleanup_errors=errors, missing_owned=[i for i in owned if i not in current],
        terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id'] != target['Id'] and current.get(c['Id']) != projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(
        c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    save()
require(report['status'] == 'PASS_SCOPED_SOURCE_FAULTS', 'incomplete-source-fault-qualification')
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))
