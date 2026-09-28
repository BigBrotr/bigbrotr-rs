#!/usr/bin/env python3
"""Finite actual-source checks, borrowing only the retained source-template PG.

No new network, compiler cache or dataset retirement. Every clone is retained.
This is not WebSocket acquisition, native admission from a socket or M1.
"""
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
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection
spec = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
m = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = m
spec.loader.exec_module(m)
from oracle import complete_source

root = Path(sys.argv[1]).resolve(strict=True)
assert SOURCE == root / 'source'
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['source'] == revision
sha = lambda value: hashlib.sha256(value).hexdigest()
for name, key in [('bigbrotr', 'binary_sha256'), ('ingest_bootstrap_probe', 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
original = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(original)
assert pg['state'] == 'stopped-retained' and pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
original_report = (original / 'source-result.json').read_bytes()
assert json.loads(original_report)['status'] == 'PASS_SCOPED_RELAY_SOURCE_FIRST_WAVE'
output = root / 'transport'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.source-transport'
client = 'bigbrotr-source-transport-' + revision[:7]
report = dict(format='bigbrotr-source-transport-native/1', source=revision, build=built,
    owner=owner, label=label, status='RUNNING', client=None, postgres=pg['postgres'],
    postgres_owner=pg['owner'], cases=[], databases=[],
    preservation_scope=['bb_source_template', 'bb_source_pass', 'source-result.json'],
    evidence_limits=['normal release checks changed tables; its previously qualified field semantics are not requalified here'],
    not_run=['delayed cleanup reply', 'concurrent changes and lock orders',
             'installed Sync', 'WebSocket/signed admission/M1', 'load/aged state', 'arm64/release'])


def save():
    lab.write_json(output / 'result.json', report)


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def rows(database):
    names = json.loads(lab.sql(pg, database,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names) == 64 and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
        f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, database,
        'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def run_case(name, mode, expected):
    database = 'bb_transport_' + revision[:7] + '_' + name
    require(re.fullmatch('[a-z0-9_]{1,63}', database), 'database-name')
    report['active_case'] = name
    save()
    lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE bb_source_template')
    report['databases'].append(database)
    require(rows(database) == template_before, 'clone-state')
    result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', mode, database,
        '/run/secrets/application', '/lab/' + name, check=False, timeout=60)
    (output / (name + '.stdout')).write_text(result.stdout)
    (output / (name + '.stderr')).write_text(result.stderr)
    require(result.returncode == 0 and not result.stderr, 'probe-failed:' + name)
    events = [json.loads(line) for line in result.stdout.splitlines()]
    kinds = [e['event'] for e in events]
    wanted = ['initialized', 'source-transport']
    if name == 'owner_loss':
        wanted.append('source-backend-loss')
    wanted.append('source-transport')
    require(kinds == wanted, 'event-order')
    phases = [e for e in events if e['event'] == 'source-transport']
    require([(e['ordinal'], e['result']) for e in phases] == [(1, 'refused-public'), (2, expected)], 'source-results')
    for phase in phases:
        require(phase['state']['native_rows'] == 16 and phase['state']['sequence'] == 17, 'no-native-admission')
        tables = {name: json.loads(value) for name, value in phase['state']['control_tables']}
        require(len(tables) == 64, 'complete-Control-observation')
        require(len(tables['producer_requests']) == len(tables['producer_outcomes']) == 1, 'no-new-producer-request')
    final = rows(database)
    latest = {name: json.loads(value) for name, value in phases[-1]['state']['control_tables']}
    # A normal release alone changes the owner and role rows after observation.
    changed = [name for name in final if final[name] != latest[name]]
    require(changed == [] if name == 'owner_loss' else set(changed) == {'owner_epochs', 'role_instances'}, 'terminal-effects')
    require(len(final['relay_endpoints']) == 1 and
        final['relay_endpoints'][0]['url_bytes'] == '\\x' + b'ws://127.0.0.1:7447/private'.hex(), 'exact-endpoint')
    require(len(final['relay_enrollments']) == 2 and
        all(r['enabled'] and r['generation'] == 1 for r in final['relay_enrollments']), 'two-exact-enrollments')
    require(len(final['network_routes']) == 1 and final['network_routes'][0]['route_bytes'] == '\\x0104207f000001', 'exact-private-route')
    access = final['relay_access_contexts']
    require(len(access) == 2 and {r['ordinal'] for r in access} == {1, 2} and
        all(r['access_class'] == 'public' and r['dictionary_kind'] == 4 and
            r['allocation_domain'] == '\\x' + '40'*16 for r in access), 'exact-visibility-refs')
    dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/' + name)
    (output / (name + '-native.json')).write_text(dumped.stdout)
    actual = json.loads(dumped.stdout)['rows']
    dataset = bytes.fromhex(final['datasets'][0]['dataset_id'][2:])
    expected_rows, inputs, _ = complete_source(dataset, False)
    require([(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in actual] == expected_rows,
        'independent-complete-native-bytes')
    originals = sorted((output / name / 'producer-inputs-v1').glob('*.cbor'))
    require(len(originals) == 1 and originals[0].read_bytes() == inputs[1], 'exact-original-input')
    require(lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'remaining-Control-backends')
    lab.write_json(output / (name + '-final.json'), final)
    report['cases'].append(dict(name=name, status='PASS', database=database,
        transcript=kinds, result=expected, full_native_bytes=True, complete_control_tables=64))
    save()


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
    template_before, original_before = rows('bb_source_template'), rows('bb_source_pass')
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={original}/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(300)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    run_case('pass', 'source-transport', 'received-private')
    run_case('policy', 'source-transport-policy-refused', 'refused-second-policy')
    run_case('owner_loss', 'source-transport-owner-loss', 'closed-after-backend-loss')
    require(rows('bb_source_template') == template_before and rows('bb_source_pass') == original_before and
        (original / 'source-result.json').read_bytes() == original_report, 'original-input-changed')
    report.update(status='PASS_SCOPED_SOURCE_TRANSPORT', active_case=None, original_inputs_unchanged=True)
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    errors = []
    owned = [target['Id']] + ([report['client']['id']] if report['client'] else [])
    for ident in reversed(owned):
        try:
            item = json.loads(lab.docker('inspect', ident).stdout)[0]
            expected_label, expected_owner = (lab.LABEL, pg['owner']) if ident == target['Id'] else (label, owner)
            require(item['Config']['Labels'][expected_label] == expected_owner, 'cleanup-owner')
            if item['State']['Running']:
                lab.docker('stop', '--time', '10', ident)
        except BaseException as error:
            errors.append(type(error).__name__)
    logs = lab.docker('logs', '--since', report['started_at'], target['Id'], check=False)
    (output / 'postgres.stdout').write_text(logs.stdout)
    (output / 'postgres.stderr').write_text(logs.stderr)
    current = {c['Id']: projection(c) for c in containers()}
    report.update(cleanup_errors=errors, missing_owned=[i for i in owned if i not in current],
        terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id'] != target['Id'] and current.get(c['Id']) != projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(
        c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    save()
require(report['status'] == 'PASS_SCOPED_SOURCE_TRANSPORT', 'incomplete-source-transport')
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))
