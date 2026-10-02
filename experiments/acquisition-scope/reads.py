#!/usr/bin/env python3
"""Bounded scope reads on real PG; retained originals and new fault clones only."""
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
from concurrency_cases import Session
from fault_cases import Process
sys.path.insert(0, str(SOURCE / 'experiments/relay-source'))
from locks import blocked
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
source_reads = len(sys.argv) == 3 and sys.argv[2] == '--source'
candidate_reads = len(sys.argv) == 3 and sys.argv[2] == '--candidates'
assert (len(sys.argv) == 2 or source_reads or candidate_reads) and SOURCE == root / 'source'
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['source'] == revision and built['probe_name'] == 'acquisition_scope_probe'
sha = lambda value: hashlib.sha256(value).hexdigest()
assert sha((build / built['probe_name']).read_bytes()) == built['probe_sha256']
pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(pg_root)
assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
basis = root.parent / 'acquisition-scope-54d1a18/engine'
basis_report = (basis / 'result.json').read_bytes()
assert json.loads(basis_report)['status'] == 'PASS_SCOPED_ACQUISITION_REGISTRATION'
output = root / 'reads'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-scope-read'
client = 'bigbrotr-acquisition-scope-read-' + revision[:7]
report = dict(format='bigbrotr-acquisition-scope-read/1', source=revision, build=built,
    status='RUNNING', owner=owner, label=label, client=None, cases=[], databases=[],
    postgres=pg['postgres'], postgres_owner=pg['owner'],
    not_run=['scope-bound admission/CertifyExisting', 'installed Sync/progress/M1',
             'measured memory capacity/load/aged-state', 'arm64/release'])
processes, sessions = [], []


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def save():
    lab.write_json(output / 'result.json', report)


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def rows(db):
    names = json.loads(lab.sql(pg, db,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names) in (64, 66, 67) and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
             f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, db, 'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def event(process, wanted):
    deadline = time.monotonic() + 10
    with process.condition:
        while True:
            require(not process.overflow, 'probe-output-overflow')
            end = process.stdout.find(b'\n', process.offset)
            if end >= 0:
                value = json.loads(bytes(process.stdout[process.offset:end]))
                process.offset = end + 1
                require(value.get('event') == wanted, 'probe-event-' + wanted)
                return value
            require(time.monotonic() < deadline and
                (process.process.poll() is None or process.threads[0].is_alive()), 'missing-probe-event')
            process.condition.wait(.02)


def mutate(db, sql):
    # Deliberate clone-only fault injection; no claim of product registration or
    # management activation. Check constraints require explicit changes below.
    lab.sql(pg, db, 'BEGIN; SET LOCAL session_replication_role=replica; ' + sql + ' COMMIT;')


DOCUMENT = b'{"version":1,"filters":[{"kinds":[1,2]}],"history_since":0,"ephemeral":"retain"}'


def verify_source_owner(before, after, backend, start_us, end_us, released):
    """Only the probe's one claim and optional release may change Control state."""
    owner = next(r for r in after['owner_epochs'] if r not in before['owner_epochs'])
    role = next(r for r in after['role_instances'] if r not in before['role_instances'])
    prior = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    require(prior['state'] == 'released', 'fixture-owner-not-released')
    nonce = '\\x' + 'f1' * 16
    # Backend identity comes from a query on the actual connection before claim;
    # server clock bounds constrain timestamps without deriving them from the reader.
    require(type(backend['pid']) is int and backend['pid'] > 0, 'backend-identity')
    parse_time = datetime.datetime.fromisoformat
    require(parse_time(owner['control_backend_start']) == parse_time(backend['start']), 'backend-start')
    backend_us = int(parse_time(backend['start']).timestamp() * 1000000)
    require(start_us <= backend_us <= role['created_at_us'] <= owner['acquired_at_us'] <= end_us,
        'claim-clock-bounds')
    require(role['xmin'] == owner['xmin'] and role['xmin'].isdigit(), 'owner-transaction')
    expected_owner = dict(resource_id=prior['resource_id'], epoch=prior['epoch'] + 1,
        dataset_id=prior['dataset_id'], role='control', role_instance_id=nonce,
        process_incarnation=nonce, acquired_at_us=owner['acquired_at_us'],
        lease_expires_at_us=owner['acquired_at_us'] + 60_000_000,
        state='released' if released else 'active', released_at_us=None,
        exclusion_method=None, exclusion_digest=None, control_fence_version=1,
        control_backend_pid=backend['pid'], control_backend_start=owner['control_backend_start'],
        archive_frontier_protocol=None, xmin=role['xmin'])
    expected_role = dict(role_instance_id=nonce, dataset_id=prior['dataset_id'], role='control',
        created_at_us=role['created_at_us'], retired_at_us=None, xmin=role['xmin'])
    if released:
        require(owner['acquired_at_us'] <= owner['released_at_us'] <= role['retired_at_us'] <= end_us,
            'release-clock-bounds')
        digest = sha(b'bigbrotr/control-exclusion/1\0' + bytes.fromhex(prior['dataset_id'][2:])
            + bytes.fromhex(prior['resource_id'][2:]) + (prior['epoch'] + 1).to_bytes(8, 'big')
            + bytes.fromhex('f1' * 32))
        expected_owner.update(released_at_us=owner['released_at_us'], exclusion_method='storage_fenced',
            exclusion_digest='\\x' + digest)
        expected_role['retired_at_us'] = role['retired_at_us']
    require(owner == expected_owner and role == expected_role, 'complete-owner-metadata')


def seed_candidate(db, ordinal, *, kind='acquisition', state='accepted', revision=1,
                   protocol=1, owner_epoch=None, incarnation=None, resource=None, checkpoint=0):
    """Clone-only scheduling metadata, not a qualified acceptance/claim journey."""
    identity = f'{ordinal:032x}'
    decode = lambda value: "decode('%s','hex')" % value if value else 'NULL'
    mutate(db, f"""
        INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,
            kind,config_revision,policy_revision,created_at_us,state,job_revision,checkpoint_revision,
            owner_resource_id,owner_epoch,owner_incarnation,input_complete,acquisition_job_protocol)
        SELECT decode('{identity}','hex'),dataset_id,decode(repeat('10',16),'hex'),
            decode('{identity}','hex'),decode(repeat('00',32),'hex'),'{kind}',1,1,1,'{state}',
            {revision},{checkpoint},{decode(resource)},{owner_epoch or 'NULL'},
            {decode(incarnation)},true,{protocol if protocol is not None else 'NULL'}
        FROM control.datasets;
        INSERT INTO control.job_admissions VALUES(decode('{identity}','hex'),{ordinal});
    """)


def prepare_candidates(db, name, expected):
    config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
    migrated = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
        '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '23')
    (output / (name+'-migration.stdout')).write_text(migrated.stdout)
    (output / (name+'-migration.stderr')).write_text(migrated.stderr)
    require(migrated.returncode == 0, 'explicit-scan-migration')
    require(not rows(db)['jobs'], 'empty-job-fixture')
    expected.update(cancel=name=='candidates_cancel', append=name=='candidates_sparse')
    step = lambda cut, after, job=None, restart=False: dict(
        restart=restart, result=dict(cut=cut, after=after, job=f'{job:032x}' if job else None))
    if name == 'candidates_empty':
        expected['steps'] = [step(0,0)]
    elif name == 'candidates_max':
        maximum = 18446744073709551615
        seed_candidate(db, maximum)
        expected['steps'] = [step(maximum,maximum,maximum)]
    elif name == 'candidates_cancel':
        seed_candidate(db, 1)
    else:
        require(name=='candidates_sparse', 'candidate-case')
        previous = max((r for r in basic['owner_epochs'] if r['role']=='control'), key=lambda r:r['epoch'])
        for ordinal in range(1,67):
            options = dict(kind='export',protocol=None)
            if ordinal in (33,34): options = {}
            if ordinal in (35,36,37,38,39):
                options = dict(state='queued',revision=2,resource=previous['resource_id'][2:],
                    owner_epoch=previous['epoch']+1,incarnation='f1'*16)
                if ordinal==36: options['owner_epoch'] -= 1
                if ordinal==37: options['incarnation'] = 'f2'*16
                if ordinal==38: options['resource'] = 'f3'*16
                if ordinal==39: options.update(state='running',revision=3)
            if ordinal==40: options = dict(protocol=None)
            if ordinal==41: options = dict(checkpoint=1)
            if ordinal==42: options = dict(revision=2)
            seed_candidate(db, ordinal, **options)
        expected['steps'] = [step(66,32),step(66,33,33),step(66,34,34),step(66,35,35),step(66,66),
            step(67,32,restart=True),step(67,33,33),step(67,34,34),step(67,35,35),step(67,67,67)]


def run_case(name):
    db = 'bb_scope_read_' + revision[:7] + '_' + name
    require(re.fullmatch('[a-z0-9_]{1,63}', db), 'database-name')
    template = 'bb_transport_8c4d12c_pass' if name == 'old_schema' else 'bb_scope_54d1a18_basic'
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE {template}')
    report['databases'].append(db)
    save()
    before = rows(db)
    require(before == (old_basis if name == 'old_schema' else basic), 'exact-retained-clone')
    require(not any(row['role_instance_id'] == '\\x' + 'f1' * 16 for row in before['role_instances']), 'read-incarnation-already-present')
    original = next(s for s in basic['acquisition_selectors'] if s['document'] == '\\x' + DOCUMENT.hex())
    selected = next(s for s in basic['acquisition_scopes'] if s['selector_id'] == original['selector_id'] and s['visibility_ordinal'] == 1)
    scope_id, selector_id = selected['scope_id'][2:], original['selector_id'][2:]
    mode, document = 'read-valid', DOCUMENT
    expected = dict(scope_id=scope_id, current_policy=1, scope=dict(
        dataset=selected['dataset_id'][2:], endpoint=selected['endpoint_id'][2:],
        selector=selector_id, access=selected['access_scope_id'][2:],
        visibility_domain='40' * 16, visibility_ordinal=1, policy=1))
    if name.startswith('candidates_'):
        mode = 'read-candidates-' + name.removeprefix('candidates_')
        prepare_candidates(db, name, expected)
    elif name.startswith('source_'):
        mode = 'read-source-' + name.removeprefix('source_').replace('_','-')
        enrollment = next(e for e in basic['relay_enrollments'] if e['endpoint_id']==selected['endpoint_id']
            and e['access_scope_id']==selected['access_scope_id'])
        endpoint = next(e for e in basic['relay_endpoints'] if e['endpoint_id']==enrollment['endpoint_id'])
        route = next(r for r in basic['network_routes'] if r['route_id']==enrollment['route_id'])
        expected['source'] = dict(endpoint_id=endpoint['endpoint_id'][2:],
            access_scope_id=enrollment['access_scope_id'][2:], enrollment_generation=enrollment['generation'],
            route_id=route['route_id'][2:], url=bytes.fromhex(endpoint['url_bytes'][2:]).decode(),
            route_bytes=list(bytes.fromhex(route['route_bytes'][2:])))
        bound = "endpoint_id=decode('%s','hex') AND access_scope_id=decode('%s','hex')" % (
            enrollment['endpoint_id'][2:], enrollment['access_scope_id'][2:])
        if name == 'source_disabled':
            mutate(db, f'UPDATE control.relay_enrollments SET enabled=false WHERE {bound};')
            expected['source'] = None
        elif name == 'source_changed':
            route_id, route_bytes = 'ee' * 16, '0104087f000000'
            require(not any(r['route_id'] == '\\x' + route_id or r['route_bytes'] == '\\x' + route_bytes
                for r in basic['network_routes']), 'new-route-collision')
            mutate(db, f"""
                INSERT INTO control.network_routes
                    SELECT decode('{route_id}','hex'),dataset_id,endpoint_id,route_version,
                        decode('{route_bytes}','hex'),actor_id,request_key
                    FROM control.network_routes WHERE route_id=decode('{route['route_id'][2:]}','hex');
                UPDATE control.relay_enrollments SET generation=generation+1,
                    route_id=decode('{route_id}','hex') WHERE {bound};
            """)
            expected['source']['enrollment_generation'] += 1
            expected['source'].update(route_id=route_id, route_bytes=list(bytes.fromhex(route_bytes)))
        elif name == 'source_missing':
            mutate(db, f"UPDATE control.acquisition_scopes SET access_scope_id=decode(repeat('ff',16),'hex') WHERE scope_id=decode('{scope_id}','hex');")
            expected['source'] = None
        elif name in ('source_long','source_bad_url'):
            url = 'wss://source.example/'
            url += 'x' * (8192-len(url)) if name=='source_long' else '%61'
            mutate(db, f"UPDATE control.relay_endpoints SET url_bytes=decode('{url.encode().hex()}','hex') WHERE endpoint_id=decode('{endpoint['endpoint_id'][2:]}','hex');")
            expected['source']['url'] = url
            if name=='source_bad_url': expected['source_error']='InvalidMetadata'
        elif name == 'source_bad_route':
            lab.sql(pg, db, 'ALTER TABLE control.network_routes DROP CONSTRAINT network_routes_route_bytes_check;')
            mutate(db, f"UPDATE control.network_routes SET route_bytes=decode('0000','hex') WHERE route_id=decode('{route['route_id'][2:]}','hex');")
            expected['source_error']='InvalidMetadata'
        else:
            require(name in ('source_valid','source_cancel'), 'source-case')
    elif name == 'old_schema':
        mode, expected['error'] = 'read-old-schema', 'Prerequisite'
    elif name == 'missing':
        mode, expected['scope_id'] = 'read-missing', 'ff' * 16
        require(not any(s['scope_id'] == '\\x' + expected['scope_id'] for s in basic['acquisition_scopes']), 'missing-id-is-present')
    elif name == 'foreign':
        mode = 'read-missing'
        mutate(db, f"UPDATE control.acquisition_scopes SET dataset_id=decode(repeat('ff',16),'hex') WHERE scope_id=decode('{scope_id}','hex');")
    elif name == 'historical':
        mode, expected['current_policy'] = 'read-historical', 2
        mutate(db, """
            INSERT INTO control.config_revisions
                SELECT 2,dataset_id,1,2,authority_mode,document,document_digest,accepted_source_digest,
                    accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1;
            INSERT INTO control.policy_revisions
                SELECT 2,dataset_id,1,2,2,document,document_digest,accepted_at_us+1,actor_id
                FROM control.policy_revisions WHERE revision=1;
            UPDATE control.datasets SET config_revision=2,policy_revision=2;
        """)
    elif name in ('noncanonical', 'malformed', 'large', 'oversize', 'working'):
        mode = 'read-invalid'
        expected['error'] = 'InvalidMetadata'
        if name == 'noncanonical': document = DOCUMENT + b' '
        if name == 'malformed': document = b'{'
        if name in ('large', 'oversize'):
            prefix = b'{"version":1,"filters":[{"#t":["'
            suffix = b'"]}],"history_since":0,"ephemeral":"retain"}'
            count, odd = divmod(2097152 - len(prefix) - len(suffix), 2)
            document = prefix + b'\\n' * count + b'a' * odd + suffix
            require(len(document) == 2097152 and len(json.loads(document)['filters'][0]['#t'][0]) <= 1048576, 'literal-boundary')
            if name == 'large': mode = 'read-large'
            else:
                document += b' '
                lab.sql(pg, db, 'ALTER TABLE control.acquisition_selectors DROP CONSTRAINT acquisition_selectors_document_check;')
        if name == 'working':
            filters = [{'#t': [f'{group:02}-{item:04}' + '\x01' * 16 for item in range(1024)]} for group in range(16)]
            document = json.dumps(dict(version=1, filters=filters, history_since=0, ephemeral='retain'), separators=(',', ':')).encode()
            require(len(document) < 2097152, 'working-document-size')
            expected['error'] = 'Capacity'
        mutate(db, f"UPDATE control.acquisition_selectors SET document=decode('{document.hex()}','hex') WHERE selector_id=decode('{selector_id}','hex');")
    elif name == 'profile':
        mode, expected['error'] = 'read-invalid', 'InvalidMetadata'
        lab.sql(pg, db, 'ALTER TABLE control.acquisition_selectors DROP CONSTRAINT acquisition_selectors_profile_version_check;')
        mutate(db, f"UPDATE control.acquisition_selectors SET profile_version=2 WHERE selector_id=decode('{selector_id}','hex');")
    elif name == 'missing_selector':
        mode, expected['error'] = 'read-invalid', 'InvalidMetadata'
        mutate(db, f"DELETE FROM control.acquisition_selectors WHERE selector_id=decode('{selector_id}','hex');")
    elif name == 'zero_epoch':
        mode, expected['error'] = 'read-invalid', 'InvalidMetadata'
        lab.sql(pg, db, 'ALTER TABLE control.acquisition_scopes DROP CONSTRAINT acquisition_scopes_policy_revision_check;')
        mutate(db, f"UPDATE control.acquisition_scopes SET policy_revision=0 WHERE scope_id=decode('{scope_id}','hex');")
    elif name == 'cancel':
        mode = 'read-cancel'
    else:
        require(name == 'valid', 'unknown-case')
    expected.update(document_bytes=len(document), document_sha256=sha(document))
    lab.write_json(output / (name + '-expected.json'), expected)
    before = rows(db)
    lab.write_json(output / (name + '-before.json'), before)
    holder = None
    if name in ('cancel','source_cancel','candidates_cancel'):
        holder = Session(pg, db, 'scope_read_holder', sessions)
        relation = {'source_cancel':'network_routes','candidates_cancel':'jobs'}.get(name,'acquisition_scopes')
        holder.execute(f'BEGIN; LOCK TABLE control.{relation} IN ACCESS EXCLUSIVE MODE;')
    clock_sql = 'SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint'
    start_us = int(lab.sql(pg, db, clock_sql))
    process = Process(['docker', 'exec', client, '/build/acquisition_scope_probe', db,
        '/run/secrets/application', '/lab/policy.json', mode, '/lab/' + name + '-expected.json'])
    processes.append((name, process))
    barrier = None
    if name == 'candidates_sparse':
        require(event(process, 'scan-cut')['cut']==66, 'frozen-initial-cut')
        seed_candidate(db, 67)
        inputs = rows(db)
        # External fixture insertion is the only new job/admission. Preserve the
        # original before capture and separately record this read baseline.
        before['jobs'] = inputs['jobs']
        before['job_admissions'] = inputs['job_admissions']
        lab.write_json(output / (name+'-read-baseline.json'), before)
        (output / (name+'-expected.json.continue')).touch()
    if holder:
        event(process, 'read-start')
        waiter = json.loads(lab.sql(pg, db, f"SELECT json_build_object('pid',pid,'backend_start',backend_start::text) FROM pg_stat_activity WHERE datname='{db}' AND usename='bigbrotr_control'"))
        barrier = blocked(pg, db, waiter, holder.backend)
        require(any(lock['relation'] == 'control.' + relation and lock['mode'] == 'AccessShareLock'
            for lock in barrier['waiting_locks']), 'scope-query-lock-not-observed')
        event(process, 'read-cancelled')
        holder.execute('ROLLBACK;')
    require(process.wait(15) == 0 and not process.stderr, 'actual-reader-' + name)
    end_us = int(lab.sql(pg, db, clock_sql))
    lines = [json.loads(line) for line in bytes(process.stdout).splitlines()]
    result = dict(lines[-1])
    backend = None
    if name.startswith(('source_','candidates_')):
        backend = dict(pid=result.pop('backend_pid'), start=result.pop('backend_start'))
    require(result == dict(status='PASS', mode=mode, scope_id=expected['scope_id']), 'reader-result')
    after = rows(db)
    lab.write_json(output / (name + '-after.json'), after)
    require(set(after) == set(before) and all(after[t] == before[t] for t in before
        if t not in ('owner_epochs', 'role_instances')), 'read-mutated-data')
    for table in ('owner_epochs', 'role_instances'):
        require(all(row in after[table] for row in before[table]), 'read-mutated-owner-history')
        added = [row for row in after[table] if row not in before[table]]
        require(len(added) == 1, 'unexpected-owner-state-count')
        row = added[0]
        if table == 'role_instances':
            require(row['role_instance_id'] == '\\x' + 'f1' * 16, 'unexpected-role-identity')
        else:
            prior_epoch = max(row['epoch'] for row in before[table] if row['role'] == 'control')
            require(row['role'] == 'control' and row['process_incarnation'] == '\\x' + 'f1' * 16
                and row['epoch'] == prior_epoch + 1, 'unexpected-control-epoch')
    if backend:
        verify_source_owner(before, after, backend, start_us, end_us,
            released=name not in ('source_bad_url', 'source_bad_route', 'source_cancel', 'candidates_cancel'))
        lab.write_json(output / (name + '-owner-observation.json'),
            dict(backend=backend, start_us=start_us, end_us=end_us))
    require(lab.sql(pg, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'unjoined-reader-driver')
    report['cases'].append(dict(name=name, status='PASS', expected=expected,
        prior_data_unchanged=True, lock_barrier=barrier))
    save()


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect', client, check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    postgres_id=target['Id'], free_bytes_before=shutil.disk_usage(root).free)
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
    basic, old_basis = rows('bb_scope_54d1a18_basic'), rows('bb_transport_8c4d12c_pass')
    require(basic == json.loads((basis / 'basic-after.json').read_text()), 'original-scope-basis')
    require(old_basis == json.loads((root.parent / 'source-transport-8c4d12c/transport/pass-final.json').read_text()), 'original-transport-basis')
    policy = basic['config_revisions'][0]['document']
    require(policy == basic['policy_revisions'][0]['document'], 'complete-both-documents')
    lab.write_json(output / 'policy.json', policy)
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    cases = (['candidates_sparse','candidates_empty','candidates_max','candidates_cancel'] if candidate_reads else
             ['source_valid','source_disabled','source_changed','source_missing','source_long',
              'source_bad_url','source_bad_route','source_cancel'] if source_reads else
             ['valid', 'missing', 'foreign', 'historical', 'noncanonical', 'malformed',
              'large', 'oversize', 'working', 'profile', 'missing_selector', 'zero_epoch', 'cancel', 'old_schema'])
    for name in cases:
        run_case(name)
    require(rows('bb_scope_54d1a18_basic') == basic and rows('bb_transport_8c4d12c_pass') == old_basis
        and (basis / 'result.json').read_bytes() == basis_report, 'retained-original-changed')
    report['status'] = 'PASS_SCOPED_ACQUISITION_READ'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    errors = []
    for session in sessions:
        try:
            session.close(lambda: require(json.loads(lab.docker('inspect', target['Id']).stdout)[0]['Config']['Labels'][lab.LABEL] == pg['owner'], 'cleanup-pg-owner'))
        except BaseException as error:
            errors.append('session-' + type(error).__name__)
    for name, process in processes:
        try:
            process.stop()
            (output / (name + '.stdout')).write_bytes(process.stdout)
            (output / (name + '.stderr')).write_bytes(process.stderr)
        except BaseException as error:
            errors.append('process-' + type(error).__name__)
    owned = [target['Id']] + ([report['client']['id']] if report['client'] else [])
    for ident in reversed(owned):
        try:
            item = json.loads(lab.docker('inspect', ident).stdout)[0]
            key, wanted = (lab.LABEL, pg['owner']) if ident == target['Id'] else (label, owner)
            require(item['Config']['Labels'][key] == wanted, 'cleanup-owner')
            if item['State']['Running']:
                lab.docker('stop', '--time', '10', ident)
        except BaseException as error:
            errors.append('stop-' + type(error).__name__)
    try:
        current = {c['Id']: projection(c) for c in containers()}
    except BaseException as error:
        current = {}
        errors.append('inventory-' + type(error).__name__)
    report.update(cleanup_errors=errors, missing_owned=[i for i in owned if i not in current],
        terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id'] != target['Id'] and current.get(c['Id']) != projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(
        c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    save()
require(report['status'] == 'PASS_SCOPED_ACQUISITION_READ', 'incomplete-reader')
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))
