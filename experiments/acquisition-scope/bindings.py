#!/usr/bin/env python3
"""Scope-bound actual admission/certification; new PG clones and native stores only."""
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
sys.path.insert(0, str(SOURCE / 'experiments/relay-source'))
spec = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
oracle = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = oracle
spec.loader.exec_module(oracle)
from oracle import complete_source
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
assert len(sys.argv) in (2, 3, 4) and SOURCE == root / 'source'
guards = len(sys.argv) == 4 and sys.argv[2] == '--guards'
refusals = len(sys.argv) == 3 and sys.argv[2] == '--refusals'
policy = len(sys.argv) == 3 and sys.argv[2] == '--policy'
lease = len(sys.argv) == 3 and sys.argv[2] == '--lease'
installed = len(sys.argv) == 3 and sys.argv[2] == '--installed-ingest'
interruptions = len(sys.argv) == 4 and sys.argv[2] == '--installed-interruptions'
commits = len(sys.argv) == 4 and sys.argv[2] == '--installed-commits'
assert len(sys.argv) == 2 or guards or refusals or policy or lease or installed or interruptions or commits
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = Path(sys.argv[3]).resolve(strict=True) if guards or interruptions or commits else root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['probe_name'] == 'ingest_bootstrap_probe'
if guards or interruptions or commits:
    # Reuse only reviewed exact artifacts whose complete Rust/build inputs match.
    changed = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert changed and all(n.startswith(('docs/', 'experiments/')) for n in changed), changed
else:
    assert built['source'] == revision
sha = lambda value: hashlib.sha256(value).hexdigest()
for name, key in [('bigbrotr', 'binary_sha256'), (built['probe_name'], 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(pg_root)
assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
basis = root.parent / 'acquisition-scope-54d1a18/engine'
basis_report = (basis / 'result.json').read_bytes()
assert json.loads(basis_report)['status'] == 'PASS_SCOPED_ACQUISITION_REGISTRATION'
output = root / 'bindings'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-binding'
client = 'bigbrotr-acquisition-binding-' + revision[:7]
report = dict(format='bigbrotr-acquisition-binding/1', source=revision, build=built,
    status='RUNNING', wave='installed-commits' if commits else 'installed-interruptions' if interruptions else 'installed-ingest' if installed else 'ingest-lease' if lease else 'policy' if policy else 'guards' if guards else 'refusals' if refusals else 'first', owner=owner, label=label,
    client=None, cases=[], databases=[],
    postgres=pg['postgres'], postgres_owner=pg['owner'],
    not_run=['installed Sync/progress/M1', 'measured memory capacity/load/aged-state', 'arm64/release']
        + (['Adapter COMMIT/recovery is not repeated in this raw-helper wave; see50b4136 evidence']
           if guards else ['concurrent scope-policy locks and lost COMMIT recovery']))


if installed:
    report['not_run'] = ['native syscall/close delay and worker panic', 'lost/delayed COMMIT and cancellation lock schedules',
        'crash-resumable initialization/successor', 'installed acquisition/M1', 'capacity/load/aged-state', 'arm64/release']

if interruptions:
    report['not_run'] = ['worker panic and arbitrary uninterruptible kernel I/O',
        'lost/delayed COMMIT and cancellation lock schedules', 'crash-resumable initialization/successor',
        'installed acquisition/M1', 'capacity/load/aged-state', 'arm64/release']

if commits:
    report['not_run'] = ['worker panic', 'crash-resumable initialization/successor',
        'renewal-specific lost COMMIT and policy races', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if lease:
    report['not_run'] = ['installed Ingest/Sync/M1', 'lost or delayed COMMIT',
        'renewal-write old-deadline predicate negative control', 'policy/closing lock races',
        'native worker interruption', 'capacity/load/aged-state', 'arm64/release']


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
    require(len(names) in (64, 66) and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
             f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, db, 'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def physical(db):
    return json.loads(lab.sql(pg, db, """
        SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='control' AND c.relkind='r'
    """))


def clone(name, template):
    db = 'bb_scope_bind_' + revision[:7] + '_' + name.replace('-', '_')
    require(re.fullmatch('[a-z0-9_]{1,63}', db), 'database-name')
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE {template}')
    report['databases'].append(db)
    save()
    require(rows(db) == rows(template), 'exact-retained-clone')
    return db


def upgrade(db, name, target=21):
    before, files = rows(db), physical(db)
    config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
    def apply():
        return lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', str(target))
    result = apply()
    (output / (name + '-migration.stdout')).write_text(result.stdout)
    (output / (name + '-migration.stderr')).write_text(result.stderr)
    require(result.returncode == 0, 'actual-upgrade-' + name)
    after = rows(db)
    stripped = json.loads(json.dumps(after))
    if target == 21:
        for row in stripped['producer_requests']:
            require(row.pop('acquisition_scope_id') is None, 'historical-scope-adoption')
    require(all(stripped[t] == before[t] for t in before if t != 'schema_versions'), 'upgrade-rewrote-old-rows')
    new_versions = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    wanted = ([4] if 'acquisition_scopes' not in before else []) + ([21] if target == 21 else [])
    require(sorted(r['migration'] for r in new_versions) == sorted(wanted), 'upgrade-ledger')
    for row in new_versions:
        filename = {4:'0004_acquisition.sql',21:'0021_acquisition_bindings.sql'}[row['migration']]
        require(row['checksum'] == '\\x' + sha((SOURCE / 'migrations/postgres' / filename).read_bytes()), 'upgrade-checksum')
    require(all(row in after['schema_versions'] for row in before['schema_versions']), 'old-ledger-changed')
    current_files = physical(db)
    require(all(current_files[t] == value for t, value in files.items()), 'nullable-column-heap-rewrite')
    require(apply().returncode == 0 and rows(db) == after and physical(db) == current_files, 'reapply-mutated-data')
    lab.write_json(output / (name + '-upgrade.json'), dict(before=before, after=after, physical_before=files, physical_after=current_files))
    lab.sql(pg, db, 'GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes TO bigbrotr_control; '
        'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    return before, after


def old_source_schema():
    db = 'bb_scope_bind_' + revision[:7] + '_old_source_schema'
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator')
    report['databases'].append(db)
    save()
    config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
    result = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
        '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '19')
    require(result.returncode == 0, 'old-source-schema-setup')
    local = output / Path(config).name
    local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/run/secrets/application'))
    lab.sql(pg, db, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; '
        'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; '
        'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control; '
        'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
    for args in [('config','initialize'), ('config','activate-initial','--request-id','77'*16,'--expected-revision','1')]:
        result = lab.cli(dict(pg, client=client), *args, '--file', config)
        require(result.returncode == 0, 'old-source-' + args[1])
    lab.sql(pg, db, """
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'local',0,'fixture',0 FROM control.datasets;
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'alice',true,true,1,1 FROM control.datasets;
        INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash)
            SELECT principal_id,dataset_id,1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),
                decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex') FROM control.local_accounts;
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'policy.change','dataset',dataset_id FROM control.local_accounts;
    """)
    return db


def rejected_source(dataset):
    """Literal forged-signature request/rejection; no product codec or dump input."""
    native, inputs, _ = complete_source(dataset, False)
    event = json.loads((SOURCE / 'tests/fixtures/ingest/import-event.jsonl').read_bytes())
    be = lambda n: n.to_bytes(8, 'big')
    slot = bytes([66]) * 16
    visibility = [dataset, 4, bytes([64]) * 16, 1]
    observer = [dataset, 2, bytes([65]) * 16, 1]
    body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
            event['kind'], event['tags'], event['content'], bytes(64)]
    inputs[2] = b'bigbrotr/producer-request/1\0' + oracle.cbor([
        [slot, 1, 2], 'admit', 1, [1, visibility, body, 'network', None,
        [456, observer, [observer, 1, None, 0], None]]])
    rejection = (b'bigbrotr/rejection/1\0' + slot + be(1) + be(2)
                 + hashlib.sha256(inputs[2]).digest() + dataset + be(1) + b'\x04')
    marker = b'\x01\x31' + slot + be(1)
    native = [r for r in native if (r[0], r[1]) != ('receipts', marker)]
    native += [('receipts', marker, b'\x01\x31' + be(2) + be(0) + be(0)),
               ('receipts', b'\x01\x30' + slot + be(1) + be(2), rejection)]
    return sorted(native), inputs, rejection


def refuse_dispatch(db):
    # Key the fault to an existing durable scoped reservation, not call counts.
    # The actual reservation trigger still checks the real current scope first.
    lab.sql(pg, db, """
        ALTER FUNCTION control.require_acquisition_scope(bytea,bytea,bytea,bytea)
            RENAME TO require_acquisition_scope_original;
        CREATE FUNCTION control.require_acquisition_scope(d bytea,s bytea,e bytea,a bytea)
        RETURNS void LANGUAGE plpgsql AS $$ BEGIN
            IF EXISTS(SELECT 1 FROM control.producer_requests
                WHERE dataset_id=d AND acquisition_scope_id=s) THEN
                RAISE EXCEPTION 'acquisition scope is not current for this source'
                    USING ERRCODE='23514';
            END IF;
            PERFORM control.require_acquisition_scope_original(d,s,e,a);
        END $$;
        REVOKE ALL ON FUNCTION control.require_acquisition_scope(bytea,bytea,bytea,bytea) FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION control.require_acquisition_scope(bytea,bytea,bytea,bytea) TO bigbrotr_control;
    """)


def arrival_policy(db, name):
    """Independent policy documents injected into this fresh clone before ownership.

    This is no management activation/transition claim. The original template and
    every applied migration remain unchanged; neither expected bytes nor policy
    fingerprints call the production codec.
    """
    before = rows(db)
    require(not before['owner_epochs'] and not before['producer_requests'], 'policy-fixture-not-empty')
    expected = json.loads(json.dumps(before))
    mutations = []
    for table in ['config_revisions', 'policy_revisions']:
        require(len(before[table]) == 1, 'policy-fixture-single-revision')
        document = json.loads((SOURCE / 'experiments/configuration-inspection/policy.json').read_text())
        document['format'] = 'bigbrotr-product-policy/2'
        settings = document['settings']
        settings['relay_policy.banned'] = []
        settings['sync.provenance'] = 'off'
        if table == 'config_revisions':
            if name == 'policy-provenance':
                settings['sync.provenance'] = 'local_first_witness'
            if name == 'policy-limit':
                # Keep import.max_event_bytes <= ingest.max_event_bytes so
                # full restoration succeeds and only the prepared limit fails.
                settings['ingest.max_event_bytes'] = 524288
            if name == 'policy-floor':
                settings['sync.history_since'] = 1
            if name == 'policy-ephemeral':
                settings['sync.ephemeral'] = 'drop'
        elif name == 'policy-second':
            settings['sync.provenance'] = 'local_first_witness'
        encoded = json.dumps(document, separators=(',', ':'), sort_keys=True, ensure_ascii=False).encode()
        digest = sha(b'bigbrotr/product-policy/2\0' + encoded)
        expected[table][0].update(document=document, document_digest='\\x' + digest, schema_version=2)
        mutations.append(f"UPDATE control.{table} SET document=convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb, "
                         f"document_digest=decode('{digest}','hex'),schema_version=2")
    lab.sql(pg, db, 'BEGIN; SET LOCAL session_replication_role=replica; ' + '; '.join(mutations) + '; COMMIT;')
    after = rows(db)
    comparable = json.loads(json.dumps(after))
    for table in ['config_revisions', 'policy_revisions']:
        comparable[table][0]['xmin'] = before[table][0]['xmin']
    require(comparable == expected, 'policy-fixture-unexpected-effects')
    lab.write_json(output / (name + '-policy-setup.json'), dict(before=before, after=after))


def run_case(name):
    db = old_source_schema() if name == 'old-source-schema' else clone(name, 'bb_source_template')
    if name != 'old-source-schema':
        upgrade(db, name, target=4 if name == 'old-binding-schema' else 21)
    if name == 'dispatch-refused':
        refuse_dispatch(db)
    if policy:
        arrival_policy(db, name)
    if guards:
        fixture = SOURCE / 'experiments/acquisition-scope/binding_guards.sql'
        report['guard_fixture_sha256'] = sha(fixture.read_bytes())
        lab.sql(pg, db, fixture.read_text())
    before = rows(db)
    require(not before['producer_requests'] and not before['owner_epochs'], 'fresh-consumer-prerequisite')
    lab.write_json(output / (name + '-before.json'), before)
    mode = ('source-arrival-' if policy else 'source-scope-') + name
    result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', mode, db,
        '/run/secrets/application', '/lab/' + name, check=False, timeout=90)
    (output / (name + '.stdout')).write_text(result.stdout)
    (output / (name + '.stderr')).write_text(result.stderr)
    require(result.returncode == 0 and not result.stderr, 'actual-scope-adapter-' + name)
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    require(messages[-1]['event'] == 'scope-finished' and messages[-1]['mode'] == 'source-scope-' + name
        and messages[-1]['status'] == 'PASS'
        and messages[-1]['zero_writes'] == (name not in ('invalid', 'dispatch-refused')), 'phase-state-comparison')
    after = rows(db)
    lab.write_json(output / (name + '-after.json'), after)
    if policy:
        decisions = [m for m in messages if m['event'] == 'arrival-policy']
        if name.startswith('policy-'):
            expected_count = 2 if name == 'policy-second' else 1
            require(len(decisions) == expected_count and not decisions[-1]['accepted']
                and all(m['accepted'] for m in decisions[:-1]), 'policy-refusal-phase')
            require(all(m['operation'] == 'admission' for m in decisions), 'policy-operation')
            expected_digests = [before[t][0]['document_digest'][2:] for t in ['config_revisions', 'policy_revisions']]
            require([m['digest'] for m in decisions] == expected_digests[:expected_count], 'actual-accepted-documents')
        else:
            require(decisions and all(m['accepted'] for m in decisions), 'unexpected-arrival-policy-refusal')
            require(any(m['operation'] == 'admission' for m in decisions), 'missing-admission-policy')
            if name in ('pass', 'invalid-retained'):
                require(any(m['operation'] == 'certification' for m in decisions), 'missing-certification-policy')
        for table in ['config_revisions', 'policy_revisions']:
            require(after[table] == before[table], 'accepted-policy-mutated')
    committed = name in ('pass','changed','omitted','legacy-adopt','certify-withdrawn','invalid-retained')
    dataset = bytes.fromhex(after['datasets'][0]['dataset_id'][2:])
    expected, inputs, receipts = complete_source(dataset, committed)
    if name == 'invalid':
        expected, inputs, rejection = rejected_source(dataset)
        require(messages[-1]['retry_zero_writes'], 'missing-rejection-retry')
        outcome = next(r for r in after['producer_outcomes'] if r['request_sequence'] == 2)
        require(outcome['outcome_bytes'] == '\\x' + rejection.hex()
            and outcome['outcome_sha256'] == '\\x' + sha(rejection), 'exact-PG-rejection')
    if name == 'dispatch-refused':
        _, inputs, _ = complete_source(dataset, True)
        first = next(m['state'] for m in messages if m['event'] == 'scope-before')
        last = messages[-1]['state']
        states = [{n: json.loads(v) for n, v in state['control_tables']} for state in (first, last)]
        old, new = states
        require({n for n in old if old[n] != new[n]} == {'producer_requests', 'producer_epochs'}, 'dispatch-effects')
        require(len(new['producer_requests']) == 2 and old['producer_requests'][0] in new['producer_requests'], 'dispatch-reservation')
        previous, current = [dict(t['producer_epochs'][0]) for t in states]
        require(previous.pop('reserved_through_request') == 1 and current.pop('reserved_through_request') == 2, 'dispatch-reserved-prefix')
        previous.pop('xmin'); current.pop('xmin')
        require(previous == current and len(new['producer_epochs']) == 1, 'dispatch-producer-mutation')
    dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/' + name)
    (output / (name + '-native.json')).write_text(dumped.stdout)
    actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    require(actual == expected, 'independent-complete-native-bytes-' + name)
    originals = sorted((output / name / 'producer-inputs-v1').glob('*.cbor'))
    require(len(originals) == len(inputs) and all(p.read_bytes() == inputs[int(p.stem[-16:],16)] for p in originals), 'exact-original-inputs')
    require(len(after['producer_requests']) == len(inputs)
        and len(after['producer_outcomes']) == (1 if name == 'dispatch-refused' else len(inputs)), 'exact-operational-counts')
    for row in after['producer_requests']:
        sequence = int(row['request_sequence'])
        require(row['request_digest'] == '\\x' + sha(inputs[sequence]) and row['input_bytes'] == len(inputs[sequence]), 'original-request-binding')
        if 'acquisition_scope_id' in row:
            if sequence == 1 or name == 'legacy-adopt':
                require(row['acquisition_scope_id'] is None, 'legacy-row-adopted')
            else:
                identities = next(m for m in messages if m['event'] == 'scope-identities')
                require(row['acquisition_scope_id'] == '\\x' + identities['original'], 'changed-reservation-scope')
                scope = next(s for s in after['acquisition_scopes'] if s['scope_id'] == row['acquisition_scope_id'])
                require([row[k] for k in ['dataset_id','relay_endpoint_id','relay_access_scope_id']] ==
                    [scope[k] for k in ['dataset_id','endpoint_id','access_scope_id']], 'complete-source-fk')
    if name != 'old-source-schema':
        documents = {r['document'] for r in after['acquisition_selectors']}
        kinds = [2] if name == 'outside-collection' else [1]
        def canonical(values):
            return '\\x' + json.dumps(dict(version=1,filters=[dict(kinds=values)],history_since=0,ephemeral='retain'),separators=(',',':')).encode().hex()
        require(documents == {canonical(kinds), canonical([1,2])} and len(after['acquisition_scopes']) == 2, 'literal-retained-selectors')
    if name == 'pass':
        require(any(m.get('event') == 'scope-needs-admission' and m['zero_writes'] for m in messages), 'read-before-admission')
    if name in ('certify-withdrawn', 'invalid-withdrawn'):
        require(sum(not r['enabled'] and r['generation'] == 2 for r in after['relay_enrollments']) == 1, 'actual-withdrawal')
    require(lab.sql(pg, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'unjoined-driver')
    if guards:
        checks = json.loads(lab.sql(pg, db, 'SELECT json_agg(r ORDER BY position) FROM binding_fixture.results r'))
        require(len(checks) == 23 and all(c['unchanged'] for c in checks)
            and len({c['backend_pid'] for c in checks}) == 1
            and [c['position'] for c in checks] == list(range(1,24)), 'nested-SQL-guards')
        require(sum(c['sqlstate'] == 'ZX001' for c in checks) == 5
            and sum(c['sqlstate'] == '23514' for c in checks) == 18, 'guard-control-counts')
        lab.write_json(output / 'relational-checks.json', checks)
        report['relational_checks'] = len(checks)
    report['cases'].append(dict(name=name, status='PASS', actual_pg_rocks=True, complete_native_rows=len(actual), original_inputs=len(inputs)))
    save()
    return db


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
    instrument_mount = []
    if interruptions:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from interruptions import prepare_instrument
        instrument = prepare_instrument(root, prior, SOURCE)
        instrument_mount = ['--mount', f'type=bind,src={instrument},dst=/instrument,readonly']
    if commits:
        instrument_mount = ['--mount', f'type=bind,src={SOURCE},dst=/source,readonly']
    lab.docker('start', target['Id'])
    deadline = time.monotonic() + 30
    while lab.docker('exec', target['Id'], 'pg_isready', '-U', 'postgres', check=False).returncode:
        require(time.monotonic() < deadline, 'postgres-readiness')
        time.sleep(.1)
    basic = rows('bb_scope_54d1a18_basic')
    template = rows('bb_source_template')
    require(basic == json.loads((basis / 'basic-after.json').read_text()), 'original-scope-basis')
    require(not template['producer_requests'] and not template['owner_epochs'], 'original-template-preconditions')
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly', *instrument_mount, pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    if commits:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from commits import CASES, run as run_commits
        cases = list(CASES)
    elif interruptions:
        cases = ['trace-pass','initialize-signal','initialize-deadline','close-signal']
    elif installed:
        cases = ['pass','zero-topology','expired','policy-admin','existing-directory','registered-only']
    elif lease:
        cases = ['pass','expired','control-expired','closing','draining','policy-first',
                 'policy-second','delayed','wrong-registration','wrong-control','cancelled']
    elif guards:
        cases = ['pass']
    elif policy:
        cases = ['pass', 'invalid', 'invalid-retained', 'certify-withdrawn',
                 'policy-provenance', 'policy-limit', 'policy-floor', 'policy-ephemeral', 'policy-second']
    elif refusals:
        cases = ['invalid', 'invalid-retained', 'invalid-withdrawn', 'dispatch-refused']
    else:
        db = clone('populated', 'bb_scope_54d1a18_basic')
        before, after = upgrade(db, 'populated')
        require(before['producer_requests'], 'nonempty-upgrade-workload')
        report['cases'].append(dict(name='populated-upgrade', status='PASS', prior_rows_xmin_filenodes_unchanged=True))
        save()
        cases = ['pass','changed','omitted','legacy-adopt','outside-collection','outside-request',
                 'no-source','certify-withdrawn','old-source-schema','old-binding-schema']
    installed_basis = None
    for name in cases:
        if commits:
            completed = clone(name, 'bb_source_template')
            report['cases'].append(run_commits(pg, client, output, completed, name, rows))
            save()
        elif interruptions:
            from interruptions import run as run_interruption
            completed = clone(name, 'bb_source_template')
            report['cases'].append(run_interruption(pg,client,output,completed,name,rows))
            save()
        elif installed:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from installed import run as run_installed
            completed = clone(name, installed_basis if name == 'registered-only' else 'bb_source_template')
            report['cases'].append(run_installed(pg,client,output,completed,name,rows))
            if name == 'pass': installed_basis = completed
            save()
        elif lease:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from lease import run as run_lease
            completed = clone(name, 'bb_source_template')
            report['cases'].append(run_lease(pg, client, output, completed, name, rows))
            save()
        else:
            completed = run_case(name)
    if guards:
        from binding_guards import run_policy_race

        def owned():
            item = json.loads(lab.docker('inspect', target['Id']).stdout)[0]
            require(item['Config']['Labels'][lab.LABEL] == pg['owner'] and item['State']['Running'], 'race-pg-owner')

        basis_rows = rows(completed)
        for name in ['writer_commit', 'writer_rollback', 'scope_commit', 'scope_rollback', 'repeatable']:
            db = clone(name, completed)
            report['cases'].append(run_policy_race(pg, db, name, rows, output, owned))
            save()
        # Discriminate the scope epoch predicate specifically. Only this new
        # clone loses it; no source, applied migration or prior outcome changes.
        from concurrency_cases import Mismatch
        db = clone('negative_no_epoch', completed)
        definition = lab.sql(pg, db, "SELECT pg_get_functiondef('control.require_acquisition_scope(bytea,bytea,bytea,bytea)'::regprocedure)")
        predicate = 'AND s.policy_revision = d.policy_revision'
        require(definition.count(predicate) == 1, 'negative-control-definition')
        altered = definition.replace(predicate, 'AND true /* isolated negative control */')
        lab.sql(pg, db, altered)
        negative = output / 'negative_no_epoch'
        negative.mkdir()
        try:
            run_policy_race(pg, db, 'writer_commit', rows, negative, owned)
        except Mismatch as error:
            require(str(error) == 'unexpected-sqlstate', 'wrong-negative-failure')
        else:
            raise RuntimeError('epoch-negative-control-was-not-detected')
        result = json.loads((negative / 'writer_commit-race.json').read_text())
        require(result['status'] == 'FAIL' and not result['cleanup_errors']
            and len(result['barriers']) == 1
            and result['responses'][-1]['answer']['sqlstate'] == '00000', 'wrong-negative-outcome')
        report['negative_control'] = dict(database=db, status='EXPECTED_FAIL',
            original_definition_sha256=sha(definition.encode()), altered_definition_sha256=sha(altered.encode()),
            reason='Writer-first policy commit incorrectly permits original scope when its epoch predicate is removed')
        save()
        require(rows(completed) == basis_rows, 'signed-parent-changed')
    require(rows('bb_scope_54d1a18_basic') == basic and rows('bb_source_template') == template
        and (basis / 'result.json').read_bytes() == basis_report, 'retained-original-changed')
    report['status'] = 'PASS_INITIAL_INGEST_COMMIT_WAVE' if commits else 'PASS_INITIAL_INGEST_INTERRUPTION_WAVE' if interruptions else 'PASS_INITIAL_INGEST_LIFETIME_FIRST_WAVE' if installed else 'PASS_INITIAL_INGEST_LEASE' if lease else 'PASS_SCOPED_ACQUISITION_BINDING'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    errors = []
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
require(report['status'] == ('PASS_INITIAL_INGEST_COMMIT_WAVE' if commits else 'PASS_INITIAL_INGEST_INTERRUPTION_WAVE' if interruptions else 'PASS_INITIAL_INGEST_LIFETIME_FIRST_WAVE' if installed else 'PASS_INITIAL_INGEST_LEASE' if lease else 'PASS_SCOPED_ACQUISITION_BINDING'), 'incomplete-binding')
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))
