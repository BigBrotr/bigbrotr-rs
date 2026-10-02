"""Installed finite HTTP→socket→native journey under the original living owner.

Run only through bindings.py's separately owned containers and database clones.
Missing/empty work is a failure, never a skipped successful acquisition case.
"""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import time
import lab
from installed import messages, policy_setup, set_value, stop, terminal, wait_for
from sessions import configuration
from registrations import upgrade as upgrade_registry
from scopes import upgrade as upgrade_scopes
from initial_jobs import released, unchanged, validate_job
from installed_sync_oracle import ENDPOINT, SIGNED_SHA256, decode, expected

EVENT_CASES = ('sync-signed', 'sync-peer-refusals')
CASES = ['sync-signed', 'sync-quiet', 'sync-function', 'sync-lock', 'sync-public-visibility',
    'sync-source-function', 'sync-scope-function', 'sync-change-read', 'sync-peer-refusals']
SOURCE = Path(__file__).resolve().parents[2]
EXECUTION = 'control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea)'
BOOTSTRAP = {'datasets', 'owner_epochs', 'role_instances', 'topology_generations', 'topology_streams',
    'source_streams', 'allocation_domains', 'producer_slots', 'producer_epochs', 'producer_requests', 'producer_outcomes'}
COMMANDS = {'browser_sessions', 'login_buckets', 'management_audit', 'management_commands',
    'relay_endpoints', 'relay_registration_commands', 'relay_enrollments', 'network_routes',
    'relay_access_contexts', 'relay_enrollment_changes', 'acquisition_selectors', 'acquisition_scopes',
    'acquisition_scope_commands', 'jobs', 'acquisition_jobs', 'job_admissions'}


def validate_queued(state, observations):
    receipt, = [r['body'] for r in observations if r['path'] == 'jobs']
    job, = state['jobs']
    owner, = [r for r in state['owner_epochs'] if r['role'] == 'control']
    assert job['state'] == 'queued' and job['job_revision'] == 2 and job['checkpoint_revision'] == 0
    assert (job['owner_resource_id'], job['owner_epoch'], job['owner_incarnation']) == (
        owner['resource_id'], owner['epoch'], owner['process_incarnation'])
    # Existing independent accepted-row oracle validates every remaining column,
    # digest, original deadline, command/audit, session, scope and receipt.
    accepted = copy.deepcopy(state)
    accepted['jobs'][0].update(state='accepted', job_revision=1, owner_resource_id=None,
        owner_epoch=None, owner_incarnation=None)
    validate_job(accepted, receipt)


def validate_source(state, report, observations, *, public=False):
    visibility = report['public_scope' if public else 'private_scope']
    ordinal = 1 if public else 3
    assert visibility['ordinal'] == str(ordinal)
    dataset = state['datasets'][0]['dataset_id']
    endpoint, = state['relay_endpoints']; access, = state['relay_access_contexts']
    enrollment, = state['relay_enrollments']; route, = state['network_routes']
    scope, = state['acquisition_scopes']; selector, = state['acquisition_selectors']
    assert bytes.fromhex(endpoint['url_bytes'][2:]).decode() == ENDPOINT
    canonical = b'{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}'
    assert selector['document'] == '\\x'+canonical.hex()
    assert selector['candidate_digest'] == '\\x'+hashlib.sha256(canonical).hexdigest()
    assert scope['selector_id'] == selector['selector_id'] and scope['endpoint_id'] == endpoint['endpoint_id']
    assert scope['access_scope_id'] == access['access_scope_id'] == enrollment['access_scope_id']
    assert scope['dataset_id'] == endpoint['dataset_id'] == access['dataset_id'] == dataset
    assert (scope['visibility_kind'], scope['visibility_domain'], scope['visibility_ordinal']) == (
        4, '\\x'+visibility['allocation_domain'], ordinal)
    assert (access['dictionary_kind'], access['allocation_domain'], access['ordinal']) == (
        scope['visibility_kind'], scope['visibility_domain'], ordinal)
    assert enrollment['enabled'] is True and enrollment['generation'] == 1
    assert enrollment['endpoint_id'] == endpoint['endpoint_id'] and enrollment['route_id'] == route['route_id']
    assert route['route_version'] == 1 and route['route_bytes'] == '\\x0104207f000001'
    assert access['access_class'] == 'public'
    assert len(state['relay_enrollment_changes']) == len(state['relay_registration_commands']) == len(state['acquisition_scope_commands']) == 1
    owner, = [r for r in state['owner_epochs'] if r['role'] == 'control']
    for row in (endpoint, scope, selector):
        assert row['config_revision'] == row['policy_revision'] == 1
        assert (row['owner_resource_id'], row['owner_epoch'], row['owner_incarnation']) == (
            owner['resource_id'], owner['epoch'], owner['process_incarnation'])
    for observation, table, ident in [('relay-endpoints', 'relay_registration_commands', 'endpoint_id'),
            ('acquisition-scopes', 'acquisition_scope_commands', 'scope_id')]:
        wire, = [r['body'] for r in observations if r['path'] == observation]
        assert wire[ident] == state[table][0][ident][2:]
    wire, = [r['body'] for r in observations if r['path'] == 'relay-enrollments']
    assert wire['route_id'] == route['route_id'][2:] and wire['access_scope_id'] == access['access_scope_id'][2:]
    actor, = state['local_accounts']; session, = state['browser_sessions']
    raw = lambda value: bytes.fromhex(value[2:])
    be = lambda n: n.to_bytes(8, 'big')
    qualified = raw(dataset)+b'\0\4'+raw(scope['visibility_domain'])+be(ordinal)
    for path, table, action, nonce in [
            ('relay-endpoints', 'relay_registration_commands', 'relay.endpoint.register', '91'),
            ('relay-enrollments', 'relay_enrollment_changes', 'relay.enrollment.change', '92'),
            ('acquisition-scopes', 'acquisition_scope_commands', 'acquisition.scope.register', 'a1')]:
        key = bytes.fromhex(nonce*16)
        detail, = state[table]
        command, = [r for r in state['management_commands'] if r['request_key'] == '\\x'+key.hex()]
        audit, = [r for r in state['management_audit'] if r['audit_id'] == command['audit_id']]
        assert detail['dataset_id'] == command['dataset_id'] == audit['dataset_id'] == dataset
        assert detail['actor_id'] == command['actor_id'] == audit['actor_id'] == actor['principal_id']
        assert detail['request_key'] == command['request_key'] == '\\x'+key.hex()
        assert detail['session_id'] == session['session_id'] and session['principal_id'] == actor['principal_id']
        for field in ('credential_revision', 'authentication_revision', 'grant_revision'):
            assert detail['actor_'+field] == actor[field]
        assert detail['owner_role'] == 'control'
        assert (detail['owner_resource_id'], detail['owner_epoch'], detail['owner_incarnation']) == (
            owner['resource_id'], owner['epoch'], owner['process_incarnation'])
        prefix = raw(dataset)+raw(actor['principal_id'])+key
        if path == 'relay-endpoints':
            url = ENDPOINT.encode()
            preimage = b'bigbrotr/relay-registration/1\0'+prefix+be(1)*2+b'\0\1'+len(url).to_bytes(4, 'big')+url
        elif path == 'relay-enrollments':
            preimage = (b'bigbrotr/relay-enrollment-change/1\0'+prefix+raw(endpoint['endpoint_id'])
                +qualified+b'\1'+be(0)+be(1)*2+bytes.fromhex('0104207f000001'))
        else:
            preimage = (b'bigbrotr/acquisition-scope-registration/1\0'+prefix+raw(endpoint['endpoint_id'])
                +raw(access['access_scope_id'])+qualified+be(1)*2+len(canonical).to_bytes(4, 'big')+canonical)
        assert command['request_digest'] == '\\x'+hashlib.sha256(preimage).hexdigest()
        clean = lambda row: {k:v for k,v in row.items() if k != 'xmin'}
        assert clean(command) == dict(dataset_id=dataset, actor_id=actor['principal_id'], request_key='\\x'+key.hex(),
            request_digest='\\x'+hashlib.sha256(preimage).hexdigest(), action=action,
            accepted_at_us=command['accepted_at_us'], expected_config_revision=1, policy_revision=1,
            audit_id=audit['audit_id'], authority_mode=None, document_digest=None)
        assert clean(audit) == dict(dataset_id=dataset, actor_id=actor['principal_id'], audit_id=command['audit_id'],
            action=action, outcome='accepted', subject_id='\\x'+key.hex(), created_at_us=command['accepted_at_us'],
            config_revision=1, policy_revision=1, reason=None, requester_os_uid=None, requester_database_role=None)
        wire, = [r['body'] for r in observations if r['path'] == path]
        assert int(wire['accepted_at_us']) == command['accepted_at_us']


def native(client, output, name, state, initialized, transcript, completed_us):
    directory = output/name
    dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/'+name+'/store')
    (output/(name+'-native.json')).write_text(dumped.stdout)
    actual = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    originals = sorted((directory/'store/producer-inputs-v1').glob('*.cbor'))
    signed = name in EVENT_CASES
    assert len(originals) == (4 if signed else 3)
    actual_inputs = [p.read_bytes() for p in originals]
    facts = {int.from_bytes(k[-8:], 'big'): v for cf, k, v in actual if cf == 'facts' and k[:2] == b'\x01\x20'}
    # Select known sequence keys, extract ONLY time scalars. Full keys/payloads,
    # exact sequence inventory and timestamp equality are independently checked below.
    fact_prefix = b'bigbrotr/fact/1\0'
    def recorded(sequence):
        data = facts[sequence][34:]
        assert data.startswith(fact_prefix)
        value = decode(data[len(fact_prefix):])[4]
        assert type(value) is int
        return value
    dictionary_time = recorded(4)
    job, = state['jobs']
    assert job['created_at_us'] <= dictionary_time <= transcript[0]['connected_at_us']
    received = recorded_at = None
    if signed:
        prefix = b'bigbrotr/producer-request/1\0'
        assert actual_inputs[3].startswith(prefix)
        received = decode(actual_inputs[3][len(prefix):])[3][5][0]
        recorded_at = recorded(8)
        assert type(received) is int
        assert transcript[0]['sent_at_us'] <= received <= transcript[0]['closed_at_us']
        assert received <= recorded_at <= completed_us
    artifact = (directory/'event.jsonl').read_bytes()
    assert hashlib.sha256(artifact).hexdigest() == SIGNED_SHA256
    wanted, inputs, receipts = expected(state, initialized, json.loads(artifact), dictionary_time,
        received, recorded_at, transcript[0]['clock_resolution_us'])
    assert actual == wanted, 'complete independently encoded native rows'
    assert actual_inputs == inputs, 'complete original producer inputs'
    requests = sorted(state['producer_requests'], key=lambda r: r['request_sequence'])
    outcomes = sorted(state['producer_outcomes'], key=lambda r: r['request_sequence'])
    assert len(requests) == len(outcomes) == len(inputs)
    assert [r['outcome_bytes'] for r in outcomes] == ['\\x'+v.hex() for v in receipts]
    for sequence, (row, original, outcome, receipt) in enumerate(zip(requests, inputs, outcomes, receipts), 1):
        assert row['request_sequence'] == sequence and row['input_bytes'] == len(original)
        assert row['request_digest'] == '\\x'+hashlib.sha256(original).hexdigest()
        assert outcome['outcome_sha256'] == '\\x'+hashlib.sha256(receipt).hexdigest()
        assert row['producer_slot_id'] == '\\x'+initialized['producer_slot_id'] and row['producer_epoch'] == 1
        columns = ('relay_endpoint_id', 'relay_access_scope_id', 'relay_enrollment_generation',
            'relay_route_id', 'acquisition_scope_id', 'acquisition_job_id')
        if sequence < 4:
            assert all(row[k] is None for k in columns)
        else:
            source, = state['relay_enrollments']; scope, = state['acquisition_scopes']
            assert tuple(row[k] for k in columns) == (source['endpoint_id'], source['access_scope_id'],
                source['generation'], source['route_id'], scope['scope_id'], job['job_id'])
    assert state['producer_epochs'][0]['reserved_through_request'] == len(inputs)
    assert state['producer_epochs'][0]['outstanding_request_limit'] == 32
    assert state['source_streams'][0]['state'] == 'open'
    control, = [r for r in state['owner_epochs'] if r['role'] == 'control']
    manifest = next(v for cf, k, v in wanted if cf == 'default' and k == b'\x01\x01')
    common = dict(dataset_id=state['datasets'][0]['dataset_id'],
        producer_slot_id='\\x'+initialized['producer_slot_id'], producer_epoch=1,
        manifest_sha256='\\x'+hashlib.sha256(manifest).hexdigest())
    for sequence, (row, outcome, original, receipt) in enumerate(zip(requests, outcomes, inputs, receipts), 1):
        attribution = {k: row[k] for k in ('relay_endpoint_id', 'relay_access_scope_id',
            'relay_enrollment_generation', 'relay_route_id', 'acquisition_scope_id', 'acquisition_job_id')}
        request = dict(common, request_sequence=sequence, request_digest='\\x'+hashlib.sha256(original).hexdigest(),
            input_bytes=len(original), input_profile=1, issuer_role='control', issuer_resource=control['resource_id'],
            issuer_epoch=control['epoch'], issuer_incarnation=control['process_incarnation'], **attribution)
        assert {k:v for k,v in row.items() if k not in ('xmin', 'reserved_at_us')} == request
        wanted_outcome = dict(common, request_sequence=sequence, request_digest=request['request_digest'],
            outcome_kind=1, outcome_bytes='\\x'+receipt.hex(), outcome_sha256='\\x'+hashlib.sha256(receipt).hexdigest(),
            policy_epoch=1, recorder_role='control', recorder_resource=control['resource_id'],
            recorder_epoch=control['epoch'], recorder_incarnation=control['process_incarnation'])
        assert {k:v for k,v in outcome.items() if k not in ('xmin', 'recorded_at_us')} == wanted_outcome
        assert control['acquired_at_us'] <= row['reserved_at_us'] <= outcome['recorded_at_us'] < control['lease_expires_at_us']
    return dict(native_rows=len(wanted), original_requests=len(inputs), dictionary_recorded_us=dictionary_time,
        event_received_us=received, event_recorded_us=recorded_at)


def stop_during_quiet(client, prefix, signed):
    # Poll and signal inside the owned container, avoiding another Docker round
    # trip after observing the active request. The first two signed rounds must
    # already be complete; only a subsequent deliberately quiet round qualifies.
    identity = json.loads(Path(str(prefix)+'.pid.json').read_text())
    script = r'''import json,os,signal,sys,time
from pathlib import Path
r=json.loads(sys.argv[1]); directory=Path(r['prefix']); minimum=int(sys.argv[2])
p=Path('/proc')/str(r['pid'])
assert p.joinpath('stat').read_text().split()[21]==r['start_ticks']
assert p.joinpath('cmdline').read_bytes().split(b'\0')[:4]==[b'/build/bigbrotr',b'serve',b'--file',r['config'].encode()]
deadline=time.monotonic()+4
while True:
    opened=sorted(directory.glob('relay-open-*.json'))
    if opened:
        trace=json.loads(opened[-1].read_text())
        closed=directory/('relay-closed-%03d.json'%trace['number'])
        # Select a newly opened request with no observed completion. Close/read
        # racing the signal is
        # still safe and remains visible in the retained terminal transcript.
        now=time.time_ns()//1000
        if trace['number']>=minimum and 0<=now-trace['request_at_us']<20000 and not closed.exists():
            assert p.joinpath('stat').read_text().split()[21]==r['start_ticks']
            (directory/'stop-requested').write_text('SIGTERM during observed quiet receive\n')
            observed=dict(round=trace['number'],request=trace['request'],request_at_us=trace['request_at_us'],signal_at_us=time.time_ns()//1000,signal_monotonic_ns=time.monotonic_ns(),closed_before_signal=closed.exists())
            # Recheck directly before the signal; a completed quantum is not
            # selected as evidence for the quiet-cancellation schedule.
            if observed['closed_before_signal']:
                (directory/'stop-requested').unlink()
                continue
            os.kill(r['pid'],signal.SIGTERM)
            (directory/'signal-observation.json').write_text(json.dumps(observed))
            break
    assert time.monotonic()<deadline,'no active quiet request at signal'
    time.sleep(.001)
'''
    lab.docker('exec', client, 'python3', '-c', script, json.dumps(identity), '3' if signed else '1')
    selected = json.loads((prefix/'signal-observation.json').read_text())
    assert not selected['closed_before_signal']
    assert 0 <= selected['signal_at_us']-selected['request_at_us'] < 100000
    return selected


def run(pg, client, output, database, name, rows):
    assert name in CASES
    visibility_refusal = name == 'sync-public-visibility'
    upgrade_registry(pg, client, output, database, name, rows)
    upgrade_scopes(pg, client, output, database, name, rows)
    lab.sql(pg, database, '''
        GRANT SELECT,INSERT ON control.acquisition_jobs TO bigbrotr_control;
        GRANT EXECUTE ON FUNCTION control.acquisition_command_digest(control.acquisition_jobs,control.management_commands),
            control.require_acquisition_scope(bytea,bytea,bytea,bytea),
            control.require_acquisition_job_authority(bytea,bytea),
            control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea) TO bigbrotr_control;
        INSERT INTO control.principal_grants
            SELECT dataset_id,principal_id,a,'dataset',dataset_id
            FROM control.local_accounts CROSS JOIN (VALUES ('roles.control'),('status.read')) v(a);
    ''')
    functions = {'sync-function': EXECUTION,
        'sync-source-function': 'control.require_relay_source(bytea,bytea,bytea,numeric,bytea)',
        'sync-scope-function': 'control.require_acquisition_scope(bytea,bytea,bytea,bytea)'}
    fault = name in functions or name in ('sync-lock', 'sync-change-read')
    policy_setup(pg, database, name, rows, output, administration=not fault)
    if name in functions:
        signature = functions[name]
        lab.sql(pg, database, 'REVOKE EXECUTE ON FUNCTION '+signature+' FROM bigbrotr_control')
        assert lab.sql(pg, database, "SELECT has_function_privilege('bigbrotr_control','"+signature+"','EXECUTE')") == 'f'
    if name == 'sync-change-read':
        lab.sql(pg, database, 'REVOKE SELECT ON control.relay_enrollment_changes FROM bigbrotr_control')
        assert lab.sql(pg, database, "SELECT has_table_privilege('bigbrotr_control','control.relay_enrollment_changes','SELECT')") == 'f'
    if name == 'sync-lock':
        lab.sql(pg, database, 'REVOKE UPDATE ON control.principal_grants FROM bigbrotr_control')
        assert lab.sql(pg, database, "SELECT has_any_column_privilege('bigbrotr_control','control.principal_grants','UPDATE')") == 'f'
    before = rows(database)
    assert {23, 24, 26} <= {r['migration'] for r in before['schema_versions']}
    assert not before['owner_epochs'] and not before['producer_requests']
    assert all(before[t][0]['document']['settings']['sync.enabled'] for t in ('config_revisions', 'policy_revisions'))
    lab.write_json(output/(name+'-before.json'), before)
    state = dict(pg, client=client)
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    path = output/Path(config).name
    text = configuration(path.read_text(), before, name)
    text = set_value(text, 'topology', 'local_roles', ['control', 'ingest', 'sync', 'archive'])
    text = set_value(text, 'network.dns', 'resolver', '127.0.0.1:9')
    text = set_value(text, 'network.tls', 'extra_roots', [])
    rules = [dict(endpoint=ENDPOINT, cidr='127.0.0.1/32', port=7447)]
    text = set_value(text, 'network.private', 'rules', rules)
    text = text.replace(json.dumps(rules), '[{endpoint="'+ENDPOINT+'",cidr="127.0.0.1/32",port=7447}]')
    path.write_text(text)
    validated = lab.cli(state, 'config', 'validate', '--file', config)
    (output/(name+'-validate.stdout')).write_text(validated.stdout)
    (output/(name+'-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode == 0, validated.stderr
    for filename in ('installed_client.py', 'session_client.py', 'installed_sync_client.py'):
        target = output/filename
        if not target.exists():
            shutil.copy2(Path(__file__).with_name(filename), target)
        assert target.read_bytes() == Path(__file__).with_name(filename).read_bytes()
    directory = output/name; directory.mkdir(mode=0o700)
    shutil.copy2(SOURCE/'tests/fixtures/ingest/import-event.jsonl', directory/'event.jsonl')
    if name == 'sync-peer-refusals':
        from installed_sync_client import REFUSAL_SHA256
        artifact = SOURCE/'tests/fixtures/acquisition/selector-events.json'
        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == REFUSAL_SHA256
        shutil.copy2(artifact, directory/'selector-events.json')
    prefix = output/name
    lab.docker('exec', '-d', client, 'python3', '/lab/installed_client.py', config, '/lab/'+name, 'sync')
    wait_for(lambda: Path(str(prefix)+'.pid.json').exists(), 'Sync CLI start')
    first = wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'Sync initialization')
    if fault:
        if messages(prefix):
            stop(client, prefix); terminal(prefix)
            raise AssertionError('selected execution ignored missing prerequisite with administration disabled')
        ended = terminal(prefix)
        error = Path(str(prefix)+'.stderr').read_text()
        assert ended['exit_code'] == 4 and 'acquisition execution prerequisites unavailable' in error, error
        after = rows(database); lab.write_json(output/(name+'-after.json'), after)
        assert {t for t in before if before[t] != after[t]} <= BOOTSTRAP
        assert len(after['producer_requests']) == len(after['producer_outcomes']) == 2
        assert not after['jobs'] and not after['browser_sessions']
        lab.docker('exec', client, 'python3', '-c', 'import socket;s=socket.socket();assert s.connect_ex(("127.0.0.1",18081))!=0;s.close()')
        from commits import native_oracle as retained_oracle
        retained_oracle(client, output, name, after, number=6, healthy=True, last_commit=6)
        return dict(name=name, status='PASS', before_bind_refusal=True, administration_enabled=False, terminal=ended)
    assert isinstance(first, list) and len(first) == 1 and first[0]['state'] == 'initialized', first
    initialized = first[0]
    assert initialized['ready'] is False and initialized['acquisition'] == 'finite-initial'
    initial = rows(database); lab.write_json(output/(name+'-initialized.json'), initial)
    assert {t for t in before if before[t] != initial[t]} <= BOOTSTRAP
    original_deadline = max(r['lease_expires_at_us'] for r in initial['owner_epochs'])
    lab.write_json(directory/'visibility.json', initialized['public_scope' if visibility_refusal else 'private_scope'])
    wrapper = '''import subprocess,sys,json
from pathlib import Path
p=Path(sys.argv[1])
terminal={}
try:
 with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
  r=subprocess.run(['python3','/lab/session_client.py',str(p),'installed-sync'],stdout=out,stderr=err,timeout=38)
 terminal={'exit_code':r.returncode}
except BaseException as error:
 terminal={'exit_code':None,'error':type(error).__name__+': '+str(error)}
 raise
finally:
 partial=p/'client-exit.json.partial'
 partial.write_text(json.dumps(terminal))
 partial.rename(p/'client-exit.json')
'''
    lab.docker('exec', '-d', client, 'python3', '-c', wrapper, '/lab/'+name)
    def stage(label):
        wait_for(lambda: (directory/(label+'.json')).exists() or (directory/'client-exit.json').exists()
            or (directory/'relay-error.json').exists(), label, 18)
        assert (directory/(label+'.json')).exists(), (directory/'client.stderr').read_text()
        return json.loads((directory/(label+'.json')).read_text())
    observations = stage('prepared')
    prepared = rows(database); lab.write_json(output/(name+'-prepared.json'), prepared)
    unchanged(initial, prepared, COMMANDS)
    assert not prepared['jobs'] and len(prepared['producer_requests']) == 2
    for table in ('management_commands', 'management_audit'):
        assert all(r in prepared[table] for r in initial[table])
    assert len(prepared['management_commands']) == len(initial['management_commands'])+3
    assert len(prepared['management_audit']) == len(initial['management_audit'])+4
    validate_source(prepared, initialized, observations, public=visibility_refusal)
    (directory/'prepared.continue').write_text('observed\n')
    observations = stage('submitted')
    wanted_count = 2 if visibility_refusal else 4 if name in EVENT_CASES else 3
    def completed():
        assert not (directory/'relay-error.json').exists(), 'independent relay failed'
        assert not Path(str(prefix)+'.terminal.json').exists(), 'initial owner ended early'
        traces = sorted(directory.glob('relay-closed-*.json'))
        if visibility_refusal:
            assert not traces and (directory/'relay-monitor-started.json').exists()
        elif len(traces) < 2:
            return None
        current = rows(database)
        now = int(lab.sql(pg, database, 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        if len(current['producer_requests']) > wanted_count or len(current['producer_outcomes']) > wanted_count:
            lab.write_json(output/(name+'-unexpected-producer-state.json'), current)
            raise AssertionError('unexpected producer request or outcome before observation boundary')
        if len(current['producer_outcomes']) < wanted_count or now <= original_deadline:
            return None
        assert len(current['producer_requests']) == len(current['producer_outcomes']) == wanted_count
        assert min(r['lease_expires_at_us'] for r in current['owner_epochs']) > now
        return current, now
    current, now = wait_for(completed, 'durable events and healthy renewal beyond original lease', 16)
    lab.write_json(output/(name+'-durable.json'), current)
    lab.write_json(output/(name+'-renewed.json'), dict(original_deadline=original_deadline, observed_at_us=now,
        leases=[r['lease_expires_at_us'] for r in current['owner_epochs']]))
    allowed = ('jobs', 'acquisition_jobs', 'job_admissions', 'management_commands', 'management_audit')
    if not visibility_refusal:
        allowed += ('producer_epochs', 'producer_requests', 'producer_outcomes')
    unchanged(prepared, current, allowed)
    for table in ('management_commands', 'management_audit'):
        assert all(r in current[table] for r in prepared[table])
        assert len(current[table]) == len(prepared[table])+1
    assert all(row in current['producer_requests'] for row in prepared['producer_requests'])
    assert all(row in current['producer_outcomes'] for row in prepared['producer_outcomes'])
    old, = prepared['producer_epochs']; new, = current['producer_epochs']
    strip = lambda r: {k: v for k, v in r.items() if k not in ('reserved_through_request', 'xmin')}
    assert strip(old) == strip(new) and old['reserved_through_request'] == 2 and new['reserved_through_request'] == wanted_count
    validate_source(current, initialized, observations, public=visibility_refusal); validate_queued(current, observations)
    (directory/'submitted.continue').write_text('observed\n')
    stage('commands-finished')
    before_stop = rows(database); lab.write_json(output/(name+'-before-stop.json'), before_stop)
    from initial_jobs import logged_out
    logged_out(current, before_stop)
    selected_quiet = None
    if visibility_refusal:
        (directory/'stop-requested').write_text('SIGTERM requested by owned host fixture\n')
        signal_at = time.monotonic()
        stop(client, prefix)
    else:
        selected_quiet = stop_during_quiet(client, prefix, name in EVENT_CASES)
        signal_at = time.monotonic()
    ended = terminal(prefix)
    signal_elapsed = ((time.monotonic_ns()-selected_quiet['signal_monotonic_ns'])/1_000_000_000
        if selected_quiet is not None else time.monotonic()-signal_at)
    assert signal_elapsed < 5 and ended['exit_code'] == 0 and not Path(str(prefix)+'.stderr').read_text(), ended
    assert messages(prefix)[-1]['native'] == 'closed' and messages(prefix)[-1]['owner_release'] == 'committed'
    (directory/'commands-finished.continue').write_text('server stopped\n')
    wait_for((directory/'client-exit.json').exists, 'independent clients joined', 6)
    assert json.loads((directory/'client-exit.json').read_text())['exit_code'] == 0, (directory/'client.stderr').read_text()
    after = rows(database); lab.write_json(output/(name+'-after.json'), after)
    released(before_stop, after)
    assert {t for t in before if before[t] != after[t]} <= BOOTSTRAP | COMMANDS
    assert len(after['owner_epochs']) == len(after['role_instances']) == 2
    assert sorted(r['action'] for r in after['management_audit'] if r not in before['management_audit']) == sorted([
        'browser.session.issue', 'browser.session.revoke', 'relay.endpoint.register',
        'relay.enrollment.change', 'acquisition.scope.register', 'job.acquisition.accept'])
    transcript = [json.loads(p.read_text()) for p in sorted(directory.glob('relay-closed-*.json'))]
    assert not (directory/'relay-error.json').exists()
    assert len(list(directory.glob('relay-open-*.json'))) == len(transcript)
    if selected_quiet is not None:
        selected, = [r for r in transcript if r['number'] == selected_quiet['round']]
        assert selected['completion'] == 'quiet-eof' and not selected['frames']
        assert selected['closed_at_us'] >= selected_quiet['signal_at_us']
    if visibility_refusal:
        assert not transcript
        monitor = json.loads((directory/'relay-monitor-ended.json').read_text())
        started = json.loads((directory/'relay-monitor-started.json').read_text())
        job, = after['jobs']
        assert monitor['connections'] == 0 and monitor['started_at_us'] == started['started_at_us']
        assert monitor['started_at_us'] <= job['created_at_us'] < monitor['finished_at_us']
        assert original_deadline < monitor['finished_at_us']
        from installed import native_oracle
        native_oracle(pg, client, output, database, name, after, initialized)
        native_result = dict(native_rows=17, original_requests=2, source_visibility_refused=True,
            tcp_connections=0, monitor=monitor)
    else:
        assert len(transcript) >= 2
        assert [t['completion'] for t in transcript[:2]] == ([
            'request-and-websocket-close']*2 if name in EVENT_CASES else ['quiet-eof']*2)
        if name == 'sync-peer-refusals':
            assert all(trace['frames'] == ['malformed-event', 'signed-outside-selector', 'signed', 'EOSE']
                for trace in transcript[:2])
        native_result = native(client, output, name, after, initialized, transcript, time.time_ns()//1000)
    assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
    return dict(name=name, status='PASS', actual_cli=True, actual_http=True, actual_pg_rocks=True,
        websocket_rounds=len(transcript), same_owner_renewed=True, signal_seconds=signal_elapsed,
        selected_quiet_signal=selected_quiet,
        terminal=ended, **native_result)
