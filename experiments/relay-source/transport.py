#!/usr/bin/env python3
"""Finite actual-source checks, borrowing only the retained source-template PG.

No new network, compiler cache or dataset retirement. Every clone is retained.
This is not WebSocket acquisition, native admission from a socket or M1.
"""
import datetime
import copy
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

assert len(sys.argv) in (2, 3)
cleanup = len(sys.argv) == 3 and sys.argv[2] == '--cleanup'
races = len(sys.argv) == 3 and sys.argv[2] == '--races'
jobs = len(sys.argv) == 3 and sys.argv[2] == '--jobs'
assert len(sys.argv) == 2 or cleanup or races or jobs
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
phase = 'jobs' if jobs else 'races' if races else 'cleanup' if cleanup else 'transport'
output = root / phase
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.source-' + phase
client = 'bigbrotr-source-' + phase + '-' + revision[:7]
report = dict(format='bigbrotr-source-transport-native/1', phase=phase, source=revision, build=built,
    owner=owner, label=label, status='RUNNING', client=None, postgres=pg['postgres'],
    postgres_owner=pg['owner'], cases=[], databases=[],
    preservation_scope=['bb_source_template', 'bb_source_pass', 'source-result.json'],
    evidence_limits=(['Privileged source/policy injections qualify locks and post-wait reads, not concurrent authenticated management acceptance.'] if races else ['normal release checks changed tables; its previously qualified field semantics are not requalified here']),
    not_run=([] if races else ['deadline-check negative control'] if cleanup else ['delayed cleanup reply']) + ([] if races else ['concurrent changes and lock orders']) + [
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
    require(len(names) in ((64, 67) if jobs else (64,))
        and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
        f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, database,
        'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def verify_final(name, database, final, *, original_enrollment=True):
    require(len(final['relay_endpoints']) == 1 and
        final['relay_endpoints'][0]['url_bytes'] == '\\x' + b'ws://127.0.0.1:7447/private'.hex(), 'exact-endpoint')
    require(len(final['relay_enrollments']) == 2, 'two-enrollments')
    if original_enrollment:
        require(all(r['enabled'] and r['generation'] == 1 for r in final['relay_enrollments']), 'original-enrollments')
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
    verify_final(name, database, final)
    report['cases'].append(dict(name=name, status='PASS', database=database,
        transcript=kinds, result=expected, full_native_bytes=True, complete_control_tables=64))
    save()


def next_event(process, wanted, seconds=10):
    end = time.monotonic() + seconds
    with process.condition:
        while True:
            require(not process.overflow, 'output-overflow')
            newline = process.stdout.find(b'\n', process.offset)
            if newline >= 0:
                value = json.loads(bytes(process.stdout[process.offset:newline]))
                process.offset = newline + 1
                require(value['event'] == wanted, 'unexpected-event:' + value['event'])
                return value
            require(time.monotonic() < end and
                (process.process.poll() is None or process.threads[0].is_alive()), 'missing-event:' + wanted)
            process.condition.wait(.01)


def rollback_status(proxy, wanted):
    # Each round uses one bounded proxy control. Never exhaust its32-command cap.
    for _ in range(5):
        proxy.send('status')
        state = next_event(proxy, 'status')
        if state['rollback_ready'] == wanted:
            return state
        time.sleep(.01)
    raise RuntimeError('rollback-ready-not-forwarded')


def run_cleanup_case(name, target):
    database = 'bb_cleanup_' + revision[:7] + '_' + name
    report['active_case'] = name
    save()
    lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE bb_source_template')
    report['databases'].append(database)
    require(rows(database) == template_before, 'clone-state')
    proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
        '/source/experiments/relay-source/rollback_proxy.py', '--upstream-port', '5432',
        '--rollback-number', str(target)], input_pipe=True)
    processes.append((name + '-proxy', proxy))
    proxy_ready = next_event(proxy, 'ready')
    require(proxy_ready['proxy_sha256'] == sha((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes()), 'proxy-base-source')
    process = Process(['docker', 'exec', client, '/build/ingest_bootstrap_probe',
        'source-transport-cleanup-' + name, database, '/run/secrets/application', '/lab/' + name], limit=8*1024**2)
    processes.append((name + '-probe', process))
    next_event(process, 'initialized')
    public = next_event(process, 'source-transport')
    require(public['ordinal'] == 1 and public['result'] == 'refused-public', 'public-before-cleanup')
    ready = next_event(process, 'cleanup-ready')
    before = rows(database)
    require(before == {n:json.loads(v) for n,v in ready['state']['control_tables']}, 'ready-state')
    proxy.send('status')
    initial = next_event(proxy, 'status')
    baseline = initial['rollback_requests']
    require(baseline == initial['rollback_completions'] == initial['rollback_released'] == initial['rollback_ready'], 'setup-rollback-incomplete')
    require(not target or target == baseline + 1, 'wrong-target-rollback')
    identity = next(r for r in before['owner_epochs'] if r['role'] == 'control')
    require(identity['control_backend_pid'] == ready['pid'] and identity['state'] == 'active', 'actual-Control-identity')
    if name == 'expired':
        # Explicit privileged injection of a shorter real lease after setup.
        # The actual product backend retains all production guards.
        lab.sql(pg, database, "BEGIN; SET LOCAL session_replication_role=replica; "
            "UPDATE control.owner_epochs SET lease_expires_at_us="
            "(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+1500000 WHERE role='control' AND epoch=1; COMMIT;")
        injected = rows(database)
        require([n for n in before if before[n] != injected[n]] == ['owner_epochs'], 'lease-injection-effects')
        changed = next(r for r in injected['owner_epochs'] if r['role'] == 'control')
        require(changed == dict(identity, xmin=changed['xmin'], lease_expires_at_us=changed['lease_expires_at_us']), 'lease-injection-fields')
        before = injected
    started = time.monotonic()
    (output / (name + '.go')).touch(exist_ok=False)
    value = next_event(process, 'cleanup-value')
    peer = next_event(process, 'cleanup-peer-closed')
    require(value['elapsed_us'] < 500000 and peer['elapsed_us'] < 500000, 'late-socket-setup')
    held = released = expiry = observation = forwarded = None
    if target:
        held = next_event(proxy, 'held')
        require((held['rollback_requests'], held['rollback_completions'], held['rollback_released'], held['rollback_ready']) == (target,target,target-1,target-1), 'wrong-held-phase')
        # C arrived from the real engine; its transaction ended before forwarding.
        observation = json.loads(lab.sql(pg, database, f"SELECT json_build_object('pid',pid,'backend_start',backend_start::text,'state',state,'xact_start',xact_start) FROM pg_stat_activity WHERE pid={ready['pid']}"))
        require(observation and observation['backend_start']==ready['backend_start'] and observation['state']=='idle' and observation['xact_start'] is None, 'server-rollback-not-complete')
        if name == 'expired':
            end = time.monotonic() + 3
            while True:
                expiry = int(lab.sql(pg,database,"SELECT lease_expires_at_us-(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint FROM control.owner_epochs WHERE role='control' AND epoch=1"))
                if expiry <= 0:
                    break
                require(time.monotonic() < end, 'owner-expiry-not-observed')
                time.sleep(.03)
        if name != 'timeout':
            proxy.send('release')
            released = next_event(proxy, 'released')
            forwarded = rollback_status(proxy, target)
            if name == 'ready':
                require(time.monotonic()-started < .5, 'rollback-not-ready-before-deadline')
    finished = next_event(process, 'cleanup-finished')
    require(finished['disposed'] and {n:json.loads(v) for n,v in finished['state']['control_tables']} == before, 'cleanup-durable-effects')
    elapsed = finished['elapsed_us']
    lower, upper = {'pass':(0,500000),'expired':(1000000,4000000),
                    'timeout':(750000,2000000),'ready':(1500000,4000000)}[name]
    require(lower <= elapsed < upper, 'wrong-time-window')
    if name == 'timeout':
        proxy.send('release')
        released = next_event(proxy, 'released')
        forwarded = rollback_status(proxy,target)
    (output / (name + '.done')).touch(exist_ok=False)
    require(process.wait(10) == 0 and not process.stderr, 'cleanup-probe-failed')
    receipt = next_event(proxy, 'receipt')
    require(proxy.wait(5) == 0 and not proxy.stderr, 'cleanup-proxy-failed')
    require(receipt['rollback_requests'] == receipt['rollback_completions'] == receipt['rollback_released'] == receipt['rollback_ready'] == baseline+1, 'final-rollback-count')
    final = rows(database)
    require(final == before, 'retirement-durable-effects')
    verify_final(name, database, final)
    report['cases'].append(dict(name=name,status='PASS',database=database,baseline=baseline,
        held=held,released=released,receipt=receipt,expiry_remaining_us=expiry,
        server_after_rollback=observation,forwarded=forwarded,
        socket_value_us=value['elapsed_us'],peer_closed_us=peer['elapsed_us'],delivered_or_refused_us=elapsed,
        complete_control_tables=64,full_native_bytes=True,disposed=True))
    save()
    return baseline


def policy_fixture(subject):
    document = json.loads((SOURCE/'tests/fixtures/configuration/policy-v3.json').read_text())
    document['format'] = 'bigbrotr-product-policy/3'
    document['settings']['relay_policy.banned'] = [] if subject is None else [{'endpoint':subject}]
    encoded = json.dumps(document,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
    digest = sha(b'bigbrotr/product-policy/3\0' + encoded)
    expected = {None:'77324b5f6aea113a4e4b81e11d946c0c4e113b051851d398c240de49a7a6322b',
        'ws://127.0.0.1:7447/private':'1a9d607a52734b1c914e22a8c12ad7ad77e2d9dc6e7af379707c446e995fdb76',
        'ws://127.0.0.1:7447/other':'68b792984ec44486e2b6f2c827ee8799c7787e7fa3c493af83aba2b77103d336'}[subject]
    require(digest == expected, 'independent-policy-digest')
    return document,digest


def race_mutation(name, binding):
    endpoint,access=binding['endpoint'],binding['access']
    require(all(re.fullmatch('[0-9a-f]{32}',s) for s in (endpoint,access)), 'binding-identity')
    predicate=f"endpoint_id=decode('{endpoint}','hex') AND access_scope_id=decode('{access}','hex')"
    if name in ('withdraw','source-withdraw','rollback'):
        return f'UPDATE control.relay_enrollments SET enabled=false WHERE {predicate};'
    if name=='generation':
        return f"""INSERT INTO control.relay_enrollment_changes
            SELECT (jsonb_populate_record(NULL::control.relay_enrollment_changes,
                to_jsonb(h)||jsonb_build_object('request_key','\\x'||repeat('98',16),
                'expected_generation',2,'generation',3,'acceptance_transaction',pg_current_xact_id()::text))).*
            FROM control.relay_enrollment_changes h WHERE {predicate} AND generation=1;
            UPDATE control.relay_enrollments SET generation=3,request_key=decode(repeat('98',16),'hex') WHERE {predicate};"""
    config_subject,policy_subject=race_bans(name)
    def doc(subject):
        _,digest=policy_fixture(subject)
        rules=json.dumps([] if subject is None else [{'endpoint':subject}],separators=(',',':'))
        return f"jsonb_set(document,'{{settings,relay_policy.banned}}','{rules}'::jsonb)",f"decode('{digest}','hex')"
    config,config_digest=doc(config_subject);policy,policy_digest=doc(policy_subject)
    return f"""INSERT INTO control.config_revisions
        SELECT 2,dataset_id,1,2,authority_mode,{config},{config_digest},accepted_source_digest,accepted_at_us+1,actor_id
        FROM control.config_revisions WHERE revision=1;
        INSERT INTO control.policy_revisions
        SELECT 2,dataset_id,1,2,2,{policy},{policy_digest},accepted_at_us+1,actor_id
        FROM control.policy_revisions WHERE revision=1;
        UPDATE control.datasets SET config_revision=2,policy_revision=2;"""


def race_bans(name):
    banned='ws://127.0.0.1:7447/private'
    if name=='ban': return None,banned
    if name=='source-ban': return banned,None
    require(name=='ban-other','ban-case')
    return ('ws://127.0.0.1:7447/other',)*2


def verify_race_change(before, after, binding, name, writer_xid):
    expected=copy.deepcopy(before)
    if name=='rollback':
        require(after==before,'writer-rollback-state')
        return
    def selected(rows):
        return next(r for r in rows if r['endpoint_id']=='\\x'+binding['endpoint'] and r['access_scope_id']=='\\x'+binding['access'])
    if name in ('withdraw','source-withdraw','generation'):
        old=selected(expected['relay_enrollments'])
        old.update(xmin=writer_xid)
        if name=='generation':
            old.update(generation=3,request_key='\\x'+'98'*16)
            additions=[r for r in after['relay_enrollment_changes'] if r not in before['relay_enrollment_changes']]
            require(len(additions)==1,'generation-history-count')
            history=selected(before['relay_enrollment_changes'])
            wanted=dict(history,generation=3,expected_generation=2,request_key='\\x'+'98'*16,
                xmin=writer_xid,acceptance_transaction=writer_xid)
            require(additions==[wanted],'exact-enabled-generation-history')
            expected['relay_enrollment_changes'].append(wanted)
        else:
            old['enabled']=False
    else:
        config_subject,policy_subject=race_bans(name)
        expected['datasets'][0].update(config_revision=2,policy_revision=2,xmin=writer_xid)
        for table,subject in [('config_revisions',config_subject),('policy_revisions',policy_subject)]:
            document,digest=policy_fixture(subject)
            original=before[table][0]
            require(original['document']==policy_fixture(None)[0], 'frozen-initial-policy')
            wanted=dict(original,revision=2,previous_revision=1,schema_version=3,document=document,
                document_digest='\\x'+digest,accepted_at_us=original['accepted_at_us']+1,xmin=writer_xid)
            if table=='policy_revisions':wanted['config_revision']=2
            expected[table].append(wanted)
    # Row order may change after a write, but every original field and xmin is
    # retained unless explicitly attributed above. No unrestricted ignore list.
    normalize=lambda data:{t:sorted(rows,key=lambda r:json.dumps(r,sort_keys=True)) for t,rows in data.items()}
    require(normalize(after)==normalize(expected),'exact-race-ledger-effects')


def run_race_case(name):
    database='bb_race_'+revision[:7]+'_'+name.replace('-','_')
    report['active_case']=name;save()
    lab.sql(pg,'postgres',f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE bb_source_template')
    report['databases'].append(database)
    require(rows(database)==template_before,'race-clone-state')
    source_first=name.startswith('source-')
    proxy=Process(['docker','exec','-i',client,'python3','-B',
        '/source/experiments/relay-source/rollback_proxy.py','--upstream-port','5432',
        '--rollback-number','4' if source_first else '0','--before-forward'],input_pipe=True)
    processes.append((name+'-proxy',proxy));next_event(proxy,'ready')
    process=Process(['docker','exec',client,'/build/ingest_bootstrap_probe',
        'source-transport-race-'+name,database,'/run/secrets/application','/lab/'+name],limit=8*1024**2)
    processes.append((name+'-probe',process));next_event(process,'initialized')
    public=next_event(process,'source-transport');require(public['ordinal']==1 and public['result']=='refused-public','public-race-refusal')
    ready=next_event(process,'race-ready');before=rows(database)
    require(before=={n:json.loads(v) for n,v in ready['state']['control_tables']},'race-ready-state')
    proxy.send('status');baseline=next_event(proxy,'status')
    require([baseline[k] for k in ('rollback_requests','rollback_completions','rollback_released','rollback_ready')]==[3]*4,'race-baseline-rollback')
    owner=next(r for r in before['owner_epochs'] if r['role']=='control')
    require(owner['control_backend_pid']==ready['pid'] and owner['state']=='active','race-actual-Control')
    writer=Session(pg,database,'source-race-'+database,sessions)
    transaction=writer.execute('BEGIN; SET LOCAL session_replication_role=replica; SELECT pg_current_xact_id()::text;')
    require(len(transaction['lines'])==1 and transaction['lines'][0].isdigit(),'writer-xid')
    writer_xid=transaction['lines'][0]
    mutation='SELECT dataset_id FROM control.datasets FOR UPDATE;'+race_mutation(name,ready['binding'])
    barriers=[];held=None
    if not source_first:writer.execute(mutation)
    (output/(name+'.go')).touch(exist_ok=False)
    if source_first:
        next_event(process,'race-peer-ready')
        writer.start(mutation)
        barriers.append(blocked(pg,database,writer.backend,ready))
        (output/(name+'.peer')).touch(exist_ok=False)
        next_event(process,'race-peer-closed')
        held=next_event(proxy,'held')
        require([held[k] for k in ('rollback_requests','rollback_completions','rollback_released','rollback_ready')]==[4,3,3,3],'race-rollback-before-forward')
        barriers.append(blocked(pg,database,writer.backend,ready))
        proxy.send('release');next_event(proxy,'released')
        writer.expect(writer.finish())
        first=next_event(process,'race-finished')
        require(first['phase']==0 and first['received'] and {n:json.loads(v) for n,v in first['state']['control_tables']}==before,'source-first-result')
        writer.execute('COMMIT;')
        (output/(name+'.retry')).touch(exist_ok=False)
    else:
        barriers.append(blocked(pg,database,ready,writer.backend))
        writer.execute('ROLLBACK;' if name=='rollback' else 'COMMIT;')
        if name in ('rollback','ban-other'):
            next_event(process,'race-peer-ready');next_event(process,'race-peer-closed')
    finished=next_event(process,'race-finished')
    require(finished['phase']==int(source_first) and finished['received']==(name in ('rollback','ban-other')),'race-outcome')
    writer.close(lambda:lab.load(original));sessions.remove(writer)
    after=rows(database)
    require(after=={n:json.loads(v) for n,v in finished['state']['control_tables']},'race-final-observation')
    verify_race_change(before,after,ready['binding'],name,writer_xid)
    (output/(name+'.done')).touch(exist_ok=False)
    require(process.wait(10)==0 and not process.stderr,'race-probe-failed')
    receipt=next_event(proxy,'receipt');require(proxy.wait(5)==0 and not proxy.stderr,'race-proxy-failed')
    count=5 if source_first else 4
    require([receipt[k] for k in ('rollback_requests','rollback_completions','rollback_released','rollback_ready')]==[count]*4,'race-final-rollback-count')
    final=rows(database);require(final==after,'race-retirement-effects')
    verify_final(name,database,final,original_enrollment=False)
    report['cases'].append(dict(name=name,status='PASS',database=database,binding=ready['binding'],
        before=before,final=final,writer_xid=writer_xid,barriers=barriers,held=held,receipt=receipt,first=first if source_first else None,
        finished=finished,full_native_bytes=True,complete_control_tables=64))
    save()


processes, sessions = [], []

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
        '--mount', f'type=bind,src={build},dst=/build,readonly',
        '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(300)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    if jobs:
        from job_transport import run as run_jobs
        run_jobs(globals())
    elif races:
        for name in ('rollback', 'withdraw', 'generation', 'ban', 'ban-other', 'source-withdraw', 'source-ban'):
            run_race_case(name)
    elif cleanup:
        baseline = run_cleanup_case('pass', 0)
        for name in ('expired', 'timeout', 'ready'):
            run_cleanup_case(name, baseline + 1)
    else:
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
    for session in sessions:
        try:
            session.close(lambda: lab.load(original))
        except BaseException as error:
            errors.append('writer:' + type(error).__name__)
    for name, process in processes:
        try:
            process.stop()
        except BaseException as error:
            errors.append(name + ':stop:' + type(error).__name__)
        try:
            (output / (name + '.stdout')).write_bytes(process.stdout)
            (output / (name + '.stderr')).write_bytes(process.stderr)
        except BaseException as error:
            errors.append(name + ':logs:' + type(error).__name__)
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
