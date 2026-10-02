"""Installed SQL commit cuts; literal native/request oracle and independent PG reads."""
import hashlib
import json
from pathlib import Path
import shutil
import lab
from bootstrap_oracle import cbor, expected
from installed import append_private_expected, configure, messages, policy_setup, set_value, wait_for

CASES = dict([('commit-pass', ('pass', 4, 'stop'))]
    + [(f'{mode}-{number}', (wire, number, 'stop'))
       for number in range(1, 7)
       for mode, wire in [('before', 'before-forward'), ('after', 'after-complete')]]
    + [('cancel-reservation', ('hold', 3, 'close')),
       ('cancel-outcome', ('hold', 4, 'close')),
       ('delayed-outcome', ('hold', 4, 'release')),
       ('cancel-private-reservation', ('hold', 5, 'close')),
       ('cancel-private-outcome', ('hold', 6, 'close')),
       ('delayed-private-outcome', ('hold', 6, 'release'))])


def native_oracle(client, output, name, state, number, healthy, last_commit):
    committed = number >= 4 or healthy
    private_staged = number >= 5 or healthy
    private_committed = number >= 6 or healthy
    dump = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/'+name+'/store')
    with (output/(name+'-native.json')).open('x') as capture:
        capture.write(dump.stdout)
    actual = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value']))
              for r in json.loads(dump.stdout)['rows']]
    manifest = next(v for cf, k, v in actual if cf=='default' and k==b'\x01\x01')
    # The original process chose this opaque random identifier. All other bytes
    # come from independent SQL identities and the literal documented codec.
    recovery = manifest[78:94]
    assert len(recovery)==16 and any(recovery)
    owner = next(r for r in state['owner_epochs'] if r['role']=='ingest')
    dataset = bytes.fromhex(state['datasets'][0]['dataset_id'][2:])
    domains = sorted((r['dictionary_kind'], bytes.fromhex(r['allocation_domain'][2:]))
                     for r in state['allocation_domains'])
    assert [k for k, _ in domains]==list(range(1, 10)) and len(set(v for _, v in domains))==9
    slot = bytes.fromhex(state['producer_slots'][0]['producer_slot_id'][2:])
    wanted, receipt, _ = expected(dataset, committed, domain=dict(domains)[4], slot=slot,
        owner=bytes.fromhex(owner['resource_id'][2:]),
        incarnation=bytes.fromhex(owner['process_incarnation'][2:]), recovery=recovery,
        topology=state['datasets'][0]['topology_generation'], namespaces=domains,
        recorded_at=owner['acquired_at_us'])
    private_rows, private_input, private_receipt = append_private_expected(wanted, dataset,
        dict(domains)[4], slot, recovery, owner['acquired_at_us'], state['datasets'][0]['topology_generation'])
    if private_committed:
        wanted = private_rows
    assert actual == wanted and len(actual) == (17 if private_committed else 12 if committed else 1)
    ref = lambda n: [dataset, 4, dict(domains)[4], n]
    entries = [['dictionary', 1, [ref(n), 1, 'visibility', 1, {'class': label}]]
               for n, label in [(1, 'public'), (2, 'private')]]
    request = b'bigbrotr/producer-request/1\0'+cbor([[slot, 1, 1], 'admit-evidence', 1, [1, ref(1), entries]])
    inputs = sorted((output/name/'store/producer-inputs-v1').glob('*.cbor'))
    expected_inputs = [request, private_input] if private_staged else [request]
    assert [p.read_bytes() for p in inputs] == expected_inputs
    requests = sorted(state['producer_requests'], key=lambda r:int(r['request_sequence']))
    outcomes = sorted(state['producer_outcomes'], key=lambda r:int(r['request_sequence']))
    control = max((r for r in state['owner_epochs'] if r['role'] == 'control'),
                  key=lambda r:r['epoch'])
    common = dict(dataset_id='\\x'+dataset.hex(), producer_slot_id='\\x'+slot.hex(),
                  producer_epoch=1, manifest_sha256='\\x'+hashlib.sha256(manifest).hexdigest())
    for sequence, (row, original) in enumerate(zip(requests, expected_inputs), 1):
        wanted_request = dict(common, request_sequence=sequence,
            request_digest='\\x'+hashlib.sha256(original).hexdigest(),
            input_bytes=len(original), input_profile=1, issuer_role='control',
            issuer_resource=control['resource_id'], issuer_epoch=control['epoch'],
            issuer_incarnation=control['process_incarnation'], relay_endpoint_id=None,
            relay_access_scope_id=None, relay_enrollment_generation=None, relay_route_id=None)
        assert {k:v for k,v in row.items() if k not in ('xmin','reserved_at_us')} == wanted_request
        assert int(row['xmin']) > 0
        assert control['acquired_at_us'] <= row['reserved_at_us'] < control['lease_expires_at_us']
    for sequence, (row, original, expected_receipt) in enumerate(zip(outcomes, expected_inputs, [receipt, private_receipt]), 1):
        wanted_outcome = dict(common, request_sequence=sequence,
            request_digest='\\x'+hashlib.sha256(original).hexdigest(), outcome_kind=1,
            outcome_bytes='\\x'+expected_receipt.hex(),
            outcome_sha256='\\x'+hashlib.sha256(expected_receipt).hexdigest(),
            policy_epoch=1, recorder_role='control', recorder_resource=control['resource_id'],
            recorder_epoch=control['epoch'], recorder_incarnation=control['process_incarnation'])
        assert {k:v for k,v in row.items() if k not in ('xmin','recorded_at_us')} == wanted_outcome
        assert int(row['xmin']) > 0
        assert requests[sequence-1]['reserved_at_us'] <= row['recorded_at_us'] < control['lease_expires_at_us']
    assert state['producer_epochs'][0]['outstanding_request_limit']==32
    assert len(requests)==int(last_commit>=3)+int(last_commit>=5)
    assert len(outcomes)==int(last_commit>=4)+int(last_commit>=6)
    return len(actual)


def backend(pg, database):
    value = json.loads(lab.sql(pg, database, """
        SELECT coalesce(json_agg(json_build_object('pid',a.pid,'backend_start',a.backend_start,
            'state',a.state,'xact_start',a.xact_start,'locks',
            (SELECT json_agg(json_build_object('type',l.locktype,'mode',l.mode,'granted',l.granted,
                 'relation',l.relation::regclass::text)) FROM pg_locks l WHERE l.pid=a.pid))), '[]')
          FROM pg_stat_activity a
         WHERE a.datname=current_database() AND a.usename='bigbrotr_control'
    """))
    return value


def require_held(pg, database, rows):
    observed = backend(pg, database)
    assert len(observed)==1 and observed[0]['state']=='idle in transaction'
    assert observed[0]['xact_start'] is not None
    assert any(l['type']=='advisory' and l['granted'] for l in observed[0]['locks'])
    control = next(r for r in rows['owner_epochs'] if r['role']=='control')
    assert observed[0]['pid']==control['control_backend_pid']
    assert observed[0]['backend_start']==control['control_backend_start']
    return observed


def run(pg, client, output, database, name, rows):
    mode, number, action = CASES[name]
    script = output/'commit_client.py'
    if not script.exists():
        shutil.copy2(Path(__file__).with_name(script.name), script)
    policy_setup(pg, database, name, rows, output)
    before = rows(database)
    lab.write_json(output/(name+'-before.json'), before)
    state = dict(pg, client=client)
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    config_path = output/Path(config).name
    text = configure(config_path.read_text(), before, name)
    for key, value in [('lease_ttl', '30s'), ('renewal_timeout', '5s')]:
        text = set_value(text, 'runtime', key, value)
    # Keep a direct endpoint for retry and a distinct absent-target contender.
    direct = '/lab/'+name+'-direct.toml'
    (output/Path(direct).name).write_text(text)
    competing = '/lab/'+name+'-competing.toml'
    (output/Path(competing).name).write_text(set_value(text, 'storage', 'ingest_root', '/lab/'+name+'/competitor'))
    assert lab.cli(state, 'config', 'validate', '--file', competing).returncode==0
    (output/name).mkdir(mode=0o700)
    prefix = output/name
    path = lambda suffix: Path(str(prefix)+suffix)
    lab.docker('exec', '-d', client, 'python3', '-B', '/lab/commit_client.py', config,
               '/lab/'+name, mode, str(number), action)
    wait_for(lambda: path('.proxy-ready.json').exists() or path('.terminal.json').exists(), 'proxy ready', 5)
    ready = json.loads(path('.proxy-ready.json').read_text())
    proxy_text = set_value(text, 'postgres.control', 'endpoint', '127.0.0.1:'+str(ready['listen_port']))
    config_path.write_text(proxy_text)
    validated = lab.cli(state, 'config', 'validate', '--file', config)
    (output/(name+'-validate.stdout')).write_text(validated.stdout)
    (output/(name+'-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode==0
    with path('.config-ready').open('x') as stream:
        stream.write('validated\n')
    if mode=='hold':
        wait_for(lambda: path('.held.json').exists() or path('.terminal.json').exists(), 'commit hold', 7)
        assert path('.held.json').exists()
        held_rows = rows(database)
        observed = require_held(pg, database, held_rows)
        competitor = '''import subprocess,sys
r=subprocess.run(['/build/bigbrotr','serve','--file',sys.argv[1],'--initialize-ingest','--roles','control,ingest'],env={},timeout=.75)
sys.exit(r.returncode)
'''
        conflict = lab.docker('exec', client, 'python3', '-c', competitor, competing, check=False, timeout=3)
        (output/(name+'-competing.stdout')).write_text(conflict.stdout)
        (output/(name+'-competing.stderr')).write_text(conflict.stderr)
        assert conflict.returncode==3 and 'Control backend operation failed: Busy' in conflict.stderr
        assert not (output/name/'competitor').exists() and rows(database)==held_rows
        lab.write_json(output/(name+'-held-sql.json'), dict(backend=observed, rows=held_rows))
        with path('.observed').open('x') as stream:
            stream.write('observed\n')
        if action=='close':
            wait_for(lambda: path('.client-dead.json').exists() or path('.terminal.json').exists(), 'client death', 4)
            assert path('.client-dead.json').exists()
            assert rows(database)==held_rows
            lab.write_json(output/(name+'-after-client-death.json'), dict(
                backend=require_held(pg, database, held_rows), rows=rows(database)))
            with path('.upstream-observed').open('x') as stream:
                stream.write('observed\n')
    wait_for(path('.terminal.json').exists, 'commit terminal', 10)
    terminal = json.loads(path('.terminal.json').read_text())
    assert terminal['status']=='PASS' and not terminal['cleanup']['forced'], terminal
    wait_for(lambda: not backend(pg, database), 'server transaction ended', 3)
    after = rows(database)
    lab.write_json(output/(name+'-after.json'), after)
    healthy = mode=='pass' or action=='release'
    last_commit = terminal['receipt']['server_commit_complete'] if healthy else number-(mode!='after-complete')
    assert len(after['owner_epochs'])==len(after['role_instances'])==(2 if last_commit>=2 else last_commit)
    for table in ['source_streams', 'topology_generations', 'topology_streams', 'producer_slots', 'producer_epochs']:
        assert len(after[table])==int(last_commit>=2), table
    assert len(after['allocation_domains'])==9*int(last_commit>=2)
    assert len(after['producer_requests'])==int(last_commit>=3)+int(last_commit>=5)
    assert len(after['producer_outcomes'])==int(last_commit>=4)+int(last_commit>=6)
    allowed = {'datasets','role_instances','owner_epochs','topology_generations','topology_streams',
               'source_streams','allocation_domains','producer_slots','producer_epochs','producer_requests','producer_outcomes'}
    assert {t for t in before if before[t]!=after[t]}<=allowed
    if last_commit==0:
        assert before==after
    if mode=='hold' and action=='close':
        assert after==held_rows
        assert terminal['cli_death_monotonic']-terminal['signal_monotonic']<3.5
    report = messages(prefix)
    if healthy:
        assert [r['state'] for r in report]==['initialized', 'stopped']
        assert report[0]['ready'] is False and report[-1]['native']=='closed'
        assert report[-1]['owner_release']=='committed'
        assert next(r for r in after['owner_epochs'] if r['role']=='control')['state']=='released'
        assert not path('.stderr').read_text()
    else:
        assert not report and path('.stderr').read_text()
        assert all(r['state']=='active' and r['released_at_us'] is None and r['exclusion_digest'] is None
                   for r in after['owner_epochs'])
    native_count = 0
    if number>=3 or healthy:
        native_count = native_oracle(client, output, name, after, number, healthy, last_commit)
        retry = lab.cli(state, 'serve', '--file', direct, '--initialize-ingest', '--roles', 'control,ingest')
        (output/(name+'-retry.stderr')).write_text(retry.stderr)
        assert retry.returncode==3 and 'target exists' in retry.stderr and not retry.stdout
        assert rows(database)==after
    else:
        assert not (output/name/'store').exists()
    return dict(name=name, status='PASS', actual_cli=True, control_tables=len(after),
                committed_prefix=last_commit, native_rows=native_count,
                terminal=terminal)
