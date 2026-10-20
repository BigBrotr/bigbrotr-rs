#!/usr/bin/env python3
"""Prepare one fresh signed publication and genuine grants for Export acceptance.

Run only in a separately inventoried root-owned setup container/namespace.
This script does not create engines, build executables, execute measurement,
copy native roots, or recover an uncertain earlier attempt.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tomllib

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
# Explicit inert helper resolution: export-execution/lab.py is a runtime runner
# and must never be imported under the short name used by old pure oracles.
sys.path[:0] = [str(SOURCE / 'experiments/control-migrations'), str(SOURCE / 'experiments/ingest-bootstrap')]
module = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
bootstrap = importlib.util.module_from_spec(module)
sys.modules['bootstrap_oracle'] = bootstrap
module.loader.exec_module(bootstrap)

from installed import set_value
from memory_relay import signed_event, RECIPE_SHA256
from memory_setup_io import IO, census, pinned, publication_evidence, regular, require, sha, write
from memory_setup_client import ACTIONS, Client, Peer
from memory_setup_oracle import grants, one, publication

KEYS = {'activate', 'owner', 'endpoint', 'enrollment', 'scope', 'acquisition', 'accept-export', *ACTIONS}


def read_spec(path, *, fresh=True):
    spec = json.loads(regular(path, 65536))
    require(set(spec) - {'event_kind'} == {'format', 'name', 'root', 'config', 'application', 'application_source',
        'probe', 'ingest_probe', 'psql', 'worker', 'recipe', 'inventory', 'source_manifest',
        'operator', 'migrator', 'username', 'password_ref', 'keys', 'limits'},
        'closed fresh setup descriptor')
    require(spec['format'] == 'bigbrotr-export-fresh-setup/1' and
            re.fullmatch('[A-Za-z0-9-]{1,64}', spec['name']), 'setup identity')
    require(re.fullmatch('[0-9a-f]{40}', spec['application_source']), 'explicit actual application revision')
    require(set(spec['keys']) == KEYS and len(set(spec['keys'].values())) == len(KEYS)
            and all(re.fullmatch('[0-9a-f]{32}', key) and int(key, 16) for key in spec['keys'].values()),
            'unique persistent command keys')
    for key in ('application', 'probe', 'ingest_probe', 'psql'):
        pinned(spec[key], executable=True)
    config = tomllib.loads(pinned(spec['config']).decode())
    pinned(spec['inventory'])
    pinned(spec['source_manifest'])
    kind = spec.get('event_kind', 'frozen-archive-recipe')
    require(kind in ('frozen-archive-recipe', 'frozen-signed-event'), 'explicit frozen input kind')
    require(kind != 'frozen-archive-recipe' or spec['recipe']['sha256'] == RECIPE_SHA256,
            'frozen independent signed recipe')
    pinned(spec['recipe'])
    signed_event(spec['recipe']['path'], kind=kind)
    require(set(spec['worker']) == {'root', 'bytes', 'sha256'}, 'explicit packaged worker')
    require(Path(spec['worker']['root']) == Path(spec['application']['path']).parent / 'archive-worker',
            'ordinary Serve selects the exact application-adjacent packaged worker')
    pinned(dict(path=str(Path(spec['worker']['root']) / 'bigbrotr-archive-worker'),
                bytes=spec['worker']['bytes'], sha256=spec['worker']['sha256']), executable=True)
    for key in ('operator', 'migrator'):
        require(set(spec[key]) == {'user', 'password_ref'}
                and re.fullmatch('[a-z][a-z0-9_]{0,62}', spec[key]['user'])
                and spec[key]['password_ref'].startswith('file:'), 'protected operator identity')
        regular(spec[key]['password_ref'][5:], 4096)
    require(spec['password_ref'].startswith('file:') and re.fullmatch('[a-z][a-z0-9_-]{0,63}', spec['username']),
            'actual enrollment credential reference')
    regular(spec['password_ref'][5:], 4096)
    root = Path(spec['root'])
    require(root.is_absolute() and root.resolve() == root, 'canonical setup root')
    require(not root.exists() if fresh else root.is_dir() and root.stat().st_mode & 0o777 == 0o700,
            'fresh root or original private continuation root')
    require(config['config']['authority_mode'] == 'managed', 'accept probe managed authority')
    require(config['topology']['local_roles'] == ['control', 'ingest', 'sync', 'archive'], 'explicit initial role profile')
    require(config['archive']['publisher']['enabled'] and not config['archive']['reader']['enabled'],
            'initial publisher policy and disabled initial reader')
    require(config['administration']['enabled'] and config['administration']['listen']['port'] == 18081,
            'actual private administration listener')
    for key, directory in [('ingest_root', 'store'), ('archive_root', 'data'),
                           ('archive_state_root', 'state'), ('scratch_root', 'scratch')]:
        require(config['storage'][key] == str(root / directory), 'same fresh physical root: ' + key)
    require(set(config['export']['destination']) == {'id', 'root'}
            and config['export']['destination']['root'] == str(root / 'destination'), 'fresh paired destination')
    for name in ('control', 'catalog'):
        entry = config['postgres'][name]
        require(entry['endpoint'] == '127.0.0.1:5432' and entry['tls']['mode'] == 'local-plaintext'
                and entry['tls']['local_boundary'] == 'loopback'
                and re.fullmatch('[a-z][a-z0-9_]{0,62}', entry['database'])
                and re.fullmatch('[a-z][a-z0-9_]{0,62}', entry['user']), 'explicit loopback database binding')
    require(config['postgres']['control']['database'] != config['postgres']['catalog']['database'], 'separate databases')
    refs = [spec['password_ref'], spec['operator']['password_ref'], spec['migrator']['password_ref'],
            config['postgres']['control']['password_ref'], config['postgres']['catalog']['password_ref'],
            config['postgres']['catalog']['read_password_ref']]
    for reference in refs:
        require(reference.startswith('file:'), 'explicit local credential file')
        regular(reference[5:], 4096)
        require(Path(reference[5:]).stat().st_mode & 0o077 == 0, 'private credential permissions')
    require(set(spec['limits']) == {'memory_max_bytes', 'baseline_bytes', 'data_bytes', 'native_bytes',
        'control_bytes', 'engine_memory_bytes', 'native_reserve_bytes', 'row_bytes', 'scan_bytes',
        'temp_bytes', 'export_bytes', 'disk_reserve_bytes', 'query_ms'}
        and all(type(value) is int and value > 0 for value in spec['limits'].values()), 'closed finite limits')
    require(spec['limits']['row_bytes'] == 1048576 and spec['limits']['disk_reserve_bytes'] == 64 * 1024**3,
            'unchanged full row and disk reserve profile')
    require(shutil.disk_usage(root.parent).free >= spec['limits']['disk_reserve_bytes'] + 577 * 1024**2,
            'unchanged completion reserve plus setup headroom')
    return spec, config


def stage_profiles(io):
    """Freeze deployment selections; both destination fields are policy-excluded roots."""
    original = pinned(io.spec['config'])
    text = original.decode()
    publisher, count = re.subn(r'(?ms)^\[export\.destination\][ \t]*(?:#[^\n]*)?\n.*?(?=^\[|\Z)', '', text)
    require(count == 1, 'one explicit paired destination section in the supplied recipe')
    expected = tomllib.loads(text)
    del expected['export']['destination']
    selected = tomllib.loads(publisher)
    if not expected['export'] and 'export' not in selected:
        del expected['export']
    require(selected == expected, 'publisher differs only by omitting both destination fields')
    bodies = dict(destination=original, publisher=publisher.encode(),
                  ordinary=set_value(text, 'archive.reader', 'enabled', True).encode())
    for name, raw in bodies.items():
        path = io.root / (name + '.toml')
        with path.open('xb') as stream:
            stream.write(raw)
        path.chmod(0o400)
        io.profiles[name] = dict(path=str(path), bytes=len(raw), sha256=sha(raw))
    write(io.root / 'profiles.json', io.profiles)
    reports = {name: io.cli('validate-' + name, 'config', 'validate', '--file', pin['path'])
               for name, pin in io.profiles.items()}
    for name, report in reports.items():
        require(report['status'] == 'valid-static' and report['document_sha256'] == io.profiles[name]['sha256']
                and report['initial_policy'] == reports['destination']['initial_policy'],
                'each actual profile validates with the same accepted-policy candidate')
    write(io.root / 'profile-validation.json', reports)


def initialize(io, config_path):
    """Real migration/initialization/enrollment; SQL is operator privileges only."""
    config, spec = io.config, io.spec
    control, catalog = (config['postgres'][name]['database'] for name in ('control', 'catalog'))
    require(not io.tables(control) and not io.tables(catalog), 'both databases actually pristine')
    io.backends('fresh', absent=True)
    info = io.cli('build-info', 'build-info')
    require(info['source']['revision'] == spec['application_source'] and not info['source']['dirty']
            and info['archive_worker'] == {key: spec['worker'][key] for key in ('bytes', 'sha256')},
            'real parent/worker build binding')
    verification = io.cli('verify-worker', 'archive', 'verify-worker', '--root', spec['worker']['root'])
    require(verification['result'] == 'verified', 'real full native bundle verification')
    io.cli('migrate', 'migrate', 'apply', '--file', config_path, '--user', spec['migrator']['user'],
           '--password-ref', spec['migrator']['password_ref'])
    application = config['postgres']['control']['user']
    # SQL grants are only database/operator provisioning. Every scientific,
    # policy, principal, scope, receipt and job row is written by real consumers.
    io.sql(control, f'GRANT USAGE ON SCHEMA control TO {application}; '
        f'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO {application}; '
        f'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO {application}; '
        f'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO {application};', mutation=True)
    schema = io.capture('migrated')
    require(all(not rows for table, rows in schema['control'].items() if table != 'schema_versions'),
            'migration did not seed authority')
    initialized = io.cli('config-initialize', 'config', 'initialize', '--file', config_path)
    require(initialized['status'] == 'initialized', 'genuine fresh config initialization')
    activated = io.cli('activate-initial', 'config', 'activate-initial', '--file', config_path,
        '--request-id', spec['keys']['activate'], '--expected-revision', '1')
    require(activated['status'] == 'activated', 'genuine first policy activation')
    issued = io.cli('owner-token', 'access', 'owner-token', 'issue', '--file', config_path,
        '--username', spec['username'], '--request-id', spec['keys']['owner'])
    token = io.root / 'owner-token.private'
    require(re.fullmatch('[0-9a-f]{64}', issued['token']), 'actual enrollment token')
    with token.open('x') as stream:
        stream.write(issued['token'])
    io.cli('enroll-owner', 'access', 'enroll-owner', '--file', config_path,
        '--challenge-id', spec['keys']['owner'], '--token-ref', 'file:' + str(token),
        '--password-ref', spec['password_ref'])
    io.cli('owner-status', 'access', 'owner-token', 'status', '--file', config_path)
    io.cli('owner-me', 'access', 'me', '--file', config_path, '--username', spec['username'],
           '--password-ref', spec['password_ref'])
    io.cli('archive-initialize', 'archive', 'initialize', '--file', config_path,
        '--initializer-user', spec['operator']['user'], '--initializer-password-ref', spec['operator']['password_ref'],
        '--worker-root', spec['worker']['root'])
    io.cli('state-initialize', 'archive', 'state-initialize', '--file', config_path)
    io.cli('coordinator-initialize', 'archive', 'coordinator-initialize', '--file', config_path)
    io.cli('destination-initialize', 'export', 'destination-initialize', '--file', config_path)
    reader = config['postgres']['catalog']['read_user']
    require(re.fullmatch('[a-z][a-z0-9_]{0,62}', reader), 'reader role identifier')
    io.sql(catalog, f'GRANT CONNECT ON DATABASE {catalog} TO {reader}; '
        f'GRANT USAGE ON SCHEMA public,bigbrotr_archive TO {reader}; '
        f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO {reader}; '
        f'GRANT SELECT ON bigbrotr_archive.installation,bigbrotr_archive.scientific_schema TO {reader};',
        mutation=True)
    before = io.capture('initialized')
    io.backends('initialized', absent=True)
    require(not before['control']['scope_grants'] and not before['control']['jobs']
            and not before['catalog']['bigbrotr_archive.attempts'], 'no fabricated grants/job/publication')
    return before


def start_serve(io, config_path, initial):
    label = 'publisher' if initial else 'grant-control'
    args = [io.spec['application']['path'], 'serve', '--file', str(config_path), '--roles',
            'control,ingest,sync,archive' if initial else 'control']
    if initial:
        args.append('--initialize-ingest')
    child = io.start(label, args)
    def started():
        require(child.child.poll() is None, 'Serve exited before initialization')
        return child.messages()
    messages = io.wait(started, label + ' startup')
    require(messages[0]['state'] == ('initialized' if initial else 'idle'), 'actual Serve startup state')
    io.backends(label + '-active')
    return child, messages[0]


def immutable(root):
    return {name: census(root / name) for name in ('store', 'data', 'state', 'destination')}


def publish(io):
    config_path = Path(io.profiles['destination']['path'])
    before = initialize(io, config_path)
    principal = one(before['control']['local_accounts'])['principal_id'][2:]
    kind = io.spec.get('event_kind', 'frozen-archive-recipe')
    body = signed_event(io.spec['recipe']['path'], kind=kind)
    with (io.root / 'event.json').open('xb') as stream:
        stream.write(body)
    peer = Peer(io.root, body, kind=kind)
    publisher = None
    try:
        publisher, initialized = start_serve(io, Path(io.profiles['publisher']['path']), True)
        client = Client(io, 'acquisition', principal)
        dataset = one(before['control']['datasets'])
        revisions = dict(expected_config_revision=str(dataset['config_revision']),
                         expected_policy_revision=str(dataset['policy_revision']))
        receipts = client.acquire(initialized['private_scope'], revisions)

        def positive():
            require(not peer.errors and publisher.child.poll() is None, 'publisher or peer failed')
            state = io.tables(io.config['postgres']['control']['database'], 'control')
            # A candidate only. Full independent native/cut verification follows
            # after the actual publisher and native/SQL owners have joined.
            snapshots = state['archive_frontier_snapshots']
            return state if any(row['payload_count'] == row['eligibility_count'] == row['receive_count'] ==
                row['provenance_count'] == 1 for row in snapshots) and peer.transcript() else None

        io.wait(positive, 'actual published signed event candidate', 50)
        client.logout()
        (peer.root / 'stop-requested').write_text('owned publisher stop\n')
        io.stop(publisher)
    finally:
        if publisher is not None and not publisher.ended:
            # Failure preserves unknown effects. The outer handler audits rather
            # than proceeding to native reads, grants or a replacement process.
            (peer.root / 'stop-requested').touch(exist_ok=True)
            io.failure_custody()
        peer.joined()
    published = io.capture('published')
    require(published['catalog']['bigbrotr_archive.attempts'], 'actual native publications')
    fixed = immutable(io.root)
    write(io.root / 'published-files.json', fixed)
    write(io.root / 'publisher-scratch.json', census(io.root / 'scratch'))
    # No native observer or ordinary reader can run in this invocation. Root
    # must first join the entire outer container/tracer and authenticate custody.
    write(io.root / 'publication.json', dict(format='bigbrotr-export-publication/1',
        status='PUBLISHED_AWAITING_ROOT_CUSTODY', root=str(io.root), profiles=io.profiles,
        sql_serial=io.serial, files=fixed, evidence=publication_evidence(io.root),
        native_observers='NOT_RUN', grants='NOT_RUN', acceptance='NOT_RUN', measurement='NOT_RUN'))


def prepare(io, checkpoint):
    """Continue only after the caller-authenticated outer custody boundary."""
    config_path = Path(io.profiles['destination']['path'])
    before = json.loads(regular(io.root / 'initialized.json'))
    published = json.loads(regular(io.root / 'published.json'))
    initialized = json.loads(regular(io.root / 'publisher.stdout').splitlines()[0])
    receipts = json.loads(regular(io.root / 'acquisition-receipts.json'))
    transcript = [json.loads(regular(path)) for path in sorted((io.root / 'relay').glob('relay-closed-*.json'))]
    require(json.loads(regular(io.root / 'relay/thread-join.json')) == dict(alive=False, errors=[]),
            'original relay joined without error')
    principal = one(before['control']['local_accounts'])['principal_id'][2:]
    dataset = one(before['control']['datasets'])
    revisions = dict(expected_config_revision=str(dataset['config_revision']),
                     expected_policy_revision=str(dataset['policy_revision']))
    fixed = checkpoint['files']
    require(fixed == json.loads(regular(io.root / 'published-files.json'))
            and immutable(io.root) == fixed, 'original preobserver physical baseline after root custody')
    require(census(io.root / 'scratch') == json.loads(regular(io.root / 'publisher-scratch.json')),
            'original publication scratch after root custody')
    io.backends('continuation-before', absent=True)
    require(io.capture('continuation-before') == published, 'all original 75 tables/xmin and catalog after root custody')
    native = json.loads(io.run('native-dump', [io.spec['ingest_probe']['path'], 'dump', str(io.root / 'store')]))
    io.backends('native-dump', absent=True)
    io.bind_profile('scientific-census', config_path)
    row_option = ['--row-bytes', '1048576'] if io.spec.get('event_kind') == 'frozen-signed-event' \
        else ['--large-signed-row']
    readback = json.loads(io.run('scientific-census', [sys.executable, '-B',
        str(SOURCE / 'experiments/ingest-bootstrap/installed_archive_read.py'), str(config_path),
        str(io.root / 'census-scratch'), *row_option, '--worker-root', io.spec['worker']['root']]))
    io.backends('scientific-census', absent=True)
    require(io.capture('publication-readback') == published, 'read-only native census preserved whole SQL/catalog')
    require(immutable(io.root) == fixed, 'native observers preserved original physical science')
    selection = publication(io, before, published, initialized, native, readback, transcript, receipts)
    # Select the previously frozen ordinary profile; no live file or policy edit.
    ordinary = Path(io.profiles['ordinary']['path'])
    child, _ = start_serve(io, ordinary, False)
    grants_client = Client(io, 'grants', principal)
    allocation = dict(catalog_id=selection['catalog'], frontier_revision=str(selection['frontier_revision']))
    grants_receipts = grants_client.grant(initialized['private_scope'], allocation, revisions,
        one(published['control']['local_accounts'])['grant_revision'])
    grants_client.logout()
    io.stop(child)
    after = io.capture('granted')
    require(after['catalog'] == published['catalog'] and immutable(io.root) == fixed,
            'entire original scientific/root custody preserved through grants')
    grants(published['control'], after['control'], principal, initialized['private_scope'], selection, grants_receipts)
    common = dict(name=io.spec['name'], dataset=dataset['dataset_id'][2:], instance=dataset['instance_id'][2:],
        control={key: io.config['postgres']['control'][key] for key in ('endpoint', 'database', 'user', 'password_ref')},
        catalog={key: io.config['postgres']['catalog'][key] for key in ('endpoint', 'database', 'user', 'password_ref')},
        reader_user=io.config['postgres']['catalog']['read_user'],
        reader_password_ref=io.config['postgres']['catalog']['read_password_ref'],
        data_root=str(io.root / 'data') + '/', state_root=str(io.root / 'state'), destination=str(io.root / 'destination'),
        worker_root=io.spec['worker']['root'], worker_bytes=io.spec['worker']['bytes'],
        worker_sha256=io.spec['worker']['sha256'], limits=io.spec['limits'])
    for name, table in [('config', 'config_revisions'), ('policy', 'policy_revisions')]:
        stored = one(after['control'][table])
        path = io.root / (name + '-accepted.json')
        write(path, stored['document'])
        common[name] = dict(path=str(path), sha256=sha(path.read_bytes()), stored_digest=stored['document_digest'][2:])
    query = dict(version=1, resource='event_history', mode='snapshot', scope=initialized['private_scope'],
                 filters=[dict(ids=[selection['query']['event_id']])])
    marker = io.root / 'destination/destination.json'
    command = dict(format='signed-events-jsonl', query=query, allocation=allocation,
        destination=dict(id=io.config['export']['destination']['id'], binding_sha256=sha(marker.read_bytes())), **revisions)
    scratch = io.root / 'accept-scratch'
    scratch.mkdir(mode=0o700)
    case = dict(format='bigbrotr-export-accept-setup/1', **common,
        scratch=str(scratch), ledger=str(io.root / 'accept-ledger.jsonl'),
        username=io.spec['username'], password_ref=io.spec['password_ref'],
        command_key=io.spec['keys']['accept-export'], command=command)
    write(io.root / 'accept.json', case)
    for pin in io.profiles.values():
        pinned(pin)
    write(io.root / 'prepared.json', dict(status='PREPARED_FOR_ROOT_ACCEPTANCE', principal=principal,
        acceptance_runtime='NOT_RUN', measurement_runtime='NOT_RUN', selection=selection, files=fixed,
        profiles=io.profiles))
    return case, selection, after, fixed


def run_publish(spec, config):
    os.umask(0o077)
    def interrupted(_signal, _frame):
        raise InterruptedError('owned setup signal')
    signal.signal(signal.SIGTERM, interrupted)
    root = Path(spec['root'])
    root.mkdir(mode=0o700)
    for name in ('data', 'state', 'scratch', 'destination'):
        (root / name).mkdir(mode=0o700)
    write(root / 'input.json', spec)
    io = IO(root, spec, config)
    try:
        stage_profiles(io)
        publish(io)
    except BaseException as error:
        io.failure_custody()
        write(root / 'failure.json', dict(status='UNKNOWN_PRESERVED', error=type(error).__name__,
            acceptance='NOT_RUN', measurement='NOT_RUN', retry='forbidden until root resolves actual custody/outcome'))
        raise


def root_custody(root, path, expected_sha256):
    """Authenticate root's explicit receipt; its raw-evidence audit is external."""
    require(path.is_absolute() and root not in path.parents
            and re.fullmatch('[0-9a-f]{64}', expected_sha256), 'external explicit root custody receipt')
    raw = regular(path, 65536)
    require(sha(raw) == expected_sha256, 'caller-pinned root custody receipt bytes')
    receipt = json.loads(raw)
    require(set(receipt) == {'format', 'status', 'root', 'publication', 'source_manifest', 'inventory',
        'container_id', 'cgroup_path', 'tracer', 'identity', 'evidence', 'checks'}, 'closed root custody receipt')
    require(receipt['format'] == 'bigbrotr-export-publication-custody/1'
            and receipt['status'] == 'VERIFIED_FOR_NATIVE_REOPEN' and receipt['root'] == str(root)
            and re.fullmatch('[0-9a-f]{64}', receipt['container_id'])
            and Path(receipt['cgroup_path']).is_absolute()
            and receipt['identity'] == 'trace-birth-exec-lineage', 'explicit outer custody identity')
    checks = {'natural_exit', 'empty_cgroup', 'backends_absent', 'physical_roots_unchanged',
              'complete_worker_lifetimes', 'no_other_credential_holders'}
    require(set(receipt['checks']) == checks and all(value is True for value in receipt['checks'].values()),
            'root verified every required custody boundary')
    require(set(receipt['evidence']) == {'trace', 'trace_audit', 'container', 'cgroup', 'backends', 'mounts'},
            'complete root evidence pins')
    pinned(receipt['tracer'], executable=True)
    for pin in receipt['evidence'].values():
        require(0 < pin['bytes'] <= 16 * 1024 * 1024, 'bounded nonempty outer custody evidence')
        pinned(pin)
    require(receipt['publication']['path'] == str(root / 'publication.json')
            and 0 < receipt['publication']['bytes'] <= 16 * 1024 * 1024, 'same bounded publication checkpoint')
    checkpoint = json.loads(pinned(receipt['publication']))
    require(set(checkpoint) == {'format', 'status', 'root', 'profiles', 'sql_serial', 'files', 'evidence',
        'native_observers', 'grants', 'acceptance', 'measurement'}
        and checkpoint['format'] == 'bigbrotr-export-publication/1'
        and checkpoint['status'] == 'PUBLISHED_AWAITING_ROOT_CUSTODY'
        and checkpoint['root'] == str(root)
        and all(checkpoint[key] == 'NOT_RUN' for key in ('native_observers', 'grants', 'acceptance', 'measurement')),
        'known original publication before any observer')
    require(publication_evidence(root, {'publication.json', 'continuation-started.json'}) == checkpoint['evidence'],
            'all original publication inputs/profiles/PID/terminal/relay/raw evidence unchanged')
    spec, config = read_spec(root / 'input.json', fresh=False)
    require(spec['root'] == str(root)
            and all(receipt[key] == spec[key] for key in ('source_manifest', 'inventory')),
            'same original setup source and root inventory')
    profiles = checkpoint['profiles']
    require(set(profiles) == {'destination', 'publisher', 'ordinary'}
            and profiles == json.loads(regular(root / 'profiles.json')), 'same frozen profile inventory')
    for name, pin in profiles.items():
        require(pin['path'] == str(root / (name + '.toml'))
                and Path(pin['path']).stat().st_mode & 0o777 == 0o400, 'original frozen profile selection')
        pinned(pin)
    require(pinned(profiles['destination']) == pinned(spec['config']), 'original supplied recipe')
    serials = [int(Path(name).name[4:-11]) for name in checkpoint['evidence']
               if re.fullmatch(r'sql-[0-9]+\.query\.json', name)]
    require(type(checkpoint['sql_serial']) is int and 1 <= checkpoint['sql_serial'] <= 8192
            and sorted(serials) == list(range(1, checkpoint['sql_serial'] + 1)), 'complete prior SQL evidence serial')
    return receipt, checkpoint, spec, config


def run_continue(root, receipt_path, expected_sha256):
    os.umask(0o077)
    require(root.is_absolute() and root.resolve() == root and root.is_dir()
            and root.stat().st_mode & 0o777 == 0o700, 'original private root')
    require(not (root / 'failure.json').exists() and not (root / 'continuation-failure.json').exists()
            and not (root / 'result.json').exists(), 'no failed or previously completed continuation')
    # The marker is never removed. Even validation failure requires root recovery,
    # not a second continuation or a new receipt hiding the original attempt.
    write(root / 'continuation-started.json', dict(receipt=str(receipt_path), sha256=expected_sha256))
    io = IO(root, {}, {})
    def interrupted(_signal, _frame):
        raise InterruptedError('owned continuation signal')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        receipt, checkpoint, spec, config = root_custody(root, receipt_path, expected_sha256)
        io.spec, io.config = spec, config
        io.profiles, io.serial = checkpoint['profiles'], checkpoint['sql_serial']
        write(root / 'authenticated-custody.json', receipt)
        prepare(io, checkpoint)
        write(root / 'result.json', dict(status='PREPARED_FOR_ROOT_ACCEPTANCE',
            accept=str(root / 'accept.json'), accept_sha256=sha((root / 'accept.json').read_bytes()),
            command=[spec['probe']['path'], 'accept', str(root / 'accept.json')],
            publication_sha256=receipt['publication']['sha256'], root_custody_sha256=expected_sha256,
            qualification='source candidate; execution outcome must be audited',
            acceptance='NOT_RUN', measurement='NOT_RUN', seconds=120-io.remaining()))
    except BaseException as error:
        io.failure_custody()
        write(root / 'continuation-failure.json', dict(status='UNKNOWN_PRESERVED', error=type(error).__name__,
            acceptance='NOT_RUN', measurement='NOT_RUN', retry='forbidden until root resolves actual custody/outcome'))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='phase', required=True)
    publisher = commands.add_parser('publish')
    publisher.add_argument('descriptor', type=Path)
    continuation = commands.add_parser('continue')
    continuation.add_argument('root', type=Path)
    continuation.add_argument('custody_receipt', type=Path)
    continuation.add_argument('custody_sha256')
    args = parser.parse_args()
    if args.phase == 'publish':
        run_publish(*read_spec(args.descriptor))
    else:
        run_continue(args.root, args.custody_receipt, args.custody_sha256)
