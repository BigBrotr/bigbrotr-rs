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
from memory_relay import signed_event, EVENT_ID, RECIPE_SHA256
from memory_setup_io import IO, census, pinned, regular, require, sha, write
from memory_setup_client import ACTIONS, Client, Peer
from memory_setup_oracle import grants, one, publication

KEYS = {'activate', 'owner', 'endpoint', 'enrollment', 'scope', 'acquisition', 'accept-export', *ACTIONS}


def read_spec(path):
    spec = json.loads(regular(path, 65536))
    require(set(spec) == {'format', 'name', 'root', 'config', 'application', 'application_source',
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
    require(spec['recipe']['sha256'] == RECIPE_SHA256, 'frozen independent signed recipe')
    pinned(spec['recipe'])
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
    require(root.is_absolute() and root.resolve() == root and not root.exists(), 'fresh root only')
    require(config['config']['authority_mode'] == 'managed', 'accept probe managed authority')
    require(config['topology']['local_roles'] == ['control', 'ingest', 'sync', 'archive'], 'explicit initial role profile')
    require(config['archive']['publisher']['enabled'] and not config['archive']['reader']['enabled'],
            'initial publisher policy and disabled initial reader')
    require(config['administration']['enabled'] and config['administration']['listen']['port'] == 18081,
            'actual private administration listener')
    for key, directory in [('ingest_root', 'store'), ('archive_root', 'data'),
                           ('archive_state_root', 'state'), ('scratch_root', 'scratch')]:
        require(config['storage'][key] == str(root / directory), 'same fresh physical root: ' + key)
    require(config['export']['destination']['root'] == str(root / 'destination'), 'fresh destination')
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
    io.cli('config-validate', 'config', 'validate', '--file', config_path)
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


def prepare(io, config_path):
    before = initialize(io, config_path)
    principal = one(before['control']['local_accounts'])['principal_id'][2:]
    body = signed_event(io.spec['recipe']['path'])
    with (io.root / 'event.json').open('xb') as stream:
        stream.write(body)
    peer = Peer(io.root, body)
    publisher = None
    try:
        publisher, initialized = start_serve(io, config_path, True)
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
    native = json.loads(io.run('native-dump', [io.spec['ingest_probe']['path'], 'dump', str(io.root / 'store')]))
    io.backends('native-dump', absent=True)
    readback = json.loads(io.run('scientific-census', [sys.executable, '-B',
        str(SOURCE / 'experiments/ingest-bootstrap/installed_archive_read.py'), str(config_path),
        str(io.root / 'census-scratch'), '--large-signed-row', '--worker-root', io.spec['worker']['root']]))
    io.backends('scientific-census', absent=True)
    require(io.capture('publication-readback') == published, 'read-only native census preserved whole SQL/catalog')
    require(immutable(io.root) == fixed, 'native observers preserved original physical science')
    selection = publication(io, before, published, initialized, native, readback, peer.transcript(), receipts)
    # This is the existing static reader-selection path. Accepted initial policy
    # documents stay byte-for-byte unchanged; no live policy row is rewritten.
    ordinary = io.root / 'ordinary.toml'
    ordinary.write_text(set_value(config_path.read_text(), 'archive.reader', 'enabled', True))
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
        data_root=str(io.root / 'data'), state_root=str(io.root / 'state'), destination=str(io.root / 'destination'),
        worker_root=io.spec['worker']['root'], worker_bytes=io.spec['worker']['bytes'],
        worker_sha256=io.spec['worker']['sha256'], limits=io.spec['limits'])
    for name, table in [('config', 'config_revisions'), ('policy', 'policy_revisions')]:
        stored = one(after['control'][table])
        path = io.root / (name + '-accepted.json')
        write(path, stored['document'])
        common[name] = dict(path=str(path), sha256=sha(path.read_bytes()), stored_digest=stored['document_digest'][2:])
    query = dict(version=1, resource='event_history', mode='snapshot', scope=initialized['private_scope'],
                 filters=[dict(ids=[EVENT_ID])])
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
    write(io.root / 'prepared.json', dict(status='PREPARED_FOR_ROOT_ACCEPTANCE', principal=principal,
        acceptance_runtime='NOT_RUN', measurement_runtime='NOT_RUN', selection=selection, files=fixed,
        config_file_sha256=sha(config_path.read_bytes()), ordinary_config_sha256=sha(ordinary.read_bytes())))
    return case, selection, after, fixed


def run(spec, config):
    os.umask(0o077)
    def interrupted(_signal, _frame):
        raise InterruptedError('owned setup signal')
    signal.signal(signal.SIGTERM, interrupted)
    root = Path(spec['root'])
    root.mkdir(mode=0o700)
    for name in ('data', 'state', 'scratch', 'destination'):
        (root / name).mkdir(mode=0o700)
    config_path = root / 'initial.toml'
    config_path.write_bytes(pinned(spec['config']))
    write(root / 'input.json', spec)
    io = IO(root, spec, config)
    try:
        case, selection, after, fixed = prepare(io, config_path)
        # Deliberate boundary: the finite accept-only process and fresh measurement
        # cgroup are root-owned follow-ups. No accepted job is guessed here.
        write(root / 'result.json', dict(status='PREPARED_FOR_ROOT_ACCEPTANCE',
            accept=str(root / 'accept.json'), accept_sha256=sha((root / 'accept.json').read_bytes()),
            command=[spec['probe']['path'], 'accept', str(root / 'accept.json')],
            qualification='source candidate; execution outcome must be audited',
            acceptance='NOT_RUN', measurement='NOT_RUN', seconds=120-io.remaining()))
    except BaseException as error:
        io.failure_custody()
        write(root / 'failure.json', dict(status='UNKNOWN_PRESERVED', error=type(error).__name__,
            acceptance='NOT_RUN', measurement='NOT_RUN', retry='forbidden until root resolves actual custody/outcome'))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('descriptor', type=Path)
    args = parser.parse_args()
    run(*read_spec(args.descriptor))
