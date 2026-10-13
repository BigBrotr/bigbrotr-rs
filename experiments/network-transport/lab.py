#!/usr/bin/env python3
"""Trusted exact-Git Tux runner with exclusive reuse of an owned compiler cache."""
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import tarfile

sys.dont_write_bytecode = True
from inventory import projection
revision, tree = sys.argv[1:3]
profile = sys.argv[3] if len(sys.argv) == 4 else 'network'
assert len(sys.argv) in (3, 4) and profile in (
    'ingest-activated-installed', 'ingest-activated-recovery', 'ingest-installed-live', 'ingest-live-refusals', 'ingest-live-guards', 'ingest-live', 'ingest-empty-installed', 'ingest-empty-preparer', 'ingest-empty-locks', 'ingest-empty-faults', 'ingest-gap-faults', 'ingest-history', 'ingest-recovery', 'archive-worker-budgets', 'acquisition-transport', 'dictionary-append', 'relay-arrival', 'relay-transfer', 'observation', 'network', 'websocket', 'relay-wire', 'selector', 'request-session', 'relay-registry', 'relay-registry-faults',
    'relay-enrollment', 'relay-enrollment-faults', 'relay-registration-faults', 'ingest-visibility',
    'policy-versions', 'relay-source', 'relay-source-faults',
    'source-transport', 'source-cleanup', 'source-races', 'acquisition-scope', 'acquisition-scope-faults', 'acquisition-scope-read', 'acquisition-binding', 'acquisition-binding-refusals', 'acquisition-binding-recovery', 'sync-arrival-policy', 'ingest-lease', 'initial-ingest', 'acquisition-jobs', 'acquisition-jobs-faults', 'acquisition-claims',
)
assert all(re.fullmatch('[0-9a-f]{40}', value) for value in [revision, tree])
base = Path('/home/tux/bigbrotr-implementation-20260917')
root = base / 'runs' / (profile + '-' + revision[:7])
source = root / 'source'
cache_root = base / 'runs/network-1154757'
image = 'sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d'
label = 'org.bigbrotr.network-transport'


def command(*args):
    return subprocess.check_output(args, text=True, timeout=120).strip()


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def containers():
    return json.loads(command('docker', 'inspect', *command('docker', 'ps', '-aq').split()))


assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == revision
assert command('git', '-C', str(source), 'rev-parse', 'HEAD^{tree}') == tree
assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
assert Path(__file__).resolve() == source / 'experiments/network-transport/lab.py'
assert not (root / 'resources.json').exists()
prior = containers()
old = json.loads((cache_root / 'resources.json').read_text())
assert old['source'] == '115475799ce11a841e9aacc814d3576ee00cc6ca'
assert old['owner'] == '8b21a13162699a6038154874c2ac89da' and old['state'] == 'stopped-retained'
for entry in old['containers']:
    item = next(c for c in prior if c['Id'] == entry['id'])
    assert item['Config']['Labels'][old['owner_label']] == old['owner']
    assert item['State']['Status'] == 'exited' and item['State']['Pid'] == 0
for item in prior:
    if item['State']['Running']:
        assert 'bigbrotr' not in item['Name']
        assert not any(Path(m['Source']).is_relative_to(cache_root) for m in item['Mounts'])
# This cache is mutable build acceleration, not historical evidence. Verify the
# original executables are retained separately before sharing it with this run.
original_result = json.loads((cache_root / 'out/tests/result.json').read_text())
assert original_result['source'] == old['source']
for name, digest in original_result['executables_sha256'].items():
    assert re.fullmatch('[a-z_]+', name)
    assert sha(cache_root / 'out/artifacts' / name) == digest
cache_lock = os.open(cache_root / '.compiler-cache.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
fcntl.flock(cache_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
preflight = dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                 containers=[projection(c) for c in prior],
                 directories=sorted(p.name for p in (base / 'runs').iterdir()),
                 ports=command('ss', '-lntup'), processes=command('ps', '-eo', 'pid,ppid,stat,comm'),
                 volumes=command('docker', 'volume', 'ls'), networks=command('docker', 'network', 'ls'),
                 architecture=command('uname', '-m'), free_bytes=shutil.disk_usage(base).free,
                 required_free_bytes=12 * 1024**3)
write(root / 'preflight.json', preflight)
assert preflight['architecture'] == 'x86_64' and preflight['free_bytes'] > preflight['required_free_bytes']
# This profile creates no archive/export; its capacity prerequisite is compiler
# headroom, not the separate product export completion reserve of 64 GiB.
owner = secrets.token_hex(16)
state = dict(source=revision, tree=tree, owner=owner, owner_label=label,
             root=str(root), state='preparing', containers=[], image=image,
             cache_origin=str(cache_root), cache_mode='exclusive mutable reuse; original evidence retained outside caches',
             started_at=preflight['observed_at'])
write(root / 'resources.json', state)
out = root / 'out'
build = out / 'build'
build.mkdir(parents=True)
export = build / 'source'
export.mkdir()
archive = build / 'source.tar'
command('git', '-C', str(source), 'archive', '--format=tar', '--output', str(archive), revision)
with tarfile.open(archive) as bundle:
    assert all(not Path(e.name).is_absolute() and '..' not in Path(e.name).parts
               and (e.isfile() or e.isdir()) for e in bundle.getmembers())
    bundle.extractall(export, filter='data')
write(build / 'source-manifest.json', dict(revision=revision, tree=tree, archive_sha256=sha(archive),
      files_sha256={str(p.relative_to(export)): sha(p) for p in sorted(export.rglob('*')) if p.is_file()}))
for name in ['target', 'cargo-home']:
    original = cache_root / 'out/build' / name
    assert original.is_dir() and not original.is_symlink()
    if name == 'cargo-home':
        assert not any((original / p).exists() for p in ['credentials', 'credentials.toml', 'config', 'config.toml'])
    (build / name).mkdir()


def run_container(name, network, argv, seconds):
    archive_mount = []
    if profile == 'archive-worker-budgets' and name == 'native':
        # Retained assets are read-only build inputs. The old executable is not
        # reused: the exact source builds a worker before binding its parent.
        bundle = base / 'runs/installed-publication-61271e2/out/archive-worker'
        assert bundle.is_dir() and bundle.resolve() == bundle
        archive_mount = ['--mount', f'type=bind,src={bundle},dst=/archive-inputs,readonly']
    ident = command('docker', 'create', '--init', '--name', 'bigbrotr-' + profile + '-' + revision[:7] + '-' + name,
                    '--label', label + '=' + owner, '--cpus', '2', '--memory', '6g', '--memory-swap', '6g',
                    '--pids-limit', '512', '--network', network, '--read-only', '--cap-drop', 'ALL',
                    '--security-opt', 'no-new-privileges', '--user', f'{os.getuid()}:{os.getgid()}',
                    '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=512m',
                    '--mount', f'type=bind,src={out},dst=/out',
                    '--mount', f'type=bind,src={export},dst=/out/build/source,readonly',
                    '--mount', f'type=bind,src={cache_root}/out/build/target,dst=/out/build/target',
                    '--mount', f'type=bind,src={cache_root}/out/build/cargo-home,dst=/out/build/cargo-home', *archive_mount, image,
                    'env', '-i', 'PATH=/usr/local/cargo/bin:/usr/local/bin:/usr/bin:/bin',
                    'RUSTUP_HOME=/usr/local/rustup', 'CARGO_HOME=/out/build/cargo-home', *argv)
    state['containers'].append(dict(name=name, id=ident))
    write(root / 'resources.json', state)
    command('docker', 'start', ident)
    try:
        subprocess.run(['docker', 'wait', ident], check=True, timeout=seconds, stdout=subprocess.DEVNULL)
    finally:
        current = json.loads(command('docker', 'inspect', ident))[0]
        assert current['Config']['Labels'][label] == owner
        if current['State']['Running']:
            command('docker', 'kill', ident)
            current = json.loads(command('docker', 'inspect', ident))[0]
        (root / (name + '.log')).write_text(command('docker', 'logs', ident) + '\n')
        write(root / (name + '-terminal.json'), projection(current))
    assert current['State']['Status'] == 'exited' and current['State']['Pid'] == 0
    assert current['State']['ExitCode'] == 0 and not current['State']['OOMKilled']


try:
    run_container('fetch', 'bridge', ['cargo', 'fetch', '--locked', '--target', 'x86_64-unknown-linux-gnu',
                  '--manifest-path', '/out/build/source/Cargo.toml'], 180)
    assert shutil.disk_usage(base).free > 8 * 1024**3
    state['state'] = 'running'
    write(root / 'resources.json', state)
    native_path = 'network-transport' if profile in ('network', 'websocket', 'relay-wire', 'selector', 'request-session', 'acquisition-transport') else 'relay-registry'
    native_args = {
        'acquisition-transport': ['acquisition-transport'],
        'dictionary-append': ['dictionary-append'],
        'websocket': ['websocket'],
        'relay-wire': ['relay-wire'],
        'selector': ['selector'],
        'request-session': ['request-session'],
        'relay-registry-faults': ['faults'],
        'relay-enrollment': ['enrollment'],
        'relay-enrollment-faults': ['enrollment-faults'],
        'relay-registration-faults': ['registration-faults'],
        'ingest-visibility': ['visibility'],
        'observation': ['observation'],
        'relay-transfer': ['relay-transfer'],
        'relay-arrival': ['relay-arrival'],
        'policy-versions': ['policy-versions'],
        'relay-source': ['relay-source'],
        'relay-source-faults': ['relay-source-faults'],
        'source-transport': ['source-transport'],
        'source-cleanup': ['source-cleanup'],
        'source-races': ['source-races'],
        'acquisition-scope': ['acquisition-scope'],
        'acquisition-scope-faults': ['acquisition-scope-faults'],
        'acquisition-scope-read': ['acquisition-scope-read'],
        'acquisition-binding': ['acquisition-binding'],
        'acquisition-binding-refusals': ['acquisition-binding-refusals'],
        'acquisition-binding-recovery': ['acquisition-binding-recovery'],
        'sync-arrival-policy': ['sync-arrival-policy'],
        'ingest-lease': ['ingest-lease'],
        'initial-ingest': ['initial-ingest'],
        'ingest-recovery': ['ingest-recovery'],
        'ingest-history': ['ingest-history'],
        'ingest-activated-installed': ['ingest-activated-installed'],
        'ingest-activated-recovery': ['ingest-activated-recovery'],
        'ingest-installed-live': ['ingest-installed-live'],
        'ingest-live-refusals': ['ingest-live-refusals'],
        'ingest-live-guards': ['ingest-live-guards'],
        'ingest-live': ['ingest-live'],
        'ingest-gap-faults': ['ingest-gap-faults'],
        'ingest-empty-faults': ['ingest-empty-faults'],
        'ingest-empty-locks': ['ingest-empty-locks'],
        'ingest-empty-preparer': ['ingest-empty-preparer'],
        'ingest-empty-installed': ['ingest-empty-installed'],
        'acquisition-jobs': ['acquisition-jobs'],
        'acquisition-jobs-faults': ['acquisition-jobs-faults'],
        'acquisition-claims': ['acquisition-claims'],
    }.get(profile, [])
    native_script = ('archive-engine/worker_budgets_native.py' if profile == 'archive-worker-budgets'
                     else native_path + '/native.py')
    run_container('native', 'none', ['python3', '-B', '/out/build/source/experiments/' + native_script, *native_args], 1860)
    qualification = json.loads((out / 'qualification.json').read_text())
    expected = {'ingest-activated-installed':'PASS_ACTIVATED_INSTALLED_BUILD_ONLY', 'ingest-activated-recovery':'PASS_ACTIVATED_INITIAL_RECOVERY_BUILD_ONLY', 'ingest-installed-live':'PASS_INSTALLED_LIVE_SUCCESSOR_BUILD_ONLY', 'ingest-live-refusals':'PASS_LIVE_INITIAL_REFUSALS_PROBE_BUILD_ONLY', 'ingest-live-guards':'PASS_LIVE_INITIAL_GUARDS_PROBE_BUILD_ONLY', 'ingest-live':'PASS_LIVE_INITIAL_BUILD_ONLY', 'ingest-empty-installed':'PASS_INSTALLED_EMPTY_RETIREMENT_BUILD_ONLY', 'ingest-empty-preparer':'PASS_EMPTY_PREPARER_CLAIM_PROBE_BUILD_ONLY', 'ingest-empty-locks':'PASS_EMPTY_RETIREMENT_LOCK_PROBE_BUILD_ONLY', 'ingest-empty-faults':'PASS_EMPTY_RETIREMENT_FAULT_PROBE_BUILD_ONLY', 'ingest-gap-faults':'PASS_INITIAL_GAP_FAULT_PROBE_BUILD_ONLY', 'ingest-history':'PASS_INGEST_HISTORY_BUILD_ONLY', 'ingest-recovery': 'PASS_INGEST_RECOVERY_INSPECTION', 'archive-worker-budgets': 'PASS_ARCHIVE_WORKER_BUDGETS', 'acquisition-transport': 'PASS_ACQUISITION_TRANSPORT_ROOTS', 'dictionary-append': 'PASS_DICTIONARY_APPEND_COMPONENTS', 'network': 'PASS_SCOPED_NETWORK_COMPONENTS',
                'websocket': 'PASS_SCOPED_WEBSOCKET_COMPONENTS',
                'relay-wire': 'PASS_SCOPED_RELAY_WIRE_COMPONENTS',
                'selector': 'PASS_SCOPED_SELECTOR_COMPONENTS',
                'request-session': 'PASS_SCOPED_REQUEST_SESSION_COMPONENTS',
                'relay-registry': 'PASS_REGISTRY_BUILD_ONLY',
                'relay-registry-faults': 'PASS_REGISTRY_FAULT_BUILD_ONLY',
                'relay-enrollment': 'PASS_RELAY_ENROLLMENT_BUILD_ONLY',
                'relay-enrollment-faults': 'PASS_RELAY_ENROLLMENT_FAULT_BUILD_ONLY',
                'relay-registration-faults': 'PASS_RELAY_REGISTRATION_FAULT_BUILD_ONLY',
                'ingest-visibility': 'PASS_INGEST_VISIBILITY_BUILD_ONLY',
                'observation': 'PASS_OBSERVATION_BUILD_ONLY',
                'relay-transfer': 'PASS_RELAY_TRANSFER_BUILD_ONLY',
                'relay-arrival': 'PASS_RELAY_ARRIVAL_BUILD_ONLY',
                'policy-versions': 'PASS_POLICY_VERSIONS_BUILD_ONLY',
                'relay-source': 'PASS_RELAY_SOURCE_BUILD_ONLY',
                'relay-source-faults': 'PASS_RELAY_SOURCE_FAULT_BUILD_ONLY',
                'source-transport': 'PASS_SOURCE_TRANSPORT_BUILD_ONLY',
                'source-cleanup': 'PASS_SOURCE_CLEANUP_BUILD_ONLY',
                'source-races': 'PASS_SOURCE_RACES_BUILD_ONLY',
                'acquisition-scope': 'PASS_ACQUISITION_SCOPE_BUILD_ONLY',
                'acquisition-scope-faults': 'PASS_ACQUISITION_SCOPE_FAULT_BUILD_ONLY',
                'acquisition-scope-read': 'PASS_ACQUISITION_SCOPE_READ_BUILD_ONLY',
                'acquisition-binding': 'PASS_ACQUISITION_BINDING_BUILD_ONLY',
                'acquisition-binding-refusals': 'PASS_ACQUISITION_BINDING_REFUSAL_BUILD_ONLY',
                'acquisition-binding-recovery': 'PASS_ACQUISITION_BINDING_RECOVERY_BUILD_ONLY',
                'sync-arrival-policy': 'PASS_SYNC_ARRIVAL_POLICY_BUILD_ONLY',
                'ingest-lease': 'PASS_INITIAL_INGEST_LEASE_BUILD_ONLY',
                'initial-ingest': 'PASS_INITIAL_INGEST_BUILD_ONLY',
                'acquisition-jobs': 'PASS_ACQUISITION_JOBS_BUILD_ONLY',
                'acquisition-jobs-faults': 'PASS_ACQUISITION_JOBS_FAULT_BUILD_ONLY',
                'acquisition-claims': 'PASS_ACQUISITION_CLAIMS_BUILD_ONLY'}[profile]
    assert qualification['source'] == revision and qualification['status'] == expected
    state['qualification'] = qualification['status']
    if profile in ('relay-registry', 'relay-enrollment', 'ingest-visibility', 'policy-versions', 'relay-source'):
        engine_script = 'ingest-bootstrap/visibility.py' if profile == 'ingest-visibility' else profile + '/run.py'
        engine_args = [] if profile in ('ingest-visibility', 'policy-versions', 'relay-source') else [str(source)]
        subprocess.run(['python3', '-B', str(source / ('experiments/' + engine_script)),
                        str(root / 'pg'), str(out / 'artifacts'), *engine_args], check=True, timeout=300)
        pg = json.loads((root / 'pg/resources.json').read_text())
        state['postgres_lab'] = dict(root=pg['root'], owner=pg['owner'],
                                    postgres=pg['postgres'], client=pg['client'], network=pg['network'],
                                    state=pg['state'])
        assert pg['state'] == 'stopped-retained'
        result_name, result_status = {
            'relay-registry': ('registry-result.json', 'PASS_SCOPED_ENDPOINT_REGISTRY'),
            'relay-enrollment': ('enrollment-result.json', 'PASS_INITIAL_ENROLLMENT_BOUNDARY'),
            'ingest-visibility': ('visibility-result.json', 'PASS_SCOPED_INGEST_VISIBILITY'),
            'policy-versions': ('policy-versions-result.json', 'PASS_SCOPED_POLICY_VERSIONS'),
            'relay-source': ('source-result.json', 'PASS_SCOPED_RELAY_SOURCE_FIRST_WAVE'),
        }[profile]
        engine = json.loads((root / 'pg' / result_name).read_text())
        assert engine['status'] == result_status
        state['qualification'] = engine['status']
finally:
    pg_receipt = root / 'pg/resources.json'
    if profile in ('relay-registry', 'relay-enrollment', 'ingest-visibility', 'policy-versions', 'relay-source') and pg_receipt.exists():
        pg = json.loads(pg_receipt.read_text())
        assert pg['root'] == str(root / 'pg') and pg['source'] == str(source)
        assert pg['format'] == 'bigbrotr-pg-lab/1'
        prior_ids = {c['Id'] for c in prior}
        current = {c['Name'].lstrip('/'): c for c in containers()}
        for name in ('client', 'postgres'):
            item = current.get(pg[name])
            if item is None:
                continue  # partial setup; no unobserved resource is removed
            assert item['Id'] not in prior_ids
            assert item['Config']['Labels']['org.bigbrotr.control-migration-lab'] == pg['owner']
            if item['State']['Running']:
                command('docker', 'stop', '--time', '10', item['Id'])
            state['containers'].append(dict(name=name, id=item['Id'],
                                           owner_label='org.bigbrotr.control-migration-lab', owner=pg['owner']))
        state['postgres_lab'] = dict(root=pg['root'], owner=pg['owner'], network=pg['network'])
    after = {c['Id']: projection(c) for c in containers()}
    changed = [c['Id'] for c in prior if after.get(c['Id']) != projection(c)]
    stopped = all(not after[c['id']]['State']['Running'] and after[c['id']]['State']['Pid'] == 0
                  for c in state['containers'])
    state.update(state='stopped-retained' if stopped else 'cleanup-required',
                 inventory_changed=changed, free_bytes_after=shutil.disk_usage(base).free,
                 finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    if changed or not stopped:
        state['qualification'] = 'INCOMPLETE_LAB_AUDIT'
    write(root / 'resources.json', state)
    assert not changed and stopped
print(json.dumps(state))
