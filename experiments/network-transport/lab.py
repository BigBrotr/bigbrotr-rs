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
assert len(sys.argv) in (3, 4) and profile in ('network', 'relay-registry', 'relay-registry-faults', 'relay-enrollment', 'relay-enrollment-faults')
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
    ident = command('docker', 'create', '--init', '--name', 'bigbrotr-' + profile + '-' + revision[:7] + '-' + name,
                    '--label', label + '=' + owner, '--cpus', '2', '--memory', '6g', '--memory-swap', '6g',
                    '--pids-limit', '512', '--network', network, '--read-only', '--cap-drop', 'ALL',
                    '--security-opt', 'no-new-privileges', '--user', f'{os.getuid()}:{os.getgid()}',
                    '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=512m',
                    '--mount', f'type=bind,src={out},dst=/out',
                    '--mount', f'type=bind,src={export},dst=/out/build/source,readonly',
                    '--mount', f'type=bind,src={cache_root}/out/build/target,dst=/out/build/target',
                    '--mount', f'type=bind,src={cache_root}/out/build/cargo-home,dst=/out/build/cargo-home', image,
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
    native_path = 'network-transport' if profile == 'network' else 'relay-registry'
    native_args = {'relay-registry-faults':['faults'], 'relay-enrollment':['enrollment'], 'relay-enrollment-faults':['enrollment-faults']}.get(profile, [])
    run_container('native', 'none', ['python3', '-B', '/out/build/source/experiments/' + native_path + '/native.py', *native_args], 1860)
    qualification = json.loads((out / 'qualification.json').read_text())
    expected = {'network': 'PASS_SCOPED_NETWORK_COMPONENTS', 'relay-registry': 'PASS_REGISTRY_BUILD_ONLY',
                'relay-registry-faults': 'PASS_REGISTRY_FAULT_BUILD_ONLY',
                'relay-enrollment': 'PASS_RELAY_ENROLLMENT_BUILD_ONLY',
                'relay-enrollment-faults': 'PASS_RELAY_ENROLLMENT_FAULT_BUILD_ONLY'}[profile]
    assert qualification['source'] == revision and qualification['status'] == expected
    state['qualification'] = qualification['status']
    if profile in ('relay-registry', 'relay-enrollment'):
        subprocess.run(['python3', '-B', str(source / ('experiments/' + profile + '/run.py')),
                        str(root / 'pg'), str(out / 'artifacts'), str(source)], check=True, timeout=300)
        pg = json.loads((root / 'pg/resources.json').read_text())
        state['postgres_lab'] = dict(root=pg['root'], owner=pg['owner'],
                                    postgres=pg['postgres'], client=pg['client'], network=pg['network'],
                                    state=pg['state'])
        assert pg['state'] == 'stopped-retained'
        engine = json.loads((root / ('pg/registry-result.json' if profile == 'relay-registry' else 'pg/enrollment-result.json')).read_text())
        assert engine['status'] == ('PASS_SCOPED_ENDPOINT_REGISTRY' if profile == 'relay-registry' else 'PASS_INITIAL_ENROLLMENT_BOUNDARY')
        state['qualification'] = engine['status']
finally:
    pg_receipt = root / 'pg/resources.json'
    if profile in ('relay-registry', 'relay-enrollment') and pg_receipt.exists():
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
