#!/usr/bin/env python3
"""Owner-authorized Tux recipe; exact trusted Git input, no untrusted PR runner.

The Mac transfers an ordinary Git bundle, creates the new run/source checkout,
and invokes this file with the exact revision/tree. This recipe never deletes
old labs/caches, host containers/volumes, datasets or unknown resources.
"""
import datetime
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
import time
import traceback

revision, tree = sys.argv[1:]
assert all(re.fullmatch('[0-9a-f]{40}', value) for value in [revision, tree])
base = Path('/home/tux/bigbrotr-implementation-20260917')
root = base / 'runs' / ('export-files-' + revision[:7])
source, out = root / 'source', root / 'out'
cache_root = base / 'runs/export-checker-fixed-4e6b5c0'
cache = cache_root / 'out/build'
instrument_root = base / 'runs/strace-exit-9b30c35'
instrument = instrument_root / 'out/strace-exit-fixed'
image = 'sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d'
label = 'org.bigbrotr.export-files'


def cmd(*args):
    return subprocess.check_output(args, text=True, timeout=120).strip()


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def inspect(ident):
    return json.loads(cmd('docker', 'inspect', ident))[0]


def stable(c):
    return dict(id=c['Id'], name=c['Name'], image=c['Image'],
                lifecycle={k: c['State'][k] for k in ['Status', 'Running', 'Paused', 'Restarting', 'OOMKilled', 'Dead', 'Pid', 'ExitCode', 'Error', 'StartedAt', 'FinishedAt']},
                mounts=sorted(c['Mounts'], key=lambda m: m['Destination']),
                networks=c['NetworkSettings']['Networks'], restart=c['HostConfig']['RestartPolicy'])


assert cmd('git', '-C', str(source), 'rev-parse', 'HEAD') == revision
assert cmd('git', '-C', str(source), 'rev-parse', 'HEAD^{tree}') == tree
assert not cmd('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
prior = json.loads(cmd('docker', 'inspect', *cmd('docker', 'ps', '-aq').split()))
for c in prior:
    c['Config'].pop('Env', None)
assert not any('bigbrotr' in c['Name'] and c['State']['Running'] for c in prior)
preflight = dict(containers=prior, directories=sorted(p.name for p in (base / 'runs').iterdir()),
                 processes=cmd('ps', '-eo', 'pid,ppid,stat,comm'), ports=cmd('ss', '-lntup'),
                 volumes=cmd('docker', 'volume', 'ls'), networks=cmd('docker', 'network', 'ls'),
                 free_bytes=shutil.disk_usage(base).free, architecture=cmd('uname', '-m'),
                 mounts=Path('/proc/self/mountinfo').read_text())
write(root / 'preflight.json', preflight)
assert preflight['architecture'] == 'x86_64' and preflight['free_bytes'] > 12 * 1024**3
for origin, expected in [(cache_root, '3f840fe30a3ffef7f9944c72ea6e38f9'),
                         (instrument_root, '84ac8cbf5292679b8ef7043fed7a2799')]:
    old = json.loads((origin / 'resources.json').read_text())
    assert old['owner'] == expected and old['state'] == 'stopped-retained'
    for entry in old['containers']:
        c = next(c for c in prior if c['Id'] == entry['id'])
        assert c['State']['Status'] == 'exited' and c['State']['Pid'] == 0
        assert expected in c['Config']['Labels'].values()
assert sha(instrument) == '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
assert sha(source / 'Cargo.lock') == sha(cache / 'source/Cargo.lock')
original = json.loads((cache / 'source-manifest.json').read_text())
assert original['revision'] == '4e6b5c011c067800f52e67365b2b848b7943d4bc'
assert all(sha(cache / 'source' / name) == digest for name, digest in original['files_sha256'].items())
owner = secrets.token_hex(16)
state = dict(source=revision, tree=tree, owner=owner, root=str(root), state='preparing', containers=[],
             image=image, owner_label=label, cache_origin=str(cache_root), tracer_sha256=sha(instrument),
             started_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
audit = dict(status='INCOMPLETE', source=revision, owner=owner)


def save():
    write(root / 'resources.json', state)


def owned(ident):
    value = inspect(ident)
    assert value['Config']['Labels'][label] == owner
    return value


def mount(host, target, readonly=False):
    return ['--mount', f'type=bind,src={host},dst={target}' + (',readonly' if readonly else '')]


def start(name, arguments, extra=()):
    ident = cmd('docker', 'create', '--init', '--name', 'bigbrotr-export-files-' + revision[:7] + '-' + name,
                '--label', label + '=' + owner, '--cpus', '2', '--memory', '6g', '--memory-swap', '6g',
                '--pids-limit', '512', '--network', 'none', '--read-only', '--cap-drop', 'ALL',
                '--security-opt', 'no-new-privileges', '--user', f'{os.getuid()}:{os.getgid()}',
                '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=512m',
                '--tmpfs', f'/limited:rw,noexec,nosuid,nodev,size=128k,mode=0700,uid={os.getuid()},gid={os.getgid()}',
                *mount(out, '/out'), *mount(out / 'build/source', '/out/build/source', True),
                *mount(instrument, '/instrument/strace', True), *extra, image, *arguments)
    state['containers'].append(dict(name=name, id=ident)); save()
    cmd('docker', 'start', ident)
    return ident


def finish(ident, seconds):
    deadline = time.monotonic() + seconds
    while owned(ident)['State']['Running'] and time.monotonic() < deadline:
        time.sleep(.5)
    value = owned(ident)
    assert not value['State']['Running'] and value['State']['Pid'] == 0
    assert value['State']['ExitCode'] == 0 and not value['State']['OOMKilled']


save()
try:
    build = out / 'build'; build.mkdir(parents=True, mode=0o700)
    export = build / 'source'; export.mkdir()
    archive = build / 'source.tar'
    cmd('git', '-C', str(source), 'archive', '--format=tar', '--output', str(archive), revision)
    with tarfile.open(archive) as tar:
        assert all(not Path(e.name).is_absolute() and '..' not in Path(e.name).parts and (e.isfile() or e.isdir()) for e in tar.getmembers())
        tar.extractall(export, filter='data')
    manifest = dict(format='bigbrotr-source/1', revision=revision, tree=tree, archive_sha256=sha(archive),
                    files_sha256={str(p.relative_to(export)): sha(p) for p in sorted(export.rglob('*')) if p.is_file()})
    write(build / 'source-manifest.json', manifest)
    write(root / 'cache-origin.json', dict(root=str(cache_root), source=original['revision'],
          source_manifest_sha256=sha(cache / 'source-manifest.json'),
          purpose='Copied compiler/dependency caches only. Fresh compilation/checks and artifact hashes required; no reused ELF claim.'))
    for name in ['target', 'cargo-home']:
        subprocess.run(['cp', '-a', '--reflink=auto', str(cache / name), str(build / name)], check=True, timeout=120)
    assert shutil.disk_usage(base).free > 8 * 1024**3
    state['state'] = 'building-and-qualifying'; save()
    runner = start('native', ['python3', '-B', '/out/build/source/experiments/export-files/native.py'])
    finish(runner, 2100)
    assert json.loads((out / 'checks/complete.json').read_text())['status'] == 'PASS'
    cases = json.loads((out / 'cases/result.json').read_text())
    assert cases['status'] == 'PASS' and len(cases['cases']) == 14
    state['state'] = 'qualifying-alias'; save()
    ID = '10' * 16
    alias = start('alias', ['python3', '-B', '/out/build/source/experiments/export-files/cases.py',
                  '/out/artifacts/export-files-probe', '/out/cases', '/instrument/strace', 'alias'],
                  mount(out / 'cases/bind-alias' / ID, '/out/cases/bind-alias/.artifact-' + ID + '.partial'))
    finish(alias, 30)
    alias_result = json.loads((out / 'cases/alias-result.json').read_text())
    assert alias_result['status'] == 'PASS' and len(alias_result['cases']) == 1
    audit['result'] = 'PASS'
except BaseException as error:
    audit['failure'] = dict(kind=type(error).__name__,
        frames=[dict(file=Path(f.filename).name, line=f.lineno, function=f.name) for f in traceback.extract_tb(error.__traceback__)])
finally:
    errors = []
    for ident in cmd('docker', 'ps', '-aq', '--filter', 'label=' + label + '=' + owner).split():
        if not any(c['id'].startswith(ident) for c in state['containers']):
            c = owned(ident); state['containers'].append(dict(name=c['Name'], id=c['Id']))
    save()
    for entry in state['containers']:
        try:
            value = owned(entry['id'])
            if value['State']['Running']:
                cmd('docker', 'stop', '--time', '10', entry['id'])
            value = owned(entry['id'])
            assert not value['State']['Running'] and value['State']['Pid'] == 0 and not value['State']['OOMKilled']
            audit.setdefault('container_terminals', {})[entry['name']] = stable(value)
            logs = subprocess.run(['docker', 'logs', entry['id']], capture_output=True, timeout=10)
            assert logs.returncode == 0
            (root / (entry['name'] + '.stdout')).write_bytes(logs.stdout)
            (root / (entry['name'] + '.stderr')).write_bytes(logs.stderr)
        except BaseException as error:
            errors.append(dict(id=entry['id'], kind=type(error).__name__))
    after = json.loads(cmd('docker', 'inspect', *[c['Id'] for c in prior]))
    audit['preexisting_preserved'] = all(stable(a) == stable(b) for a, b in zip(prior, after))
    audit['preexisting_count'] = len(prior)
    audit['unrelated_running'] = sum(c['State']['Running'] for c in after)
    write(root / 'preexisting-terminal.json', [stable(c) for c in after])
    audit['source_unchanged'] = all(sha(export / name) == digest for name, digest in manifest['files_sha256'].items()) if 'manifest' in globals() else False
    audit['original_source_unchanged'] = all(sha(cache / 'source' / name) == digest for name, digest in original['files_sha256'].items())
    audit['source_checkout_clean'] = not cmd('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    audit['cleanup_errors'] = errors
    audit['free_bytes'] = shutil.disk_usage(base).free
    audit['status'] = 'PASS_PENDING_CAPTURE' if not audit.get('failure') and not errors and audit.get('result') == 'PASS' and audit['preexisting_preserved'] and audit['source_unchanged'] and audit['original_source_unchanged'] and audit['source_checkout_clean'] else 'INCOMPLETE'
    write(root / 'terminal-audit.json', audit)
    state['state'] = 'stopped-retained' if not errors else 'cleanup-incomplete'; save()
    print(json.dumps(dict(status=audit['status'], owner=owner, root=str(root))), flush=True)
    if audit['status'] != 'PASS_PENDING_CAPTURE':
        sys.exit(1)
