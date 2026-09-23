#!/usr/bin/env python3
"""Actual CLI, mount aliases, cross-process locks and interrupted marker writes.

strace injects a syscall error or delays a selected fsync so the fixture can kill
the actual process. This is not host power-loss or physical writeback evidence.
All roots and retained failed writes stay under the caller's new owned output.
"""
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path

binary, home = map(Path, sys.argv[1:])
home.mkdir(mode=0o700)
names = ['ordered-sync', 'bind-alias', 'cross-process-lock',
         'file-eio', 'root-eio', 'parent-eio', 'retry-file-eio',
         'kill-before-rename', 'kill-after-rename', 'kill-retry-before-rename']
report = dict(status='INCOMPLETE', binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
              strace=subprocess.check_output(['strace', '--version'], text=True),
              cases=[dict(name=n, status='NOT_RUN') for n in names])
current = None
sequence = 0


def save():
    (home / 'result.json').write_text(json.dumps(report, indent=2) + '\n')


def case(name):
    global current
    current = next(c for c in report['cases'] if c['name'] == name)
    current['status'] = 'RUNNING'
    save()


def passed(**details):
    current.update(status='PASS', **details)
    save()


def cli(args, expected=0):
    global sequence
    sequence += 1
    result = subprocess.run([str(binary), *args], env={}, capture_output=True, timeout=5)
    stem = home / ('command-' + str(sequence))
    stem.with_suffix('.stdout').write_bytes(result.stdout)
    stem.with_suffix('.stderr').write_bytes(result.stderr)
    current.setdefault('commands', []).append(dict(args=args, exit=result.returncode, log=stem.name))
    assert result.returncode == expected, (args, result.returncode, result.stderr)
    if expected:
        assert not result.stdout
        return result.stderr.decode()
    assert not result.stderr
    return json.loads(result.stdout)


def command(config, operation='initialize'):
    return ['export', 'destination-' + operation, '--file', str(config)]


def fixture(name, destination=None):
    root = home / name
    root.mkdir(mode=0o700)
    if destination is None:
        destination = root / 'destination'
        destination.mkdir(mode=0o700)
    text = (Path(__file__).resolve().parents[2] / 'config/recipes/archive-only.toml').read_text()
    # These real existing selected roots exercise Linux mount-map checks; the
    # extra Docker bind at /store-alias points inside this same archive root.
    for store in ['archive', 'archive-state', 'index', 'ingest', 'scratch']:
        text = text.replace('/var/lib/bigbrotr/' + store + '"', '/out/stores/' + store + '"')
    text += '\n[export.destination]\nid = "acacacacacacacacacacacacacacacac"\nroot = ' + json.dumps(str(destination)) + '\n'
    config = root / 'instance.toml'
    config.write_text(text)
    return destination, config


def marker(root):
    data = (root / 'destination.json').read_bytes()
    meta = root.stat()
    expected = dict(format='bigbrotr-export-destination/1', dataset_id='22' * 16,
                    destination_id='ac' * 16, root=str(root), device=meta.st_dev, inode=meta.st_ino)
    # An independent serializer also checks exact compact bytes and field order.
    assert data == json.dumps(expected, separators=(',', ':')).encode()
    assert (root / 'destination.json').stat().st_mode & 0o7777 == 0o600
    return hashlib.sha256(data).hexdigest()


def trace_command(config, path, injection=None):
    args = ['strace', '-f', '-yy', '-ttt', '-o', str(path), '-e',
            'trace=fsync,write,renameat,openat,fchmod,flock']
    if injection:
        args += ['-e', 'inject=fsync:' + injection]
    return [*args, str(binary), *command(config)]


def traced(root, config, injection=None, expected=0):
    path = root.parent / 'syscalls.trace'
    result = subprocess.run(trace_command(config, path, injection), env={}, capture_output=True, timeout=5)
    (root.parent / 'stdout').write_bytes(result.stdout)
    (root.parent / 'stderr').write_bytes(result.stderr)
    assert result.returncode == expected, (result.returncode, result.stderr)
    if expected:
        assert not result.stdout and b'Filesystem' in result.stderr
    else:
        assert not result.stderr
    text = path.read_text()
    current.update(trace=str(path.relative_to(home)), trace_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), exit=result.returncode)
    return text


try:
    save()
    case('ordered-sync')
    root, config = fixture('ordered')
    text = traced(root, config)
    calls = [line for line in text.splitlines() if 'fsync(' in line or 'renameat(' in line]
    assert len(calls) == 4 and ['fsync(' in s for s in calls] == [True, False, True, True], calls
    assert '.partial>' in calls[0] and all('= 0' in line for line in calls)
    assert str(root) + '>' in calls[2] and str(root.parent) + '>' in calls[3]
    digest = marker(root)
    first = cli(command(config, 'inspect'))
    inode = (root / 'destination.json').stat().st_ino
    assert cli(command(config)) == first and marker(root) == digest
    assert (root / 'destination.json').stat().st_ino != inode
    assert first['binding_sha256'] == digest and first['job_authority'] == 'not-granted'
    passed(binding_sha256=digest, calls=calls, complete_retry_uses_new_inode=True)

    case('bind-alias')
    alias = Path('/store-alias')
    # Require an actual separate mount, not a missing directory or symlink.
    mountinfo = Path('/proc/self/mountinfo').read_text()
    assert ' /store-alias ' in mountinfo
    assert alias.stat().st_ino == Path('/out/stores/archive/nested').stat().st_ino
    before = list(alias.iterdir())
    assert not before
    _, config = fixture('alias', alias)
    cli(['config', 'validate', '--file', str(config)])
    error = cli(command(config), 4)
    assert 'placement cannot be verified' in error and not list(alias.iterdir())
    (home / 'mountinfo').write_text(mountinfo)
    passed(refused_without_writes=True)

    case('cross-process-lock')
    root, config = fixture('locking')
    # Python's independent flock holds the actual root against the Rust CLI.
    held = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        started = time.monotonic()
        assert 'Busy' in cli(command(config), 4)
        assert 'Busy' in cli(command(config, 'inspect'), 4)
        elapsed = time.monotonic() - started
        assert elapsed < 2 and not list(root.iterdir())
    finally:
        os.close(held)
    first = cli(command(config))
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        assert cli(command(config, 'inspect')) == first
        assert 'Busy' in cli(command(config), 4)
    finally:
        os.close(fd)
    passed(exclusive_refusal_seconds=elapsed, shared_inspection=True)

    for name, point in [('file-eio', 1), ('root-eio', 2), ('parent-eio', 3), ('retry-file-eio', 1)]:
        case(name)
        root, config = fixture(name)
        retry = name == 'retry-file-eio'
        if retry:
            first = cli(command(config))
            old_inode = (root / 'destination.json').stat().st_ino
        text = traced(root, config, 'error=EIO:when=' + str(point), 4)
        injected = [line for line in text.splitlines() if '(INJECTED)' in line]
        assert len(injected) == 1 and '= -1 EIO ' in injected[0] and 'fsync(' in injected[0]
        if point == 1 and not retry:
            assert not (root / 'destination.json').exists()
            retained = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
            assert len(retained) == 1 and next(iter(retained)).endswith('.partial')
            assert 'NotEmpty' in cli(command(config), 4)
            assert retained == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
            passed(injected=injected, retained=retained, retry='refused nonempty uninitialized root')
        else:
            digest = marker(root)
            inode = (root / 'destination.json').stat().st_ino
            if retry:
                assert inode == old_inode and cli(command(config, 'inspect')) == first
                retained = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in root.glob('*.partial')}
                assert len(retained) == 1
            assert cli(command(config))['binding_sha256'] == digest
            assert marker(root) == digest and (root / 'destination.json').stat().st_ino != inode
            if retry:
                assert retained == {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in root.glob('*.partial')}
            passed(injected=injected, binding_sha256=digest, retry='new synced inode, same binding')

    for name, point in [('kill-before-rename', 1), ('kill-after-rename', 2), ('kill-retry-before-rename', 1)]:
        case(name)
        root, config = fixture(name)
        retry = name == 'kill-retry-before-rename'
        if retry:
            first = cli(command(config))
            old_inode = (root / 'destination.json').stat().st_ino
        path = root.parent / 'syscalls.trace'
        started = time.monotonic()
        with (root.parent / 'stdout').open('xb') as stdout, (root.parent / 'stderr').open('xb') as stderr:
            process = subprocess.Popen(trace_command(config, path, 'delay_enter=10s:when=' + str(point)),
                                       env={}, stdout=stdout, stderr=stderr, start_new_session=True)
            child = None
            try:
                deadline = started + 3
                while time.monotonic() < deadline:
                    children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
                    if children:
                        assert len(children) == 1
                        child = int(children[0])
                    files = list(root.iterdir())
                    staged = [p for p in files if p.name.endswith('.partial')]
                    visible = ((len(staged) == 1 and staged[0].stat().st_size > 0)
                               if point == 1 else (root / 'destination.json').exists())
                    if child and visible:
                        break
                    assert process.poll() is None
                    time.sleep(.005)
                else:
                    raise TimeoutError('selected fsync boundary not observed')
                # Parent remains blocked before the injected 10s could finish.
                assert process.poll() is None and time.monotonic() - started < 3
                observed = dict(child_pid=child, files=[p.name for p in files],
                                marker_present=(root / 'destination.json').exists())
            finally:
                os.killpg(process.pid, signal.SIGKILL)
                assert process.wait(timeout=3) == -signal.SIGKILL
            deadline = time.monotonic() + 3
            while child and Path(f'/proc/{child}').exists() and time.monotonic() < deadline:
                time.sleep(.01)
            assert child and not Path(f'/proc/{child}').exists()
        assert not (root.parent / 'stdout').read_bytes()
        if point == 1 and not retry:
            assert not (root / 'destination.json').exists()
            retained = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
            assert 'NotEmpty' in cli(command(config), 4)
            assert retained == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
        else:
            digest = marker(root)
            inode = (root / 'destination.json').stat().st_ino
            if retry:
                assert inode == old_inode and cli(command(config, 'inspect')) == first
                retained = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in root.glob('*.partial')}
                assert len(retained) == 1
            assert cli(command(config))['binding_sha256'] == digest
            assert marker(root) == digest and (root / 'destination.json').stat().st_ino != inode
            if retry:
                assert retained == {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in root.glob('*.partial')}
        passed(observed=observed, joined=True, child_absent=True,
               trace_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    assert all(c['status'] == 'PASS' for c in report['cases'])
    report['status'] = 'PASS'
except BaseException as error:
    report['failure'] = dict(kind=type(error).__name__, detail=str(error),
                             frames=[dict(file=Path(f.filename).name, line=f.lineno)
                                     for f in traceback.extract_tb(error.__traceback__)])
    raise
finally:
    save()
