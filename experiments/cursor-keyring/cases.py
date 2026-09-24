#!/usr/bin/env python3
"""Real Linux CLI/filesystem failure boundaries; no Control or power-loss claim.

Generated keys stay in private lab directories, never output/trace payloads.
strace renders write arguments raw to avoid recording key material.
"""
import fcntl
import hashlib
import json
import os
import re
import signal
import struct
import subprocess
import sys
import time
import traceback
from pathlib import Path

binary, home, strace = map(Path, sys.argv[1:])
home.mkdir(mode=0o700)
names = ['ordered-sync', 'independent-wire', 'cross-process-lock', 'parallel-processes',
         'file-eio', 'directory-eio', 'parent-eio', 'retry-file-eio', 'write-enospc',
         'kill-before-rename', 'kill-after-rename', 'kill-before-parent-sync',
         'kill-retry-before-rename', 'late-file-collision', 'blocked-stdout']
report = dict(status='INCOMPLETE', binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
              strace_sha256=hashlib.sha256(strace.read_bytes()).hexdigest(),
              cases=[dict(name=name, status='NOT_RUN') for name in names])
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


def command(config, operation='prepare'):
    return [str(binary), 'access', 'cursor-keyring', operation, '--file', str(config)]


def record(result, expected=0):
    global sequence
    sequence += 1
    stem = home / ('command-' + str(sequence))
    stem.with_suffix('.stdout').write_bytes(result.stdout)
    stem.with_suffix('.stderr').write_bytes(result.stderr)
    current.setdefault('commands', []).append(dict(exit=result.returncode, log=stem.name))
    assert result.returncode == expected, (result.returncode, result.stderr)
    if expected:
        assert not result.stdout
        return result.stderr.decode()
    assert not result.stderr
    value = json.loads(result.stdout)
    assert len(value) == 8 and value['cursor_authority'] == 'not-granted'
    assert value['control_registration'] == 'not-checked'
    return value


def cli(config, operation='prepare', expected=0):
    return record(subprocess.run(command(config, operation), env={}, capture_output=True, timeout=5), expected)


def fixture(name):
    root = home / name
    root.mkdir(mode=0o700)
    keys = root / 'keys'
    keys.mkdir(mode=0o700)
    text = (Path(__file__).resolve().parents[2] / 'config/recipes/archive-only.toml').read_text()
    config = root / 'instance.toml'
    config.write_text(text + '\n[read.cursor]\nkeyring_ref=' + json.dumps('file:' + str(keys / 'keyring')) + '\n')
    return keys, config


def inspect_keys(root):
    data = (root / 'keyring').read_bytes()
    assert data[:8] == b'BBCKEY\x00\x01' and data[8:24] == bytes.fromhex('22'*16)
    count, = struct.unpack('>H', data[40:42])
    assert 1 <= count <= 16 and len(data) == 42 + 48*count + 32
    assert hashlib.sha256(data[:-32]).digest() == data[-32:]
    records = [data[42+i*48:90+i*48] for i in range(count)]
    assert data[24:40] != bytes(16)
    assert len({r[:16] for r in records}) == count
    assert all(r[:16] != bytes(16) and r[16:] != bytes(32) for r in records)
    meta = (root / 'keyring').stat()
    assert meta.st_mode & 0o7777 == 0o600 and meta.st_nlink == 1 and meta.st_uid == os.geteuid()
    return dict(keyring_id=data[24:40].hex(), key_ids=[r[:16].hex() for r in records],
                fingerprint_sha256=data[-32:].hex(), file_sha256=hashlib.sha256(data).hexdigest())


def snapshot(root):
    return {p.name: dict(sha256=hashlib.sha256(p.read_bytes()).hexdigest(), inode=p.stat().st_ino,
                         bytes=p.stat().st_size) for p in root.iterdir()}


def trace_command(config, path, injection=None):
    # Injection counts are per thread. For ENOSPC trace only the main thread,
    # otherwise the diagnostic writer's first write would fail independently.
    follow = [] if injection and injection.startswith('write:') else ['-f']
    args = [str(strace), *follow, '-yy', '-ttt', '-o', str(path), '-e',
            'trace=fsync,write,renameat2,openat,fchmod,flock', '-e', 'raw=write']
    if injection:
        args += ['-e', 'inject=' + injection]
    return args + command(config)


def traced(root, config, injection=None, expected=0):
    path = root.parent / 'syscalls.trace'
    record(subprocess.run(trace_command(config, path, injection), env={}, capture_output=True, timeout=5), expected)
    current.update(trace=str(path.relative_to(home)), trace_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return path.read_text()


def wait_fsync(process, path, target):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
        if children:
            assert len(children) == 1
            child = int(children[0])
            text = path.read_text() if path.exists() else ''
            match = re.search(r'^' + str(child) + r'\s+[0-9.]+ fsync\(\d+<([^>]+)>', text, re.M)
            # Later fsyncs require the last entry, not an already completed call.
            matches = re.findall(r'^' + str(child) + r'\s+[0-9.]+ fsync\(\d+<([^>]+)>(.*)$', text, re.M)
            if match and matches:
                name, suffix = matches[-1]
                if target(name) and '= ' not in suffix:
                    assert Path(f'/proc/{child}/exe').resolve() == binary.resolve()
                    return child
        assert process.poll() is None
        time.sleep(.005)
    raise TimeoutError('actual delayed fsync entry not observed')


def killed(process, child):
    os.killpg(process.pid, signal.SIGKILL)
    assert process.wait(timeout=3) == -signal.SIGKILL
    deadline = time.monotonic() + 3
    while child and Path(f'/proc/{child}').exists() and time.monotonic() < deadline:
        time.sleep(.01)
    assert child and not Path(f'/proc/{child}').exists()


try:
    save()
    case('ordered-sync')
    root, config = fixture('ordered')
    text = traced(root, config)
    calls = [s for s in text.splitlines() if 'fsync(' in s or 'renameat2(' in s]
    assert len(calls) == 4 and all('= 0' in s for s in calls)
    assert '.partial>' in calls[0] and 'fsync(' in calls[0]
    assert 'renameat2(' in calls[1] and 'RENAME_NOREPLACE' in calls[1]
    assert str(root) + '>' in calls[2] and str(root.parent) + '>' in calls[3]
    original = inspect_keys(root)
    inode = (root / 'keyring').stat().st_ino
    observed = cli(config, 'inspect')
    assert observed['state'] == 'inspected-local'
    for key in ['keyring_id', 'key_ids', 'fingerprint_sha256']:
        assert observed[key] == original[key]
    assert cli(config)['state'] == 'prepared-local'
    assert inspect_keys(root) == original and (root / 'keyring').stat().st_ino != inode
    passed(calls=calls, metadata=original, retry_new_inode=True)

    case('independent-wire')
    vectors = json.loads((Path(__file__).resolve().parents[2] / 'tests/fixtures/read/keyring-vectors.json').read_text())
    for vector in vectors['cases']:
        root, config = fixture('wire-' + vector['name'])
        # Frozen vectors bind dataset11; this fixture otherwise uses recipe22.
        config.write_text(config.read_text().replace('dataset_id = "' + '22'*16 + '"', 'dataset_id = "' + '11'*16 + '"'))
        path = root / 'keyring'
        path.write_bytes(bytes.fromhex(vector['hex']))
        path.chmod(0o600)
        before = snapshot(root)
        if vector['error']:
            assert vector['error'] in cli(config, 'inspect', 4)
        else:
            assert cli(config, 'inspect')['keyring_id'] == vectors['keyring_id']
        assert snapshot(root) == before
    passed(vectors=len(vectors['cases']))

    case('cross-process-lock')
    root, config = fixture('locking')
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        started = time.monotonic()
        assert 'Busy' in cli(config, expected=4) and 'Busy' in cli(config, 'inspect', 4)
        elapsed = time.monotonic() - started
        assert elapsed < 2 and not list(root.iterdir())
    finally:
        os.close(fd)
    cli(config)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        cli(config, 'inspect')
        assert 'Busy' in cli(config, expected=4)
    finally:
        os.close(fd)
    passed(exclusive_refusal_seconds=elapsed, shared_inspection=True)

    case('parallel-processes')
    root, config = fixture('parallel')
    processes = [subprocess.Popen(command(config), env={}, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(8)]
    observations = []
    try:
        for process in processes:
            stdout, stderr = process.communicate(timeout=5)
            value = record(subprocess.CompletedProcess([], process.returncode, stdout, stderr), process.returncode)
            assert process.returncode in (0, 4)
            if process.returncode == 0:
                observations.append(value)
            else:
                assert 'Busy' in value
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait()
    assert observations and all(v == observations[0] for v in observations)
    assert len(list(root.iterdir())) == 1
    passed(successes=len(observations), busy=8-len(observations), metadata=inspect_keys(root))

    for name, injection, partial, retry in [
        ('file-eio', 'fsync:error=EIO:when=1', True, False),
        ('directory-eio', 'fsync:error=EIO:when=2', False, False),
        ('parent-eio', 'fsync:error=EIO:when=3', False, False),
        ('retry-file-eio', 'fsync:error=EIO:when=1', True, True),
        ('write-enospc', 'write:error=ENOSPC:when=1', True, False),
    ]:
        case(name)
        root, config = fixture(name)
        if retry:
            cli(config)
            original = snapshot(root)
        text = traced(root, config, injection, 4)
        injected = [s for s in text.splitlines() if '(INJECTED)' in s]
        assert len(injected) == 1
        assert ('ENOSPC' if name == 'write-enospc' else 'EIO') in injected[0]
        before = snapshot(root)
        if name == 'write-enospc':
            assert 'write(' in injected[0]
            assert len(before) == 1 and next(iter(before.values()))['bytes'] == 0
        if partial:
            assert len([p for p in before if p.endswith('.partial')]) == 1
            assert 'Incomplete' in cli(config, expected=4)
            assert snapshot(root) == before
            if retry:
                assert before['keyring'] == original['keyring']
                cli(config, 'inspect')
            else:
                assert 'keyring' not in before and 'NotInitialized' in cli(config, 'inspect', 4)
        else:
            original = inspect_keys(root)
            cli(config)
            assert inspect_keys(root) == original
            assert (root/'keyring').stat().st_ino != before['keyring']['inode']
        passed(injected=injected, retained=before)

    for name, point, retry in [('kill-before-rename', 1, False), ('kill-after-rename', 2, False),
                                ('kill-before-parent-sync', 3, False), ('kill-retry-before-rename', 1, True),
                                ('late-file-collision', 1, False)]:
        case(name)
        root, config = fixture(name)
        if retry:
            cli(config)
            original = snapshot(root)
        path = root.parent/'syscalls.trace'
        child = None
        with (root.parent/'stdout').open('xb') as stdout, (root.parent/'stderr').open('xb') as stderr:
            process = subprocess.Popen(trace_command(config, path, 'fsync:delay_enter=10s:when='+str(point)),
                                       env={}, stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                child = wait_fsync(process, path, lambda p: (p.endswith('.partial') if point == 1 else
                                   p == str(root) if point == 2 else p == str(root.parent)))
                before = snapshot(root)
                if name == 'late-file-collision':
                    (root/'keyring').write_bytes(b'preserve external public fixture')
                    (root/'keyring').chmod(0o600)
                    assert process.wait(timeout=15) == 4
                    assert (root/'keyring').read_bytes() == b'preserve external public fixture'
                    assert 'EEXIST' in path.read_text() and 'RENAME_NOREPLACE' in path.read_text()
                else:
                    killed(process, child)
            finally:
                if process.poll() is None:
                    if child:
                        killed(process, child)
                    else:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=3)
        assert not (root.parent/'stdout').read_bytes()
        if point == 1:
            retained = snapshot(root)
            assert len([p for p in retained if p.endswith('.partial')]) == 1
            error = 'Mismatch' if name == 'late-file-collision' else 'Incomplete'
            assert error in cli(config, expected=4) and snapshot(root) == retained
            if retry:
                assert retained['keyring'] == original['keyring']
                cli(config, 'inspect')
        else:
            original = inspect_keys(root)
            cli(config)
            assert inspect_keys(root) == original and snapshot(root)['keyring']['inode'] != before['keyring']['inode']
        passed(observed=before, child_pid=child, trace_sha256=hashlib.sha256(path.read_bytes()).hexdigest())

    case('blocked-stdout')
    root, config = fixture('blocked-output')
    read_fd, write_fd = os.pipe()
    process = None
    try:
        os.set_blocking(write_fd, False)
        filled = 0
        while True:
            try:
                filled += os.write(write_fd, b'x'*4096)
            except BlockingIOError:
                break
        assert filled > 0
        os.set_blocking(write_fd, True)
        started = time.monotonic()
        process = subprocess.Popen(command(config), env={}, stdout=write_fd, stderr=subprocess.PIPE)
        assert process.wait(timeout=3) == 1
        elapsed = time.monotonic()-started
        assert not process.stderr.read()
    finally:
        if process and process.poll() is None:
            process.kill()
            process.wait()
        os.close(write_fd)
        os.close(read_fd)
    original = inspect_keys(root)
    assert cli(config, 'inspect')['keyring_id'] == original['keyring_id']
    cli(config)
    assert inspect_keys(root) == original
    passed(full_pipe_bytes=filled, exit=1, seconds=elapsed, durable_local_metadata=original)
    assert all(c['status'] == 'PASS' for c in report['cases'])
    report['status'] = 'PASS'
except BaseException as error:
    if current is not None:
        current['status'] = 'FAIL'
    report['failure'] = dict(kind=type(error).__name__, detail=str(error),
                            frames=[dict(file=Path(f.filename).name, line=f.lineno)
                                    for f in traceback.extract_tb(error.__traceback__)])
    raise
finally:
    save()
