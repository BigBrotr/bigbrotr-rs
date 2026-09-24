#!/usr/bin/env python3
"""Real Linux filesystem calls with error/kill controls; no SQL or power-loss claim.

Run only inside the inventoried, unprivileged, no-network laboratory container.
The separate alias invocation needs an explicit Docker bind of the owned final
onto the same unit's staging path. No mount syscall or global host setting here.
"""
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import select
import signal
import subprocess
import sys
import time
import traceback

probe, home, tracer = map(Path, sys.argv[1:4])
alias_only = sys.argv[4:] == ['alias']
assert len(sys.argv) == (5 if alias_only else 4)
ID = '10' * 16
STAGE, LOCK = '.artifact-' + ID + '.partial', '.artifact-' + ID + '.lock'
source = Path(__file__).resolve().parents[2]
manifest = (source / 'tests/fixtures/exports/signed-jsonl-manifest.json').read_bytes()
golden = json.loads(manifest)
expected_hash = golden['files'][0]['sha256']
report = dict(status='INCOMPLETE', probe_sha256=hashlib.sha256(probe.read_bytes()).hexdigest(),
              tracer_sha256=hashlib.sha256(tracer.read_bytes()).hexdigest(), cases=[])
report_path = home / ('alias-result.json' if alias_only else 'result.json')
current = None
sequence = 0


def save():
    report_path.write_text(json.dumps(report, indent=2) + '\n')


def case(name):
    global current
    current = dict(name=name, status='RUNNING')
    report['cases'].append(current)
    save()


def passed(**details):
    current.update(status='PASS', **details)
    save()


def run(root, operation='write', expected=0, inject=None, trace=False, file_limit=None):
    global sequence
    sequence += 1
    stem = home / ('command-' + ('alias-' if alias_only else '') + str(sequence))
    command = [str(probe), operation, str(root)]
    path = stem.with_suffix('.trace')
    if trace or inject:
        command = [str(tracer), '-f', '-yy', '-ttt', '-o', str(path), '-e',
                   'trace=fsync,write,renameat2,openat,fchmodat,flock,statx,unlinkat,mkdirat',
                   *(['-e', 'inject=' + inject] if inject else []), *command]
    def limit():
        resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    result = subprocess.run(command, capture_output=True, env={}, timeout=5,
                            preexec_fn=limit if file_limit is not None else None)
    stem.with_suffix('.stdout').write_bytes(result.stdout)
    stem.with_suffix('.stderr').write_bytes(result.stderr)
    current.setdefault('commands', []).append(dict(command=command, exit=result.returncode,
                                                  log=stem.name))
    assert result.returncode == expected and not result.stderr, (result.returncode, result.stderr)
    value = json.loads(result.stdout)
    if expected == 0 and operation == 'write':
        assert value == dict(data_bytes=334, data_sha256=expected_hash,
                            manifest_bytes=len(manifest), manifest_sha256=hashlib.sha256(manifest).hexdigest())
    return value, path.read_text() if path.exists() else ''


def fixture(name):
    root = home / name
    root.mkdir(mode=0o700)
    run(root, 'initialize')
    return root


def files(root):
    """Independent byte/hash/field and private-mode oracle, not product output."""
    final = root / ID
    assert sorted(p.name for p in final.iterdir()) == ['events.jsonl', 'manifest.json']
    data = (final / 'events.jsonl').read_bytes()
    assert len(data) == 334 and hashlib.sha256(data).hexdigest() == expected_hash
    assert data.endswith(b'\n') and data.count(b'\n') == 1
    event = json.loads(data)
    fixture = json.loads((source / 'tests/fixtures/protocol/signed-extremes.json').read_bytes())
    assert event == fixture['events']['timestamp-zero']['event']
    assert (final / 'manifest.json').read_bytes() == manifest
    assert final.stat().st_mode & 0o7777 == 0o700
    for path in final.iterdir():
        assert path.stat().st_mode & 0o7777 == 0o600 and path.stat().st_nlink == 1
    return {p.name: dict(inode=p.stat().st_ino, bytes=p.stat().st_size,
                         sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in final.iterdir()}


def complete_calls(trace):
    return [line for line in trace.splitlines() if re.search(r' (fsync|renameat2)\(', line)]


try:
    if alias_only:
        assert home.is_dir()
        case('same-filesystem-bind-alias-preserved')
        root = home / 'bind-alias'
        before = files(root)
        a, b = (root / ID).stat(), (root / STAGE).stat()
        assert (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)
        mountinfo = Path('/proc/self/mountinfo').read_text()
        assert ' ' + str(root / STAGE) + ' ' in mountinfo
        (home / 'alias-mountinfo').write_text(mountinfo)
        value, trace = run(root, expected=4, trace=True)
        assert value == dict(error='Conflict') and not complete_calls(trace)
        assert 'unlinkat(' not in trace and 'mkdirat(' not in trace
        assert files(root) == before
        assert (root / STAGE / 'manifest.json').read_bytes() == manifest
        passed(final_unchanged=before, no_unlink=True, actual_mount=True)
    else:
        home.mkdir(mode=0o700)
        case('ordered-sync-and-identical-exchange')
        root = fixture('ordered')
        _, trace = run(root, trace=True)
        calls = complete_calls(trace)
        assert len(calls) == 5 and all('= 0' in line for line in calls), calls
        for line, path in zip(calls[:3], [root / STAGE / 'events.jsonl', root / STAGE / 'manifest.json', root / STAGE]):
            assert 'fsync(' in line and '<' + str(path) + '>' in line
        assert 'RENAME_NOREPLACE' in calls[3] and 'fsync(' in calls[4] and '<' + str(root) + '>' in calls[4]
        before = files(root)
        _, trace = run(root, trace=True)
        assert 'RENAME_EXCHANGE' in trace
        after = files(root)
        assert all(before[name]['inode'] != after[name]['inode'] for name in before)
        assert (root / STAGE / 'events.jsonl').stat().st_ino == before['events.jsonl']['inode']
        for _ in range(3):
            run(root)
            files(root)
            assert sorted(p.name for p in root.iterdir()) == sorted(['destination.json', ID, STAGE, LOCK])
        passed(calls=calls, before=before, after=after, fixed_staging_count=1)

        for point in range(1, 5):
            case('fsync-eio-' + str(point))
            root = fixture('eio-' + str(point))
            value, trace = run(root, expected=4, inject='fsync:error=EIO:when=' + str(point))
            assert value == dict(error='Filesystem')
            injected = [line for line in trace.splitlines() if '(INJECTED)' in line]
            assert len(injected) == 1 and '= -1 EIO ' in injected[0]
            assert (root / ID).exists() == (point == 4)
            before_inode = next(root.rglob('events.jsonl')).stat().st_ino
            run(root)
            result = files(root)
            if point == 4:
                assert result['events.jsonl']['inode'] != before_inode
                assert (root / STAGE / 'events.jsonl').stat().st_ino == before_inode
            passed(injected=injected, same_unit_retry=result)

        case('exchange-root-eio-and-cleanup-eio')
        root = fixture('exchange-eio')
        run(root)
        before = files(root)
        value, trace = run(root, expected=4, inject='fsync:error=EIO:when=4')
        assert value == dict(error='Filesystem') and 'RENAME_EXCHANGE' in trace
        assert len([s for s in trace.splitlines() if '(INJECTED)' in s and '= -1 EIO' in s]) == 1
        after = files(root)
        assert after['events.jsonl']['inode'] != before['events.jsonl']['inode']
        assert (root / STAGE / 'events.jsonl').stat().st_ino == before['events.jsonl']['inode']
        value, trace = run(root, expected=4, inject='fsync:error=EIO:when=1')
        assert value == dict(error='Filesystem') and not (root / STAGE).exists()
        assert 'unlinkat(' in trace and 'renameat2(' not in trace
        assert files(root) == after
        run(root)
        result = files(root)
        assert result['events.jsonl']['inode'] != after['events.jsonl']['inode']
        passed(before=before, ambiguous_final=after, recovered=result)

        case('actual-partial-write-and-retry')
        root = fixture('partial-write')
        # Do not trace under this limit: it would also limit strace's own log.
        value, _ = run(root, expected=4, file_limit=17)
        assert value == dict(error='Filesystem') and not (root / ID).exists()
        partial = (root / STAGE / 'events.jsonl').read_bytes()
        assert partial == b'{"id":"e258cc73c1' and len(partial) == 17
        assert not (root / STAGE / 'manifest.json').exists()
        run(root)
        passed(kernel_rlimit_fsize=17, actual_partial_hex=partial.hex(), recovered=files(root))

        case('action-expiry-during-actual-data-write-with-negative-control')
        observations=[]
        for gated in [False,True]:
            root=fixture('action-expiry-'+str(gated).lower())
            value,trace=run(root, operation='write-window' if gated else 'write-window-negative',
                expected=4 if gated else 0, inject='write:delay_exit=250ms:when=1')
            assert value['witness']==dict(action_expired=True,control_valid=True,cancelled=False),value
            data_calls=[line for line in trace.splitlines() if 'write(' in line and '<'+str(root/STAGE/'events.jsonl')+'>' in line]
            assert len(data_calls)==1 and '= 334' in data_calls[0] and '(DELAYED)' in data_calls[0],data_calls
            if gated:
                assert value['error']=='Cancelled' and not (root/ID).exists()
                data=(root/STAGE/'events.jsonl').read_bytes()
                assert len(data)==334 and hashlib.sha256(data).hexdigest()==expected_hash
                assert not (root/STAGE/'manifest.json').exists()
                assert not complete_calls(trace)
            else:
                assert value['data_bytes']==334
                files(root)
                assert len(complete_calls(trace))==5
            observations.append(dict(gated=gated,result=value,actual_delayed_write=data_calls[0]))
        passed(observations=observations,scope='One admitted data write may finish after expiry; no later write/sync/manifest publication on the gated path')

        case('kernel-mount-identity-unavailable')
        root = fixture('statx-unavailable')
        # Rust's ordinary metadata also calls statx(STATX_ALL) and can fall back
        # after ENOSYS. Fail every statx, including the explicit mount-ID query;
        # failing only the first metadata call leaves the tested feature present.
        value, trace = run(root, expected=4, inject='statx:error=ENOSYS:when=1+')
        assert value == dict(error='Filesystem')
        assert any('STATX_MNT_ID, ' in line and '= -1 ENOSYS' in line and '(INJECTED)' in line
                   for line in trace.splitlines())
        assert not (root / ID).exists() and not (root / STAGE).exists()
        passed(refused=True)

        case('cross-process-lock')
        root = fixture('locking')
        lock = root / LOCK
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            start = time.monotonic()
            value, _ = run(root, expected=4)
            assert value == dict(error='Busy') and time.monotonic() - start < 2
            assert not (root / ID).exists() and not (root / STAGE).exists()
        finally:
            os.close(fd)
        run(root)
        passed(recovered=files(root))

        for name, point, retry in [('kill-data-sync', 1, False), ('kill-final-sync', 4, False), ('kill-exchange-sync', 4, True)]:
            case(name)
            root = fixture(name)
            if retry:
                run(root)
            path = home / (name + '.trace')
            target = root / STAGE / 'events.jsonl' if point == 1 else root
            command = [str(tracer), '-f', '-yy', '-ttt', '-o', str(path), '-e', 'trace=fsync,renameat2',
                       '-e', 'inject=fsync:delay_enter=10s:when=' + str(point), str(probe), 'write', str(root)]
            child = None
            pidfd = None
            with (home / (name + '.stdout')).open('xb') as stdout, (home / (name + '.stderr')).open('xb') as stderr:
                process = subprocess.Popen(command, env={}, stdout=stdout, stderr=stderr, start_new_session=True)
                try:
                    deadline = time.monotonic() + 3
                    while time.monotonic() < deadline:
                        assert process.poll() is None
                        children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
                        if children and child is None:
                            assert len(children) == 1
                            child = int(children[0]); pidfd = os.pidfd_open(child)
                        entries = path.read_text().splitlines() if path.exists() else []
                        entered = child and any(re.fullmatch(str(child) + r'\s+[0-9.]+ fsync\(\d+<' + re.escape(str(target)) + r'>', line) for line in entries)
                        if entered:
                            break
                        time.sleep(.005)
                    else:
                        raise TimeoutError('no actual blocked fsync entry')
                    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                    assert process.wait(timeout=3) in [-signal.SIGKILL, 128 + signal.SIGKILL]
                    poll = select.poll(); poll.register(pidfd, select.POLLIN)
                    assert poll.poll(1000)
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)
                    if pidfd is not None:
                        try:
                            if not select.select([pidfd], [], [], 0)[0]:
                                signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                            assert select.select([pidfd], [], [], 3)[0]
                        finally:
                            os.close(pidfd)
            assert not (home / (name + '.stdout')).read_bytes()
            assert not (home / (name + '.stderr')).read_bytes()
            assert '+++ killed by SIGKILL +++' in path.read_text()
            assert child and not Path(f'/proc/{child}').exists()
            assert (root / ID).exists() == (point == 4 or retry)
            before = files(root) if point == 4 else None
            run(root)
            after = files(root)
            if before:
                assert before['events.jsonl']['inode'] != after['events.jsonl']['inode']
            passed(blocked_fsync_entry=True, pidfd_kill=True, actual_child_absent=True, recovered=after)

        case('actual-tmpfs-enospc-and-retry')
        limited = Path('/limited')
        assert ' /limited ' in Path('/proc/self/mountinfo').read_text()
        root = limited / 'export'
        root.mkdir(mode=0o700)
        run(root, 'initialize')
        filler = limited / 'owned-filler'
        filled = 0
        try:
            with filler.open('xb', buffering=0) as out:
                while True:
                    filled += out.write(b'x' * 4096)
        except OSError as error:
            assert error.errno == errno.ENOSPC and filled > 0
        value, trace = run(root, expected=4, trace=True)
        assert value == dict(error='Filesystem') and 'ENOSPC' in trace
        assert not (root / ID).exists()
        filler.unlink()
        run(root)
        passed(filler_bytes=filled, recovered=files(root), filesystem='bounded tmpfs, not durability qualification')

        # Leave an initialized, verified root for the separately inventoried bind
        # alias container, which uses these SAME underlying inodes.
        case('prepare-bind-alias')
        root = fixture('bind-alias')
        run(root)
        (root / STAGE).mkdir(mode=0o700)
        passed(prepared=files(root))
    assert report['cases'] and all(c['status'] == 'PASS' for c in report['cases'])
    report['status'] = 'PASS'
except BaseException as error:
    report['failure'] = dict(kind=type(error).__name__, detail=str(error),
                            frames=[dict(file=Path(f.filename).name, line=f.lineno) for f in traceback.extract_tb(error.__traceback__)])
    raise
finally:
    save()
