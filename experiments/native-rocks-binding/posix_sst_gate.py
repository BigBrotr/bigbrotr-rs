#!/usr/bin/env python3
"""Experimental delay-only SST gate controller; no syscall error substitution.

Run inside the existing isolated 64MiB /posix container envelope. The unchanged
Rust test owns behavioral oracles. Control records must be outside /evidence.
The outer envelope must freeze/copy the container on its unchanged deadline.
"""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import stat
import subprocess
import time

if not __debug__:
    raise RuntimeError('controller requires Python assertions enabled')

TEST = ('storage::rocks::store::native_posix_tests::'
        'real_posix_background_nospace_latches_application_owner')
CAPACITY = 64 * 1024 * 1024
MAX_RECORDS = 65


def sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def validate_native(records, pid):
    """Check raw observations, never a substituted error or allocation result."""
    assert records and len(records) <= MAX_RECORDS
    records = sorted(records, key=lambda record: record['seq'])
    assert [r['seq'] for r in records] == list(range(1, len(records) + 1))
    assert all(r['pid'] == pid and r['invalid'] == 0 for r in records)
    assert all(r['event'] in ('ready', 'gate', 'released', 'io', 'end') for r in records)
    assert records[0]['event'] == 'ready' and records[-1]['event'] == 'end'
    def one(event):
        selected = [r for r in records if r['event'] == event]
        assert len(selected) == 1, 'missing/duplicate ' + event
        return selected[0]
    one('ready'); one('end')
    gate, released = one('gate'), one('released')
    assert re.fullmatch(r'/posix/db/[0-9]+\.sst', gate['path'])
    assert gate['fd'] >= 0 and gate['returned'] == gate['fd'] and gate['tid'] != pid
    assert gate['flags'] & (os.O_CREAT | os.O_TRUNC) == os.O_CREAT | os.O_TRUNC
    assert gate['flags'] & os.O_ACCMODE in (os.O_WRONLY, os.O_RDWR)
    assert gate['size'] == gate['blocks'] == 0 and gate['dev'] and gate['ino']
    assert released['seq'] > gate['seq']
    for key in ('fd', 'tid', 'path', 'dev', 'ino', 'flags', 'size', 'blocks', 'returned'):
        assert released[key] == gate[key]
    writes = [r for r in records if r['event'] == 'io']
    assert writes and all(r['seq'] > released['seq'] for r in writes)
    for record in writes:
        assert record['operation'] in ('write', 'pwrite', 'pwrite64')
        assert record['count'] > 0
        assert record['returned'] == -1 or 0 <= record['returned'] <= record['count']
        if record['operation'] == 'write':
            assert record['offset'] == -1
        else:
            assert record['offset'] >= 0
        assert all(record[k] == gate[k] for k in ('fd', 'tid', 'dev', 'ino'))
    failed = [r for r in writes if r['returned'] == -1 and r['errno'] == errno.ENOSPC]
    assert failed, 'no actual selected-SST write/pwrite returned raw ENOSPC'
    return dict(gate=gate, real_enospc_writes=failed)


def validate_rust(evidence):
    events = [json.loads(line) for line in (evidence/'events.jsonl').read_text().splitlines()]
    assert [e['event'] for e in events] == [
        'identity', 'healthy', 'filler_enospc', 'flush_scheduled',
        'native_nospace', 'sticky_refusal', 'close_return', 'terminal']
    assert events[-1]['status'] == 'PASS' and events[-2]['owner_native_bytes'] == 0
    assert events[1]['sequence'] == events[5]['sequence']
    assert len(events[1]['originals_sha256']) == 2 and events[1]['staged_request'] == 2
    retained = json.loads((evidence/'retained-files.json').read_text())
    assert retained
    for item in retained:
        path = evidence/Path(item['path']).relative_to('/evidence')
        assert path.is_file() and not path.is_symlink()
        assert path.stat().st_size == item['bytes'] and sha(path) == item['sha256']
    return events


def best_effort_emit(emit, event, **fields):
    # A broken evidence device must not turn a custody failure into tmpfs loss.
    try:
        emit(event, **fields)
    except BaseException:
        pass


def keep_alive_for_outer(emit, event, **fields):
    best_effort_emit(emit, event, **fields)
    while True:
        time.sleep(1)


def join_and_preserve(child, root, evidence, emit, failed):
    """Return only after join and, on failure, a complete DB custody copy.

    The unchanged outer deadline freezes/copies a live controller if either
    operation cannot complete. Cleanup logging can never bypass that fallback.
    """
    if child is None:
        return
    try:
        running = child.poll() is None
    except BaseException as error:
        keep_alive_for_outer(emit, 'unknown_child_keep_alive_for_outer_custody',
                             failure=repr(error))
    if running:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            # It may have exited between poll and killpg. Still join it.
            pass
        except BaseException as error:
            best_effort_emit(emit, 'child_signal_failed', failure=repr(error))
    try:
        child.wait(timeout=5)
    except BaseException as error:
        keep_alive_for_outer(emit, 'unjoined_keep_alive_for_outer_custody',
                             pid=child.pid, failure=repr(error))
    if running:
        best_effort_emit(emit, 'child_join_after_failure',
                         pid=child.pid, returncode=child.returncode)
    if failed:
        try:
            if (root/'db').exists():
                # A partial retained-files.json is not proof of preservation.
                shutil.copytree(root/'db', evidence/'abnormal-db', symlinks=True)
                best_effort_emit(emit, 'abnormal_db_preserved')
        except BaseException as error:
            keep_alive_for_outer(emit, 'copy_failed_keep_alive_for_outer_custody',
                                 failure=repr(error))


def supervise(binary, binary_sha256, library, library_sha256, root, evidence, control):
    assert root == Path('/posix') and root.resolve() == root
    for path in (evidence, control):
        assert path.is_absolute() and path.resolve() == path and path.is_dir()
        assert path.stat().st_uid == os.getuid() and not any(path.iterdir())
        assert path.stat().st_dev != root.stat().st_dev
    assert evidence != control and evidence not in control.parents and control not in evidence.parents
    assert not any(root.iterdir()) and root.stat().st_uid == os.getuid()
    assert os.statvfs(root).f_blocks * os.statvfs(root).f_frsize == CAPACITY
    for path, expected in ((binary, binary_sha256), (library, library_sha256)):
        assert path.is_absolute() and stat.S_ISREG(path.lstat().st_mode) and sha(path) == expected
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    root_identity = os.fstat(root_fd)
    journal = (control/'events.jsonl').open('x')
    def emit(event, **fields):
        journal.write(json.dumps(dict(event=event, monotonic=time.monotonic(), **fields))+'\n')
        journal.flush(); os.fsync(journal.fileno())
    read_fd, write_fd = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
    release_read, release_write = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
    env = dict(LC_ALL='C', BIGBROTR_NATIVE_POSIX_ROOT=str(root),
               BIGBROTR_NATIVE_POSIX_EVIDENCE=str(evidence), LD_PRELOAD=str(library),
               BIGBROTR_SST_GATE_JOURNAL_FD=str(write_fd),
               BIGBROTR_SST_GATE_RELEASE_FD=str(release_read))
    argv = [str(binary), '--exact', TEST, '--ignored', '--nocapture']
    child = None; filler_fd = None; filler_name = None; filler_identity = None
    native = []; buffer = bytearray(); failure = None; eof = False
    gate_count = 0; released = False; refill = None; started = time.monotonic()
    selector = selectors.DefaultSelector()
    def proc_identity(record):
        assert child is not None and child.poll() is None and record['pid'] == child.pid
        assert re.fullmatch(r'/posix/db/[0-9]+\.sst', record['path'])
        descriptor = Path(f'/proc/{child.pid}/fd/{record["fd"]}')
        info = descriptor.stat()
        assert os.readlink(descriptor) == record['path']
        assert stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
        assert (info.st_dev, info.st_ino, info.st_size, info.st_blocks) == (
            record['dev'], record['ino'], 0, 0)
        assert info.st_dev == root_identity.st_dev
        fdinfo = dict(line.split(':', 1) for line in Path(
            f'/proc/{child.pid}/fdinfo/{record["fd"]}').read_text().splitlines())
        assert int(fdinfo['flags'], 8) & os.O_ACCMODE in (os.O_WRONLY, os.O_RDWR)
        comm = Path(f'/proc/{child.pid}/task/{record["tid"]}/comm').read_text().strip()
        assert comm.startswith('rocksdb:'), 'selected open is not a named RocksDB background thread'
        return dict(pid=child.pid, tid=record['tid'], comm=comm, fd=record['fd'],
                    path=record['path'], dev=info.st_dev, ino=info.st_ino,
                    size=info.st_size, blocks=info.st_blocks, fdinfo=fdinfo)
    def handle(line):
        nonlocal gate_count, filler_fd, filler_name, filler_identity, released, refill
        assert len(line) <= 1024 and len(native) < MAX_RECORDS
        record = json.loads(line)
        native.append(record); emit('native', record=record)
        assert record['pid'] == child.pid and record['invalid'] == 0
        assert record['event'] in ('ready', 'gate', 'released', 'io', 'end')
        if record['event'] != 'gate':
            return
        gate_count += 1; assert gate_count == 1
        identity = proc_identity(record); emit('gate_verified', **identity)
        before = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
        filler_name = 'sst-gate-pressure-' + str(child.pid)
        filler_fd = os.open(filler_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                            os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=root_fd)
        filler_identity = os.fstat(filler_fd)
        assert filler_identity.st_dev == root_identity.st_dev and filler_identity.st_nlink == 1
        refill_started = time.monotonic(); total = 0
        while True:
            assert time.monotonic() - refill_started < 3, 'refill exceeded bounded gate budget'
            try:
                count = os.write(filler_fd, b'\xa5' * 65536)
                assert count > 0
                total += count; assert total <= CAPACITY
            except OSError as error:
                assert error.errno == errno.ENOSPC, 'refill did not reach real raw ENOSPC'
                break
        available = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
        assert available == 0
        after = os.fstat(filler_fd); proc_identity(record)
        refill = dict(before_available=before, available_bytes=available, bytes=total,
                      errno=errno.ENOSPC, dev=after.st_dev, ino=after.st_ino,
                      allocated_bytes=after.st_blocks * 512, path=str(root/filler_name))
        emit('refill_enospc', **refill)
        assert os.write(release_write, b'R') == 1
        released = True; emit('release_sent', fd=record['fd'], tid=record['tid'])
    try:
        emit('controller_start', argv=argv, binary_sha256=binary_sha256,
             library_sha256=library_sha256, deadline_seconds=65, gate_seconds=5)
        child = subprocess.Popen(argv, env=env, pass_fds=(write_fd, release_read),
                                 start_new_session=True)
        os.close(write_fd); write_fd = None
        os.close(release_read); release_read = None
        emit('child_started', pid=child.pid, stat=Path(f'/proc/{child.pid}/stat').read_text())
        selector.register(read_fd, selectors.EVENT_READ)
        while child.poll() is None or not eof:
            assert time.monotonic() - started < 65, 'controller deadline; no runtime/close PASS'
            if eof:
                time.sleep(.01); continue
            for _, _ in selector.select(.05):
                data = os.read(read_fd, 4096)
                if not data:
                    eof = True; selector.unregister(read_fd); break
                buffer.extend(data); assert len(buffer) <= 65536
                while b'\n' in buffer:
                    line, _, rest = buffer.partition(b'\n'); buffer[:] = rest
                    handle(line)
        assert not buffer
        child.wait(); emit('child_join', pid=child.pid, returncode=child.returncode)
        assert child.returncode == 0
        assert gate_count == 1 and released and refill is not None
        observation = validate_native(native, child.pid)
        validate_rust(evidence)
        emit('validated', observation=observation)
    except BaseException as error:
        failure = type(error).__name__ + ': ' + str(error)
        best_effort_emit(emit, 'controller_failure', failure=failure)
    finally:
        join_and_preserve(child, root, evidence, emit, failed=failure is not None)
        # Only the joined child can no longer need or mutate these exact files.
        if filler_fd is not None:
            current = os.stat(filler_name, dir_fd=root_fd, follow_symlinks=False)
            assert stat.S_ISREG(current.st_mode) and current.st_nlink == 1
            assert (current.st_dev, current.st_ino) == (filler_identity.st_dev, filler_identity.st_ino)
            assert (os.fstat(root_fd).st_dev, os.fstat(root_fd).st_ino) == (root_identity.st_dev, root_identity.st_ino)
            os.unlink(filler_name, dir_fd=root_fd); os.close(filler_fd)
            emit('owned_pressure_unlinked_after_join', path=str(root/filler_name))
        for fd in (read_fd, write_fd, release_read, release_write, root_fd):
            if fd is not None: os.close(fd)
        selector.close()
    assert sha(binary) == binary_sha256 and sha(library) == library_sha256
    result = dict(status='PASS' if failure is None else 'FAIL', failure=failure,
                  child_returncode=child.returncode if child else None, gate_count=gate_count,
                  released=released, refill=refill, native_records=native,
                  scope='Experimental delay-only interposition plus actual kernel ENOSPC; Rust oracles unchanged. No production Env, option or syscall result replacement.')
    (control/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    emit('controller_terminal', status=result['status']); journal.close()
    return 0 if failure is None else 1


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--binary-sha256', required=True)
    p.add_argument('--library', type=Path, required=True)
    p.add_argument('--library-sha256', required=True)
    p.add_argument('--root', type=Path, default=Path('/posix'))
    p.add_argument('--evidence', type=Path, default=Path('/evidence'))
    p.add_argument('--control', type=Path, required=True)
    args = p.parse_args()
    for value in (args.binary_sha256, args.library_sha256):
        assert re.fullmatch('[0-9a-f]{64}', value)
    return supervise(args.binary, args.binary_sha256, args.library,
                     args.library_sha256, args.root, args.evidence, args.control)


if __name__ == '__main__':
    raise SystemExit(main())
