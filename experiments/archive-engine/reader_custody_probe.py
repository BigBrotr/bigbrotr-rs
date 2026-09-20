#!/usr/bin/env python3
"""Trusted Linux lab observer, executed inside the inventoried runner only.

Reads /proc without printing command lines or unrelated descriptors. Signals use
pidfds after exact process-start checks; a numeric saved PID is not authority.
"""
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import sys
import time


def identity(pid):
    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return dict(pid=pid, start=stat[19], parent=int(stat[1]))


def inspect(binary, request, root):
    parents = []
    for p in Path('/proc').iterdir():
        if not p.name.isdecimal():
            continue
        try:
            args = (p/'cmdline').read_bytes().split(b'\0')
            if args[:2] == [os.fsencode(binary), os.fsencode(request)]:
                parents.append(identity(int(p.name)))
        except (FileNotFoundError, ProcessLookupError):
            pass
    assert len(parents) == 1, 'unique fixture parent required'
    parent = parents[0]
    children = []
    for p in Path('/proc').iterdir():
        if not p.name.isdecimal():
            continue
        try:
            found = identity(int(p.name))
            if found['parent'] == parent['pid'] and os.readlink(p/'exe') == '/out/archive-worker/bigbrotr-archive-worker':
                children.append(found)
        except (FileNotFoundError, ProcessLookupError):
            pass
    assert len(children) == 1, 'unique actual native worker required'
    child = children[0]
    lock = Path(root)/'.bigbrotr-initialization/custody.lock'
    m = lock.stat()
    held = []
    for fd in Path(f"/proc/{child['pid']}/fd").iterdir():
        try:
            found = fd.stat()
            if (found.st_dev, found.st_ino) == (m.st_dev, m.st_ino):
                held.append(dict(fd=int(fd.name), device=found.st_dev, inode=found.st_ino,
                                 info=Path(f"/proc/{child['pid']}/fdinfo/{fd.name}").read_text()))
        except FileNotFoundError:
            pass
    assert identity(parent['pid']) == parent and identity(child['pid']) == child
    return dict(parent=parent, child=child, custody=held, lock=dict(device=m.st_dev, inode=m.st_ino))


def terminate(observed):
    parent, child = observed['parent'], observed['child']
    result = dict(status='INCOMPLETE', cleanup='NOT_RUN')
    with os.fdopen(os.pidfd_open(parent['pid']), 'rb') as parent_fd, os.fdopen(os.pidfd_open(child['pid']), 'rb') as child_fd:
        identified = False
        def exited(seconds):
            wait = select.poll()
            for fd in (parent_fd, child_fd):
                wait.register(fd, select.POLLIN)
            remaining = {parent_fd.fileno(), child_fd.fileno()}
            until = time.monotonic()+seconds
            while remaining and time.monotonic() < until:
                for fd, events in wait.poll(max(0, int((until-time.monotonic())*1000))):
                    if events & select.POLLIN:
                        remaining.discard(fd)
                        wait.unregister(fd)
            return not remaining
        try:
            assert identity(parent['pid']) == parent and identity(child['pid']) == child
            identified = True
            signal.pidfd_send_signal(child_fd.fileno(), signal.SIGSTOP)
            until = time.monotonic()+5
            while Path(f"/proc/{child['pid']}/stat").read_text().rsplit(')', 1)[1].split()[0] != 'T':
                assert time.monotonic() < until, 'native worker stop unobserved'
                time.sleep(.002)
            result['worker_stopped_before_parent_death'] = True
            # Death must kill even a stopped native child. Positive pidfd exit
            # observations remain distinct from cleanup after a failed case.
            signal.pidfd_send_signal(parent_fd.fileno(), signal.SIGKILL)
            assert exited(5), 'actual parent/native worker exit unobserved'
            result.update(status='PASS', parent_exited=True, worker_exited=True)
        except Exception as error:
            result.update(status='FAIL', failure=type(error).__name__)
        finally:
            if identified:
                failures = []
                for fd in (parent_fd, child_fd):
                    try:
                        signal.pidfd_send_signal(fd.fileno(), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    except OSError as error:
                        failures.append(type(error).__name__)
                result['cleanup'] = 'EXIT_OBSERVED' if exited(5) else 'UNKNOWN'
                result['cleanup_errors'] = failures
    return result


def reacquire(root):
    with (Path(root)/'.bigbrotr-initialization/custody.lock').open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return dict(lock_reacquired=True)


if __name__ == '__main__':
    if sys.argv[1] == 'inspect':
        result = inspect(*sys.argv[2:])
    elif sys.argv[1] == 'terminate':
        result = terminate(json.load(sys.stdin))
    elif sys.argv[1] == 'reacquire':
        result = reacquire(sys.argv[2])
    else:
        raise ValueError('unknown probe')
    print(json.dumps(result))
