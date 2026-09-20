#!/usr/bin/env python3
"""Finite supervisor for a real old native owner; laboratory input is trusted.

Observe its POSIX Rocks lock while its PostgreSQL backend is independently killed.
Every exit path kills and joins the child. No claim about mid-FFI interruption.
"""
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time


def main(args):
    assert len(args) in (4, 6)
    executable, database, secret, home = args[:4]
    mode, marker = args[4:] if len(args) == 6 else ('before', 'before-native')
    p = None
    selector = selectors.DefaultSelector()
    output = b''
    def emit(value): print(json.dumps(value), flush=True)
    def observe(event):
        assert p.poll() is None
        metadata = (Path(home)/'LOCK').stat()
        inode = metadata.st_ino
        device = [os.major(metadata.st_dev), os.minor(metadata.st_dev)]
        locks = []
        for line in Path('/proc/locks').read_text().splitlines():
            fields = line.split()
            if len(fields) != 8 or fields[1:4] != ['POSIX','ADVISORY','WRITE'] or fields[4] != str(p.pid): continue
            major,minor,number = fields[5].split(':')
            if [int(major,16),int(minor,16)] == device and int(number) == inode:
                # The initial number is the current iterator position, not a
                # stable kernel lock identity. Compare the actual lock fields.
                locks.append(fields[1:])
        assert len(locks) == 1
        emit(dict(event=event, pid=p.pid, start=Path(f'/proc/{p.pid}/stat').read_text().split()[21],
                  executable_sha256=hashlib.sha256(Path(f'/proc/{p.pid}/exe').read_bytes()).hexdigest(),
                  device=device, inode=inode, locks=locks))
    try:
        p = subprocess.Popen([executable, mode, database, secret, home],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        selector.register(p.stdout, selectors.EVENT_READ)
        deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            assert p.poll() is None
            if selector.select(.1):
                part = os.read(p.stdout.fileno(), 65536)
                assert part
                output += part
                assert len(output) <= 65536
                if any(json.loads(line).get('event') == marker
                       for line in output.split(b'\n')[:-1]): break
        else: raise RuntimeError('old native marker deadline')
        selector.unregister(p.stdout)
        selector.register(sys.stdin, selectors.EVENT_READ)
        observe('old-held')
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            if not selector.select(.1): continue
            action = sys.stdin.readline().strip()
            if action == 'observe': observe('old-still-held')
            elif action == 'kill':
                os.killpg(p.pid, signal.SIGKILL)
                stdout, stderr = p.communicate(timeout=5)
                assert p.returncode == -9
                emit(dict(event='old-joined', exit=p.returncode, stdout=(output+stdout).decode(),
                          stderr=stderr.decode(), joined=True))
                return
            else: raise RuntimeError('invalid/closed supervisor input')
        raise RuntimeError('supervisor command deadline')
    finally:
        try:
            if p is not None and p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait(timeout=5)
        finally:
            selector.close()
            if p is not None:
                p.stdout.close(); p.stderr.close()


if __name__ == '__main__':
    main(sys.argv[1:])
