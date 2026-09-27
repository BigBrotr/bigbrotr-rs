#!/usr/bin/env python3
"""Actual finite command stdout or fsync failure; no secret payloads in traces."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('case', choices=['blocked-output', 'sync-error'])
parser.add_argument('config')
parser.add_argument('output', type=Path)
args = parser.parse_args()
assert not args.output.exists()
command = ['/build/bigbrotr', 'access', 'cursor-keyring', 'register', '--file', args.config]
if args.case == 'blocked-output':
    reader, writer = os.pipe()
    flags = fcntl.fcntl(writer, fcntl.F_GETFL)
    fcntl.fcntl(writer, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    written = 0
    try:
        while True:
            written += os.write(writer, b'x' * 4096)
    except BlockingIOError:
        pass
    capacity = fcntl.fcntl(writer, fcntl.F_GETPIPE_SZ)
    assert written == capacity and capacity > 0
    fcntl.fcntl(writer, fcntl.F_SETFL, flags)
    started = time.monotonic()
    try:
        # Read end deliberately held open and never drained. Timeout covers
        # connect5 + operation30 + driver2 + finite output, not a latency SLO.
        result = subprocess.run(command, stdout=writer, stderr=subprocess.PIPE, timeout=40)
        report = dict(exit=result.returncode, elapsed_seconds=time.monotonic()-started,
                      stderr=result.stderr.decode(), pipe_bytes=capacity)
        assert result.returncode == 1 and not result.stderr
    finally:
        os.close(writer); os.close(reader)
else:
    tracer = Path('/instrument/strace-exit-fixed')
    digest = hashlib.sha256(tracer.read_bytes()).hexdigest()
    assert digest == '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
    trace = args.output.with_suffix('.trace')
    result = subprocess.run([str(tracer), '-f', '-qq', '-o', str(trace), '-e', 'trace=fsync,connect',
                             '-e', 'inject=fsync:error=EIO:when=1', *command], capture_output=True, timeout=40)
    text = trace.read_text()
    report = dict(exit=result.returncode, stderr=result.stderr.decode(),
                  trace_sha256=hashlib.sha256(trace.read_bytes()).hexdigest(), tracer_sha256=digest)
    assert result.returncode == 4 and not result.stdout and 'Filesystem' in report['stderr']
    assert 'fsync(' in text and 'EIO' in text and '(INJECTED)' in text
    # Any outbound connection would violate preparation-before-Control here.
    assert 'connect(' not in text
report['status'] = 'PASS'
args.output.write_text(json.dumps(report, indent=2)+'\n')
