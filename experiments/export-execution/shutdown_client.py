#!/usr/bin/env python3
"""Signal the actual installed parent; retain its exit without assuming success."""
import json
from pathlib import Path
import signal
import socket
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available

private = sys.argv[1].startswith('private-')
server = http.Server('job-shutdown-' + sys.argv[1])
try:
    emit('ready', owner=server.ready(3 if private else 2), parent_pid=server.process.pid)
    assert json.loads(sys.stdin.readline()) == dict(operation='signal')
    started = time.monotonic()
    server.process.send_signal(signal.SIGTERM)
    emit('signalled')
    if private:
        time.sleep(.1)
        running = server.process.poll() is None
        try:
            with socket.create_connection(('127.0.0.1',18080),timeout=.2) as peer:
                peer.sendall(b'GET /management/v1/me HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nConnection: close\r\n\r\n')
                closed = peer.recv(1) == b''
        except (ConnectionRefusedError, ConnectionResetError):
            closed = True
        emit('admission', parent_running=running, closed=closed)
    code = server.process.wait(timeout=10)
    emit('terminal', exit=code, elapsed_seconds=time.monotonic()-started,
         parent_absent=not Path(f'/proc/{server.process.pid}').exists(),
         lock_available=lock_available(), scratch_empty=not list((http.ROOT/'scratch').iterdir()),
         stdout=server.output.read_text(), stderr=server.error.read_text())
finally:
    server.cleanup()
