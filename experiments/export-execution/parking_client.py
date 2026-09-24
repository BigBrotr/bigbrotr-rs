#!/usr/bin/env python3
"""Observe the real parent even if a known export refusal ends it before polling."""
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available


def main(label):
    assert label in ('attempt', 'restart')
    server = http.Server('executor-parking-' + label)
    try:
        end = time.monotonic() + 10
        while True:
            lines = server.output.read_text().splitlines()
            if lines:
                owner = json.loads(lines[0])
                break
            assert server.process.poll() is None, server.error.read_text()
            if time.monotonic() >= end:
                raise TimeoutError('parking runtime readiness')
            time.sleep(.01)
        assert owner['state'] == 'idle' and owner['policy_revision'] == '4' and owner['listeners'] == []
        emit('ready', owner=owner)
        assert sys.stdin.readline() == 'stop\n'
        alive = server.process.poll() is None
        if alive:
            server.stop()
        code = server.process.wait(timeout=10)
        assert lock_available() and not Path(f'/proc/{server.process.pid}').exists()
        emit('terminal', alive_before_stop=alive, exit=code, stderr=server.error.read_text(),
             parent_absent=True, lock_available=True, scratch_empty=not list((http.ROOT/'scratch').iterdir()))
    finally:
        server.cleanup()


if __name__ == '__main__': main(sys.argv[1])
