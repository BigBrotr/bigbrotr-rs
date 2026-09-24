#!/usr/bin/env python3
"""Installed headless runtime, with host-side independent SQL observation."""
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http

if sys.argv[1] == 'prepare':
    http.prepare(migration_target='501')
    text = http.CONFIG.read_text().replace('worker_memory_bytes="512MiB"', 'worker_memory_bytes="640MiB"')
    text += '''
[export.destination]
id="e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"
root="/out/signed_normal/export-destination"
[runtime.processes.application]
payload_bytes="512MiB"
[runtime.processes.native]
memory_max_bytes="3GiB"
native_bytes="1536MiB"
'''
    http.CONFIG.write_text(text)
    http.cli('config', 'validate')
else:
    assert sys.argv[1] == 'run'
    server = http.Server('executor')
    try:
        ready = server.ready()
        assert ready['listeners'] == [], 'Headless selection started a listener'
        (http.ROOT / 'executor-ready.json').write_text(json.dumps(ready) + '\n')
        deadline = time.monotonic() + 30
        while not (http.ROOT / 'executor-stop').exists():
            assert server.process.poll() is None, server.error.read_text()
            if time.monotonic() >= deadline:
                raise TimeoutError('host completion observation')
            time.sleep(.025)
        server.stop()
    finally:
        server.cleanup()
