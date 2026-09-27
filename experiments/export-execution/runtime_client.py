#!/usr/bin/env python3
"""Installed headless runtime, with host-side independent SQL observation."""
import json
import os
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http

if sys.argv[1] == 'prepare':
    assert sys.argv[1:] in (['prepare'], ['prepare', '815'])
    http.prepare(migration_target='815' if sys.argv[2:] else '501')
    text = http.CONFIG.read_text().replace('worker_memory_bytes="512MiB"', 'worker_memory_bytes="640MiB"')
    text += '''
[export.destination]
id="e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"
root="/out/signed_normal/export-destination"
[runtime.processes.native]
memory_max_bytes="3GiB"
native_bytes="1536MiB"
'''
    http.CONFIG.write_text(text)
    http.cli('config', 'validate')
elif sys.argv[1] == 'capacity':
    assert len(sys.argv) == 3 and sys.argv[2].isdecimal()
    growth = int(sys.argv[2])
    reserve = http.cli('config', 'show')['settings']['runtime.disk_completion_reserve_bytes']
    filesystems = []
    for path in [Path('/out/signed_normal/export-destination'), http.ROOT/'scratch']:
        stats = os.statvfs(path)
        filesystems.append(dict(path=str(path), block_bytes=stats.f_frsize,
            available_blocks=stats.f_bavail, free_bytes=stats.f_bavail*stats.f_frsize))
    print(json.dumps(dict(reserve=reserve, growth_bytes=growth, filesystems=filesystems)))
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
