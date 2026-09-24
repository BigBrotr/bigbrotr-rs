#!/usr/bin/env python3
"""Installed Control with the exact production bundle; the host owns SQL checks."""
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available
from resume_http_client import DESTINATION

assert len(sys.argv) == 2 and sys.argv[1] in ('root', 'control')
mount = sys.argv[1]
base_path = '/' if mount == 'root' else '/control/'
original = http.CONFIG.read_text()
assert '[administration.assets]' not in original and original.count(DESTINATION) == 1
configured = original + f'\n[administration.assets]\ndirectory="/out/bundles/{mount}"\nbase_path="{base_path}"\n'
server = None
def start(name, destination=True):
    http.CONFIG.write_text(configured if destination else configured.replace(DESTINATION, ''))
    current = http.Server('job-resume-browser-' + name)
    try:
        ready = current.ready(2)
        assert ready['listeners'][0]['private_assets']['base_path'] == base_path
        emit(name, owner=ready, parent_pid=current.process.pid)
        return current
    except BaseException:
        current.cleanup(); raise
def stop(operation):
    server.stop()
    assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
    assert not list((http.ROOT/'scratch').iterdir())
    emit(operation+'-terminal', exit=0, parent_absent=True, lock_available=True, scratch_empty=True)
try:
    server = start('ready')
    for line in sys.stdin:
        operation = json.loads(line)['operation']
        assert operation in ('restart', 'stop')
        stop(operation)
        if operation == 'stop': break
        server = start('restarted', destination=False)
    else: raise AssertionError('missing explicit stop')
finally:
    if server: server.cleanup()
    http.CONFIG.write_text(original)
