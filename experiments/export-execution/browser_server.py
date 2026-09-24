#!/usr/bin/env python3
"""Installed process with real production assets; host owns SQL and browsers."""
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available

name, mount = sys.argv[1:]
assert mount in ('root', 'control') and name in [e+'-'+m for e in ['chromium','firefox','webkit'] for m in ['root','control']]
original = http.CONFIG.read_text()
assert '[administration.assets]' not in original
base = '/' if mount == 'root' else '/control/'
http.CONFIG.write_text(original + f'\n[administration.assets]\ndirectory="/out/bundles/{mount}"\nbase_path="{base}"\n')
server = None
try:
    def start(suffix):
        server = http.Server('job-browser-' + name + '-' + suffix)
        try:
            ready = server.ready(3)
            assert ready['listeners'][0]['private_assets']['base_path'] == base
            emit(suffix, owner=ready, parent_pid=server.process.pid)
            return server
        except BaseException:
            server.cleanup(); raise
    server = start('ready')
    for line in sys.stdin:
        command = json.loads(line)
        assert command['operation'] in ('restart','stop')
        server.stop()
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT/'scratch').iterdir())
        emit(command['operation']+'-terminal', exit=0, parent_absent=True, lock_available=True, scratch_empty=True)
        if command['operation'] == 'stop': break
        server = start('restarted')
    else: raise AssertionError('missing explicit stop')
finally:
    if server: server.cleanup()
    http.CONFIG.write_text(original)
