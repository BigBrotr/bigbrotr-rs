#!/usr/bin/env python3
"""Installed resume HTTP; session secrets remain only in this client's memory."""
from http.cookies import SimpleCookie
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available

DESTINATION = '\n[export.destination]\nid="' + 'e1'*16 + '"\nroot="/out/signed_normal/export-destination"\n'


def main():
    if sys.argv[1:] == ['prepare']:
        http.prepare(migration_target='814')
        text = http.CONFIG.read_text()
        assert text.count('[archive.reader]\nenabled=true') == 1
        http.CONFIG.write_text(text.replace('[archive.reader]\nenabled=true', '[archive.reader]\nenabled=false') + DESTINATION)
        http.cli('config', 'validate')
        return
    assert len(sys.argv) == 2
    server = http.Server('job-resume-' + sys.argv[1])
    original = http.CONFIG.read_text()
    auth = {}
    marker = Path('/out/signed_normal/export-destination/destination.json')
    parked = marker.with_name('destination.fixture-held')
    def terminal():
        server.stop()
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT/'scratch').iterdir())
        return dict(exit=0, parent_absent=True, lock_available=True, scratch_empty=True)
    try:
        emit('ready', owner=server.ready(2), parent_pid=server.process.pid)
        for line in sys.stdin:
            command = json.loads(line); op = command['operation']; name = command['request']
            if op == 'stop':
                emit(name, operation=op, **terminal()); return
            if op == 'restart':
                stopped = terminal()
                http.CONFIG.write_text(original if command['destination'] else original.removesuffix(DESTINATION))
                server = http.Server('job-resume-' + name)
                emit(name, operation=op, stopped=stopped, owner=server.ready(2), parent_pid=server.process.pid)
                continue
            if op == 'marker':
                if command['available']:
                    assert parked.is_file() and not marker.exists(); parked.rename(marker)
                else:
                    assert marker.is_file() and not parked.exists(); marker.rename(parked)
                emit(name, operation=op, available=command['available']); continue
            if op == 'login':
                status, value, headers = http.request('POST', '/management/v1/sessions', dict(username=command['username'], password='Independent owner password 2026'))
                assert status == 201, (status, value)
                cookies = SimpleCookie()
                for key, value in headers:
                    if key.lower() == 'set-cookie': cookies.load(value)
                auth = {'Cookie':'BigBrotrLocalSession=' + cookies['BigBrotrLocalSession'].value, 'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
                emit(name, operation=op, status=status)
                continue
            if op == 'resume':
                headers = {**auth, 'Idempotency-Key':command['key']}
                if command.get('wrong_csrf'): headers['X-CSRF-Token']='f0'*32
                status, value, response_headers = http.request('POST', '/management/v1/jobs/' + command['job'] + '/resume', command['body'], headers)
                assert ('cache-control','no-store') in [(k.lower(),v) for k,v in response_headers]
                assert not any(k.lower()=='set-cookie' for k,_ in response_headers)
            elif op == 'me':
                status, value, _ = http.request('GET', '/management/v1/me', auth=auth)
            else: raise AssertionError('unknown fixture instruction')
            emit(name, operation=op, status=status, observation=value)
        raise AssertionError('missing explicit stop')
    finally:
        server.cleanup()
        http.CONFIG.write_text(original)
        if parked.exists() and not marker.exists(): parked.rename(marker)


if __name__ == '__main__': main()
