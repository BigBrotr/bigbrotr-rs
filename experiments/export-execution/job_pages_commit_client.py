#!/usr/bin/env python3
"""Actual page requests across unknown COMMIT; credentials never leave memory."""
from http.client import HTTPConnection, HTTPException
from http.cookies import SimpleCookie
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available
from job_pages_client import request


def main(port):
    original = http.CONFIG
    config = original.read_text()
    needle = '[postgres.control]\nendpoint="127.0.0.1:5432"'
    assert config.count(needle) == 1
    server = None
    tokens = http.ROOT/'page-commit-cursors.private'
    assert not tokens.exists()
    tokens.write_text('{}'); tokens.chmod(0o600)
    cursors = {}

    def start(name, port):
        nonlocal server
        assert 0 < port < 65536
        http.CONFIG = http.ROOT/('job-pages-commit-'+name+'.toml')
        http.CONFIG.write_text(config.replace(needle, f'[postgres.control]\nendpoint="127.0.0.1:{port}"'))
        server = http.Server('job-pages-commit-'+name)
        return server.ready(3)

    def terminal():
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT/'scratch').iterdir())
        return dict(exit=server.process.returncode, parent_absent=True,
                    lock_available=True, scratch_empty=True)

    try:
        ready = start('before', port)
        status, _, headers = request('POST', '/management/v1/sessions',
            dict(username='alice', password='Independent owner password 2026'))
        assert status == 201
        cookies = SimpleCookie()
        for key, value in headers:
            if key.lower() == 'set-cookie': cookies.load(value)
        auth = {'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
                'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
        for mode in ['before', 'after']:
            emit('ready-'+mode, owner=ready, parent_pid=server.process.pid)
            assert sys.stdin.readline() == 'submit\n'
            connection = HTTPConnection('127.0.0.1', 18080, timeout=8)
            observed = {}
            try:
                connection.request('GET', '/management/v1/jobs?page_size=2',
                    headers={'Origin':'http://127.0.0.1:18080', **auth})
                response = connection.getresponse()
                observed['status'] = response.status
                # Check headers before reading: even a truncated 200 must fail.
                assert response.status != 200
                body = response.read(16385)
                assert len(body) <= 16384 and b'next_cursor' not in body
                observed['error'] = json.loads(body) if body else None
            except (OSError, HTTPException) as error:
                observed['closed'] = type(error).__name__
            finally:
                connection.close()
            assert server.process.wait(timeout=12) == 4
            error = server.error.read_text()
            assert 'CommitUnknown' in error
            assert all(json.loads(line).get('release') != 'committed'
                       for line in server.output.read_text().splitlines())
            emit('failed-'+mode, response=observed, error=error, **terminal())
            if mode == 'before':
                instruction = json.loads(sys.stdin.readline())
                assert set(instruction) == {'port'}
                ready = start('after', instruction['port'])
        assert sys.stdin.readline() == 'recover\n'
        ready = start('recover', 5432)
        emit('ready-recover', owner=ready, parent_pid=server.process.pid)
        for name, previous in [('first', None), ('second', 'first'), ('repeat', 'first')]:
            assert sys.stdin.readline() == name+'\n'
            query = 'page_size=2' + ('&cursor='+cursors[previous] if previous else '')
            status, value, headers = request('GET', '/management/v1/jobs?'+query, auth=auth)
            assert status == 200 and value['next_cursor'] is not None
            assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
            assert not any(k.lower() == 'set-cookie' for k,_ in headers)
            cursors[name] = value.pop('next_cursor')
            tokens.write_text(json.dumps(cursors))
            emit(name, status=status, page=value, has_next=True)
        assert sys.stdin.readline() == 'stop\n'
        server.stop()
        emit('terminal', **terminal())
    finally:
        if server: server.cleanup()
        http.CONFIG = original


if __name__ == '__main__': main(int(sys.argv[1]))
