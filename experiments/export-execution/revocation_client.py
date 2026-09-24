#!/usr/bin/env python3
"""Reauthenticate and revoke through the installed parent while its reader waits."""
from http.cookies import SimpleCookie
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available


def main(mode, port):
    assert mode in ('attempt', 'restart')
    original = http.CONFIG
    if mode == 'attempt':
        assert 0 < port < 65536
        config = original.read_text()
        needle = '[postgres.catalog]\nendpoint="127.0.0.1:5432"'
        assert config.count(needle) == 1
        http.CONFIG = http.ROOT / 'executor-revocation.toml'
        http.CONFIG.write_text(config.replace(needle, f'[postgres.catalog]\nendpoint="127.0.0.1:{port}"'))
    server = http.Server('executor-revocation-' + mode)
    try:
        owner = server.ready(4)
        assert len(owner['listeners']) == 1
        emit('ready', owner=owner, parent_pid=server.process.pid)
        if mode == 'attempt':
            assert sys.stdin.readline() == 'revoke\n'
            status, _, headers = http.request('POST', '/management/v1/sessions',
                dict(username='alice', password='Independent owner password 2026'))
            assert status == 201, status
            cookies = SimpleCookie()
            for name, value in headers:
                if name.lower() == 'set-cookie':
                    cookies.load(value)
            auth = {'Cookie': 'BigBrotrLocalSession=' + cookies['BigBrotrLocalSession'].value,
                    'X-CSRF-Token': cookies['BigBrotrLocalCsrf'].value,
                    'Idempotency-Key': 'e5'*16}
            status, me, _ = http.request('GET', '/management/v1/me', auth=auth)
            assert status == 200 and me['grant_revision'] == '4', (status, me)
            principal = me['principal_id']
            command = dict(principal_id=principal, action='history.read', enabled=False,
                scope=dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1'),
                expected_config_revision='1', expected_policy_revision='4',
                expected_grant_revision='4', allocation=None)
            status, proof, _ = http.request('POST', '/management/v1/sessions/current/reauthenticate',
                dict(password='Independent owner password 2026', command=command), auth)
            assert status == 201, (status, proof)
            body = dict(command=command, proof_id=proof['proof_id'])
            status, receipt, _ = http.request('PUT', f'/management/v1/principals/{principal}/grants', body, auth)
            assert status == 200 and receipt['created'] and not receipt['enabled'], (status, receipt)
            assert receipt['scope_generation'] == '2' and receipt['account_grant_revision'] == '5'
            assert receipt['application'] == 'unobserved' and receipt['authority'] == 'acceptance-only'
            emit('revoked', command=command, receipt=receipt, status=status)
        assert sys.stdin.readline() == 'stop\n'
        assert server.process.poll() is None, server.error.read_text()
        server.stop()
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT/'scratch').iterdir())
        emit('terminal', exit=0, parent_absent=True, lock_available=True, scratch_empty=True)
    finally:
        server.cleanup()
        http.CONFIG = original


if __name__ == '__main__':
    main(sys.argv[1], int(sys.argv[2]))
