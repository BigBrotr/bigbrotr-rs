#!/usr/bin/env python3
"""Actual installed client; session/cursor bytes stay in the private owned root."""
import base64
import hashlib
from http.client import HTTPConnection
from http.cookies import SimpleCookie
import json
import os
import secrets
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available
from signed_publication_oracle import EVENT


def request(method, path, body=None, auth=None):
    connection = HTTPConnection('127.0.0.1', 18080, timeout=8)
    headers = {'Origin': 'http://127.0.0.1:18080', 'Content-Type': 'application/json', **(auth or {})}
    connection.request(method, path, json.dumps(body) if body is not None else None, headers)
    response = connection.getresponse()
    data = response.read(131073)
    assert len(data) <= 131072
    result = response.status, json.loads(data) if data else None, response.getheaders()
    connection.close()
    return result


def main(lifetime=False):
    server = None
    accounts, pages = {}, {}
    tokens = http.ROOT / ('job-pages-lifetime-tokens.private' if lifetime else 'job-page-tokens.json')
    assert not tokens.exists()
    tokens.write_text('{}\n'); tokens.chmod(0o600)
    try:
        for line in sys.stdin:
            command = json.loads(line)
            name, op = command['request'], command['operation']
            if op == 'start':
                assert server is None
                prefix = 'job-pages-lifetime-' if lifetime else 'job-pages-'
                server = http.Server(prefix + name)
                ready = server.ready(command.get('policy_revision',3))
                assert len(ready['listeners']) == 1
                emit(name, operation=op, owner=ready)
            elif op in ('pause', 'stop'):
                assert server is not None
                server.stop()
                assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
                server.cleanup(); server = None
                emit(name, operation=op, parent_absent=True, lock_available=True)
                if op == 'stop': return
            elif op == 'login':
                user = command['username']
                status, value, headers = request('POST','/management/v1/sessions',
                    dict(username=user,password='Independent owner password 2026'))
                assert status == 201, (status,value)
                cookies = SimpleCookie()
                for key, value in headers:
                    if key.lower() == 'set-cookie': cookies.load(value)
                accounts[user] = {'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
                                  'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
                emit(name,operation=op,status=status)
            elif op == 'prepare-short':
                # A separately seeded short-lived session tests the clock boundary
                # without mutating an immutable login receipt or disabling guards.
                token,csrf = secrets.token_bytes(32),secrets.token_bytes(32)
                identity = secrets.token_hex(16)
                accounts[name] = {'Cookie':'BigBrotrLocalSession='+token.hex(),'X-CSRF-Token':csrf.hex()}
                path = http.ROOT / (name+'.private')
                with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as stream:
                    json.dump(dict(id=identity,
                        token=hashlib.sha256(b'bigbrotr/browser-session/1\0'+token).hexdigest(),
                        csrf=hashlib.sha256(b'bigbrotr/browser-csrf/1\0'+csrf).hexdigest()),stream)
                emit(name,operation=op,session=identity)
            elif op == 'accept':
                body = dict(kind='export',format='signed-events-jsonl',
                    query=dict(version=1,resource='event_history',mode='snapshot',
                        scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
                        filters=[dict(ids=[EVENT['id']])]),
                    allocation=dict(catalog_id='5a'*16,frontier_revision='1'),
                    destination=dict(id='e1'*16,binding_sha256=hashlib.sha256(
                        Path('/out/signed_normal/export-destination/destination.json').read_bytes()).hexdigest()),
                    expected_config_revision='1',expected_policy_revision='3')
                key = command['key']
                status,value,headers = request('POST','/management/v1/jobs',body,
                    {**accounts[command['username']],'Idempotency-Key':key})
                assert status == 202 and value['created'] and value['execution']=='unobserved', (status,value)
                emit(name,operation=op,status=status,receipt=value,submitted=body,request_key=key)
            elif op == 'page':
                query = command.get('query','page_size=2')
                if 'continue' in command:
                    token = pages[command['continue']]
                    assert token is not None
                    query += '&cursor='+token
                if 'invalid' in command: query += '&cursor='+command['invalid']
                # Isolated independent envelope fixtures arrive over the private
                # command pipe; never copy them to emitted observations.
                if 'fixture_cursor' in command: query += '&cursor='+command['fixture_cursor']
                auth = accounts[command['username']] if 'username' in command else {}
                status,value,headers = request('GET','/management/v1/jobs?'+query,auth=auth)
                assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
                assert not any(k.lower()=='set-cookie' for k,_ in headers)
                if status == 200:
                    token = value.pop('next_cursor')
                    pages[name] = token
                    tokens.write_text(json.dumps(pages)+'\n')
                    value['has_next'] = token is not None
                    if token:
                        raw = base64.urlsafe_b64decode(token+'='*((-len(token))%4))
                        value['cursor_sha256'] = hashlib.sha256(token.encode()).hexdigest()
                        value['cursor_key_id'] = raw[1:17].hex()
                emit(name,operation=op,status=status,observation=value)
            else: raise AssertionError('unknown fixture operation')
        raise AssertionError('missing explicit stop')
    finally:
        if server is not None: server.cleanup()


if __name__ == '__main__':
    assert sys.argv[1:] in ([], ['--lifetime'])
    main(lifetime=bool(sys.argv[1:]))
