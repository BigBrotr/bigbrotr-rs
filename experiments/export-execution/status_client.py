#!/usr/bin/env python3
"""Actual installed HTTP job observations; credentials never enter case output."""
from http.cookies import SimpleCookie
import json
import hashlib
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available
from signed_publication_oracle import EVENT


def main():
    server = http.Server('job-status-' + sys.argv[1])
    auth = {}
    try:
        ready = server.ready(3)
        assert len(ready['listeners']) == 1
        emit('ready', owner=ready, parent_pid=server.process.pid)
        for line in sys.stdin:
            command = json.loads(line)
            op = command['operation']
            if op == 'stop':
                server.stop()
                assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
                assert not list((http.ROOT/'scratch').iterdir())
                emit('terminal', exit=0, parent_absent=True, lock_available=True, scratch_empty=True)
                return
            if op == 'login':
                status, observation, headers = http.request('POST', '/management/v1/sessions',
                    dict(username=command['username'], password='Independent owner password 2026'))
                assert status == 201, status
                cookies = SimpleCookie()
                for name, value in headers:
                    if name.lower() == 'set-cookie': cookies.load(value)
                auth = {'Cookie': 'BigBrotrLocalSession=' + cookies['BigBrotrLocalSession'].value,
                        'X-CSRF-Token': cookies['BigBrotrLocalCsrf'].value}
                emit(command['request'],operation=op, status=status, observation=observation)
            elif op == 'status':
                status, observation, headers = http.request('GET', '/management/v1/jobs/' + command['job'], auth=auth)
                assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
                assert not any(k.lower()=='set-cookie' for k,_ in headers)
                emit(command['request'],operation=op, status=status, observation=observation)
            elif op == 'me':
                status, observation, _ = http.request('GET', '/management/v1/me', auth=auth)
                emit(command['request'],operation=op, status=status, observation=observation)
            elif op in ('accept', 'retry'):
                body = dict(kind='export',format='signed-events-jsonl',
                    query=dict(version=1,resource='event_history',mode='snapshot',
                        scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
                        filters=[dict(ids=[EVENT['id']])]),
                    allocation=dict(catalog_id='5a'*16,frontier_revision='1'),
                    destination=dict(id='e1'*16,binding_sha256=hashlib.sha256(
                        Path('/out/signed_normal/export-destination/destination.json').read_bytes()).hexdigest()),
                    expected_config_revision='1',expected_policy_revision='3')
                status, receipt, _ = http.request('POST','/management/v1/jobs',body,{**auth,'Idempotency-Key':'e8'*16})
                assert status==(202 if op=='accept' else 200) and receipt['created']==(op=='accept') and receipt['execution']=='unobserved', (status,receipt)
                emit(command['request'],operation=op,status=status,receipt=receipt,submitted=body,request_key='e8'*16)
            elif op == 'revoke-history':
                current = command['principal']
                change = dict(principal_id=current, action='history.read', enabled=False,
                    scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
                    expected_config_revision='1', expected_policy_revision='3', expected_grant_revision='4', allocation=None)
                keyed = {**auth, 'Idempotency-Key':'e7'*16}
                status, proof, _ = http.request('POST','/management/v1/sessions/current/reauthenticate',
                    dict(password='Independent owner password 2026',command=change), keyed)
                assert status==201, (status,proof)
                status, receipt, _ = http.request('PUT',f'/management/v1/principals/{current}/grants',
                    dict(command=change,proof_id=proof['proof_id']),keyed)
                assert status==200 and receipt['created'] and not receipt['enabled'], (status,receipt)
                emit(command['request'],operation=op,status=status,receipt=receipt)
            else:
                raise AssertionError('unknown fixture instruction')
        raise AssertionError('missing explicit stop')
    finally:
        server.cleanup()


if __name__ == '__main__': main()
