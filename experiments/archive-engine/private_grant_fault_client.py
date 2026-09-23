#!/usr/bin/env python3
"""Real HTTP client while host-owned PostgreSQL blocks the actual native query."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import time
import private_grant_http as http


def emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def main(mode, port):
    assert mode in ('deadline', 'signal', 'cleanup') and 0 < port < 65536
    original = http.CONFIG
    text = original.read_text()
    needle = '[postgres.catalog]\nendpoint="127.0.0.1:5432"'
    assert text.count(needle) == 1
    config = http.ROOT/f'fault-{mode}.toml'
    config.write_text(text.replace(needle, f'[postgres.catalog]\nendpoint="127.0.0.1:{port}"'))
    http.CONFIG = config
    server = http.Server('fault-'+mode)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        server.ready()
        session = http.ROOT/'session.private'
        assert session.stat().st_mode & 0o777 == 0o600
        auth = json.loads(session.read_text())
        assert set(auth) == {'Cookie', 'X-CSRF-Token'}
        status, value, _ = http.request('GET', '/management/v1/me', auth=auth)
        assert status == 200, (status, value.get('error'))
        auth['Idempotency-Key'] = ('d1' if mode == 'deadline' else 'd2')*16
        command = dict(principal_id=(http.ROOT/'principal').read_text(), action='history.read', enabled=True,
            scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
            expected_config_revision='1',expected_policy_revision='2',expected_grant_revision='3',
            allocation=dict(catalog_id='5a'*16,frontier_revision='1'))
        proof_path = http.ROOT/'signal-proof.private'
        if mode == 'cleanup':
            assert proof_path.stat().st_mode & 0o777 == 0o600
            proof = json.loads(proof_path.read_text())
        else:
            status, proof, _ = http.request('POST','/management/v1/sessions/current/reauthenticate',dict(password=http.PASSWORD,command=command),auth)
            assert status == 201, (status, proof.get('error'))
            if mode == 'signal':
                with os.fdopen(os.open(proof_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
                    json.dump(dict(proof_id=proof['proof_id']), stream)
        def change():
            try:
                status, _, _ = http.request('PUT', f'/management/v1/principals/{command["principal_id"]}/grants',dict(command=command,proof_id=proof['proof_id']),auth)
                return dict(status=status)
            except (OSError, http.http.client.HTTPException) as error:
                timing = dict(request_ms=round((time.monotonic()-started)*1000,3)) if mode=='cleanup' else {}
                return dict(closed=type(error).__name__, **timing)
        started = time.monotonic()
        future = pool.submit(change)
        emit('submitted', parent_pid=server.process.pid, mode=mode)
        if mode == 'cleanup':
            assert sys.stdin.readline() == 'check\n'
            assert not future.done(), 'HTTP request closed before host signal'
            emit('request_pending')
        assert sys.stdin.readline() == 'observe\n'
        response = future.result(timeout=10)
        assert response.get('status') != 200, response
        if mode == 'cleanup':
            assert response.get('closed')=='RemoteDisconnected' and response['request_ms']<5000, response
            emit('request_closed', response=response)
        if mode == 'deadline':
            # A child held through the five-second request window must join and
            # retire its pin while Control stays usable beyond its original TTL.
            deadline = time.monotonic()+5
            while True:
                assert server.process.poll() is None, server.error.read_text()
                try:
                    status, _, _ = http.request('GET','/management/v1/me',auth=auth)
                    if status == 200: break
                except (OSError, http.http.client.HTTPException): pass
                assert time.monotonic()<deadline, 'healthy Control did not resume'
                time.sleep(.02)
            server.stop()
        else:
            # The host signals only at an observed native wait or catalog
            # rollback, with Control idle and no ambiguous SQL write.
            code = server.process.wait(timeout=15)
            assert code == 0, (code,server.error.read_text())
            assert json.loads(server.output.read_text().splitlines()[-1])['release']=='committed'
        retry = None
        if mode == 'cleanup':
            server = http.Server('fault-cleanup-restart')
            server.ready()
            status, retry, _ = http.request('PUT', f'/management/v1/principals/{command["principal_id"]}/grants',dict(command=command,proof_id=proof['proof_id']),auth)
            assert status == 200 and not retry['created'] and retry['enabled'], (status,retry)
            assert retry['scope_generation']=='3' and retry['account_grant_revision']=='4'
            server.stop()
        assert not list((http.ROOT/'scratch').iterdir())
        emit('terminal', retry=retry, mode=mode, response=response, parent_exit=server.process.returncode, scratch_entries=0)
    finally:
        server.cleanup()
        pool.shutdown(wait=True,cancel_futures=True)
        http.CONFIG = original


if __name__=='__main__': main(sys.argv[1],int(sys.argv[2]))
