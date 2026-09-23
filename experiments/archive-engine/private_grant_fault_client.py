#!/usr/bin/env python3
"""Real HTTP client while host-owned PostgreSQL blocks the actual native query."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import time
import private_grant_http as http


def emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def main(mode, port):
    assert mode in ('deadline', 'signal') and 0 < port < 65536
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
        status, proof, _ = http.request('POST','/management/v1/sessions/current/reauthenticate',dict(password=http.PASSWORD,command=command),auth)
        assert status == 201, (status, proof.get('error'))
        def change():
            try:
                status, _, _ = http.request('PUT', f'/management/v1/principals/{command["principal_id"]}/grants',dict(command=command,proof_id=proof['proof_id']),auth)
                return dict(status=status)
            except (OSError, http.http.client.HTTPException) as error:
                return dict(closed=type(error).__name__)
        future = pool.submit(change)
        emit('submitted', parent_pid=server.process.pid, mode=mode)
        assert sys.stdin.readline() == 'observe\n'
        response = future.result(timeout=10)
        assert response.get('status') != 200, response
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
            # The host signals only after verifying an actual blocked native
            # query and an idle Control backend, without an ambiguous SQL write.
            code = server.process.wait(timeout=10)
            assert code == 0, (code,server.error.read_text())
            assert json.loads(server.output.read_text().splitlines()[-1])['release']=='committed'
        assert not list((http.ROOT/'scratch').iterdir())
        emit('terminal', mode=mode, response=response, parent_exit=server.process.returncode, scratch_entries=0)
    finally:
        server.cleanup()
        pool.shutdown(wait=True,cancel_futures=True)
        http.CONFIG = original


if __name__=='__main__': main(sys.argv[1],int(sys.argv[2]))
