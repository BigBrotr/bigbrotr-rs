#!/usr/bin/env python3
"""Real private history client, controlled only after an observed native SQL wait."""
from concurrent.futures import ThreadPoolExecutor
import json
import socket
import sys
import time
import private_grant_http as http
from signed_publication_oracle import EVENT


def emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def main(mode, port, policy_revision):
    assert mode in ('deadline', 'disconnect', 'signal')
    assert 0 < port < 65536 and policy_revision > 0
    original = http.CONFIG
    needle = '[postgres.catalog]\nendpoint="127.0.0.1:5432"'
    text = original.read_text()
    assert text.count(needle) == 1
    config = http.ROOT / f'history-{mode}.toml'
    config.write_text(text.replace(needle, f'[postgres.catalog]\nendpoint="127.0.0.1:{port}"'))
    http.CONFIG = config
    server = http.Server('history-' + mode)
    pool = ThreadPoolExecutor(max_workers=1)
    connection = http.http.client.HTTPConnection('127.0.0.1', 18080, timeout=8)
    try:
        server.ready(policy_revision)
        session = http.ROOT / 'session.private'
        assert session.stat().st_mode & 0o777 == 0o600
        auth = json.loads(session.read_text())
        assert http.request('GET', '/management/v1/me', auth=auth)[0] == 200
        value = dict(version=1, resource='event_history', mode='snapshot',
                     scope=dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1'),
                     filters=[dict(ids=[EVENT['id']])])
        request_started = time.monotonic()
        connection.connect()
        peer = connection.sock
        def query():
            observed = {}
            try:
                connection.request('POST', '/v1/resources/event_history', json.dumps(value),
                                   {**auth, 'Origin': 'http://' + http.ADDRESS, 'Content-Type': 'application/json'})
                response = connection.getresponse()
                observed['status'] = response.status
                data = response.read(16385)
                assert len(data) <= 16384
                return {**observed, 'bytes':len(data)}
            except http.http.client.IncompleteRead as error:
                return {**observed, 'closed':type(error).__name__, 'partial_bytes':len(error.partial)}
            except (OSError, http.http.client.HTTPException) as error:
                return {**observed, 'closed':type(error).__name__}
        future = pool.submit(query)
        emit('submitted', parent_pid=server.process.pid, mode=mode)
        assert sys.stdin.readline() == 'observe\n'
        started = time.monotonic()
        if mode == 'disconnect':
            # Refuse a late schedule: the ordinary connection deadline must not
            # masquerade as prompt cancellation caused by this disconnect.
            assert started - request_started < 2
            peer.shutdown(socket.SHUT_RDWR)
            peer.close()
        response = future.result(timeout=10)
        assert response.get('status') != 200, response
        if mode == 'signal':
            code = server.process.wait(timeout=15)
            assert code == 0, (code, server.error.read_text())
            assert json.loads(server.output.read_text().splitlines()[-1])['release'] == 'committed'
        else:
            end = time.monotonic() + 5
            while True:
                assert server.process.poll() is None, server.error.read_text()
                try:
                    if http.request('GET', '/management/v1/me', auth=auth)[0] == 200:
                        break
                except (OSError, http.http.client.HTTPException):
                    pass
                assert time.monotonic() < end, 'Control failed to resume after cancelled history'
                time.sleep(.02)
            if mode == 'disconnect':
                # Distinguish actual disconnect cancellation from the5s ceiling.
                assert time.monotonic() - started < 1
                assert time.monotonic() - request_started < 3
            emit('healthy', parent_pid=server.process.pid,
                 cancellation_ms=round((time.monotonic()-started)*1000, 3),
                 request_ms=round((time.monotonic()-request_started)*1000, 3))
            assert sys.stdin.readline() == 'stop\n'
            server.stop()
        assert not list((http.ROOT / 'scratch').iterdir())
        emit('terminal', mode=mode, response=response, parent_exit=server.process.returncode,
             completion_ms=round((time.monotonic()-started)*1000, 3),
             request_ms=round((time.monotonic()-request_started)*1000, 3), scratch_entries=0)
    finally:
        connection.close()
        server.cleanup()
        pool.shutdown(wait=True, cancel_futures=True)
        http.CONFIG = original


if __name__ == '__main__':
    main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
