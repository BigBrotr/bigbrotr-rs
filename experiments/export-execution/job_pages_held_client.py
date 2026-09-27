#!/usr/bin/env python3
"""Sample actual destination exclusion through a held page COMMIT and driver drain."""
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection, HTTPException
from http.cookies import SimpleCookie
import json
import os
from pathlib import Path
import select
import signal
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from export_job_custody_client import children, sockets, start_ticks
from commit_client import emit, lock_available
from job_pages_client import request

SCHEDULES = [('commit','deadline'), ('reply','deadline'), ('commit','signal'), ('reply','signal')]


def main(port):
    original = http.CONFIG
    text = original.read_text()
    needle = '[postgres.control]\nendpoint="127.0.0.1:5432"'
    assert text.count(needle) == 1
    server = None
    pool = ThreadPoolExecutor(max_workers=1)
    auth = None
    try:
        for index, (stage, trigger) in enumerate(SCHEDULES):
            name = f'job-pages-held-{stage}-{trigger}'
            http.CONFIG = http.ROOT/(name+'.toml')
            assert 0 < port < 65536
            http.CONFIG.write_text(text.replace(needle, f'[postgres.control]\nendpoint="127.0.0.1:{port}"'))
            settings = http.cli('config','show')['settings']
            sql_limits = {key:settings['runtime.'+key] for key in ['renewal_timeout','authorization_ttl']}
            sql_upper_seconds = min(int(sql_limits['renewal_timeout']['value']),
                                    int(sql_limits['authorization_ttl']['value'])/2)/1_000_000
            server = http.Server(name)
            ready = server.ready(3)
            if auth is None:
                status, _, headers = request('POST', '/management/v1/sessions',
                    dict(username='alice',password='Independent owner password 2026'))
                assert status == 201
                cookies = SimpleCookie()
                for key, value in headers:
                    if key.lower() == 'set-cookie': cookies.load(value)
                auth = {'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
                        'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
            result = dict(status='INCOMPLETE',stage=stage,trigger=trigger,owner=ready,
                          sql_limits=sql_limits,sql_upper_seconds=sql_upper_seconds,samples=[])
            path = http.ROOT/(name+'.json'); assert not path.exists()
            def save():
                temporary = path.with_suffix('.tmp')
                temporary.write_text(json.dumps(result,indent=2)+'\n')
                temporary.replace(path)
            pid = server.process.pid
            pidfd = os.pidfd_open(pid)
            poll = select.poll(); poll.register(pidfd, select.POLLIN)
            initial = sockets(pid,port)
            result.update(parent_pid=pid,parent_start_ticks=start_ticks(pid),control_socket=initial)
            assert len(initial) == 1 and not children(pid) and not lock_available()
            info = Path('/out/signed_normal/export-destination').stat()
            result['destination'] = dict(device=info.st_dev,inode=info.st_ino)
            save(); emit(f'ready-{index}',index=index,owner=ready,parent_pid=pid)
            try:
                assert sys.stdin.readline() == 'submit\n'
                def fetch():
                    connection = HTTPConnection('127.0.0.1',18080,timeout=8)
                    observed = {}
                    try:
                        connection.request('GET','/management/v1/jobs?page_size=2',
                            headers={'Origin':'http://127.0.0.1:18080',**auth})
                        response = connection.getresponse()
                        observed['status'] = response.status
                        assert response.status != 200
                        data = response.read(16385)
                        assert len(data) <= 16384 and b'next_cursor' not in data
                    except (OSError, HTTPException) as error:
                        observed['closed'] = type(error).__name__
                    finally: connection.close()
                    return observed
                submitted = time.monotonic()
                future = pool.submit(fetch)
                emit(f'submitted-{index}',index=index)
                assert sys.stdin.readline() == 'observe\n'
                assert not future.done() and sockets(pid,port) == initial and not children(pid)
                if trigger == 'signal': signal.pidfd_send_signal(pidfd,signal.SIGTERM)
                observing = time.monotonic()
                result['timing'] = dict(submitted=submitted,observing=observing)
                emit(f'observing-{index}',index=index,trigger=trigger)
                while not poll.poll(0):
                    assert time.monotonic()-observing < 8, 'parent termination deadline'
                    assert len(result['samples']) < 1200
                    try:
                        identity = start_ticks(pid)
                        before = sockets(pid,port); free = lock_available(); after = sockets(pid,port)
                        result['samples'].append(dict(monotonic=time.monotonic(),socket_before=before,
                            socket_after=after,lock_available=free,
                            same_process=identity == start_ticks(pid) == result['parent_start_ticks'],
                            children=children(pid),parent_live=not bool(poll.poll(0))))
                    except PermissionError:
                        if not poll.poll(100): raise
                        result['sampling_ended_on_process_exit'] = True
                        break
                    time.sleep(.01)
                code = server.process.wait(timeout=2)
                result['terminal'] = dict(exit=code,response=future.result(timeout=2),
                    lock_available=lock_available(),parent_present=Path(f'/proc/{pid}').exists(),
                    elapsed_seconds=time.monotonic()-submitted,
                    stderr=server.error.read_text(),stdout=server.output.read_text())
                save()
                terminal = result['terminal']
                assert code == 4 and terminal['lock_available'] and not terminal['parent_present']
                expected = ('private management SQL outcome unresolved' if trigger == 'deadline'
                            else 'private session operation cancelled; outcome unresolved')
                assert expected in terminal['stderr'] and '"release":"committed"' not in terminal['stdout']
                assert terminal['response'].get('status') != 200
                assert not list((http.ROOT/'scratch').iterdir())
                supported = [v for v in result['samples'] if v['same_process'] and v['parent_live']
                    and v['socket_before'] == v['socket_after'] == initial and not v['children']]
                assert len(supported) >= 2
                result['observed_interval_seconds'] = supported[-1]['monotonic']-supported[0]['monotonic']
                # SQL was already pending when observe arrived. Its actual
                # deadline cannot exceed this configured upper bound from then;
                # use only later samples for the post-timeout drain claim.
                drain_after = observing + (sql_upper_seconds if trigger == 'deadline' else 0)
                drain = [v for v in supported if v['monotonic'] >= drain_after]
                assert len(drain) >= 2
                result['drain_observation'] = dict(after_monotonic=drain_after,
                    samples=len(drain),interval_seconds=drain[-1]['monotonic']-drain[0]['monotonic'])
                assert result['drain_observation']['interval_seconds'] >= .5
                result['violations'] = sum(v['lock_available'] for v in supported)
                result['status'] = 'FAIL' if result['violations'] else 'PASS'
                save(); assert not result['violations'], 'destination released while actual driver socket remained open'
                emit(f'terminal-{index}',index=index,status=result['status'])
            finally:
                os.close(pidfd); server.cleanup(); save()
            instruction = json.loads(sys.stdin.readline())
            if index == len(SCHEDULES)-1:
                assert instruction == dict(operation='stop')
            else:
                assert set(instruction) == {'port'}
                port = instruction['port']
        emit('finished')
    finally:
        if server: server.cleanup()
        pool.shutdown(wait=True,cancel_futures=True)
        http.CONFIG = original


if __name__ == '__main__': main(int(sys.argv[1]))
