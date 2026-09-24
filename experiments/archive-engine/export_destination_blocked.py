#!/usr/bin/env python3
"""Hold an actual destination openat on strace6.1; never replace product I/O.

The trace barrier identifies a pending syscall on a non-main thread in the
actual server. Host SQL observes renewals while it remains held. SIGTERM must
end admission, exhaust the bounded worker join and fail stop with custody held.
"""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import private_grant_http as http


def pending_checker(trace, parent):
    """Only an unreturned exact marker open on this server's worker qualifies."""
    for line in reversed(trace.splitlines()):
        match = re.fullmatch(r'(\d+)\s+\d+\.\d+ openat\(\d+</out/private-grant-http/destination>, "destination.json", (.*)', line)
        if not match:
            continue
        tid = int(match[1])
        if tid == parent or ' = ' in match[2] or '<... openat resumed>' in trace[trace.rfind(line) + len(line):]:
            continue
        if all(flag in match[2] for flag in ['O_RDONLY','O_NOFOLLOW','O_NONBLOCK']):
            return tid
    return None


def lock_available():
    descriptor=os.open(http.ROOT/'destination', os.O_RDONLY|os.O_DIRECTORY)
    try:
        try:fcntl.flock(descriptor, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return False
        return True
    finally:os.close(descriptor)


class TracedServer(http.Server):
    def __init__(self):
        self.output=http.ROOT/'checker.stdout';self.error=http.ROOT/'checker.stderr'
        self.trace=http.ROOT/'checker.trace';self.parent=None
        self.command=['/usr/bin/strace','-f','-ttt','-yy','-P','destination.json',
            '-e','trace=openat','-e','inject=openat:delay_enter=60s:when=3',
            '-o',str(self.trace),str(http.BINARY),'serve','--roles','control','--file',str(http.CONFIG)]
        with self.output.open('xb') as out,self.error.open('xb') as err:
            self.process=subprocess.Popen(self.command,env={},stdout=out,stderr=err,start_new_session=True)
    def ready(self):
        result=super().ready()
        children=Path(f'/proc/{self.process.pid}/task/{self.process.pid}/children').read_text().split()
        assert len(children)==1,children
        self.parent=int(children[0]);assert Path(f'/proc/{self.parent}/exe').resolve()==http.BINARY
        return result
    def cleanup(self):
        if self.process.poll() is None:
            os.killpg(self.process.pid,signal.SIGKILL);self.process.wait(timeout=10)


def exercise(auth, body):
    # Deployment-owned timings, unchanged60s owner TTL; fit a real renewal and
    # bounded fail-stop inside the externally imposed60s syscall hold.
    with http.CONFIG.open('a') as stream:
        stream.write('\n[runtime]\nheartbeat_interval="500ms"\ngraceful_shutdown_timeout="2s"\n')
    http.cli('config','validate')
    version=subprocess.check_output(['/usr/bin/strace','--version'],text=True)
    assert version.splitlines()[0]=='strace -- version 6.1',version
    server=TracedServer();pool=ThreadPoolExecutor(max_workers=1)
    result=dict(status='INCOMPLETE',strace=version,command=server.command,primed=[])
    path=http.ROOT/'checker-result.json'
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    future=None
    try:
        result['ready']=server.ready();result['parent_pid']=server.parent;save()
        bad={**body,'destination':{**body['destination'],'binding_sha256':'00'*32}}
        def request():
            try:return dict(status=http.request('POST','/management/v1/jobs',bad,{**auth,'Idempotency-Key':'e2'*16})[0])
            except (OSError,http.http.client.HTTPException) as error:return dict(closed=type(error).__name__)
        # Injection counters are per tracee. Prime only with rejecting commands;
        # never assume Tokio reuses a particular thread or infer a block from time.
        for _ in range(8):
            future=pool.submit(request);deadline=time.monotonic()+4;tid=None;seen=None;since=None
            while not future.done() and time.monotonic()<deadline:
                current=pending_checker(server.trace.read_text(),server.parent)
                if current != seen:
                    seen=current;since=time.monotonic()
                if current is not None and time.monotonic()-since>=.15:
                    tid=current;break
                time.sleep(.01)
            if tid is not None:break
            response=future.result(timeout=8);assert response==dict(status=409),response
            result['primed'].append(response);save()
        else:raise AssertionError('No actual worker marker syscall hold observed')
        status=dict(line.split(':',1) for line in Path(f'/proc/{tid}/status').read_text().splitlines() if ':' in line)
        assert int(status['Tgid'])==server.parent and int(status['TracerPid'])==server.process.pid
        assert status['State'].strip().startswith('t'),status['State']
        assert not future.done() and not lock_available()
        result['blocked']=dict(tid=tid,tracer=server.process.pid,state=status['State'].strip(),lock_available=False,trace=server.trace.read_text());save()
        (http.ROOT/'checker-blocked.waiting').write_text('observed\n')
        end=time.monotonic()+4
        while not (http.ROOT/'checker-blocked.ready').exists():
            assert not future.done() and server.process.poll() is None
            assert pending_checker(server.trace.read_text(),server.parent)==tid
            assert time.monotonic()<end,'host renewal observation timeout'
            time.sleep(.01)
        assert not lock_available()
        started=time.monotonic();os.kill(server.parent,signal.SIGTERM)
        code=server.process.wait(timeout=7)
        result['terminal']=dict(exit=code,elapsed_ms=round((time.monotonic()-started)*1000,3),response=future.result(timeout=2),parent_present=Path(f'/proc/{server.parent}').exists(),worker_present=Path(f'/proc/{tid}').exists(),lock_available=lock_available(),stderr=server.error.read_text(),stdout=server.output.read_text());save()
        terminal=result['terminal']
        assert terminal['exit']==4 and terminal['elapsed_ms']<7000,terminal
        assert 'native grant worker not joined; retain pin and terminate process' in terminal['stderr']
        assert not terminal['parent_present'] and not terminal['worker_present'] and terminal['lock_available']
        assert '"release":"committed"' not in terminal['stdout']
        assert terminal['response'].get('status') not in (200,202)
        assert not list((http.ROOT/'scratch').iterdir())
        result['status']='PASS';save()
    finally:
        server.cleanup();pool.shutdown(wait=True,cancel_futures=True);save()
    return result
