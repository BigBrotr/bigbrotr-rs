#!/usr/bin/env python3
"""Hold an actual destination openat on strace6.1; never replace product I/O.

The trace barrier identifies a pending syscall on a non-main thread in the
actual server. Host SQL observes renewals while it remains held. SIGTERM must
end admission, exhaust the bounded worker join and fail stop with custody held.
"""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import time
import private_grant_http as http


TRACER=Path('/out/checker-instrument/strace-exit-fixed')
TRACER_SHA='509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'


def instrument():
    """Consume only the tool qualified by the independent native discriminator."""
    proof_bytes=(TRACER.parent/'result.json').read_bytes()
    proof_sha=hashlib.sha256(proof_bytes).hexdigest()
    assert proof_sha=='cf0317fc8934eb70f3cf2d74ac6ba0680be09366a4ade586aabd95991c6c3abc'
    proof=json.loads(proof_bytes)
    assert proof['status']=='PASS'
    assert hashlib.sha256(TRACER.read_bytes()).hexdigest()==TRACER_SHA
    patch=Path(__file__).with_name('strace-6.1-exit-delay.patch')
    assert hashlib.sha256(patch.read_bytes()).hexdigest()==proof['source_inputs'][patch.name]
    cases={case['profile']:case for case in proof['cases']}
    assert set(cases)=={'original-pass','original-held','fixed-pass','fixed-held'}
    assert all(case['tracees_absent'] for case in cases.values())
    assert cases['original-held']['exit']=='TIMEOUT'
    assert 'delayed wait data set already' in cases['original-held']['stderr_before_cleanup']
    assert all(cases[key]['exit']==4 for key in ['original-pass','fixed-pass','fixed-held'])
    assert cases['fixed-held']['observed_hold'] and cases['fixed-held']['exit_observation_ms']<2000
    assert 'returned' not in cases['fixed-held']['stdout_before_cleanup']
    assert cases['fixed-held']['tool_sha256']==TRACER_SHA
    return dict(sha256=TRACER_SHA,proof_sha256=proof_sha,research_patch=proof['source_inputs'][patch.name])


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
        self.trace=http.ROOT/'checker.trace';self.parent=None;self.parent_fd=None
        self.command=[str(TRACER),'-f','-ttt','-yy','-P','destination.json',
            '-e','trace=openat','-e','inject=openat:delay_enter=60s:when=3',
            '-o',str(self.trace),str(http.BINARY),'serve','--roles','control','--file',str(http.CONFIG)]
        with self.output.open('xb') as out,self.error.open('xb') as err:
            self.process=subprocess.Popen(self.command,env={},stdout=out,stderr=err,start_new_session=True)
    def ready(self):
        result=super().ready()
        children=Path(f'/proc/{self.process.pid}/task/{self.process.pid}/children').read_text().split()
        assert len(children)==1,children
        self.parent=int(children[0]);assert Path(f'/proc/{self.parent}/exe').resolve()==http.BINARY
        self.parent_fd=os.pidfd_open(self.parent)
        assert os.getpgid(self.parent)==self.process.pid
        assert Path(f'/proc/{self.parent}/exe').resolve()==http.BINARY
        self.identity=dict(pid=self.parent,pgid=self.process.pid,start_ticks=Path(f'/proc/{self.parent}/stat').read_text().rsplit(')',1)[1].split()[19])
        return result
    def signal(self, number):
        # A pidfd always addresses this process, even after numeric PID reuse.
        signal.pidfd_send_signal(self.parent_fd,number)
    def cleanup(self):
        try:
            if self.parent_fd is not None:
                poll=select.poll();poll.register(self.parent_fd,select.POLLIN)
                if not poll.poll(0):
                    try:self.signal(signal.SIGKILL)
                    except ProcessLookupError:pass # Concurrent death; positively check below.
            if self.process.poll() is None:
                try:os.killpg(self.process.pid,signal.SIGKILL)
                except ProcessLookupError:pass
            self.process.wait(timeout=10)
            if self.parent_fd is not None:
                assert poll.poll(2000),'Owned tracee death unobserved; container teardown required'
        finally:
            if self.parent_fd is not None:
                os.close(self.parent_fd);self.parent_fd=None


def exercise(auth, body, responsive=False):
    # Deployment-owned timings, unchanged60s owner TTL; fit a real renewal and
    # bounded fail-stop inside the externally imposed60s syscall hold.
    with http.CONFIG.open('a') as stream:
        stream.write('\n[runtime]\nheartbeat_interval="500ms"\ngraceful_shutdown_timeout="2s"\n')
    http.cli('config','validate')
    proof=instrument()
    version=subprocess.check_output([str(TRACER),'--version'],text=True)
    assert version.splitlines()[0]=='strace -- version 6.1',version
    server=TracedServer();pool=ThreadPoolExecutor(max_workers=1)
    result=dict(status='INCOMPLETE',strace=version,instrument=proof,command=server.command,primed=[])
    path=http.ROOT/'checker-result.json'
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    future=None
    try:
        result['ready']=server.ready();result['parent_pid']=server.parent;result['parent_identity']=server.identity;save()
        bad={**body,'destination':{**body['destination'],'binding_sha256':'00'*32}}
        def request():
            try:return dict(status=http.request('POST','/management/v1/jobs',bad,{**auth,'Idempotency-Key':'e2'*16})[0])
            except (OSError,http.http.client.HTTPException) as error:return dict(closed=type(error).__name__)
        # Injection counters are per tracee. Prime only with rejecting commands;
        # never assume Tokio reuses a particular thread or infer a block from time.
        for _ in range(8):
            submitted=time.monotonic();future=pool.submit(request);deadline=submitted+4;tid=None;seen=None;since=None
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
        result['blocked']=dict(tid=tid,tracer=server.process.pid,state=status['State'].strip(),lock_available=False,trace=server.trace.read_text(),submitted_monotonic=submitted,observed_monotonic=time.monotonic());save()
        if responsive:
            # Bound the observer independently of the ordinary five-second
            # request. Record failure, then still qualify signal and cleanup.
            connection=http.http.client.HTTPConnection('127.0.0.1',18080,timeout=.75)
            started=time.monotonic()
            observation=dict(expected_status=200,timeout_seconds=.75)
            try:
                connection.request('GET','/management/v1/me',headers={
                    'Origin':'http://'+http.ADDRESS,**auth})
                response=connection.getresponse()
                observation['status']=response.status
                data=response.read(16385)
                assert len(data)<=16384
                value=json.loads(data)
                observation['principal_matches']=value.get('principal_id')==(http.ROOT/'principal').read_text()
            except (OSError,http.http.client.HTTPException) as error:
                observation['error']=type(error).__name__
            finally:
                connection.close()
                observation['elapsed_ms']=round((time.monotonic()-started)*1000,3)
                observation['worker_still_blocked']=pending_checker(server.trace.read_text(),server.parent)==tid
                observation['original_request_pending']=not future.done()
                result['management_during_native']=observation;save()
        (http.ROOT/'checker-blocked.waiting').write_text('observed\n')
        end=time.monotonic()+4
        while not (http.ROOT/'checker-blocked.ready').exists():
            assert not future.done() and server.process.poll() is None
            assert pending_checker(server.trace.read_text(),server.parent)==tid
            assert time.monotonic()<end,'host renewal observation timeout'
            time.sleep(.01)
        assert not lock_available()
        assert pending_checker(server.trace.read_text(),server.parent)==tid
        assert not future.done()
        started=time.monotonic()
        server.signal(signal.SIGTERM)
        sent=time.monotonic();margin=5-(sent-submitted)
        result['signal']=dict(monotonic=sent,wall=time.time(),request_deadline_margin_seconds=margin,request_pending=True)
        assert margin>=1.25,('Cannot distinguish signal from request expiry',margin)
        # Observe actual signal delivery while the ordinary request is still
        # unexpired. No synchronous result-file write separates timing and send.
        while True:
            delivered=re.search(r'^'+str(server.parent)+r'\s+\d+\.\d+ --- SIGTERM ',server.trace.read_text(),re.MULTILINE)
            remaining=5-(time.monotonic()-submitted)
            assert remaining>=1,('No timely SIGTERM delivery observed',remaining)
            if delivered:break
            time.sleep(.01)
        result['signal']['delivery_observed_margin_seconds']=remaining;save()
        code=server.process.wait(timeout=7)
        result['terminal']=dict(exit=code,elapsed_ms=round((time.monotonic()-started)*1000,3),response=future.result(timeout=2),parent_present=Path(f'/proc/{server.parent}').exists(),worker_present=Path(f'/proc/{tid}').exists(),lock_available=lock_available(),stderr=server.error.read_text(),stdout=server.output.read_text());save()
        terminal=result['terminal']
        assert terminal['exit']==4 and terminal['elapsed_ms']<7000,terminal
        assert 'native grant worker not joined; retain pin and terminate process' in terminal['stderr']
        assert not terminal['parent_present'] and not terminal['worker_present'] and terminal['lock_available']
        assert '"release":"committed"' not in terminal['stdout']
        assert terminal['response'].get('status') not in (200,202)
        assert not list((http.ROOT/'scratch').iterdir())
        if responsive:
            observation=result['management_during_native']
            assert observation.get('status')==200 and observation.get('principal_matches'),observation
            assert observation['worker_still_blocked'] and observation['original_request_pending'],observation
        result['status']='PASS';save()
    finally:
        try:server.cleanup()
        finally:
            pool.shutdown(wait=True,cancel_futures=True);save()
    return result
