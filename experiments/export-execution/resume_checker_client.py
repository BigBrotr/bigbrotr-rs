#!/usr/bin/env python3
"""Actual resume destination syscall hold; no accepted priming requests."""
from concurrent.futures import ThreadPoolExecutor
from http.cookies import SimpleCookie
import hashlib
import json
from pathlib import Path
import re
import select
import signal
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from export_destination_blocked import TracedServer, instrument, lock_available, pending_checker
from commit_client import emit

DESTINATION = Path('/out/signed_normal/export-destination')
KEY = 'f6'*16


def main(job):
    assert re.fullmatch('[0-9a-f]{32}', job)
    original_config = http.CONFIG.read_bytes()
    marker = DESTINATION/'destination.json'
    original_marker = marker.read_bytes()
    marker_stat = marker.stat()
    with (http.ROOT/'resume-checker-marker-original.json').open('xb') as retained:
        retained.write(original_marker)
    result = dict(status='INCOMPLETE', job=job, key=KEY, primed=[], instrument=instrument(),
                  marker_sha256=hashlib.sha256(original_marker).hexdigest(),
                  marker_identity=dict(device=marker_stat.st_dev,inode=marker_stat.st_ino))
    path = http.ROOT/'resume-checker-result.json'
    def save(): path.write_text(json.dumps(result, indent=2)+'\n')
    server = None; future = None; passed = False; marker_changed = False
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        with http.CONFIG.open('a') as stream:
            stream.write('\n[runtime]\nheartbeat_interval="500ms"\ngraceful_shutdown_timeout="2s"\n')
        http.cli('config', 'validate')
        server = TracedServer()
        result['ready'] = server.ready(); result['parent_identity'] = server.identity
        result['tracer_command'] = server.command
        status, value, headers = http.request('POST', '/management/v1/sessions',
            dict(username='alice', password='Independent owner password 2026'))
        assert status == 201 and value['config_revision']=='1' and value['policy_revision']=='2'
        cookies = SimpleCookie()
        for name, value in headers:
            if name.lower() == 'set-cookie': cookies.load(value)
        auth = {'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
                'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
        command = dict(expected_job_revision='7', expected_config_revision='1', expected_policy_revision='2')
        result['command'] = command; save()
        emit('ready', owner=result['ready'], parent_identity=server.identity, job=job, key=KEY)
        assert select.select([sys.stdin],[],[],4)[0], 'host baseline deadline'
        assert sys.stdin.readline() == 'run\n'
        # This owned lab temporarily makes the marker incompatible after startup.
        # Every unheld priming request must refuse; none can accept the resume.
        # Restore the same bytes/inode in finally, after positive tracee death.
        marker_changed = True
        marker.write_bytes(b'{}')
        result['priming_marker_sha256'] = hashlib.sha256(marker.read_bytes()).hexdigest()
        def request():
            try:
                status, value, headers = http.request('POST', '/management/v1/jobs/'+job+'/resume',
                    command, {**auth, 'Idempotency-Key':KEY})
                assert not any(k.lower()=='set-cookie' for k,_ in headers)
                assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
                return dict(status=status, code=value.get('error',{}).get('code'))
            except (OSError, http.http.client.HTTPException) as error:
                return dict(closed=type(error).__name__)
        pending = lambda: pending_checker(server.trace.read_text(), server.parent, DESTINATION)
        for _ in range(8):
            submitted=time.monotonic(); future=pool.submit(request)
            deadline=submitted+4; seen=None; since=None; tid=None
            while not future.done() and time.monotonic()<deadline:
                current=pending()
                if current!=seen: seen=current; since=time.monotonic()
                if current is not None and time.monotonic()-since>=.15:
                    tid=current; break
                time.sleep(.01)
            if tid is not None: break
            observed=future.result(timeout=8)
            assert observed==dict(status=503, code='authority_unavailable'), observed
            result['primed'].append(observed); save()
        else: raise AssertionError('No actual resume destination marker hold observed')
        status=dict(line.split(':',1) for line in Path(f'/proc/{tid}/status').read_text().splitlines() if ':' in line)
        assert int(status['Tgid'])==server.parent and int(status['TracerPid'])==server.process.pid
        assert status['State'].strip().startswith('t') and not future.done() and not lock_available(DESTINATION)
        stat=DESTINATION.stat()
        result['blocked']=dict(tid=tid, tracer=server.process.pid, state=status['State'].strip(),
            submitted_monotonic=submitted, observed_monotonic=time.monotonic(),
            destination=dict(path=str(DESTINATION),device=stat.st_dev,inode=stat.st_ino),
            lock_available=False, trace=server.trace.read_text()); save()
        emit('blocked', tid=tid, parent=server.parent)
        # Host independently measures actual Control renewal within the same hold.
        assert select.select([sys.stdin],[],[],4)[0], 'host renewal deadline'
        assert sys.stdin.readline()=='signal\n'
        assert pending()==tid and not future.done() and not lock_available(DESTINATION)
        started=time.monotonic(); server.signal(signal.SIGTERM); sent=time.monotonic()
        result['signal']=dict(monotonic=sent,wall=time.time(),request_deadline_margin_seconds=5-(sent-submitted))
        assert result['signal']['request_deadline_margin_seconds']>=1.25
        while True:
            delivered=re.search(r'^'+str(server.parent)+r'\s+\d+\.\d+ --- SIGTERM ',server.trace.read_text(),re.MULTILINE)
            remaining=5-(time.monotonic()-submitted)
            assert remaining>=1, 'SIGTERM not distinguished from request expiry'
            if delivered: break
            time.sleep(.01)
        result['signal']['delivery_observed_margin_seconds']=remaining; save()
        code=server.process.wait(timeout=7)
        terminal=result['terminal']=dict(exit=code,elapsed_ms=(time.monotonic()-started)*1000,
            response=future.result(timeout=2),parent_present=Path(f'/proc/{server.parent}').exists(),
            worker_present=Path(f'/proc/{tid}').exists(),lock_available=lock_available(DESTINATION),
            stdout=server.output.read_text(),stderr=server.error.read_text()); save()
        assert code==4 and terminal['elapsed_ms']<7000
        assert 'native grant worker not joined; retain pin and terminate process' in terminal['stderr']
        assert not terminal['parent_present'] and not terminal['worker_present'] and terminal['lock_available']
        assert '"release":"committed"' not in terminal['stdout']
        assert terminal['response'].get('status') not in (200,202)
        assert not list((http.ROOT/'scratch').iterdir())
        passed=True
    except BaseException as error:
        result['failure']=type(error).__name__
        raise
    finally:
        try:
            identity_known = server is None or server.parent_fd is not None
            try:
                if server: server.cleanup()
            finally:
                pool.shutdown(wait=True,cancel_futures=True)
            if marker_changed:
                assert identity_known
                marker.write_bytes(original_marker)
                assert marker.read_bytes()==original_marker
                restored=marker.stat()
                assert (restored.st_dev,restored.st_ino)==(marker_stat.st_dev,marker_stat.st_ino)
                result['marker_restored']=True
            if identity_known:
                http.CONFIG.write_bytes(original_config)
            else:
                # Startup failed before pidfd capture: enclosing owned-container
                # teardown must establish exclusion. Do not rewrite its inputs.
                result['config_retained_until_container_exclusion']=True
            result['status']='PASS' if passed else 'INCOMPLETE'
        except BaseException as error:
            result['status']='INCOMPLETE'; result['cleanup_error']=type(error).__name__
            raise
        finally: save()
    emit('terminal', status=result['status'])


if __name__=='__main__':
    assert len(sys.argv)==2
    main(sys.argv[1])
