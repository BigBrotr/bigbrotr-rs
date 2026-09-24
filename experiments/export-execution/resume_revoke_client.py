#!/usr/bin/env python3
"""Authenticated grant revocation during an actual timed resume marker syscall."""
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
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'archive-engine'))
import private_grant_http as http
from export_destination_blocked import TracedServer, instrument, lock_available, pending_checker
from commit_client import emit

DESTINATION=Path('/out/signed_normal/export-destination')
RESUME_KEY='e6'*16
GRANT_KEY='e7'*16


def main(job):
    assert re.fullmatch('[0-9a-f]{32}',job)
    original=http.CONFIG
    http.CONFIG=http.ROOT/'job-resume-revoke.toml'
    http.CONFIG.write_bytes(original.read_bytes()+b'\n[runtime]\nheartbeat_interval="500ms"\ngraceful_shutdown_timeout="2s"\n')
    marker=DESTINATION/'destination.json';original_marker=marker.read_bytes();identity=marker.stat()
    (http.ROOT/'job-resume-revoke-marker-original.json').write_bytes(original_marker)
    result=dict(status='INCOMPLETE',job=job,key=RESUME_KEY,grant_key=GRANT_KEY,primed=[],instrument=instrument(),marker_sha256=hashlib.sha256(original_marker).hexdigest(),marker_identity=dict(device=identity.st_dev,inode=identity.st_ino))
    path=http.ROOT/'job-resume-revoke-result.json'
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    server=None;future=None;marker_changed=False;pool=ThreadPoolExecutor(max_workers=1)
    try:
        http.cli('config','validate')
        server=TracedServer(delay_seconds=2,name='job-resume-revoke-checker')
        result['ready']=server.ready();result['parent_identity']=server.identity;result['tracer_command']=server.command
        status,value,headers=http.request('POST','/management/v1/sessions',dict(username='alice',password='Independent owner password 2026'))
        assert status==201 and value['config_revision']=='1' and value['policy_revision']=='2' and value['grant_revision']=='4'
        cookies=SimpleCookie()
        for k,v in headers:
            if k.lower()=='set-cookie':cookies.load(v)
        auth={'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
        principal=value['principal_id'];result['principal']=principal
        resume=dict(expected_job_revision='7',expected_config_revision='1',expected_policy_revision='2')
        grant=dict(principal_id=principal,action='history.read',enabled=False,scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),expected_config_revision='1',expected_policy_revision='2',expected_grant_revision='4',allocation=None)
        grant_auth={**auth,'Idempotency-Key':GRANT_KEY}
        status,proof,_=http.request('POST','/management/v1/sessions/current/reauthenticate',dict(password='Independent owner password 2026',command=grant),grant_auth)
        assert status==201
        grant_body=dict(command=grant,proof_id=proof['proof_id'])
        result['command']=resume;result['grant_command']=grant;result['proof']=proof;save()
        emit('ready',owner=result['ready'],parent_identity=server.identity,proof=proof)
        assert select.select([sys.stdin],[],[],5)[0] and sys.stdin.readline()=='run\n'
        # Reject every priming request. After the real third per-thread open is
        # held, restore this same inode before the finite delay expires.
        marker_changed=True;marker.write_bytes(b'{}')
        def request():
            started=time.monotonic()
            status,value,headers=http.request('POST','/management/v1/jobs/'+job+'/resume',resume,{**auth,'Idempotency-Key':RESUME_KEY})
            assert not any(k.lower()=='set-cookie' for k,_ in headers)
            assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
            return dict(status=status,body=value,elapsed_seconds=time.monotonic()-started)
        pending=lambda:pending_checker(server.trace.read_text(),server.parent,DESTINATION)
        for _ in range(8):
            submitted=time.monotonic();future=pool.submit(request);deadline=submitted+3;seen=None;since=None;tid=None
            while not future.done() and time.monotonic()<deadline:
                current=pending()
                if current!=seen:seen=current;since=time.monotonic()
                if current is not None and time.monotonic()-since>=.15:tid=current;break
                time.sleep(.01)
            if tid is not None:break
            observed=future.result(timeout=8);assert observed['status']==503 and observed['body']['error']['code']=='authority_unavailable'
            result['primed'].append(observed);save()
        else:raise AssertionError('actual timed resume marker hold absent')
        def stopped_worker():
            status=dict(line.split(':',1) for line in Path(f'/proc/{tid}/status').read_text().splitlines() if ':' in line)
            assert int(status['Tgid'])==server.parent and int(status['TracerPid'])==server.process.pid and status['State'].strip().startswith('t')
            assert pending()==tid and not future.done() and not lock_available(DESTINATION)
            return dict(tid=tid,tracer=server.process.pid,state=status['State'].strip(),wall=time.time(),monotonic=time.monotonic(),trace=server.trace.read_text())
        result['blocked_before']=stopped_worker();result['submitted_monotonic']=submitted
        started=time.monotonic()
        status,receipt,headers=http.request('PUT','/management/v1/principals/'+principal+'/grants',grant_body,grant_auth)
        result['grant_response']=dict(status=status,receipt=receipt,elapsed_seconds=time.monotonic()-started)
        assert status==200 and receipt['created'] and receipt['enabled'] is False
        assert not any(k.lower()=='set-cookie' for k,_ in headers)
        result['blocked_after']=stopped_worker()
        marker.write_bytes(original_marker)
        assert marker.read_bytes()==original_marker and (marker.stat().st_dev,marker.stat().st_ino)==(identity.st_dev,identity.st_ino)
        result['restored_while_blocked']=stopped_worker()
        result['marker_restored_before_return']=dict(wall=time.time(),monotonic=time.monotonic(),sha256=hashlib.sha256(marker.read_bytes()).hexdigest())
        result['response']=future.result(timeout=5)
        assert result['response']['status']==403 and result['response']['body']['error']['code']=='permission_denied'
        assert 1.8<=result['response']['elapsed_seconds']<4
        assert pending()!=tid
        result['retry']=request();assert result['retry']['status']==403
        status,receipt,headers=http.request('PUT','/management/v1/principals/'+principal+'/grants',grant_body,grant_auth)
        result['grant_retry']=dict(status=status,receipt=receipt)
        assert status==200 and receipt=={**result['grant_response']['receipt'],'created':False}
        status,body,_=http.request('GET','/management/v1/jobs/'+job,auth=auth)
        result['health']=dict(status=status,body=body);assert status==200
        server.signal(signal.SIGTERM);assert server.process.wait(timeout=7)==0
        result['terminal']=dict(parent_absent=not Path(f'/proc/{server.parent}').exists(),lock_available=lock_available(DESTINATION),stdout=server.output.read_text(),stderr=server.error.read_text())
        assert result['terminal']['parent_absent'] and result['terminal']['lock_available'] and not result['terminal']['stderr']
        assert json.loads(result['terminal']['stdout'].splitlines()[-1])['release']=='committed'
        assert not list((http.ROOT/'scratch').iterdir())
        result['status']='PASS'
    except BaseException as error:result['failure']=type(error).__name__;raise
    finally:
        try:
            known=server is None or server.parent_fd is not None
            try:
                if server:server.cleanup()
            finally:pool.shutdown(wait=True,cancel_futures=True)
            if marker_changed:
                assert known
                marker.write_bytes(original_marker)
                assert marker.read_bytes()==original_marker and (marker.stat().st_dev,marker.stat().st_ino)==(identity.st_dev,identity.st_ino)
                result['marker_restored']=True
        except BaseException as error:result['status']='INCOMPLETE';result['cleanup_error']=type(error).__name__;raise
        finally:http.CONFIG=original;save()
    emit('terminal',status=result['status'])


if __name__=='__main__':main(sys.argv[1])
