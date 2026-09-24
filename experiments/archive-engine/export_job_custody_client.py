#!/usr/bin/env python3
"""Sample real destination exclusion while a cancelled Control driver drains.

The host holds the actual committed reply and observes catalog backend closure.
Socket inode observations bracket each independent flock attempt. This is a
bounded sampled discriminator, not proof about every possible I/O schedule.
"""
from concurrent.futures import ThreadPoolExecutor
from http.cookies import SimpleCookie
import json
import os
from pathlib import Path
import select
import signal
import sys
import time
import private_grant_http as http
import export_job_http as jobs
from export_destination_blocked import lock_available as destination_lock_available


def emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)),flush=True)


def start_ticks(pid):
    try:return Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
    except (FileNotFoundError,ProcessLookupError):return None


def sockets(pid, port):
    try:
        descriptors=set()
        for entry in Path(f'/proc/{pid}/fd').iterdir():
            try:target=os.readlink(entry)
            except FileNotFoundError:continue
            if target.startswith('socket:['):descriptors.add(target[8:-1])
        rows=Path(f'/proc/{pid}/net/tcp').read_text().splitlines()[1:]
        return sorted(row[9] for line in rows if (row:=line.split())[9] in descriptors
                      and row[3]=='01' and row[2]==f'0100007F:{port:04X}')
    except (FileNotFoundError,ProcessLookupError):return []


def children(pid):
    result=[]
    try:
        for task in Path(f'/proc/{pid}/task').iterdir():
            try:result+=list(map(int,(task/'children').read_text().split()))
            except (FileNotFoundError,ProcessLookupError):pass
    except (FileNotFoundError,ProcessLookupError):pass
    return sorted(set(result))


def main(port, resume_job=None):
    assert resume_job is None or (len(resume_job)==32 and all(c in '0123456789abcdef' for c in resume_job))
    assert 0<port<65536
    original=http.CONFIG;text=original.read_text();needle='[postgres.control]\nendpoint="127.0.0.1:5432"'
    assert text.count(needle)==1
    prefix='job-resume-custody' if resume_job else 'custody'
    destination=Path('/out/signed_normal/export-destination') if resume_job else http.ROOT/'destination'
    if resume_job: assert text.count('root="/out/signed_normal/export-destination"')==1
    def lock_available():return destination_lock_available(destination)
    config=http.ROOT/(prefix+'.toml');config.write_text(text.replace(needle,f'[postgres.control]\nendpoint="127.0.0.1:{port}"'))
    http.CONFIG=config
    server=http.Server(prefix);pool=ThreadPoolExecutor(max_workers=1);pidfd=None
    path=http.ROOT/(prefix+'-result.json');assert not path.exists()
    result=dict(status='INCOMPLETE',samples=[])
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    try:
        result['ready']=server.ready();pid=server.process.pid;result['parent_pid']=pid
        pidfd=os.pidfd_open(pid);poll=select.poll();poll.register(pidfd,select.POLLIN)
        initial=sockets(pid,port);assert len(initial)==1
        result['control_socket']=initial;result['parent_start_ticks']=start_ticks(pid)
        assert not lock_available()
        info=destination.stat();result['destination']=dict(path=str(destination),device=info.st_dev,inode=info.st_ino)
        if resume_job:
            status,_,headers=http.request('POST','/management/v1/sessions',dict(username='alice',password='Independent owner password 2026'))
            assert status==201,status
            cookies=SimpleCookie()
            for k,v in headers:
                if k.lower()=='set-cookie':cookies.load(v)
            auth={'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
            body=dict(expected_job_revision='7',expected_config_revision='1',expected_policy_revision='2');key='f4'*16
            route=f'/management/v1/jobs/{resume_job}/resume'
        else:
            auth=json.loads((http.ROOT/'session.private').read_text());body=jobs.command();key='e6'*16
            route='/management/v1/jobs'
        def request():
            try:return dict(status=http.request('POST',route,body,{**auth,'Idempotency-Key':key})[0])
            except (OSError,http.http.client.HTTPException) as error:return dict(closed=type(error).__name__)
        submitted=time.monotonic();future=pool.submit(request)
        emit('submitted',pid=pid,key=key,owner=result['ready'],command=body if resume_job else None)
        assert sys.stdin.readline()=='signal\n'
        assert not future.done() and sockets(pid,port)==initial and not children(pid)
        signal.pidfd_send_signal(pidfd,signal.SIGTERM)
        sent=time.monotonic();margin=5-(sent-submitted)
        result['signal']=dict(monotonic=sent,request_margin_seconds=margin);save()
        assert margin>=1.0,margin
        emit('signal_sent',**result['signal'])
        end=sent+8
        gate=http.ROOT/(prefix+'-catalog-gone.json')
        while not poll.poll(0):
            assert time.monotonic()<end,'parent termination deadline'
            assert len(result['samples'])<1500,'sample budget'
            try:
                catalog_gone=gate.exists();start_before=start_ticks(pid)
                before=sockets(pid,port);free=lock_available();after=sockets(pid,port)
                result['samples'].append(dict(monotonic=time.monotonic(),socket_open=before==after==initial,
                    socket_before=before,socket_after=after,
                    same_process=start_before==start_ticks(pid)==result['parent_start_ticks'],
                    lock_available=free,catalog_gone=catalog_gone,children=children(pid),parent_live=not bool(poll.poll(0))))
            except PermissionError:
                # Linux may revoke /proc/<pid>/fd access while exit tears down
                # its mm, just before pidfd readability. A live denied process
                # is an unknown observation, never a successful absent socket.
                if not poll.poll(100):raise
                result['sampling_ended_on_process_exit']=True
                break
            time.sleep(.01)
        result['terminal']=dict(exit=server.process.wait(timeout=2),response=future.result(timeout=2),
            lock_available=lock_available(),parent_present=Path(f'/proc/{pid}').exists(),
            stderr=server.error.read_text(),stdout=server.output.read_text(),elapsed_ms=round((time.monotonic()-sent)*1000,3));save()
        terminal=result['terminal']
        assert terminal['exit']==4 and not terminal['parent_present'] and terminal['lock_available']
        assert ('export resume cancelled; Control outcome unresolved' if resume_job else 'native grant acceptance cancelled; outcome unresolved') in terminal['stderr']
        assert '"release":"committed"' not in terminal['stdout']
        assert terminal['response'].get('status') not in (200,202)
        assert not list((http.ROOT/'scratch').iterdir())
        assert gate.exists();result['catalog_observation']=json.loads(gate.read_text())
        supported=[sample for sample in result['samples'] if sample['same_process'] and sample['socket_open'] and sample['catalog_gone'] and sample['parent_live'] and not sample['children']]
        assert len(supported)>=2
        interval=supported[-1]['monotonic']-supported[0]['monotonic'];assert interval>=.5,interval
        violations=[sample for sample in supported if sample['lock_available']]
        result['observed_interval_seconds']=interval;result['violations']=len(violations)
        result['status']='FAIL' if violations else 'PASS'
        result['reason']='destination-released-before-driver-close' if violations else 'custody-retained-at-all-samples'
        save();emit('observed',status=result['status'],violations=len(violations),interval_seconds=interval)
        assert not violations,result['reason']
    finally:
        try:server.cleanup()
        finally:
            if pidfd is not None:os.close(pidfd)
            pool.shutdown(wait=True,cancel_futures=True);http.CONFIG=original;save()


if __name__=='__main__':
    assert len(sys.argv) in (2,3)
    main(int(sys.argv[1]),sys.argv[2] if len(sys.argv)==3 else None)
