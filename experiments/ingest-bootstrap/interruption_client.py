#!/usr/bin/env python3
"""Observe an actual native syscall hold; own and reap both tracer and tracee."""
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import socket
import subprocess
import sys
import time

config, prefix = map(Path, sys.argv[1:3])
mode = sys.argv[3]
assert mode in ['trace-pass','initialize-signal','initialize-deadline','close-signal']
assert len(sys.argv) == 4 or (len(sys.argv) == 5 and sys.argv[4] == '--session-http' and mode == 'close-signal')
session_http = len(sys.argv) == 5
tracer = Path('/instrument/strace-exit-fixed')
assert hashlib.sha256(tracer.read_bytes()).hexdigest() == '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
trace = Path(str(prefix)+'.trace')
lock = str(prefix/'store/LOCK')
operation = 'close' if mode=='close-signal' else 'openat'
# RocksDB11.8.1 UnlockFile performs fcntl unlock before close(LOCK). This
# close case holds the unfinished destructor and Control custody, not the OS lock.
command = [str(tracer),'-f','-ttt','-yy','-P',lock,'-e','trace='+operation,'-o',str(trace)]
if mode!='trace-pass':
    command += ['-e','inject='+operation+':delay_enter=60s:when=1']
command += ['/build/bigbrotr','serve','--file',str(config),'--roles','control,ingest','--initialize-ingest']
result = dict(status='INCOMPLETE',mode=mode,command=command)


def publish(suffix, value):
    path = Path(str(prefix)+suffix)
    temporary = Path(str(path)+'.partial')
    with temporary.open('x') as stream:
        stream.write(json.dumps(value,indent=2)+'\n')
    os.replace(temporary,path)


def wait(condition, label, seconds):
    deadline = time.monotonic()+seconds
    while True:
        value = condition()
        if value:
            return value
        assert time.monotonic()<deadline, label
        time.sleep(.005)


def initialized():
    lines=Path(str(prefix)+'.stdout').read_text().splitlines(keepends=True)
    return any(json.loads(line).get('state')=='initialized' for line in lines if line.endswith('\n'))


with Path(str(prefix)+'.stdout').open('xb') as out, Path(str(prefix)+'.stderr').open('xb') as err:
    result['launch_wall'] = time.time()
    result['launch_monotonic'] = time.monotonic()
    process = subprocess.Popen(command,env={},stdout=out,stderr=err,start_new_session=True)
parent = None
parent_fd = None
connection = None
try:
    children = Path(f'/proc/{process.pid}/task/{process.pid}/children')
    parent = int(wait(lambda: children.read_text().strip(), 'tracee-child', 2))
    parent_fd = os.pidfd_open(parent)
    wait(lambda: Path(f'/proc/{parent}/exe').resolve()==Path('/build/bigbrotr'), 'tracee exec', 1)
    assert os.getpgid(parent)==process.pid
    identity=dict(pid=parent,start_ticks=Path(f'/proc/{parent}/stat').read_text().rsplit(')',1)[1].split()[19],tracer_pid=process.pid)
    publish('.pid.json',identity)
    result['identity']=identity
    poll=select.poll();poll.register(parent_fd,select.POLLIN)

    def send():
        signal.pidfd_send_signal(parent_fd,signal.SIGTERM)
        result['signal']=dict(monotonic=time.monotonic(),wall=time.time())

    def held():
        if not trace.exists():return None
        text=trace.read_text()
        for line in reversed(text.splitlines()):
            match=re.match(r'^(\d+)\s+(\d+\.\d+) '+operation+r'\(',line)
            if not match or lock not in line or ' = ' in line:continue
            tid=int(match[1]);later=text[text.rfind(line)+len(line):]
            if tid==parent or re.search(r'^'+str(tid)+r'\s+.*<\.\.\. '+operation+r' resumed>',later,re.MULTILINE):continue
            status=dict(s.split(':',1) for s in Path(f'/proc/{tid}/status').read_text().splitlines() if ':' in s)
            assert int(status['Tgid'])==parent and int(status['TracerPid'])==process.pid
            assert status['State'].strip().startswith('t')
            return dict(tid=tid,syscall=operation,entry_wall=float(match[2]),observed_monotonic=time.monotonic(),line=line)
        return None

    if mode in ['trace-pass','close-signal']:
        wait(initialized,'initialization',8)
        if session_http:
            connection = socket.create_connection(('127.0.0.1', 18081), 1)
            connection.sendall(b'GET /management/v1/me HTTP/1.1\r\nHost: 127.0.0.1:18081\r\n')
            remote = f'0100007F:{connection.getsockname()[1]:04X}'
            def accepted_socket():
                for line in Path(f'/proc/{parent}/net/tcp').read_text().splitlines()[1:]:
                    row = line.split()
                    if row[1] == '0100007F:46A1' and row[2] == remote and row[3] == '01':
                        inode = row[9]
                        for fd in Path(f'/proc/{parent}/fd').iterdir():
                            try:
                                if os.readlink(fd) == f'socket:[{inode}]':
                                    return dict(fd=int(fd.name), inode=inode, remote=remote)
                            except FileNotFoundError:
                                pass
                return None
            result['http_accepted'] = wait(accepted_socket, 'accepted partial HTTP connection', 1)
        if mode=='close-signal':send()
    if mode!='trace-pass':
        blocked=wait(held,'pending-native-syscall',2)
        result['blocked']=blocked
        if session_http:
            connection.settimeout(.25)
            try:
                received = connection.recv(1)
            except ConnectionResetError:
                received = b''
            assert received == b'', 'partial HTTP connection outlived native-close entry'
            connection.close()
            connection = None
            with socket.socket() as refused:
                refused.settimeout(.25)
                assert refused.connect_ex(('127.0.0.1',18081)) != 0
            result['http_stopped'] = dict(socket_bytes=0, new_connection_refused=True,
                observed_monotonic=time.monotonic())
        publish('.waiting.json',dict(identity,**blocked))
        wait(lambda: Path(str(prefix)+'.continue').exists(),'independent SQL observer',1.25)
        assert held()['tid']==blocked['tid'] and not poll.poll(0)
    if mode in ['trace-pass','initialize-signal']:send()
    if mode=='initialize-signal':
        # Native phase cannot start before process launch. Using that earlier
        # boundary deducts all startup time, including untraced filesystem work.
        margin=5-(result['signal']['wall']-result['launch_wall'])
        result['signal']['minimum_deadline_margin_seconds']=margin
        assert margin>=1.25, 'signal indistinguishable from ordinary expiry'
    if 'signal' in result:
        delivered=wait(lambda: re.search(r'^'+str(parent)+r'\s+(\d+\.\d+) --- SIGTERM ',trace.read_text(),re.MULTILINE),'actual-signal-delivery',.5)
        result['signal']['delivered_wall']=float(delivered[1])
        if mode=='initialize-signal':
            assert 5-(float(delivered[1])-result['launch_wall'])>=1
    code=process.wait(timeout=9)
    assert poll.poll(1000) and not Path(f'/proc/{parent}').exists()
    result['terminal']=dict(exit_code=code,observed_monotonic=time.monotonic(),tracee_absent=True,tracer_reaped=True)
    stdout=Path(str(prefix)+'.stdout').read_text();stderr=Path(str(prefix)+'.stderr').read_text()
    if mode=='trace-pass':
        assert code==0 and not stderr and '"owner_release":"committed"' in stdout
    else:
        assert code==4
        # Hard exit deliberately does not wait for diagnostic output. Actual
        # held syscall, exit/death, retained fence and durable rows are the oracle.
        result['diagnostic_observed']='initial Ingest worker not joined' in stderr
        assert '"owner_release":"committed"' not in stdout
        assert not Path(f'/proc/{result["blocked"]["tid"]}').exists()
        assert '(DELAYED)' not in trace.read_text(), 'held native syscall returned to application'
        if mode=='initialize-deadline':assert '--- SIGTERM ' not in trace.read_text()
        origin=result.get('signal',result['blocked'])
        elapsed=result['terminal']['observed_monotonic']-origin.get('monotonic',origin.get('observed_monotonic'))
        assert elapsed < (3.5 if 'signal' in result else 8), elapsed
        if mode=='initialize-signal':
            assert result['terminal']['observed_monotonic']<result['launch_monotonic']+5, 'not distinguished from the earliest ordinary watchdog expiry'
        result['terminal']['elapsed_from_signal_or_hold']=elapsed
    result['status']='PASS'
finally:
    if connection is not None:
        connection.close()
    forced=False
    try:
        if parent_fd is not None:
            check=select.poll();check.register(parent_fd,select.POLLIN)
            if not check.poll(0):
                forced=True
                try:signal.pidfd_send_signal(parent_fd,signal.SIGKILL)
                except ProcessLookupError:pass
        if process.poll() is None:
            forced=True
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        process.wait(timeout=5)
        if parent_fd is not None:assert check.poll(1000),'tracee cleanup unconfirmed'
    finally:
        if parent_fd is not None:os.close(parent_fd)
        result['cleanup']=dict(forced=forced,tracer_exit=process.returncode)
        publish('.terminal.json',result)
