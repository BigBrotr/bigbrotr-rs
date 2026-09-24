#!/usr/bin/env python3
"""Research-only pinned tracer discriminator; no BigBrotr product dependencies.

Run inside an inventoried unprivileged Linux amd64 container with exact Git at
/source, the pinned upstream archive at /input/strace-6.1.tar.xz, and fresh /out.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tarfile
import time

ROOT=Path('/out')
SOURCE=Path('/source/experiments/archive-engine')
ARCHIVE=Path('/input/strace-6.1.tar.xz')
DIGEST='2579e9cec37dbb786f6ea0bebd15f40dd561ef2bde2a2a2ecdce5963b01859fd'


def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def command(name,args,cwd=None):
    with (ROOT/(name+'.log')).open('xb') as log:
        subprocess.run(args,cwd=cwd,stdout=log,stderr=subprocess.STDOUT,timeout=900,check=True)


def run(tool,profile,held):
    root=ROOT/profile;root.mkdir();(root/'destination.json').write_text('independent marker\n')
    trace=root/'trace.log'
    args=[str(tool),'-f','-ttt','-yy','-P','destination.json','-e','trace=openat','-o',str(trace)]
    if held:args+=['-e','inject=openat:delay_enter=60s:when=1']
    args += [str(ROOT/'fixture'),str(root)]
    record=dict(profile=profile,tool_sha256=sha(tool),command=args)
    def save():(root/'result.json').write_text(json.dumps(record,indent=2)+'\n')
    with (root/'stdout.log').open('xb') as out,(root/'stderr.log').open('xb') as err:
        child=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=out,stderr=err,start_new_session=True)
        parent=worker=None
        try:
            end=time.monotonic()+3
            while True:
                assert child.poll() is None
                lines=(root/'stdout.log').read_text().splitlines()
                if lines:
                    first=json.loads(lines[0]);assert first['event']=='ready';parent=first['pid']
                    if held:
                        text=trace.read_text() if trace.exists() else ''
                        matches=re.findall(r'^(\d+)\s+\d+\.\d+ openat\([^\n]*"destination.json",[^\n]*$',text,re.M)
                        if matches and ' = ' not in text.splitlines()[-1]:
                            worker=int(matches[-1]);assert worker!=parent
                            status=dict(line.split(':',1) for line in Path(f'/proc/{worker}/status').read_text().splitlines() if ':' in line)
                            assert int(status['Tgid'])==parent and int(status['TracerPid'])==child.pid
                            break
                    elif len(lines)==2:
                        assert json.loads(lines[1])==dict(event='returned');break
                assert time.monotonic()<end,'marker syscall observation timeout'
                time.sleep(.01)
            record.update(parent_pid=parent,worker_tid=worker,observed_hold=held);save()
            if held:
                time.sleep(1)
                assert child.poll() is None and len((root/'stdout.log').read_text().splitlines())==1
                assert ' = ' not in trace.read_text().splitlines()[-1]
            started=time.monotonic();child.stdin.write(b'E');child.stdin.flush()
            try:
                record['exit']=child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                record['exit']='TIMEOUT'
            record['exit_observation_ms']=round((time.monotonic()-started)*1000,3)
            record['stdout_before_cleanup']=(root/'stdout.log').read_text()
            record['stderr_before_cleanup']=(root/'stderr.log').read_text();save()
        finally:
            if child.poll() is None:os.killpg(child.pid,signal.SIGKILL)
            child.wait(timeout=5);child.stdin.close()
            # The tracer is this harness's child; all tracees share its freshly
            # created process group. Cleanup never signals a reused arbitrary PID.
            end=time.monotonic()+2
            while any(Path(f'/proc/{p}').exists() for p in (parent,worker) if p is not None):
                assert time.monotonic()<end,'tracee remains after owned group cleanup'
                time.sleep(.01)
            record['tracees_absent']=True;save()
    return record


def main():
    result=dict(status='INCOMPLETE',source_inputs={},cases=[])
    def save():(ROOT/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    try:
        assert sha(ARCHIVE)==DIGEST
        result['source_archive']=dict(url='https://strace.io/files/6.1/strace-6.1.tar.xz',sha256=DIGEST)
        result['source_inputs']={p.name:sha(p) for p in [SOURCE/'strace_exit_fixture.c',SOURCE/'strace_exit_probe.py',SOURCE/'strace-6.1-exit-delay.patch']}
        result['kernel']=subprocess.check_output(['uname','-srvm'],text=True).strip()
        result['compiler']=subprocess.check_output(['cc','--version'],text=True)
        with tarfile.open(ARCHIVE) as tar:
            for member in tar.getmembers():
                name=Path(member.name)
                assert not name.is_absolute() and '..' not in name.parts and name.parts[0]=='strace-6.1'
                assert member.isdir() or member.isfile() or member.islnk()
                if member.islnk():
                    target=Path(member.linkname)
                    assert not target.is_absolute() and '..' not in target.parts and target.parts[0]=='strace-6.1'
                    assert tar.getmember(member.linkname).isfile()
                destination=ROOT/name
                if member.isdir():destination.mkdir(exist_ok=True)
                else:
                    # Python3.11 lacks extraction filters. Copy only validated
                    # regular bytes (including archive-internal hardlink data),
                    # never create links or restore archive owner/mode metadata.
                    with tar.extractfile(member) as original,destination.open('xb') as output:
                        shutil.copyfileobj(original,output)
                    destination.chmod(0o755 if member.mode&0o111 else 0o644)
        build=ROOT/'strace-6.1'
        command('configure',['./configure','--enable-mpers=no','--with-libunwind=no','--with-libdw=no'],build)
        command('make-original',['make','-j2'],build)
        shutil.copy2(build/'src/strace',ROOT/'strace-original')
        command('fixture',['cc','-O2','-Wall','-Wextra','-Werror','-pthread',str(SOURCE/'strace_exit_fixture.c'),'-o',str(ROOT/'fixture')])
        result['fixture_sha256']=sha(ROOT/'fixture')
        for held in (False,True):
            case=run(ROOT/'strace-original','original-held' if held else 'original-pass',held)
            result['cases'].append(case);save()
            assert case['exit']==('TIMEOUT' if held else 4)
            if held:assert 'delayed wait data set already' in case['stderr_before_cleanup']
        command('patch',['patch','--batch','--fuzz=0','-p1','-i',str(SOURCE/'strace-6.1-exit-delay.patch')],build)
        command('make-fixed',['make','-j2'],build)
        shutil.copy2(build/'src/strace',ROOT/'strace-exit-fixed')
        for held in (False,True):
            case=run(ROOT/'strace-exit-fixed','fixed-held' if held else 'fixed-pass',held)
            result['cases'].append(case);save()
            assert case['exit']==4 and case['exit_observation_ms']<2000 and not case['stderr_before_cleanup']
            if held:assert 'returned' not in case['stdout_before_cleanup']
        result['status']='PASS';save()
    finally:save()


if __name__=='__main__':main()
