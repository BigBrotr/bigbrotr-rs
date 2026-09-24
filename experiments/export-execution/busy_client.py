#!/usr/bin/env python3
"""Trace only actual flock calls; retain actual parent and tracer identities."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'archive-engine'))
import private_grant_http as http
from commit_client import emit,lock_available


def main():
    output=http.ROOT/'executor-busy.stdout';error=http.ROOT/'executor-busy.stderr'
    trace=http.ROOT/'executor-busy.trace'
    parent=None;ticks=None
    with output.open('xb') as out,error.open('xb') as err:
        tracer=subprocess.Popen(['/out/checker-instrument/strace-exit-fixed','-f','-yy','-e','trace=flock','-o',str(trace),str(http.BINARY),'serve','--roles','control','--file',str(http.CONFIG)],env={},stdout=out,stderr=err)
    try:
        end=time.monotonic()+10
        while parent is None:
            assert tracer.poll() is None,error.read_text()
            children=Path(f'/proc/{tracer.pid}/task/{tracer.pid}/children').read_text().split()
            if children:
                assert len(children)==1
                parent=int(children[0]);ticks=Path(f'/proc/{parent}/stat').read_text().split()[21]
                break
            if time.monotonic()>=end:raise TimeoutError('traced parent identity')
            time.sleep(.005)
        while not output.read_text().splitlines():
            assert tracer.poll() is None,error.read_text()
            if time.monotonic()>=end:raise TimeoutError('traced runtime readiness')
            time.sleep(.01)
        owner=json.loads(output.read_text().splitlines()[0])
        assert owner['state']=='idle' and owner['policy_revision']=='4' and owner['listeners']==[]
        assert Path(f'/proc/{parent}/exe').resolve()==http.BINARY
        emit('ready',owner=owner,parent_pid=parent,parent_start_ticks=ticks,tracer_pid=tracer.pid)
        assert sys.stdin.readline()=='collect\n'
        # Observe autonomous exit; the fixture sends no shutdown signal on the
        # success path, even when catalog/Control drivers need longer to drain.
        code=tracer.wait(timeout=15)
        assert not Path(f'/proc/{parent}').exists() and not Path(f'/proc/{tracer.pid}').exists()
        assert lock_available()
        emit('terminal',autonomous_exit=True,exit=code,stderr=error.read_text(),
             parent_absent=True,tracer_absent=True,lock_available=True,
             scratch_empty=not list((http.ROOT/'scratch').iterdir()))
    finally:
        if parent is not None and Path(f'/proc/{parent}/stat').exists():
            assert Path(f'/proc/{parent}/stat').read_text().split()[21]==ticks
            os.kill(parent,signal.SIGKILL)
        if tracer.poll() is None:
            try:tracer.wait(timeout=10)
            except subprocess.TimeoutExpired:
                tracer.kill();tracer.wait(timeout=10)


if __name__=='__main__':main()
