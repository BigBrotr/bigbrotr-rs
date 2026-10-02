#!/usr/bin/env python3
"""Own one real CLI process; record its incarnation and always reap it."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

assert len(sys.argv) in (3, 4) and (len(sys.argv)==3 or sys.argv[3] in ('control', 'sync', 'archive', 'recovery', 'source-closure'))
config, prefix = map(Path, sys.argv[1:3])
mode = sys.argv[3] if len(sys.argv)==4 else 'initial'
roles = ['--roles','control'] if mode=='control' else [
    '--initialize-ingest','--roles','control,ingest,sync,archive' if mode=='archive' else 'control,ingest,sync' if mode=='sync' else 'control,ingest']
command = (['ingest','recover','--file',str(config)] if mode in ('recovery', 'source-closure')
           else ['serve','--file',str(config),*roles])
if mode == 'source-closure':
    command.append('--close-source')
assert config.is_absolute() and prefix.is_absolute()


def publish(suffix, value):
    target = Path(str(prefix)+suffix)
    partial = Path(str(target)+'.partial')
    assert not target.exists()
    with partial.open('x') as stream:
        stream.write(json.dumps(value)+'\n')
    os.replace(partial,target)


started = time.monotonic()
with Path(str(prefix)+'.stdout').open('xb') as out, Path(str(prefix)+'.stderr').open('xb') as err:
    child = subprocess.Popen(['/build/bigbrotr',*command],
                             stdout=out, stderr=err, env={})
    identity = dict(pid=child.pid, start_ticks=Path(f'/proc/{child.pid}/stat').read_text().split()[21],
                    config=str(config), prefix=str(prefix))
    publish('.pid.json',identity)
    forced = False
    try:
        code = child.wait(timeout=90 if mode=='archive' else 45)
    except BaseException:
        forced = True
        child.send_signal(signal.SIGKILL)
        code = child.wait(timeout=5)
        raise
    finally:
        publish('.terminal.json',dict(identity, exit_code=child.returncode,
            elapsed_seconds=time.monotonic()-started, forced=forced, reaped=True))
