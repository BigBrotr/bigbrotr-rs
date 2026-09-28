#!/usr/bin/env python3
"""Own one real CLI process; record its incarnation and always reap it."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

config, prefix = map(Path, sys.argv[1:])
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
    child = subprocess.Popen(['/build/bigbrotr','serve','--file',str(config),
                              '--initialize-ingest','--roles','control,ingest'],
                             stdout=out, stderr=err, env={})
    identity = dict(pid=child.pid, start_ticks=Path(f'/proc/{child.pid}/stat').read_text().split()[21],
                    config=str(config), prefix=str(prefix))
    publish('.pid.json',identity)
    forced = False
    try:
        code = child.wait(timeout=45)
    except BaseException:
        forced = True
        child.send_signal(signal.SIGKILL)
        code = child.wait(timeout=5)
        raise
    finally:
        publish('.terminal.json',dict(identity, exit_code=child.returncode,
            elapsed_seconds=time.monotonic()-started, forced=forced, reaped=True))
