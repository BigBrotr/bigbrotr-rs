"""Trusted Linux x86_64 worker parent-death experiment, with a live negative control.

The caller inventories/isolates the container and supplies an immutable verified
worker. SIGSTOP prevents stdin EOF or cooperative polling from satisfying death.
pidfds and a subreaper keep cleanup bound to the exact owned processes.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--bundle', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--worker-sha256', required=True)
parser.add_argument('--expect-survival', action='store_true')
args = parser.parse_args()
assert sys.platform == 'linux' and os.uname().machine == 'x86_64'
bundle = args.bundle.resolve(strict=True)
worker = bundle / 'bigbrotr-archive-worker'
assert hashlib.sha256(worker.read_bytes()).hexdigest() == args.worker_sha256
out = args.output
out.mkdir(mode=0o700, parents=True, exist_ok=False)
temp = out / 'temporary'
temp.mkdir(mode=0o700)
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0, ctypes.get_errno()  # PR_SET_CHILD_SUBREAPER
parent_code = '''import json,os,signal,subprocess,sys
from pathlib import Path
bundle,out=map(Path,sys.argv[1:])
env={'LC_ALL':'C','LD_LIBRARY_PATH':str(bundle/'lib'),
     'BIGBROTR_ARCHIVE_PARENT_PID':str(os.getpid())}
with (out/'worker.stdout').open('xb') as stdout,(out/'worker.stderr').open('xb') as stderr:
 child=subprocess.Popen([str(bundle/'bigbrotr-archive-worker'),'inspect-batch',str(bundle),str(out/'temporary'),'134217728','67108864','1','16','1048576'],env=env,stdin=subprocess.PIPE,stdout=stdout,stderr=stderr)
 (out/'worker.tmp').write_text(json.dumps({'pid':child.pid,'parent':os.getpid()}))
 (out/'worker.tmp').replace(out/'worker.json')
 while True:signal.pause()
'''
parent = subprocess.Popen([sys.executable, '-c', parent_code, str(bundle), str(out)])
child_pid = None
child_fd = None
reaped = False
try:
    until = time.monotonic() + 15
    while not (out / 'worker.json').exists() and time.monotonic() < until:
        assert parent.poll() is None
        time.sleep(.005)
    identity = json.loads((out / 'worker.json').read_text())
    assert identity['parent'] == parent.pid
    child_pid = identity['pid']
    child_fd = os.pidfd_open(child_pid)
    poll = select.poll()
    poll.register(child_fd, select.POLLIN)
    syscall = ''
    while time.monotonic() < until:
        assert not poll.poll(0), 'worker exited before engine/read boundary'
        syscall = Path(f'/proc/{child_pid}/syscall').read_text().strip()
        # x86_64 read(0,...): the complete offline engine has initialized and
        # preparation is now blocked reading the untouched wire header. Rust's
        # stdin buffer may request more than the 80 header bytes.
        fields = syscall.split()
        if len(fields) >= 4 and fields[:2] == ['0', '0x0'] and int(fields[3], 16) > 0:
            break
        time.sleep(.005)
    else:
        raise AssertionError('worker never reached actual header read')
    signal.pidfd_send_signal(child_fd, signal.SIGSTOP)
    until = time.monotonic() + 2
    while time.monotonic() < until:
        status = Path(f'/proc/{child_pid}/status').read_text()
        if '\nState:\tT (stopped)\n' in status:
            break
        time.sleep(.002)
    else:
        raise AssertionError('worker did not stop')
    start = time.monotonic()
    parent.kill()
    assert parent.wait(timeout=2) == -signal.SIGKILL
    exited = bool(poll.poll(1000))
    elapsed = time.monotonic() - start
    assert exited != args.expect_survival, ('unexpected worker survival', exited, elapsed)
    if exited:
        pid, status = os.waitpid(child_pid, 0)
        reaped = True
        assert pid == child_pid and os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
    report = {'status': 'PASS', 'negative_control': args.expect_survival,
              'worker_sha256': args.worker_sha256, 'identity': identity,
              'ready_syscall': syscall, 'stopped_before_parent_kill': True,
              'exited_after_parent_kill': exited, 'observation_seconds': elapsed}
    (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report), flush=True)
finally:
    if parent.poll() is None:
        parent.kill()
    parent.wait(timeout=2)
    if child_fd is not None:
        if not reaped:
            try:
                signal.pidfd_send_signal(child_fd, signal.SIGKILL)
            except ProcessLookupError:
                pass
            assert poll.poll(2000), 'cleanup did not observe worker exit'
            assert os.waitpid(child_pid, 0)[0] == child_pid
        os.close(child_fd)
    (out / 'cleanup.json').write_text(json.dumps({'parent_reaped': True,
                                                'worker_reaped': child_fd is not None}) + '\n')
