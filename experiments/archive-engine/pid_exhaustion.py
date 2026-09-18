"""Observe signed-extension startup under an externally imposed PID cap.

Run only in an owned disposable native container with /build readonly and /lab
writable. This records the actual worker outcome; it does not certify a parent
supervisor or treat an abort as a successful engine invocation.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

root = Path('/lab')
build = Path('/build')
bundle = build / 'archive-worker'
binary = bundle / 'bigbrotr-archive-worker'
receipt = json.loads((build / 'build-result.json').read_text())
assert receipt['build_info']['source']['revision'] == 'd459fcdc357ddb3b68d9ac89e044b22156cf72b0'
assert hashlib.sha256(binary.read_bytes()).hexdigest() == receipt['archive_worker']['sha256']
temp = root / 'temp'
temp.mkdir()
cgroup = Path('/sys/fs/cgroup')


def counters():
    return {name: (cgroup / name).read_text().strip()
            for name in ['pids.max', 'pids.current', 'pids.events']}


before = counters()
assert before['pids.max'] == '8'
command = ['strace', '-f', '-ttt', '-o', str(root / 'threads.log'),
           '-e', 'trace=clone,clone3', str(binary), 'inspect-engine',
           str(bundle), str(temp), '268435456', '134217728', '1']
begin = time.monotonic()
timed_out = False
with (root / 'stdout.json').open('wb') as out, (root / 'stderr.txt').open('wb') as err:
    process = subprocess.Popen(command, stdout=out, stderr=err,
                               env={'PATH': '/usr/bin:/bin', 'HOME': str(root)},
                               start_new_session=True)
    try:
        code = process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGKILL)
        code = process.wait(timeout=5)

result = {
    'source': receipt['build_info']['source'],
    'worker_sha256': receipt['archive_worker']['sha256'],
    'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'command': command, 'before': before, 'after': counters(),
    'exit_code': code, 'timed_out': timed_out, 'seconds': time.monotonic() - begin,
    'stdout_bytes': (root / 'stdout.json').stat().st_size,
    'stderr_bytes': (root / 'stderr.txt').stat().st_size,
    'scope': 'Observation only; engine failure remains failure, parent not implemented',
}
(root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
