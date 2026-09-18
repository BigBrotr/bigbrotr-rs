#!/usr/bin/env python3
"""Own a genuinely full pipe until the actual serve child is terminal.

Acceptance helper, never a production service. The observer may signal the
recorded child PID in the same isolated container; this helper always reaps it.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stream', choices=['stdout', 'stderr'], required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError('refusing existing observation')
    reader, writer = os.pipe()
    flags = fcntl.fcntl(writer, fcntl.F_GETFL)
    fcntl.fcntl(writer, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    filled = 0
    while True:
        try:
            filled += os.write(writer, b'x' * 4096)
        except BlockingIOError:
            break
    while True:
        try:
            filled += os.write(writer, b'x')
        except BlockingIOError:
            break
    fcntl.fcntl(writer, fcntl.F_SETFL, flags)
    record = {'status': 'RUNNING', 'stream': args.stream, 'filled_bytes': filled,
              'blocking': not bool(fcntl.fcntl(writer, fcntl.F_GETFL) & os.O_NONBLOCK)}
    started = time.monotonic()
    with args.output.with_suffix('.other-output').open('xb') as other:
        child = subprocess.Popen([args.binary, 'serve', '--file', args.config, '--roles', 'control'],
                                 stdout=writer if args.stream == 'stdout' else other,
                                 stderr=writer if args.stream == 'stderr' else other)
        record['pid'] = child.pid
        def save():
            temporary = args.output.with_suffix('.temporary')
            temporary.write_text(json.dumps(record, indent=2) + '\n')
            temporary.replace(args.output)
        save()
        try:
            record['exit'] = child.wait(timeout=45)
            record['status'] = 'TERMINAL'
        except subprocess.TimeoutExpired:
            child.kill()
            record.update(status='TIMEOUT_KILLED', exit=child.wait(timeout=5))
        finally:
            record['elapsed_seconds'] = time.monotonic() - started
            os.close(writer)
            # No byte was consumed before the child exited. EOF now proves that
            # no detached writer retains the write end beyond process exit.
            data = bytearray()
            os.set_blocking(reader, False)
            while block := os.read(reader, 65536):
                data.extend(block)
            os.close(reader)
            record.update(retained_bytes=len(data), unchanged=data == b'x' * filled,
                          eof_after_child_exit=True)
            save()
    print(json.dumps(record))


if __name__ == '__main__':
    main()
