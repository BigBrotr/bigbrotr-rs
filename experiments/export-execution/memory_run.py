#!/usr/bin/env python3
"""One fresh dedicated cgroup, one real measure process; no setup or SQL repair.

The root executor supplies a separately provisioned accepted job and immutable
packaged worker. This observer never imports an engine runner, creates a catalog,
accepts a job, resets a cgroup peak, or deletes failed output.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import time


def require(ok, message):
    if not ok:
        raise ValueError(message)


def read_json(path, maximum=65536):
    raw = Path(path).read_bytes()
    require(len(raw) <= maximum, 'JSON input bound')
    def pairs(values):
        out = {}
        for key, value in values:
            require(key not in out, 'duplicate JSON field')
            out[key] = value
        return out
    return json.loads(raw, object_pairs_hook=pairs)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(65536), b''):
            digest.update(block)
    return digest.hexdigest()


def proc(pid):
    root = Path('/proc') / str(pid)
    try:
        text = (root / 'stat').read_text()
        fields = text[text.rindex(')') + 2:].split()
        status = {}
        for line in (root / 'status').read_text().splitlines():
            key, _, value = line.partition(':')
            if key in ('VmRSS', 'VmHWM', 'RssAnon', 'RssFile', 'RssShmem', 'VmSwap'):
                number, unit = value.split()
                require(unit == 'kB', 'unexpected proc memory unit')
                status[key] = int(number) * 1024
        return dict(pid=pid, start_ticks=int(fields[19]), ppid=int(fields[1]),
                    state=fields[0], memory=status,
                    executable=os.readlink(root / 'exe'))
    except FileNotFoundError:
        return None


def numbers(path):
    return {key: int(value) for key, value in
            (line.split() for line in path.read_text().splitlines())}


def cgroup(root, full=False):
    values = dict(current=int((root / 'memory.current').read_text()),
                  peak=int((root / 'memory.peak').read_text()),
                  limit=(root / 'memory.max').read_text().strip(),
                  events=numbers(root / 'memory.events'),
                  pids=sorted(int(x) for x in (root / 'cgroup.procs').read_text().split()))
    stats = numbers(root / 'memory.stat')
    values['stat'] = stats if full else {key: stats[key] for key in
                                      ('anon', 'file', 'kernel', 'sock', 'shmem', 'file_dirty', 'file_writeback')}
    return values


def scratch(path):
    logical = allocated = files = entries = missing = 0
    errors = []
    for root, directories, names in os.walk(path, followlinks=False, onerror=lambda error: errors.append(repr(error))):
        for name in directories + names:
            entries += 1
            require(entries <= 8192, 'scratch entry count bound')
            try:
                value = os.lstat(Path(root) / name)
            except FileNotFoundError:
                missing += 1  # Native spill creation/removal races this sample.
                continue
            require(not stat.S_ISLNK(value.st_mode), 'scratch symlink')
            require(stat.S_ISREG(value.st_mode) or stat.S_ISDIR(value.st_mode), 'scratch special file')
            if stat.S_ISREG(value.st_mode):
                files += 1
                require(files <= 4096, 'scratch file count bound')
                logical += value.st_size
                allocated += value.st_blocks * 512
    return dict(files=files, logical_bytes=logical, allocated_bytes=allocated,
                disappeared_during_sample=missing, directory_errors=errors)


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def run(spec_path, output):
    spec = read_json(spec_path)
    require(set(spec) == {'format', 'probe', 'probe_sha256', 'case', 'case_sha256',
                          'cgroup', 'science', 'science_sha256'}, 'closed run descriptor')
    require(spec['format'] == 'bigbrotr-export-memory-run/1', 'run format')
    probe, case_path, group = (Path(spec[key]) for key in ('probe', 'case', 'cgroup'))
    require(all(p.is_absolute() for p in (probe, case_path, group, output)), 'absolute paths')
    require(sha(probe) == spec['probe_sha256'] and sha(case_path) == spec['case_sha256'], 'exact caller inputs')
    require(sha(spec['science']) == spec['science_sha256'], 'independent science descriptor')
    case = read_json(case_path)
    require(case['limits']['row_bytes'] == 1048576, 'first baseline retains full1MiB event envelope')
    require(case['format'] == 'bigbrotr-export-measure/1', 'case format')
    worker = Path(case['worker_root']) / 'bigbrotr-archive-worker'
    require(worker.stat().st_size == case['worker_bytes'] and sha(worker) == case['worker_sha256'], 'unchanged worker identity')
    require(not Path(case['ledger']).exists(), 'fresh ledger')
    # Each measurement uses a fresh dedicated cgroup. No resets/subtraction of an
    # old peak and no unrelated PostgreSQL/host process memory in this total.
    require(group.resolve() == group and group.is_dir(), 'canonical cgroup directory')
    own = [line.split(':', 2) for line in Path('/proc/self/cgroup').read_text().splitlines()]
    require(len(own) == 1 and own[0][:2] == ['0', ''], 'cgroup v2 required')
    require((Path('/sys/fs/cgroup') / own[0][2].lstrip('/')).resolve() == group, 'actual observer cgroup')
    before = cgroup(group, True)
    require(before['pids'] == [os.getpid()], 'fresh dedicated cgroup has only observer')
    require(before['limit'].isdigit(), 'finite actual cgroup memory.max required')
    require(case['limits']['memory_max_bytes'] <= int(before['limit']), 'declared envelope exceeds actual cgroup cap')
    require(before['events'].get('oom', 0) == before['events'].get('oom_kill', 0) == 0, 'no prior OOM')
    output.mkdir(mode=0o700)
    save(output / 'input.json', dict(spec=spec, case=case, observer=proc(os.getpid()),
                                  cgroup_before=before, scratch_before=scratch(case['scratch'])))
    result = dict(status='UNKNOWN', command=[str(probe), 'measure', str(case_path)],
                  forced=False, reaped=False, cleanup_errors=[],
                  environment={'LC_ALL': 'C'},
                  cgroup_freshness='external-root-receipt-required',
                  scope='observer+probe+worker; configured grants are separate')
    process = None
    seen = {}
    started = time.monotonic_ns()
    failure = None
    try:
        with (output / 'stdout').open('xb') as stdout, (output / 'stderr').open('xb') as stderr, (output / 'samples.jsonl').open('x') as samples:
            process = subprocess.Popen(result['command'], stdin=subprocess.DEVNULL, stdout=stdout,
                                       stderr=stderr, start_new_session=True, env=result['environment'])
            result['pid'] = process.pid
            while True:
                elapsed = (time.monotonic_ns() - started) / 1e9
                observed = cgroup(group)
                processes = []
                for pid in observed['pids']:
                    value = proc(pid)
                    if value is not None:
                        require(pid in (os.getpid(), process.pid) or
                                (value['ppid'] == process.pid and value['executable'] == str(worker)),
                                'unrelated process entered measured cgroup')
                        key = (pid, value['start_ticks'])
                        seen[key] = value
                        processes.append(value)
                samples.write(json.dumps(dict(elapsed_ns=time.monotonic_ns() - started,
                    processes=processes, cgroup=observed, scratch=scratch(case['scratch']))) + '\n')
                code = process.poll()
                if code is not None:
                    result.update(returncode=code, reaped=True)
                    break
                require(elapsed <= 120, 'whole-call observation timeout')
                require(stdout.tell() < 1048576 and stderr.tell() < 1048576, 'bounded caller output')
                time.sleep(.01)
    except BaseException as error:
        failure = repr(error)
    finally:
        try:
            if process is not None and process.poll() is None:
                result['forced'] = True
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                result.update(returncode=process.returncode, reaped=True)
        except BaseException as error:
            result['cleanup_errors'].append(repr(error))
        result['elapsed_ns'] = time.monotonic_ns() - started
        result['error'] = failure
        for name, call in [('cgroup_after', lambda: cgroup(group, True)),
                           ('scratch_after', lambda: scratch(case['scratch'])),
                           ('worker_sha256_after', lambda: sha(worker)),
                           ('case_sha256_after', lambda: sha(case_path))]:
            try:
                result[name] = call()
            except BaseException as error:
                result['cleanup_errors'].append(name + ': ' + repr(error))
        result['seen_processes'] = list(seen.values())
        # Current process absence, not a guess from a result marker or PID name.
        result['descendants_absent'] = result.get('cgroup_after', {}).get('pids') == [os.getpid()]
        result['status'] = 'CAPTURED' if (failure is None and result.get('returncode') == 0
            and result['reaped'] and not result['forced'] and result['descendants_absent']
            and not result['cleanup_errors']) else 'UNKNOWN_OR_FAILED'
        save(output / 'result.json', result)
    require(result['status'] == 'CAPTURED', 'failed/unknown capture preserved; no science PASS')
    # Scientific verification is deliberately a separate post-custody command.


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('descriptor', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    run(args.descriptor, args.output)
