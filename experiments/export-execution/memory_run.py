#!/usr/bin/env python3
"""Observe an explicit probe or installed Serve in a fresh dedicated cgroup.

Probe mode consumes one separately accepted job. Installed mode uses a prepared
publication and real HTTP acceptance for five jobs. The root supplies immutable
artifacts and owns engines; neither mode repairs SQL, resets peaks or deletes data.
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


def installed(spec_path, output):
    """Observe ordinary Serve and five real HTTP jobs; no probe phase telemetry."""
    import re
    import threading
    import tomllib
    # Sets only the existing explicit inert helper paths.
    import memory_setup
    from memory_setup_accept import wait_login_window
    from memory_setup_client import Client
    from memory_setup_io import IO, census, pinned

    os.umask(0o077)
    spec = read_json(spec_path)
    require(set(spec) == {'format', 'prepared_root', 'cgroup', 'keys'}
            and spec['format'] == 'bigbrotr-installed-export-memory-run/1', 'installed descriptor')
    keys = spec['keys']
    require(isinstance(keys, list) and len(keys) == len(set(keys)) == 5
            and all(re.fullmatch('[0-9a-f]{32}', key) for key in keys), 'five distinct command keys')
    root, group = Path(spec['prepared_root']), Path(spec['cgroup'])
    require(all(p.is_absolute() and p.resolve() == p for p in (root, group, output)), 'canonical paths')
    prepared = read_json(root / 'prepared.json', 16 * 1024**2)
    setup = read_json(root / 'result.json')
    require(prepared['status'] == setup['status'] == 'PREPARED_FOR_ROOT_ACCEPTANCE', 'known prepared setup')
    inputs = read_json(root / 'input.json')
    require(inputs['root'] == str(root), 'original setup root')
    for key in ('application', 'ingest_probe', 'probe', 'psql', 'recipe', 'source_manifest', 'inventory'):
        pinned(inputs[key], executable=key in ('application', 'ingest_probe', 'probe', 'psql'))
    profiles = read_json(root / 'profiles.json')
    require(profiles == prepared['profiles'], 'original frozen profiles')
    for pin in profiles.values():
        pinned(pin)
    require(profiles['ordinary']['path'] == str(root / 'ordinary.toml'), 'original ordinary profile')
    config = tomllib.loads(pinned(profiles['ordinary']).decode())
    case = read_json(root / 'accept.json')
    require(sha(root / 'accept.json') == setup['accept_sha256'], 'original command/selection descriptor')
    require(config['export']['memory']['max_bytes'] == '256MiB', 'explicit operation allowance')
    require(case['destination'] == str(root / 'destination')
            and all(config['postgres'][name]['tls']['mode'] == 'local-plaintext'
                    for name in ('control', 'catalog'))
            and config['archive']['worker_memory_bytes'] == '512MiB', 'selected plaintext/native profile')
    worker = Path(inputs['worker']['root']) / 'bigbrotr-archive-worker'
    require(worker.stat().st_size == inputs['worker']['bytes']
            and sha(worker) == inputs['worker']['sha256'], 'packaged worker identity')
    own = [line.split(':', 2) for line in Path('/proc/self/cgroup').read_text().splitlines()]
    require(len(own) == 1 and own[0][:2] == ['0', '']
            and (Path('/sys/fs/cgroup') / own[0][2].lstrip('/')).resolve() == group, 'actual observer cgroup')
    before_group = cgroup(group, True)
    require(before_group['pids'] == [os.getpid()] and before_group['limit'] == '268435456',
            'sole observer in actual256MiB cgroup')
    require((group / 'memory.swap.max').read_text().strip() == '0'
            and before_group['events'].get('oom', 0) == before_group['events'].get('oom_kill', 0) == 0,
            'no swap or earlier OOM')
    output.mkdir(mode=0o700)
    io = IO(output, inputs, config)
    io.profiles = profiles
    science = dict(format='bigbrotr-export-memory-science/1',
        event=dict(kind=inputs.get('event_kind', 'frozen-archive-recipe'),
                   **{key: inputs['recipe'][key] for key in ('path', 'sha256')}),
        query=prepared['selection']['query'], catalog=prepared['selection']['catalog'],
        frontier_revision=prepared['selection']['frontier_revision'],
        accepted_revisions=dict(configuration=case['command']['expected_config_revision'],
                                policy=case['command']['expected_policy_revision']))
    save(output / 'science.json', science)
    save(output / 'input.json', dict(spec=spec, descriptor_sha256=sha(spec_path),
        source=inputs, profiles=profiles, cgroup_before=before_group, observer=proc(os.getpid()),
        configured_application_envelope=config['runtime']['processes']['application'],
        configured_native_envelope=config['runtime']['processes']['native'],
        http_acceptance_native_reservation_bytes=536870912,
        http_acceptance_reservation_bytes=622723080,
        accepted_export_cap_bytes=268435456, plaintext_export_reservation_bytes=254541832,
        shared_control_reservation_bytes=554172424,
        scope='Physical observer+Serve+CLI/native children stress cap; PostgreSQL outside. '
              'HTTP acceptance retains its larger general profile;254541832B describes later dispatch only. '
              'Configured whole-service reservations exceed this empirical cap. No deployment sizing or live ledger claim.'))
    stop, failures, seen = threading.Event(), [], {}
    phase = 'before'
    server = None
    observer_image = os.readlink('/proc/self/exe')
    started = time.monotonic_ns()

    def sample():
        try:
            with (output / 'samples.jsonl').open('x') as stream:
                count = 0
                while not stop.is_set():
                    current = cgroup(group)
                    processes = []
                    for pid in current['pids']:
                        value = proc(pid)
                        if value is None:
                            continue
                        direct = value['ppid'] == os.getpid() and value['executable'] in (
                            observer_image, inputs['application']['path'], inputs['psql']['path'])
                        native = server is not None and value['ppid'] == server.child.pid \
                            and value['executable'] in (inputs['application']['path'], str(worker))
                        require(pid == os.getpid() or direct or native, 'unexpected measured process')
                        # A sampled fork can precede exec. Preserve that inherited
                        # image, but only actual worker images count as workers.
                        value['pre_exec_observed'] = (direct and value['executable'] == observer_image) \
                            or (native and value['executable'] == inputs['application']['path'])
                        seen[(pid, value['start_ticks'])] = value
                        processes.append(value)
                    stream.write(json.dumps(dict(elapsed_ns=time.monotonic_ns()-started, phase=phase,
                        processes=processes, cgroup=current, scratch=scratch(config['storage']['scratch_root']))) + '\n')
                    count += 1
                    require(count <= 14000 and stream.tell() <= 64 * 1024**2, 'bounded installed samples')
                    stop.wait(.01)
        except BaseException as error:
            failures.append(repr(error))

    sampler = threading.Thread(target=sample)
    commands, statuses = [], []
    result = dict(status='UNKNOWN_OR_FAILED', error=None, operation_cap_bytes=268435456,
        live_ledger='NOT_OBSERVED', internal_catalog_or_worker_phases='NOT_OBSERVED',
        qualification_256mib=False, five_exports_acceptance=False)
    sampler.start()
    try:
        io.backends('before', absent=True)
        before = io.capture('before')
        require(before == read_json(root / 'granted.json', 16*1024**2), 'original complete granted state')
        require(memory_setup.immutable(root) == prepared['files'], 'original physical roots before dispatch')
        policy, = before['control']['policy_revisions']
        require(policy['document']['settings']['export.memory.max_bytes'] == 268435456,
                'actual accepted policy cap; deployment file alone is insufficient')
        save(output / 'scratch-before.json', census(root / 'scratch'))
        phase = 'login-window'
        wait_login_window(io, before, output)
        phase = 'startup'
        server = io.start('installed-serve', [inputs['application']['path'], 'serve',
            '--file', profiles['ordinary']['path'], '--roles', 'control'])

        def ready():
            require(not failures and server.child.poll() is None, 'Serve/sample startup')
            return server.messages()

        messages = io.wait(ready, 'installed Serve startup')
        require(messages[0]['state'] == 'idle', 'ordinary Serve startup')
        save(output / 'ready.json', messages[0])
        phase = 'login'
        client = Client(io, 'exports', prepared['principal'])
        for index, key in enumerate(keys, 1):
            phase = f'export-{index}'
            command = dict(kind='export', **case['command'])
            save(output / f'command-{index}.json', dict(key=key, command=command))
            receipt = client.create('jobs', command, key)
            commands.append(dict(key=key, command=command, receipt=receipt))

            def completed():
                require(not failures and server.child.poll() is None, 'Serve/sample during export')
                status, value = client.request('GET', '/management/v1/jobs/' + receipt['job_id'])
                require(status == 200, 'actual job observation')
                require(value['state'] not in ('paused', 'failed', 'cancelled'), 'export refused or failed')
                return value if value['state'] == 'succeeded' else None

            statuses.append(io.wait(completed, 'installed signed export completion', seconds=15))
        phase = 'logout'
        client.logout()
        phase = 'shutdown'
        io.stop(server)
        phase = 'after'
        after = io.capture('after')
        require(after['catalog'] == before['catalog'], 'unchanged complete catalog')
        for name in ('store', 'data', 'state'):
            require(census(root / name) == prepared['files'][name], 'unchanged scientific root: ' + name)
        destination_after = census(root / 'destination')
        save(output / 'destination-after.json', destination_after)
        for name, original in prepared['files']['destination'].items():
            actual = destination_after[name]
            fields = original.keys() - {'links'} if name == '.' else original.keys()
            require(all(actual[key] == original[key] for key in fields),
                    'original destination identity/content preserved')
        require(census(root / 'scratch') == read_json(output / 'scratch-before.json', 16 * 1024**2),
                'no retained runtime scratch')
        save(output / 'audit-input.json', dict(selection=prepared['selection'], principal=prepared['principal'],
            commands=commands, statuses=statuses, destination=case['destination'], science=science))
        result.update(status='CAPTURED_INSTALLED_FIVE', server_pid=server.child.pid,
            commands=commands, statuses=statuses, elapsed_ns=time.monotonic_ns()-started)
    except BaseException as error:
        result['error'] = repr(error)
        io.failure_custody()
        raise
    finally:
        stop.set()
        sampler.join(2)
        result.update(sampler_errors=failures, sampler_joined=not sampler.is_alive(),
            cgroup_after=cgroup(group, True), seen_processes=list(seen.values()),
            worker_sha256_after=sha(worker), descriptor_sha256_after=sha(spec_path),
            last_phase=phase, elapsed_ns=time.monotonic_ns()-started, commands=commands, statuses=statuses)
        if failures or sampler.is_alive() or result['cgroup_after']['pids'] != [os.getpid()]:
            result['status'] = 'UNKNOWN_OR_FAILED'
        save(output / 'result.json', result)
    require(result['status'] == 'CAPTURED_INSTALLED_FIVE', 'capture failed/unknown; retain all outputs')
    # Root first authenticates termination and SQL custody, then runs the
    # independent offline oracle. A capture marker alone is never a science PASS.


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('descriptor', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--installed', action='store_true')
    args = parser.parse_args()
    (installed if args.installed else run)(args.descriptor, args.output)
