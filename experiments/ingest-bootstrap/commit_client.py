#!/usr/bin/env python3
"""Own the installed CLI and finite PG proxy; retain both actual terminations."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

sys.path.insert(0, '/source/experiments/control-migrations')
from fault_cases import Process, safe_event

config, prefix = map(Path, sys.argv[1:3])
mode, number, action = sys.argv[3], int(sys.argv[4]), sys.argv[5]
assert config.is_absolute() and prefix.is_absolute()
proxy = child = None
pidfd = None
result = dict(status='INCOMPLETE', mode=mode, commit_number=number, action=action)


def publish(suffix, value):
    target = Path(str(prefix)+suffix)
    partial = Path(str(target)+'.partial')
    assert not target.exists()
    with partial.open('x') as stream:
        stream.write(json.dumps(value)+'\n')
    os.replace(partial, target)


def wait_for(condition, label, seconds):
    end = time.monotonic()+seconds
    while not condition():
        assert time.monotonic()<end, label
        time.sleep(.01)


def initialized():
    path = Path(str(prefix)+'.stdout')
    messages = [json.loads(s) for s in path.read_text().splitlines(keepends=True) if s.endswith('\n')]
    assert child.poll() is None, 'CLI ended before initialization'
    return any(m.get('state')=='initialized' for m in messages)


def stop():
    assert child.poll() is None
    result['signal_monotonic'] = time.monotonic()
    signal.pidfd_send_signal(pidfd, signal.SIGTERM)


try:
    proxy = Process(['python3', '-B', '/source/experiments/control-migrations/fault_proxy.py',
        '--upstream-port', '5432', '--mode', mode, '--commit-number', str(number),
        '--later-commits', '16', '--deadline-seconds', '30'], input_pipe=True)
    ready = proxy.event('ready', 3)
    publish('.proxy-ready.json', ready)
    wait_for(lambda: Path(str(prefix)+'.config-ready').exists(), 'config barrier', 10)
    with Path(str(prefix)+'.stdout').open('xb') as out, Path(str(prefix)+'.stderr').open('xb') as err:
        result['launch_monotonic'] = time.monotonic()
        child = subprocess.Popen(['/build/bigbrotr', 'serve', '--file', str(config),
            '--initialize-ingest', '--roles', 'control,ingest'], stdout=out, stderr=err, env={})
        pidfd = os.pidfd_open(child.pid)
        result['identity'] = dict(pid=child.pid,
            start_ticks=Path(f'/proc/{child.pid}/stat').read_text().split()[21], config=str(config))
        if mode == 'hold':
            held = proxy.event('held', 6)
            result['held_monotonic'] = time.monotonic()
            publish('.held.json', held)
            wait_for(lambda: Path(str(prefix)+'.observed').exists(), 'SQL hold observation', 3)
            if action == 'release':
                time.sleep(.5)
                proxy.send('release')
                result['released'] = proxy.event('released', 1)
                wait_for(initialized, 'delayed bootstrap initialization', 5)
                stop()
            else:
                stop()
                child.wait(timeout=3.5)
                result['cli_death_monotonic'] = time.monotonic()
                # Every application phase starts after Popen. Death before this
                # conservative lower bound rules out the ordinary 5s watchdog.
                assert result['cli_death_monotonic'] < result['launch_monotonic']+5
                # The client is dead but the proxy still owns its upstream
                # transaction. Let the host observe that distinction directly.
                publish('.client-dead.json', dict(result, exit_code=child.returncode))
                wait_for(lambda: Path(str(prefix)+'.upstream-observed').exists(), 'upstream observation', 2)
                proxy.send('close')
        elif mode == 'pass':
            wait_for(initialized, 'healthy initialization', 8)
            stop()
        child.wait(timeout=8)
        result.setdefault('cli_death_monotonic', time.monotonic())
        result['exit_code'] = child.returncode
        assert child.returncode == (0 if mode=='pass' or action=='release' else 4)
    receipt = proxy.event('receipt', 4)
    result['proxy_exit'] = proxy.wait(3)
    assert result['proxy_exit']==0 and not proxy.stderr
    result['receipt'] = receipt
    if mode=='pass':
        assert receipt['faults']==0 and receipt['commits_seen']>=7
        assert receipt['commits_seen']==receipt['commits_forwarded']==receipt['server_commit_complete']==receipt['client_commit_complete']
    else:
        assert receipt['faults']==1
        if action=='release':
            assert receipt['hold_action']=='release' and receipt['commits_seen']>=7
            assert receipt['commits_seen']==receipt['server_commit_complete']==receipt['client_commit_complete']
        else:
            assert receipt['commits_seen']==number and receipt['client_commit_complete']==number-1
            complete = number if mode=='after-complete' else number-1
            assert receipt['commits_forwarded']==receipt['server_commit_complete']==complete
    result['status'] = 'PASS'
except BaseException as error:
    result.update(status='FAIL', failure=type(error).__name__+': '+str(error))
    raise
finally:
    forced = False
    cleanup_errors = []

    def cleanup(label, operation):
        try:
            operation()
        except BaseException as error:
            cleanup_errors.append(label+': '+type(error).__name__)

    if child is not None and child.poll() is None:
        forced = True
        cleanup('CLI kill', lambda: signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                if pidfd is not None else child.kill())
        cleanup('CLI reap', lambda: child.wait(timeout=3))
    if pidfd is not None:
        cleanup('pidfd close', lambda: os.close(pidfd))
    if proxy is not None:
        forced = forced or proxy.process.poll() is None
        cleanup('proxy stop', proxy.stop)
        for suffix, data in [('.proxy.stdout', proxy.stdout), ('.proxy.stderr', proxy.stderr)]:
            def capture(suffix=suffix, data=data):
                with Path(str(prefix)+suffix).open('xb') as stream:
                    stream.write(data)
            cleanup('capture '+suffix, capture)
        # No payload enters proxy metadata. A malformed transcript fails this
        # case but cannot bypass the other cleanup or terminal-write attempts.
        def validate_metadata():
            for line in proxy.stdout.splitlines():
                safe_event(json.loads(line))
        cleanup('proxy metadata', validate_metadata)
    result['cleanup'] = dict(forced=forced, errors=cleanup_errors,
        cli_reaped=child is not None and child.returncode is not None,
        proxy_reaped=proxy is not None and proxy.process.returncode is not None)
    if cleanup_errors or forced or not result['cleanup']['cli_reaped'] or not result['cleanup']['proxy_reaped']:
        result['status'] = 'INCOMPLETE_CLEANUP'
    publish('.terminal.json', result)
