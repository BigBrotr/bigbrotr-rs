"""Real held/ambiguous COMMIT, migration/fence lock ordering and restart."""
import hashlib
import json
from pathlib import Path
import time
import lab


def cases(f):
    for scenario, mode in [('before', 'before-forward'), ('after', 'after-complete'), ('held', 'hold')]:
        db, path, _, runtime = f.prepare('fault_' + scenario)
        baseline = f.snapshot(db)
        proxy = f.process(['docker', 'exec', '-i', f.fault, 'python3', '-B',
            '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
            '--mode', mode, '--deadline-seconds', '45'], input_pipe=True)
        ready = proxy.event('ready', 8)
        assert ready['proxy_sha256'] == hashlib.sha256((Path(__file__).parent.parent / 'control-migrations/fault_proxy.py').read_bytes()).hexdigest()
        text = (f.root / Path(path).name).read_text()
        proxied = f.root / (scenario + '-proxy.toml')
        proxied.write_text(lab.change(text, 'postgres.control', 'endpoint', f'127.0.0.1:{ready["listen_port"]}'))
        app = f.process(['docker', 'exec', f.fault, '/build/bigbrotr', 'access', 'cursor-keyring',
            'activate-key', '--file', '/lab/' + proxied.name, '--key-id', f.a,
            '--request-id', '41'*16, '--expected-revision', '0'])
        if mode == 'hold':
            held = proxy.event('held', 10)
            f.record('held-transition-uncommitted', held['commits_seen'] == 1 and held['commits_forwarded'] == 0
                     and f.snapshot(db) == baseline, held=held)
            blocked = f.start(runtime, 'fence-blocked-control')
            code = blocked.wait(10)
            # Acquisition reports Busy (exit3) before an owner exists. Unavailable
            # describes an existing owner's lost authority, not this exclusion.
            f.record('transition-fence-refuses-control', code == 3 and not blocked.stdout
                     and bytes(blocked.stderr) == b'error: Control backend operation failed: Busy\n',
                     exit=code, stderr=bytes(blocked.stderr).decode())
            f.change('concurrent-same-pending-refuses', db, path, 'activate', f.a, '41'*16, 0, code=3)
            proxy.send('release')
        code = app.wait(15)
        f.record('commit-response-' + scenario, code == (0 if mode == 'hold' else 4)
                 and (not app.stderr if mode == 'hold' else b'COMMIT outcome unknown' in app.stderr),
                 exit=code, stdout=bytes(app.stdout).decode(), stderr=bytes(app.stderr).decode())
        receipt = proxy.event('receipt', 10)
        expected = {'before': [1,0,0,0,1], 'after': [1,1,1,0,1], 'held': [1,1,1,1,1]}[scenario]
        f.record('commit-boundary-' + scenario,
                 [receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']] == expected
                 and proxy.wait(8) == 0 and not proxy.stderr, observed=receipt)
        f.quiescent(db)
        observed = f.snapshot(db)
        f.record('durable-outcome-' + scenario,
                 observed == baseline if scenario == 'before' else len(observed['cursor_key_transitions']) == 1
                 and len(observed['cursor_key_lifecycle']) == 1, observed=observed)
        f.change('identical-recovery-' + scenario, db, path, 'activate', f.a, '41'*16, 0, created=scenario == 'before')
        running = f.start(runtime, 'after-' + scenario)
        f.idle(running, 'ready-after-' + scenario)
        f.change('historical-after-start-' + scenario, db, path, 'activate', f.a, '41'*16, 0, created=False)
        f.stop(running, 'after-' + scenario)

    # Hold a dataset row before the command. Its Control transaction fence is
    # already held while it waits, so a racing runtime cannot acquire ownership.
    db, path, _, runtime = f.prepare('dataset_wait')
    baseline = f.snapshot(db)
    lock = f.process(['docker','exec','-i',f.state['postgres'],'psql','-X','-A','-t','-q',
                      '-v','ON_ERROR_STOP=1','-U','postgres','-d',db], input_pipe=True)
    lock.process.stdin.write(b"BEGIN;SET LOCAL application_name='lifecycle_dataset_lock';SELECT 1 FROM control.datasets FOR UPDATE;\n")
    lock.process.stdin.flush()
    deadline = time.monotonic()+3
    while b'1\n' not in lock.stdout:
        assert time.monotonic()<deadline
        time.sleep(.01)
    app = f.process(['docker','exec',f.fault,'/build/bigbrotr','access','cursor-keyring','activate-key',
                     '--file',path,'--key-id',f.a,'--request-id','51'*16,'--expected-revision','0'])
    while True:
        waiting = json.loads(lab.sql(f.state, db, "SELECT coalesce(json_agg(json_build_object('pid',pid,'blockers',pg_blocking_pids(pid),'query',query)),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock';"))
        if waiting:
            break
        assert time.monotonic()<deadline
        time.sleep(.01)
    blocked = f.start(runtime, 'dataset-fenced-control')
    code = blocked.wait(8)
    f.record('dataset-wait-retains-control-fence', code==3 and not blocked.stdout
             and bytes(blocked.stderr)==b'error: Control backend operation failed: Busy\n'
             and f.snapshot(db)==baseline and all(row['blockers'] for row in waiting), observed=waiting,
             exit=code, stderr=bytes(blocked.stderr).decode())
    lock.process.stdin.write(b'ROLLBACK;\n')
    lock.process.stdin.close()
    lock.process.stdin=None
    assert lock.wait(8)==0 and not lock.stderr
    f.record('dataset-wait-completes', app.wait(10)==0 and not app.stderr, stdout=bytes(app.stdout).decode())
    f.change('dataset-wait-retry',db,path,'activate',f.a,'51'*16,0,created=False)

    before = f.snapshot(db)
    lab.docker('kill','--signal','KILL',f.state['postgres'])
    lab.docker('start',f.state['postgres'])
    deadline=time.monotonic()+30
    while lab.docker('exec',f.state['postgres'],'pg_isready','-h','127.0.0.1','-U','postgres',check=False).returncode:
        assert time.monotonic()<deadline
        time.sleep(.1)
    # A container sharing the old PG namespace need not follow a recreated one.
    # Use the existing independent bridge client and its actual DNS endpoint.
    f.client = f.state
    for config in [path, runtime]:
        local = f.root / Path(config).name
        local.write_text(lab.change(lab.change(local.read_text(), 'postgres.control', 'endpoint', 'postgres:5432'),
                                    'postgres.control.tls', 'local_boundary', 'private-bridge'))
    f.change('engine-restart-original-receipt',db,path,'activate',f.a,'51'*16,0,created=False)
    f.record('engine-restart-preserves-state',f.snapshot(db)==before)
    running=f.start(runtime,'engine-restart',container=f.state['client'])
    f.idle(running,'engine-restart-ready')
    f.stop(running,'engine-restart',container=f.state['client'])
