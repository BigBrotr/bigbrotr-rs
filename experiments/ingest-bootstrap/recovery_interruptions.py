"""Actual pre-claim SQL wait: signal/deadline must join without durable effects."""
import json
from pathlib import Path
import subprocess
import time
import lab
from installed import wait_for, terminal


def run(pg, client, output, rows, prepare, dump):
    results = []
    for mode in ('signal','deadline'):
        name = 'recovery_'+mode
        db, config = prepare(name)
        before, native = rows(db), dump(name+'-before', name)
        lab.write_json(output/(name+'-before.json'),before)
        lock_out = output/(name+'-lock.stdout')
        lock_err = output/(name+'-lock.stderr')
        with lock_out.open('x') as out, lock_err.open('x') as err:
            locker = subprocess.Popen(['docker','exec','-i',pg['postgres'],'psql','-X','-A','-t','-q',
                '--no-password','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],
                stdin=subprocess.PIPE, stdout=out, stderr=err, text=True)
            try:
                locker.stdin.write('BEGIN; LOCK TABLE control.datasets IN ACCESS EXCLUSIVE MODE; SELECT pg_backend_pid();\n')
                locker.stdin.flush()
                wait_for(lambda: lock_out.read_text().strip(), 'recovery-lock-acquired', 5)
                holder = int(lock_out.read_text().strip())
                prefix = output/name
                lab.docker('exec','-d',client,'python3','/lab/installed_client.py',config,'/lab/'+name,'recovery')
                wait_for(lambda: Path(str(prefix)+'.pid.json').exists(), 'recovery-pid', 5)
                def blocked():
                    value = json.loads(lab.sql(pg,db,f"""
                        SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,
                            'wait_event',wait_event,'query',query)),'[]') FROM pg_stat_activity
                        WHERE datname=current_database() AND usename='bigbrotr_control'
                        AND wait_event_type='Lock' AND {holder}=ANY(pg_blocking_pids(pid))
                    """))
                    return value or None
                observed = wait_for(blocked, 'actual-recovery-query-lock', 1)
                assert len(observed)==1 and 'control.datasets' in observed[0]['query'], observed
                lab.write_json(output/(name+'-blocked.json'),observed)
                measured = time.monotonic()
                if mode=='signal':
                    identity = json.loads(Path(str(prefix)+'.pid.json').read_text())
                    script = '''import os,signal,json
from pathlib import Path
r=json.loads(%r)
p=Path('/proc')/str(r['pid'])
assert p.joinpath('stat').read_text().split()[21]==r['start_ticks']
assert p.joinpath('cmdline').read_bytes().split(b'\\0')[:5]==[b'/build/bigbrotr',b'ingest',b'recover',b'--file',r['config'].encode()]
os.kill(r['pid'],signal.SIGTERM)
''' % json.dumps(identity)
                    lab.docker('exec',client,'python3','-c',script)
                ended = terminal(prefix)
                elapsed = time.monotonic()-measured
                assert ended['exit_code']==4 and elapsed<6 and not ended['forced'], ended
                assert not Path(str(prefix)+'.stdout').read_bytes()
                error = Path(str(prefix)+'.stderr').read_text()
                assert ('cancelled' in error if mode=='signal' else 'deadline' in error or 'expired' in error), error
                # Record server lifetime separately from joining the Rust driver.
                # A closed client socket is not a synchronous PG query cancel.
                remaining = lab.sql(pg,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")
            finally:
                if locker.poll() is None:
                    locker.stdin.write('ROLLBACK;\n\\q\n')
                    locker.stdin.flush()
                locker.communicate(timeout=5)
                assert locker.returncode==0 and not lock_err.read_text()
        wait_for(lambda: lab.sql(pg,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0', 'recovery-backend-exit',5)
        after = rows(db)
        lab.write_json(output/(name+'-after.json'),after)
        assert after == before and dump(name+'-after',name)==native
        results.append(dict(name=name,status='PASS',actual_backend_lock=True,unchanged_control_tables=len(before),
            native_rows=len(native['rows']),terminal=ended,seconds_after_observed_block=elapsed,
            application_backends_before_fixture_unlock=int(remaining)))
    return results
