#!/usr/bin/env python3
"""Real CLI startup inventory against isolated PostgreSQL; no catalog recovery claim."""
import argparse
import hashlib
import json
from pathlib import Path
import select
import re
import subprocess
import sys
import time
sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
from lab import change


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-name', default='inventory')
    parser.add_argument('--artifact-revision')
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    require(re.fullmatch('[a-z][a-z0-9_]{1,24}', args.run_name), 'run name')
    prefix = args.run_name
    state = json.loads((root / 'resources.json').read_text())
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    require(ready['source'] == state['source'] and ready['status'] == 'READY', 'build identity')
    revision = subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip()
    require((args.artifact_revision or revision) == state['source'], 'artifact revision')
    if revision != state['source']:
        changes = subprocess.check_output(['git', '-C', str(SOURCE), 'diff', '--name-only', state['source'], revision], text=True).splitlines()
        require(changes and all(p.startswith('docs/') or p in ['experiments/control-runtime/archive_inventory.py', 'experiments/control-runtime/README.md'] for p in changes), 'non-harness artifact drift')
    require(not subprocess.check_output(['git', '-C', str(SOURCE), 'status', '--porcelain']), 'dirty harness')
    for key in ['container_id', 'postgres_id']:
        data = json.loads(subprocess.check_output(['docker', 'inspect', state[key]]))[0]
        require(data['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and data['State']['Running'], 'resource ownership')
    report_path = root / (prefix + '.json')
    require(not report_path.exists(), 'existing report')
    directory = root / 'out' / prefix
    directory.mkdir(mode=0o700)
    report = dict(format='bigbrotr-archive-inventory-acceptance/1', source=state['source'],
                  harness_revision=revision, artifact=ready['binary_sha256'], status='RUNNING', cases=[], processes=[], observations={})
    processes = []

    def save():
        report_path.write_text(json.dumps(report, indent=2) + '\n')

    def passed(name, **details):
        report['cases'].append(dict(name=name, status='PASS', **details))
        save()

    def sql(database, statement):
        value = subprocess.run(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database], input=statement, text=True, capture_output=True, timeout=15)
        require(value.returncode == 0, 'fixture SQL failed')
        return value.stdout.strip()

    def cli(*args):
        return subprocess.run(['docker', 'exec', state['container_id'], '/out/native-tests/bigbrotr', *args], text=True, capture_output=True, timeout=35)

    def prepare(database, target=9, bind=True):
        sql('postgres', f'CREATE DATABASE {database};')
        for suffix in ['data', 'state']:
            (directory / (database + '-' + suffix)).mkdir(mode=0o700)
        text = (SOURCE / 'config/recipes/archive-only.toml').read_text()
        for section, key, value in [
            ('administration', 'enabled', False), ('topology', 'dataset_id', '01' * 16),
            ('topology', 'instance_id', '02' * 16), ('archive', 'catalog_id', '5a' * 16),
            ('postgres.control', 'endpoint', '127.0.0.1:5432'),
            ('postgres.control', 'database', database),
            ('postgres.control', 'password_ref', 'file:/secrets/bigbrotr_control'),
            ('storage', 'archive_root', '/out/' + prefix + '/' + database + '-data'),
            ('storage', 'archive_state_root', '/out/' + prefix + '/' + database + '-state'),
        ]:
            text = change(text, section, key, value)
        candidate = directory / (database + '.toml')
        candidate.write_text(text)
        candidate.chmod(0o600)
        path = '/out/' + prefix + '/' + candidate.name
        result = cli('migrate', 'apply', '--file', path, '--target', str(target), '--user', 'postgres', '--password-ref', 'file:/secrets/postgres')
        require(result.returncode == 0, 'explicit migration: ' + result.stderr)
        sql(database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        for command in [('config', 'initialize', '--file', path), ('config', 'activate-initial', '--file', path, '--request-id', '44' * 16, '--expected-revision', '1')]:
            result = cli(*command)
            require(result.returncode == 0, 'explicit initialization/activation: ' + result.stderr)
        subprocess.run(['docker', 'exec', state['container_id'], '/out/native-tests/archive-coordinator-probe', 'reserve', '/out/' + prefix + '/' + database + '-data'], check=True, capture_output=True, timeout=10)
        if bind:
            result = cli('archive', 'state-initialize', '--file', path)
            require(result.returncode == 0, 'coordinator state binding: ' + result.stderr)
        return path

    def start(path, label, binary='/out/native-tests/bigbrotr'):
        stdout = (directory / (label + '.stdout')).open('w')
        stderr = (directory / (label + '.stderr')).open('w')
        process = subprocess.Popen(['docker', 'exec', state['container_id'], binary, 'serve', '--file', path, '--roles', 'control'], stdout=stdout, stderr=stderr)
        record = dict(label=label, config=path, binary=binary, state='starting')
        report['processes'].append(record)
        processes.append((process, record, stdout, stderr))
        save()
        return processes[-1]

    def output(record):
        return ((directory / (record['label'] + '.stdout')).read_text(), (directory / (record['label'] + '.stderr')).read_text())

    def idle(item):
        process, record, _, _ = item
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            out, err = output(record)
            if out:
                value = json.loads(out.splitlines()[0])
                require(value['state'] == 'idle' and value['listeners'] == [] and value['native_stores'] == [] and not err, 'idle state')
                record['state'] = 'idle'
                record['initial'] = value
                save()
                return value
            require(process.poll() is None, 'early exit: ' + err)
            time.sleep(.03)
        raise RuntimeError('idle timeout')

    def signal(item, name='TERM'):
        # Identify the real product process, never signal the docker exec client.
        process, record, _, _ = item
        program = """import os,sys,signal,json
from pathlib import Path
expected=[sys.argv[2].encode(),b'serve',b'--file',sys.argv[1].encode(),b'--roles',b'control',b'']; found=[]
for p in Path('/proc').iterdir():
 if p.name.isdigit():
  try:
   if (p/'cmdline').read_bytes().split(b'\\0')==expected: found.append(int(p.name))
  except (FileNotFoundError,PermissionError,ProcessLookupError): pass
assert len(found)==1,found
pid=found[0]; proc=Path('/proc')/str(pid)
start=(proc/'stat').read_text().rsplit(')',1)[1].split()[19]
fd=os.pidfd_open(pid)
try:
 assert (proc/'cmdline').read_bytes().split(b'\\0')==expected
 assert (proc/'stat').read_text().rsplit(')',1)[1].split()[19]==start
 actual=(proc/'exe').stat(); wanted=Path(sys.argv[2]).stat()
 assert (actual.st_dev,actual.st_ino)==(wanted.st_dev,wanted.st_ino)
 signal.pidfd_send_signal(fd,getattr(signal,'SIG'+sys.argv[3]))
finally: os.close(fd)
print(json.dumps({'pid':pid,'start_ticks':start,'signal':sys.argv[3],'pidfd':True}))
"""
        result = subprocess.check_output(['docker', 'exec', state['container_id'], 'python3', '-c', program, record['config'], record['binary'], name], text=True)
        record['signal'] = json.loads(result)
        save()

    def terminal(item, code):
        process, record, stdout, stderr = item
        actual = process.wait(timeout=8)
        stdout.close()
        stderr.close()
        require(actual == code, f'exit {actual}, expected {code}: {output(record)[1]}')
        record.update(state='terminal', exit_code=actual)
        save()
        return output(record)

    def no_clients(database):
        require(sql(database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';") == '0', 'retained backend')

    def stop(item, database):
        signal(item)
        out, err = terminal(item, 0)
        require(not err and [json.loads(v)['state'] for v in out.splitlines()] == ['idle', 'stopped'], 'release output')
        require(json.loads(out.splitlines()[-1])['release'] == 'committed', 'release unknown')
        no_clients(database)

    def expect(value, count, pages):
        actual = value.get('archive_intents')
        return actual == dict(state='observed', catalog_id='5a' * 16, issued_through=str(count), observed_through=str(count), pages=pages, resolution='not-performed', publication_authority='not-granted')

    def history(database):
        return sql(database, "SELECT coalesce(json_agg(t ORDER BY ordinal),'[]')::text FROM (SELECT a.*,a.xmin::text FROM control.archive_attempts a)t;")

    save()
    try:
        empty = prepare(prefix + '_empty', bind=False)
        # The old schema does not recognize the new deployment root. Omit only
        # that key in its separate negative-control file; accepted policy is
        # identical because all storage paths are deployment roots.
        original_path = directory / (prefix + '_empty.toml')
        baseline_path = directory / 'baseline.toml'
        baseline_path.write_text('\n'.join(line for line in original_path.read_text().splitlines() if not line.startswith('archive_state_root =')) + '\n')
        baseline = start('/out/' + prefix + '/baseline.toml', 'baseline', '/baseline/bigbrotr')
        value = idle(baseline)
        require(not expect(value, 0, 0), 'old executable unexpectedly implements inventory')
        stop(baseline, prefix + '_empty')
        passed('negative-control-old-runtime-misses-inventory', baseline_source=json.loads(subprocess.check_output(['docker', 'exec', state['container_id'], '/baseline/bigbrotr', 'build-info']))['source']['revision'])
        unbound = start(empty, 'unbound')
        out, err = terminal(unbound, 4)
        require(not out and 'Incomplete' in err, 'ordinary startup silently initialized coordinator')
        state_dir = directory / (prefix + '_empty-state')
        anchor = directory / (prefix + '_empty-data') / '.bigbrotr-initialization' / 'coordinator.json'
        require(not anchor.exists() and not list(state_dir.iterdir()), 'unbound startup mutated state')
        passed('missing-binding-refused-without-creation')
        for label in ['first', 'retry']:
            result = cli('archive', 'state-initialize', '--file', empty)
            require(result.returncode == 0 and json.loads(result.stdout)['publication_authority'] == 'not-granted', 'explicit binding')
            require(anchor.read_bytes() == (state_dir / 'coordinator.json').read_bytes(), 'two-ended binding differs')
        retained = anchor.read_bytes()
        passed('explicit-binding-and-identical-retry')
        # The runner mounts this same /out tree again at /out-alias. Unlike a
        # symlink, canonicalization preserves those different mountpoint names.
        (state_dir / 'child').mkdir(mode=0o700)
        for suffix in ['', '/child']:
            alias = directory / ('alias' + ('-child' if suffix else '') + '.toml')
            alias.write_text(change(original_path.read_text(), 'storage', 'scratch_root', '/out-alias/' + prefix + '/' + prefix + '_empty-state' + suffix))
            result = cli('archive', 'state-initialize', '--file', '/out/' + prefix + '/' + alias.name)
            require(result.returncode == 4 and 'aliases another selected writable root' in result.stderr, 'physical bind alias accepted')
            require(anchor.read_bytes() == retained, 'alias refusal changed binding')
        (state_dir / 'child').rmdir()
        passed('actual-bind-mount-alias-and-nested-alias-refused')
        current = start(empty, 'empty')
        require(expect(idle(current), 0, 0), 'empty inventory')
        contender = start(empty, 'coordinator-contender')
        out, err = terminal(contender, 4)
        require(not out and 'Occupied' in err, 'coordinator lock not enforced')
        result = cli('archive', 'state-initialize', '--file', empty)
        require(result.returncode == 4 and 'Occupied' in result.stderr, 'binding raced live coordinator')
        passed('live-coordinator-refuses-process-and-initializer')
        stop(current, prefix + '_empty')
        passed('empty-history-distinct-from-unresolved')
        other = directory / 'other-state'
        other.mkdir(mode=0o700)
        changed = directory / 'other.toml'
        changed.write_text(change(original_path.read_text(), 'storage', 'archive_state_root', '/out/' + prefix + '/other-state'))
        for command in [('archive', 'state-initialize'), ('serve', '--roles', 'control')]:
            result = cli(*command, '--file', '/out/' + prefix + '/other.toml')
            require(result.returncode == 4 and 'Identity' in result.stderr, 'substituted root accepted')
        require(not list(other.iterdir()) and anchor.read_bytes() == retained, 'substitute root mutated state')
        passed('substituted-state-root-refused-without-mutation')
        anchor.unlink() # Isolated fixture crash between durable state and reverse binding.
        result = cli('serve', '--roles', 'control', '--file', empty)
        require(result.returncode == 4 and 'Incomplete' in result.stderr and not anchor.exists(), 'runtime repaired half binding')
        result = cli('archive', 'state-initialize', '--file', empty)
        require(result.returncode == 0 and anchor.read_bytes() == retained, 'explicit complete-half retry')
        passed('complete-first-half-needs-explicit-retry')
        anchor.write_bytes(b'partial')
        for command in [('archive', 'state-initialize'), ('serve', '--roles', 'control')]:
            result = cli(*command, '--file', empty)
            require(result.returncode == 4 and 'Identity' in result.stderr and anchor.read_bytes() == b'partial', 'partial binding repaired')
        anchor.write_bytes(retained) # Restore only deliberately corrupted owned fixture.
        passed('partial-bytes-preserved-and-refused')
        path = prepare(prefix + '_full')
        seeded = subprocess.run(['docker', 'exec', state['container_id'], '/out/native-tests/archive-attempt-probe', '--seed-runtime', prefix + '_full'], text=True, capture_output=True, timeout=60)
        (directory / 'seed.stdout').write_text(seeded.stdout)
        (directory / 'seed.stderr').write_text(seeded.stderr)
        require(seeded.returncode == 0 and json.loads(seeded.stdout)['rows'] == 130, 'actual adapter seed')
        original = history(prefix + '_full')
        rows = json.loads(original)
        require(len(rows) == 130 and [r['ordinal'] for r in rows] == list(range(1, 131)), 'independent seeded membership')
        require([r['attempt_id'] for r in rows] == ['\\x' + f'{n:02x}' * 16 for n in range(100, 230)], 'independent seeded identities')
        for label in ['populated', 'restart']:
            item = start(path, label)
            require(expect(idle(item), 130, 3), 'complete inventory')
            stop(item, prefix + '_full')
            require(history(prefix + '_full') == original, 'inventory changed immutable rows')
            passed(label + '-complete-history', rows=130, pages=3)
        # Corrupt only old metadata; a latest-only lookup would falsely pass.
        sql(prefix + '_full', "BEGIN; ALTER TABLE control.archive_attempts DISABLE TRIGGER USER; UPDATE control.archive_attempts SET catalog_id=decode(repeat('5b',16),'hex') WHERE ordinal=65; ALTER TABLE control.archive_attempts ENABLE TRIGGER USER; COMMIT;")
        corrupted = history(prefix + '_full')
        item = start(path, 'historical-mismatch')
        out, err = terminal(item, 3)
        require(not out and 'archive intent inventory refused: Conflict' in err, 'historical mismatch not refused')
        require(history(prefix + '_full') == corrupted, 'refusal mutated history')
        no_clients(prefix + '_full')
        sql(prefix + '_full', "BEGIN; ALTER TABLE control.archive_attempts DISABLE TRIGGER USER; UPDATE control.archive_attempts SET catalog_id=decode(repeat('5a',16),'hex') WHERE ordinal=65; ALTER TABLE control.archive_attempts ENABLE TRIGGER USER; COMMIT;")
        passed('historical-mismatch-before-idle')
        # Hold a real relation lock while the actual startup scan runs, then
        # interrupt it. The blocker is an isolated owned psql transaction.
        blocker = subprocess.Popen(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', prefix + '_full'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            blocker.stdin.write("BEGIN; LOCK TABLE control.archive_attempts IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
            blocker.stdin.flush()
            require(select.select([blocker.stdout], [], [], 5)[0] and blocker.stdout.readline().strip() == 'locked', 'blocker acquisition')
            item = start(path, 'cancelled-scan')
            deadline = time.monotonic() + 2
            observed = None
            while time.monotonic() < deadline:
                data = sql(prefix + '_full', "SELECT json_agg(json_build_object('pid',pid,'backend_start',backend_start,'wait_event',wait_event,'blockers',pg_blocking_pids(pid))) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock' AND query LIKE '%control.archive_attempts%';")
                observed = json.loads(data) if data else None
                if observed:
                    break
                time.sleep(.01)
            require(observed and len(observed) == 1 and observed[0]['blockers'], 'scan not actually blocked')
            started = time.monotonic()
            signal(item)
            out, err = terminal(item, 4)
            elapsed = time.monotonic() - started
            require(not out and 'archive intent inventory cancelled' in err and elapsed < 5, 'bounded cancelled scan')
            # Local driver destruction is not remote query termination. With
            # the fixture's lock still held, require the residual session fence
            # to reject a new process before it can change owner history.
            residual_sql = "SELECT json_build_object('pid',a.pid,'backend_start',a.backend_start,'control_fence',EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory' AND l.classid=1111651442 AND l.objid=1129599564 AND l.objsubid=2 AND l.granted)) FROM pg_stat_activity a WHERE a.pid=" + str(observed[0]['pid'])
            residual = json.loads(sql(prefix + '_full', residual_sql))
            require(residual['backend_start'] == observed[0]['backend_start'] and residual['control_fence'], 'residual exact session fence')
            owner_sql = "SELECT json_build_object('epochs',(SELECT json_agg(t ORDER BY epoch) FROM control.owner_epochs t),'roles',(SELECT json_agg(t ORDER BY role_instance_id) FROM control.role_instances t));"
            held_owner = sql(prefix + '_full', owner_sql)
            contender = start(path, 'residual-session-contender')
            extra, refusal = terminal(contender, 3)
            require(not extra and 'Busy' in refusal, 'residual session fence not enforced')
            require(sql(prefix + '_full', owner_sql) == held_owner, 'busy contender changed owner or role history')
            require(json.loads(sql(prefix + '_full', residual_sql)) == residual, 'residual timed out before Busy was proved')
            passed('blocked-startup-cancel-and-join', elapsed_seconds=elapsed, blocker=observed, residual=residual, residual_owner='successor refused Busy')
        finally:
            blocker.stdin.write('ROLLBACK;\n')
            blocker.stdin.close()
            require(blocker.wait(timeout=5) == 0, 'blocker rollback')
        deadline = time.monotonic() + 5
        while sql(prefix + '_full', "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';") != '0':
            require(time.monotonic() < deadline, 'backend did not close after blocker rollback')
            time.sleep(.01)
        no_clients(prefix + '_full')
        item = start(path, 'after-cancellation')
        value = idle(item)
        require(expect(value, 130, 3), 'restart after scan cancellation')
        require(int(value['owner_epoch']) == max(r['epoch'] for r in json.loads(held_owner)['epochs']) + 1, 'fresh owner after residual exit')
        stop(item, prefix + '_full')
        passed('fresh-owner-after-cancelled-scan')
        old = prepare(prefix + '_old', target=7)
        item = start(old, 'migration-required')
        out, err = terminal(item, 3)
        require(not out and 'archive intent inventory refused: Prerequisite' in err, 'old migration refusal')
        no_clients(prefix + '_old')
        passed('explicit-0009-required-no-runtime-migration')
        final = json.loads(history(prefix + '_full'))
        for values in [rows, final]:
            for row in values:
                row.pop('xmin')
        require(final == rows, 'final retained identities changed')
        report['observations'] = dict(rows=130, before_sha256=hashlib.sha256(original.encode()).hexdigest(), logical_rows_unchanged=True, no_native_catalog_started=True)
        report['status'] = 'PASS'
    except BaseException as error:
        report.update(status='FAIL', failure=str(error))
        raise
    finally:
        for item in processes:
            process, record, stdout, stderr = item
            if process.poll() is None:
                signal(item)
                process.wait(timeout=8)
            stdout.close()
            stderr.close()
            record.update(state='terminal', exit_code=process.returncode)
        save()
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))


if __name__ == '__main__':
    main()
