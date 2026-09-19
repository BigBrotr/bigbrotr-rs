#!/usr/bin/env python3
"""Real CLI startup inventory against isolated PostgreSQL; no catalog recovery claim."""
import argparse
import hashlib
import json
from pathlib import Path
import select
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
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    state = json.loads((root / 'resources.json').read_text())
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    require(ready['source'] == state['source'] and ready['status'] == 'READY', 'build identity')
    require(subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip() == state['source'], 'harness revision')
    require(not subprocess.check_output(['git', '-C', str(SOURCE), 'status', '--porcelain']), 'dirty harness')
    for key in ['container_id', 'postgres_id']:
        data = json.loads(subprocess.check_output(['docker', 'inspect', state[key]]))[0]
        require(data['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and data['State']['Running'], 'resource ownership')
    report_path = root / 'archive-inventory.json'
    require(not report_path.exists(), 'existing report')
    directory = root / 'out/inventory'
    directory.mkdir(mode=0o700)
    report = dict(format='bigbrotr-archive-inventory-acceptance/1', source=state['source'],
                  artifact=ready['binary_sha256'], status='RUNNING', cases=[], processes=[], observations={})
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

    def prepare(database, target=9):
        sql('postgres', f'CREATE DATABASE {database};')
        text = (SOURCE / 'config/recipes/archive-only.toml').read_text()
        for section, key, value in [
            ('administration', 'enabled', False), ('topology', 'dataset_id', '01' * 16),
            ('topology', 'instance_id', '02' * 16), ('archive', 'catalog_id', '5a' * 16),
            ('postgres.control', 'endpoint', '127.0.0.1:5432'),
            ('postgres.control', 'database', database),
            ('postgres.control', 'password_ref', 'file:/secrets/bigbrotr_control'),
        ]:
            text = change(text, section, key, value)
        candidate = directory / (database + '.toml')
        candidate.write_text(text)
        candidate.chmod(0o600)
        path = '/out/inventory/' + candidate.name
        result = cli('migrate', 'apply', '--file', path, '--target', str(target), '--user', 'postgres', '--password-ref', 'file:/secrets/postgres')
        require(result.returncode == 0, 'explicit migration: ' + result.stderr)
        sql(database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        for command in [('config', 'initialize', '--file', path), ('config', 'activate-initial', '--file', path, '--request-id', '44' * 16, '--expected-revision', '1')]:
            result = cli(*command)
            require(result.returncode == 0, 'explicit initialization/activation: ' + result.stderr)
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
        empty = prepare('inventory_empty')
        baseline = start(empty, 'baseline', '/baseline/bigbrotr')
        value = idle(baseline)
        require(not expect(value, 0, 0), 'old executable unexpectedly implements inventory')
        stop(baseline, 'inventory_empty')
        passed('negative-control-old-runtime-misses-inventory', baseline_source=json.loads(subprocess.check_output(['docker', 'exec', state['container_id'], '/baseline/bigbrotr', 'build-info']))['source']['revision'])
        current = start(empty, 'empty')
        require(expect(idle(current), 0, 0), 'empty inventory')
        stop(current, 'inventory_empty')
        passed('empty-history-distinct-from-unresolved')
        path = prepare('inventory_full')
        seeded = subprocess.run(['docker', 'exec', state['container_id'], '/out/native-tests/archive-attempt-probe', '--seed-runtime', 'inventory_full'], text=True, capture_output=True, timeout=60)
        (directory / 'seed.stdout').write_text(seeded.stdout)
        (directory / 'seed.stderr').write_text(seeded.stderr)
        require(seeded.returncode == 0 and json.loads(seeded.stdout)['rows'] == 130, 'actual adapter seed')
        original = history('inventory_full')
        rows = json.loads(original)
        require(len(rows) == 130 and [r['ordinal'] for r in rows] == list(range(1, 131)), 'independent seeded membership')
        require([r['attempt_id'] for r in rows] == ['\\x' + f'{n:02x}' * 16 for n in range(100, 230)], 'independent seeded identities')
        for label in ['populated', 'restart']:
            item = start(path, label)
            require(expect(idle(item), 130, 3), 'complete inventory')
            stop(item, 'inventory_full')
            require(history('inventory_full') == original, 'inventory changed immutable rows')
            passed(label + '-complete-history', rows=130, pages=3)
        # Corrupt only old metadata; a latest-only lookup would falsely pass.
        sql('inventory_full', "BEGIN; ALTER TABLE control.archive_attempts DISABLE TRIGGER USER; UPDATE control.archive_attempts SET catalog_id=decode(repeat('5b',16),'hex') WHERE ordinal=65; ALTER TABLE control.archive_attempts ENABLE TRIGGER USER; COMMIT;")
        corrupted = history('inventory_full')
        item = start(path, 'historical-mismatch')
        out, err = terminal(item, 3)
        require(not out and 'archive intent inventory refused: Conflict' in err, 'historical mismatch not refused')
        require(history('inventory_full') == corrupted, 'refusal mutated history')
        no_clients('inventory_full')
        sql('inventory_full', "BEGIN; ALTER TABLE control.archive_attempts DISABLE TRIGGER USER; UPDATE control.archive_attempts SET catalog_id=decode(repeat('5a',16),'hex') WHERE ordinal=65; ALTER TABLE control.archive_attempts ENABLE TRIGGER USER; COMMIT;")
        passed('historical-mismatch-before-idle')
        # Hold a real relation lock while the actual startup scan runs, then
        # interrupt it. The blocker is an isolated owned psql transaction.
        blocker = subprocess.Popen(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'inventory_full'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            blocker.stdin.write("BEGIN; LOCK TABLE control.archive_attempts IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
            blocker.stdin.flush()
            require(select.select([blocker.stdout], [], [], 5)[0] and blocker.stdout.readline().strip() == 'locked', 'blocker acquisition')
            item = start(path, 'cancelled-scan')
            deadline = time.monotonic() + 2
            observed = None
            while time.monotonic() < deadline:
                data = sql('inventory_full', "SELECT json_agg(json_build_object('pid',pid,'backend_start',backend_start,'wait_event',wait_event,'blockers',pg_blocking_pids(pid))) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock' AND query LIKE '%control.archive_attempts%';")
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
            no_clients('inventory_full')
            passed('blocked-startup-cancel-and-join', elapsed_seconds=elapsed, blocker=observed)
        finally:
            blocker.stdin.write('ROLLBACK;\n')
            blocker.stdin.close()
            require(blocker.wait(timeout=5) == 0, 'blocker rollback')
        item = start(path, 'after-cancellation')
        require(expect(idle(item), 130, 3), 'restart after scan cancellation')
        stop(item, 'inventory_full')
        passed('fresh-owner-after-cancelled-scan')
        old = prepare('inventory_old', target=7)
        item = start(old, 'migration-required')
        out, err = terminal(item, 3)
        require(not out and 'archive intent inventory refused: Prerequisite' in err, 'old migration refusal')
        no_clients('inventory_old')
        passed('explicit-0009-required-no-runtime-migration')
        final = json.loads(history('inventory_full'))
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
