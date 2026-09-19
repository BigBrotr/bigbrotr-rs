#!/usr/bin/env python3
"""Actual reserved initializer, descriptor and PostgreSQL effects; owned lab only."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def digest(value):
    return hashlib.sha256(value).hexdigest()


def filesystem(root):
    rows = []
    for path in [root, *sorted(root.rglob('*'))]:
        m = path.lstat()
        rows.append(dict(path=str(path.relative_to(root)), mode=m.st_mode, uid=m.st_uid,
                         device=m.st_dev, inode=m.st_ino, links=m.st_nlink,
                         target=os.readlink(path) if path.is_symlink() else None,
                         sha256=digest(path.read_bytes()) if path.is_file() and not path.is_symlink() else None))
    return digest(json.dumps(rows, sort_keys=True).encode())


def fixture_child(database, label):
    assert database.startswith('bbinit_') and all(c in 'abcdefghijklmnopqrstuvwxyz_' for c in database)
    root = Path('/out/schema_' + database)
    marker = root / '.bigbrotr-initialization'
    marker.mkdir(mode=0o700)
    lock = marker / 'custody.lock'
    lock.touch(mode=0o600)
    fd = os.open(lock, os.O_RDONLY | os.O_CLOEXEC)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    rm, lm = root.stat(), os.fstat(fd)
    journal = marker / 'intent.json'
    value = dict(format='bigbrotr-local-initialization/1', initialization_id=[3]*16,
                 dataset_id='01'*16, catalog_id='02'*16, catalog_root=str(root)+'/',
                 boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                 root_device=rm.st_dev, root_inode=rm.st_ino, lock_device=lm.st_dev, lock_inode=lm.st_ino)
    journal.write_text(json.dumps(value, separators=(',', ':')))
    journal.chmod(0o600)
    proof = dict(custody_fd=fd, lock_device=lm.st_dev, lock_inode=lm.st_ino,
                 root_device=rm.st_dev, root_inode=rm.st_ino,
                 journal_sha256=list(hashlib.sha256(journal.read_bytes()).digest()))
    password = Path('/secrets/BootstrapMixed').read_text()
    assert len(password) == 64 and all(c in '0123456789abcdef' for c in password)
    request = dict(dsn=f'host=127.0.0.1 port=5432 dbname={database} user=BootstrapMixed password={password} sslmode=disable connect_timeout=5',
                   data_root=str(root)+'/', custody=proof)
    extras = []
    if label == 'changed-journal': journal.write_bytes(journal.read_bytes() + b' ')
    elif label == 'empty-journal': journal.write_bytes(b'')
    elif label == 'oversized-journal': journal.write_bytes(b'x'*32769)
    elif label == 'replaced-lock':
        lock.rename(root.with_name(root.name+'-retained-lock'))
        lock.touch(mode=0o600)
    elif label == 'missing-fd': proof['custody_fd'] = 2147483647
    elif label == 'wrong-fd':
        other_path = root.with_name(root.name+'-other-lock')
        other_path.touch(mode=0o600)
        other = os.open(other_path, os.O_RDONLY | os.O_CLOEXEC)
        extras.append(other)
        proof['custody_fd'] = other
    elif label == 'reopened-unlocked-fd':
        other = os.open(lock, os.O_RDONLY | os.O_CLOEXEC)
        extras.append(other)
        proof['custody_fd'] = other
    elif label == 'root-mode': root.chmod(0o755)
    elif label == 'marker-mode': marker.chmod(0o755)
    elif label == 'journal-mode': journal.chmod(0o644)
    elif label == 'symlink-journal':
        target = root.with_name(root.name+'-retained-journal')
        journal.rename(target)
        journal.symlink_to(target)
    elif label == 'extra-root': (root/'canary').write_bytes(b'preserve')
    elif label == 'extra-marker': (marker/'canary').write_bytes(b'preserve')
    elif label == 'wrong-root-inode': proof['root_inode'] += 1
    wire = json.dumps(request)
    if label == 'unknown-field': wire = wire[:-1] + ', "extra":1}'
    elif label == 'duplicate-field': wire = wire[:-1] + ', "data_root":'+json.dumps(str(root)+'/')+'}'
    elif label == 'trailing-value': wire += '{}'
    elif label == 'oversized-wire': wire = ' '*32769
    before = journal.read_bytes()
    files_before = filesystem(root)
    # Retain fixture scratch even on unknown termination; final owned lab audit
    # records lifecycle. An exception must not unlink a live child's files.
    temp = tempfile.mkdtemp(prefix='initialization-child-', dir='/out')
    args = ['/out/archive-worker/bigbrotr-archive-worker', 'initialize-reserved-catalog',
            '/out/archive-worker', temp, '134217728', '67108864', '1']
    env = {'BIGBROTR_ARCHIVE_PARENT_PID':str(os.getpid()), 'LD_LIBRARY_PATH':'/out/archive-worker/lib'}
    child = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             env=env, pass_fds=tuple(extras) if label == 'reopened-unlocked-fd' else (fd, *extras))
    try:
        if label == 'reopened-unlocked-fd':
            os.close(fd)
            fd = None
            retry = os.open(lock, os.O_RDONLY)
            try:
                fcntl.flock(retry, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally: os.close(retry)
        if label == 'killed-child-custody':
            # Popen has completed exec; the actual verified native image now
            # owns this inherited descriptor even before receiving credentials.
            observed = os.stat(f'/proc/{child.pid}/fd/{fd}')
            assert (observed.st_dev, observed.st_ino) == (lm.st_dev, lm.st_ino)
            os.close(fd)
            fd = None
            retry = os.open(lock, os.O_RDONLY)
            try:
                try: fcntl.flock(retry, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError: pass
                else: raise AssertionError('actual native child did not retain custody')
                child.kill()
                stdout, stderr = child.communicate(timeout=5)
                assert child.returncode == -9
                fcntl.flock(retry, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally: os.close(retry)
            result = dict(exit=child.returncode, held='occupied', joined='reacquired')
        else:
            stdout, stderr = child.communicate(wire.encode(), timeout=30)
            assert len(stdout)+len(stderr) < 4096 and password.encode() not in stdout+stderr
            result = dict(exit=child.returncode, stdout=stdout.decode(), stderr=stderr.decode())
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        if fd is not None: os.close(fd)
        for other in extras: os.close(other)
    assert journal.read_bytes() == before
    result['journal_sha256'] = digest(before)
    result['filesystem_before_sha256'] = files_before
    result['filesystem_after_sha256'] = filesystem(root)
    print(json.dumps(result))


def main(run):
    state = json.loads((run/'resources.json').read_text())
    source = Path(__file__).resolve().parents[2]
    assert source == run/'source'

    def cmd(*args, input=None):
        r = subprocess.run(args, input=input, text=True, capture_output=True, timeout=60)
        assert r.returncode == 0, 'fixture command failed: '+r.stderr[:2000]
        assert len(r.stdout) < 2*1024*1024
        return r.stdout.strip()

    assert cmd('git', '-C', str(source), 'rev-parse', 'HEAD') == state['source']
    assert not cmd('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    for key in ['container_id', 'postgres_id']:
        obj = json.loads(cmd('docker', 'inspect', state[key]))[0]
        assert obj['State']['Running'] and obj['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
    runner, pg = state['container_id'], state['postgres_id']
    ready = json.loads((run/'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    report = dict(source=state['source'], artifacts=ready, status='RUNNING', cases=[])
    path = run/'initialization.json'
    assert not path.exists()

    def save(): path.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, query):
        return cmd('docker', 'exec', '-i', pg, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, input=query)
    def database(label):
        db = 'bbinit_'+label.replace('-', '_')
        sql('postgres', 'CREATE DATABASE '+db)
        sql(db, 'REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
        (run/'out'/('schema_'+db)).mkdir(mode=0o700)
        return db
    def snapshot(db):
        value = cmd('docker', 'exec', pg, 'pg_dump', '-U', 'postgres', db)
        stable = '\n'.join(l for l in value.splitlines() if not l.startswith(('\\restrict ', '\\unrestrict ')))
        return digest(stable.encode())
    def drain():
        until = time.monotonic()+5
        while True:
            count = int(sql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename='BootstrapMixed'"))
            if not count: return
            assert time.monotonic() < until, 'bootstrap backends remain'
            time.sleep(.02)
    def scientific(db):
        names = json.loads(sql(db, 'SELECT json_agg(table_name ORDER BY table_name) FROM ducklake_table'))
        assert names == ['archive_commits','dictionaries','event_eligibility_facts','event_receive_facts','events','provenance_facts']
        assert sql(db, 'SELECT string_agg(snapshot_id::text,\',\' ORDER BY snapshot_id) FROM ducklake_snapshot') == '0,1'
        assert sql(db, 'SELECT count(*) FROM ducklake_data_file') == '0'
        schema = digest((source/'contracts/archive/v1/schema.json').read_bytes())
        assert sql(db, 'SELECT commit_extra_info FROM ducklake_snapshot_changes WHERE snapshot_id=1') == 'sha256:'+schema
        return dict(tables=names, snapshots=[0,1], data_files=0, schema_sha256=schema)

    save()
    try:
        for mode in ['reserved', 'driver-custody']:
            db = database(mode)
            before = snapshot(db)
            result = json.loads(cmd('docker', 'exec', runner, '/out/native-tests/catalog-schema-probe', db, mode, 'success'))
            drain()
            if mode == 'reserved':
                assert result == dict(success=True, active_permits=0, outcome='Ok(())')
                effects = scientific(db)
            else:
                assert result == dict(status='PASS', unpolled='occupied', live='occupied', query=42, joined='reacquired')
                assert snapshot(db) == before
                effects = dict(database_unchanged_sha256=before)
            journal = run/'out'/('schema_'+db)/'.bigbrotr-initialization/intent.json'
            value = json.loads(journal.read_bytes())
            assert value['format']=='bigbrotr-local-initialization/1' and value['initialization_id']==[3]*16
            assert value['dataset_id']=='01'*16 and value['catalog_id']=='02'*16
            assert value['catalog_root']=='/out/schema_'+db+'/'
            assert journal.read_bytes()==json.dumps(value, separators=(',', ':')).encode()
            report['cases'].append(dict(case='actual-parent-'+mode, result=result, effects=effects,
                                        journal_sha256=digest(journal.read_bytes())))
            save()
        labels = ['valid', 'changed-journal', 'empty-journal', 'oversized-journal', 'replaced-lock',
                  'missing-fd', 'wrong-fd', 'wrong-root-inode', 'root-mode', 'marker-mode', 'journal-mode',
                  'symlink-journal', 'extra-root', 'extra-marker', 'unknown-field', 'duplicate-field',
                  'trailing-value', 'oversized-wire', 'killed-child-custody', 'reopened-unlocked-fd']
        for label in labels:
            db = database(label)
            before = snapshot(db)
            actual = json.loads(cmd('docker', 'exec', runner, 'python3', '-B',
                                    '/source/experiments/archive-engine/initialization_cases.py', '--child', db, label))
            # Save observed output before assertions so classification failures are retained.
            entry = dict(case=label, result=actual, status='OBSERVED')
            report['cases'].append(entry)
            save()
            drain()
            if label in ['valid', 'reopened-unlocked-fd']:
                assert actual['exit']==0 and actual['stderr']==''
                assert json.loads(actual['stdout'])==dict(format='bigbrotr-native-reserved-initialize/1', status='initialized')
                entry['effects'] = scientific(db)
                if label == 'reopened-unlocked-fd':
                    entry['limitation'] = 'Raw fixture passed only a reopened same-inode FD; actual flock reacquired while worker waited for input, then native initialize accepted. Only the typed trusted parent establishes locked-OFD custody.'
            else:
                assert snapshot(db)==before
                assert actual['filesystem_before_sha256']==actual['filesystem_after_sha256']
                entry['database_unchanged_sha256'] = before
                if label == 'killed-child-custody':
                    assert actual['exit']==-9 and actual['held']=='occupied' and actual['joined']=='reacquired'
                else:
                    error = 'InvalidInput' if label in ['unknown-field','duplicate-field','trailing-value','oversized-wire'] else 'InvalidDataRoot'
                    assert actual['exit']==1 and actual['stdout']==''
                    assert actual['stderr']=='archive catalog bootstrap failed: '+error+'\n'
            entry['status']='PASS'
            save()
        report['status']='PASS'
        save()
        print(json.dumps(dict(status='PASS', cases=len(report['cases']))))
    except BaseException:
        report['status']='FAIL'
        save()
        raise


if __name__ == '__main__':
    if sys.argv[1:2] == ['--child']:
        fixture_child(*sys.argv[2:])
    else:
        assert len(sys.argv)==2
        main(Path(sys.argv[1]).resolve())
