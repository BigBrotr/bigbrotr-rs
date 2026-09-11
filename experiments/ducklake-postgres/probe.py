"""Isolated functional/crash probe. Requires a disposable PostgreSQL catalog.

Never point this at a production catalog. LAB_ROOT must identify a new directory.
This tests process death, not loss of OS page cache, device failure or power loss.
"""
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import sys
import time

import duckdb

ROOT = Path(os.environ['LAB_ROOT'])
DSN = os.environ.get('LAB_CATALOG_DSN', 'dbname=planning host=127.0.0.1 port=48432 user=postgres')

def lit(v):
    return "'" + str(v).replace("'", "''") + "'"

def connect(install=False):
    c = duckdb.connect(config={'threads': '1', 'memory_limit': '128MB',
        'extension_directory': str(ROOT / 'extensions'), 'temp_directory': str(ROOT / 'temp')})
    if install:
        c.execute('INSTALL ducklake')
        c.execute('INSTALL postgres')
    c.execute('LOAD ducklake')
    c.execute('LOAD postgres')
    c.execute(f"ATTACH {lit('ducklake:postgres:' + DSN)} AS lake (DATA_PATH {lit(ROOT / 'data')}, DATA_INLINING_ROW_LIMIT 0)")
    return c

def transaction(c, batch):
    c.execute('BEGIN')
    c.execute('INSERT INTO lake.events VALUES (?, ?)', [batch, 'body-' + str(batch)])
    c.execute('INSERT INTO lake.edges VALUES (?, ?)', [batch, 'wss://example.invalid'])
    c.execute('INSERT INTO lake.receipts VALUES (?)', [batch])

def child(batch, commit, ready):
    c = connect()
    transaction(c, batch)
    if commit:
        c.execute('COMMIT')
    ready.send('committed' if commit else 'uncommitted')
    time.sleep(40)

def crash_at(batch, commit):
    parent, other = mp.Pipe(False)
    p = mp.Process(target=child, args=(batch, commit, other))
    p.start()
    if not parent.poll(30):
        p.kill(); p.join()
        raise RuntimeError('child did not reach crash point')
    phase = parent.recv()
    os.kill(p.pid, signal.SIGKILL)
    p.join(5)
    assert p.exitcode == -signal.SIGKILL
    return phase

def count(c, table, batch):
    return c.execute(f'SELECT count(*) FROM lake.{table} WHERE batch = ?', [batch]).fetchone()[0]

def concurrent_writer(ready, release):
    c = connect()
    c.execute('BEGIN')
    exists = count(c, 'receipts', 4)
    ready.send(exists)
    assert release.recv() == 'commit'
    c.execute('INSERT INTO lake.events VALUES (4, \'body-4\')')
    c.execute('INSERT INTO lake.edges VALUES (4, \'wss://example.invalid\')')
    c.execute('INSERT INTO lake.receipts VALUES (4)')
    try:
        c.execute('COMMIT')
        ready.send('committed')
    except Exception as exc:
        ready.send(type(exc).__name__ + ': ' + str(exc))
    finally:
        c.close()

if __name__ == '__main__':
    mp.set_start_method('spawn')
    ROOT.mkdir(parents=True, exist_ok=True)
    c = connect(install=True)
    report = {'duckdb': duckdb.__version__, 'catalog': 'PostgreSQL 16 disposable container',
        'scope': 'tiny functional process-crash probe; no hardware durability or throughput claim'}
    report['extensions'] = c.execute("SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name IN ('ducklake','postgres')").fetchall()
    for table, schema in [('events', 'batch BIGINT, body VARCHAR'), ('edges', 'batch BIGINT, relay VARCHAR'), ('receipts', 'batch BIGINT')]:
        c.execute(f'CREATE TABLE lake.{table}({schema})')
    transaction(c, 1); c.execute('COMMIT')
    transaction(c, 2); c.execute('ROLLBACK')
    assert all(count(c, t, 1) == 1 and count(c, t, 2) == 0 for t in ['events','edges','receipts'])
    c.close()
    report['kill_before_commit'] = crash_at(2, False)
    c = connect()
    assert all(count(c, t, 2) == 0 for t in ['events','edges','receipts'])
    c.close()
    report['kill_after_commit_before_application_receipt'] = crash_at(3, True)
    c = connect()
    assert all(count(c, t, 3) == 1 for t in ['events','edges','receipts'])
    if count(c, 'receipts', 3) == 0:
        transaction(c, 3); c.execute('COMMIT')
    assert count(c, 'events', 3) == 1
    report['single_publisher_receipt_retry'] = 'one logical batch'
    c.close()
    parents, processes = [], []
    for _ in range(2):
        parent, other = mp.Pipe()
        p = mp.Process(target=concurrent_writer, args=(other, other))
        p.start(); parents.append(parent); processes.append(p)
    assert all(p.poll(30) and p.recv() == 0 for p in parents)
    for p in parents: p.send('commit')
    outcomes = []
    for p in parents:
        assert p.poll(30)
        outcomes.append(p.recv())
    for p in processes:
        p.join(5)
        assert p.exitcode == 0
    c = connect()
    report['concurrent_check_then_insert'] = {'outcomes': outcomes, 'rows': {t: count(c, t, 4) for t in ['events','edges','receipts']}}
    report['status'] = 'passed'
    c.close()
    (ROOT / 'results' / 'ducklake-postgres.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
