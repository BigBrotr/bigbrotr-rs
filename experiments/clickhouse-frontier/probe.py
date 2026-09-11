"""Tiny real-engine counterexample and immutable-fact frontier probe.

Requires a disposable isolated ClickHouse server; creates its own database.
"""
import json
import os
from pathlib import Path
import urllib.request

URL = os.environ.get('LAB_CH_URL', 'http://127.0.0.1:48123/')
ROOT = Path(os.environ['LAB_ROOT'])
DB = 'planning_frontier_20260911'

def query(sql, rows=False):
    if rows: sql += ' FORMAT JSONEachRow'
    req = urllib.request.Request(URL, data=sql.encode(), method='POST')
    try:
        raw = urllib.request.urlopen(req, timeout=20).read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()) from e
    return [json.loads(line) for line in raw.splitlines()] if rows else raw.strip()

def insert(table, seq, observed):
    query(f'INSERT INTO {DB}.{table} SETTINGS insert_deduplicate=0 VALUES (1, 7, {seq}, {observed})')

def result(table, frontier):
    return query(f'SELECT count() AS records, min(observed_at) AS first_seen FROM {DB}.{table} FINAL WHERE sequence <= {frontier}', True)[0]

report = {'scope':'tiny CH logical frontier/forced-merge test, no scale or power-loss claim'}
try:
    report['version'] = query('SELECT version()')
    query(f'CREATE DATABASE {DB}')
    schema = 'event UInt64, relay UInt32, sequence UInt64, observed_at UInt64'
    query(f'CREATE TABLE {DB}.unsafe ({schema}) ENGINE=ReplacingMergeTree(sequence) ORDER BY (event,relay) SETTINGS fsync_after_insert=1, fsync_part_directory=1')
    families = ['bodies','tags','event_relay','relay_event']
    for t in families:
        query(f'CREATE TABLE {DB}.{t} ({schema}) ENGINE=ReplacingMergeTree ORDER BY (event,relay,sequence) SETTINGS fsync_after_insert=1, fsync_part_directory=1')
        insert(t, 10, 200)
    insert('unsafe', 10, 200)
    before = result('unsafe', 10)
    insert('unsafe', 11, 100)
    query(f'OPTIMIZE TABLE {DB}.unsafe FINAL')
    after = result('unsafe', 10)
    assert before == {'records':'1', 'first_seen':'200'} or before == {'records':1,'first_seen':200}
    assert int(after['records']) == 0
    report['unsafe_logical_edge_replacement'] = {'before':before,'after':after,'old_snapshot_lost':True}
    initial = {t:result(t,10) for t in families}
    for t in families[:-1]:
        insert(t,11,100); insert(t,11,100)  # ambiguous ACK replay with engine dedup disabled
        query(f'OPTIMIZE TABLE {DB}.{t} FINAL')
    assert {t:result(t,10) for t in families} == initial
    report['delayed_family_frontier_10'] = 'unchanged after inserts, retries and merges'
    insert(families[-1],11,100)
    for t in families:
        query(f'OPTIMIZE TABLE {DB}.{t} FINAL')
        assert int(result(t,11)['records']) == 2
        assert int(result(t,11)['first_seen']) == 100
        assert int(result(t,10)['first_seen']) == 200
    report['immutable_identity'] = {'old_frontier':initial,'new_frontier':{t:result(t,11) for t in families}, 'retries_logically_unique':True}
    report['status']='passed'
except Exception as exc:
    report.update(status='failed',error=str(exc))
(ROOT/'results'/'clickhouse-frontier.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
raise SystemExit(0 if report['status']=='passed' else 1)
