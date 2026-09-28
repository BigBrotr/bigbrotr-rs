#!/usr/bin/env python3
"""Actual bound admission with nested relational refusals; no new native build."""
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection
spec = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
m = importlib.util.module_from_spec(spec); sys.modules['bootstrap_oracle'] = m; spec.loader.exec_module(m)
from oracle import expected_source

root = Path(sys.argv[1]).resolve(strict=True)
assert SOURCE == root / 'source'
revision = lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD']).stdout.strip()
assert not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout
original = root.parent / 'relay-source-recheck-e06f580'
original_report = (original / 'pg/source-result.json').read_bytes()
previous = json.loads(original_report)
assert previous['status'] == 'PASS_SCOPED_RELAY_SOURCE_FIRST_WAVE'
build = root.parent / 'relay-source-a6c28e6/out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
changed = lab.command(['git','-C',str(SOURCE),'diff','--name-only',built['source'],revision]).stdout.splitlines()
assert changed and all(n.startswith(('docs/','experiments/')) for n in changed), changed
sha = lambda b: hashlib.sha256(b).hexdigest()
for name,key in [('bigbrotr','binary_sha256'),('ingest_bootstrap_probe','probe_sha256')]:
    assert sha((build/name).read_bytes()) == built[key]
pg = lab.load(original / 'pg')
assert pg['state'] == 'stopped-retained' and pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
output = root / 'relations'; output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.relay-source-relations'
client = 'bigbrotr-relay-source-relations-' + revision[:7]
database = 'bb_source_relations_' + revision[:7]
report = dict(format='bigbrotr-relay-source-relations/1',source=revision,build=built,
    application_source=built['source'],owner=owner,label=label,status='RUNNING',
    client=None,postgres=pg['postgres'],postgres_owner=pg['owner'],database=database,
    original_report_sha256=sha(original_report),not_run=['concurrency', 'ambiguous COMMIT and recovery',
    'source grant and socket attribution', 'large ledger upgrade', 'load/aged state','arm64/release'])


def save(): lab.write_json(output / 'result.json',report)


def containers():
    ids = lab.docker('ps','-aq').stdout.split()
    return json.loads(lab.docker('inspect',*ids).stdout) if ids else []


def rows(db):
    names = json.loads(lab.sql(pg,db,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    assert len(names) == 64 and all(re.fullmatch('[a-z_]+',n) for n in names)
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
        f"FROM (SELECT *,xmin::text AS lab_xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg,db,'SELECT jsonb_object_agg(name,rows) FROM ('+' UNION ALL '.join(parts)+') t'))


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect',client,check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    free_bytes_before=shutil.disk_usage(root).free,postgres_id=target['Id'])
assert report['free_bytes_before'] > 2 * 1024**3
lab.write_json(output / 'preflight.json',dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps','-eo','pid,ppid,stat,comm']).stdout,
    ports=lab.command(['ss','-lntup']).stdout,volumes=lab.docker('volume','ls').stdout,
    directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    lab.docker('start',target['Id'])
    deadline = time.monotonic()+30
    while lab.docker('exec',target['Id'],'pg_isready','-U','postgres',check=False).returncode:
        assert time.monotonic() < deadline; time.sleep(.1)
    template_before = rows('bb_source_template')
    original_before = rows('bb_source_pass')
    assert original_before == json.loads((original / 'pg/source-pass-after.json').read_text())
    lab.sql(pg,'postgres',f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE bb_source_template')
    assert rows(database) == template_before
    lab.sql(pg,database,(SOURCE / 'experiments/relay-source/relations.sql').read_text())
    ident = lab.docker('create','--init','--name',client,'--label',label+'='+owner,
        '--network','container:'+pg['postgres'],'--user',f'{os.getuid()}:{os.getgid()}',
        '--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only',
        '--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount',f'type=bind,src={output},dst=/lab',
        '--mount',f'type=bind,src={original}/pg/secrets,dst=/run/secrets,readonly',
        '--mount',f'type=bind,src={build},dst=/build,readonly',pg['client_image'],
        'python3','-c','import time; time.sleep(180)').stdout.strip()
    report['client'] = dict(id=ident,name=client); save(); lab.docker('start',ident)
    result = lab.docker('exec',client,'/build/ingest_bootstrap_probe','source-pass',database,
        '/run/secrets/application','/lab/native',check=False,timeout=60)
    (output / 'probe.stdout').write_text(result.stdout); (output / 'probe.stderr').write_text(result.stderr)
    report['probe_exit'] = result.returncode; save()
    assert result.returncode == 0 and not result.stderr
    cases = json.loads(lab.sql(pg,database,'SELECT json_agg(r ORDER BY position) FROM source_fixture.results r'))
    report['cases'] = cases; save()
    assert len(cases) == 17 and all(c['unchanged'] for c in cases)
    assert len({c['backend_pid'] for c in cases}) == 1
    messages = [json.loads(l) for l in result.stdout.splitlines()]
    before,after = [next(m['state'] for m in messages if m['event']==event) for event in ['source-before','source-after']]
    assert before == after and messages[-1]['status'] == 'PASS'
    final = rows(database); lab.write_json(output / 'final.json',final)
    assert all(not final[t] for t in ['jobs','job_admissions','producer_input_bindings'])
    assert len(final['producer_requests']) == len(final['producer_outcomes']) == 2
    dataset = bytes.fromhex(final['datasets'][0]['dataset_id'][2:])
    expected,inputs = expected_source(dataset,True)
    dumped = lab.docker('exec',client,'/build/ingest_bootstrap_probe','dump','/lab/native')
    (output / 'native.json').write_text(dumped.stdout)
    native = json.loads(dumped.stdout)['rows']
    assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native if r['cf']!='receipts'] == expected
    originals = sorted((output / 'native/producer-inputs-v1').glob('*.cbor'))
    assert len(originals) == 2 and all(p.read_bytes()==inputs[int(p.stem[-16:],16)] for p in originals)
    assert rows('bb_source_pass') == original_before and rows('bb_source_template') == template_before
    assert (original / 'pg/source-result.json').read_bytes() == original_report
    assert lab.sql(pg,'postgres',"SELECT count(*) FROM pg_stat_activity WHERE usename='bigbrotr_control'") == '0'
    report.update(status='PASS_SCOPED_SOURCE_RELATIONS',native_rows=len(native),
        original_inputs_unchanged=True,original_databases_unchanged=True,drivers_joined=True)
except BaseException as error:
    report.update(status='FAIL',failure=type(error).__name__+': '+str(error)); raise
finally:
    errors = []
    for ident,owner_key,expected_owner in [(report['client']['id'],label,owner)] if report['client'] else []:
        try:
            item = json.loads(lab.docker('inspect',ident).stdout)[0]
            assert item['Config']['Labels'][owner_key] == expected_owner
            if item['State']['Running']: lab.docker('stop','--time','3',ident)
        except BaseException as error: errors.append('client-'+type(error).__name__)
    try:
        item = json.loads(lab.docker('inspect',target['Id']).stdout)[0]
        assert item['Config']['Labels'][lab.LABEL] == pg['owner']
        if item['State']['Running']: lab.docker('stop','--time','10',target['Id'])
        logs = lab.docker('logs','--since',report['started_at'],target['Id'])
        (output/'postgres.stdout').write_text(logs.stdout); (output/'postgres.stderr').write_text(logs.stderr)
    except BaseException as error: errors.append('postgres-'+type(error).__name__)
    current = {c['Id']:projection(c) for c in containers()}
    owned = [target['Id']] + ([report['client']['id']] if report['client'] else [])
    report.update(cleanup_errors=errors,missing_owned=[i for i in owned if i not in current],
        terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id']!=target['Id'] and current.get(c['Id'])!=projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(
        c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    save()
assert report['status'] == 'PASS_SCOPED_SOURCE_RELATIONS'
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))))
