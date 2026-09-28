#!/usr/bin/env python3
"""Actual application-login SQL guards; new clones, no compiler/client allocation."""
import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import time

sys.dont_write_bytecode=True
SOURCE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(SOURCE/'experiments/control-migrations'))
import lab
sys.path.insert(0,str(SOURCE/'experiments/network-transport'))
from inventory import projection

root=Path(sys.argv[1]).resolve(strict=True)
assert len(sys.argv)==2 and SOURCE==root/'source'
revision=lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD']).stdout.strip()
assert not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout
assert not lab.command(['git','-C',str(SOURCE),'diff','--name-only','54d1a18',revision,
    '--','apps/bigbrotr/src','crates','migrations','Cargo.toml','Cargo.lock']).stdout
pg_root=root.parent/'relay-source-recheck-e06f580/pg';pg=lab.load(pg_root)
assert pg['owner']=='5e69b0f060b1607c25230d1dbd3e5608' and pg['state']=='stopped-retained'
basis=root.parent/'acquisition-scope-54d1a18/engine'
original_report=(basis/'result.json').read_bytes()
assert json.loads(original_report)['status']=='PASS_SCOPED_ACQUISITION_REGISTRATION'
output=root/'guards';output.mkdir(mode=0o700)
fixture=SOURCE/'experiments/acquisition-scope/guards.sql'
report=dict(format='bigbrotr-acquisition-scope-guards/1',source=revision,status='RUNNING',
    fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),postgres=pg['postgres'],
    postgres_owner=pg['owner'],cases=[],databases=[],
    not_run=['installed scope loading/binding, Sync, durable admission/progress/M1','load/aged-state','arm64/release'])


def require(condition,reason):
    if not condition:raise RuntimeError(reason)


def save():lab.write_json(output/'result.json',report)


def containers():
    return json.loads(lab.docker('inspect',*lab.docker('ps','-aq').stdout.split()).stdout)


def rows(db):
    names=json.loads(lab.sql(pg,db,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names)==66 and all(re.fullmatch('[a-z_]+',n) for n in names),'table-set')
    parts=[f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
           f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg,db,'SELECT jsonb_object_agg(name,rows) FROM ('+' UNION ALL '.join(parts)+') t'))


prior=containers();target=next(c for c in prior if c['Name']=='/'+pg['postgres'])
assert target['Config']['Labels'][lab.LABEL]==pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),postgres_id=target['Id'],free_bytes_before=shutil.disk_usage(root).free)
assert report['free_bytes_before']>2*1024**3
lab.write_json(output/'preflight.json',dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps','-eo','pid,ppid,stat,comm']).stdout,ports=lab.command(['ss','-lntup']).stdout,
    volumes=lab.docker('volume','ls').stdout,directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    lab.docker('start',target['Id']);deadline=time.monotonic()+30
    while lab.docker('exec',target['Id'],'pg_isready','-U','postgres',check=False).returncode:
        require(time.monotonic()<deadline,'postgres-readiness');time.sleep(.1)
    report['server']=json.loads(lab.sql(pg,'postgres',"SELECT json_build_object('version',current_setting('server_version_num'),'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'))"))
    require(report['server']==dict(version='160015',fsync='on',full_page_writes='on'),'actual-server-settings')
    for name in ['basic','collision']:
        require(rows('bb_scope_54d1a18_'+name)==json.loads((basis/(name+'-after.json')).read_text()),'retained-basis-before')
    for name,template,repeatable,expected in [
        ('basic','basic',False,dict(refusals=30,selectors=3,scopes=4)),
        ('collision','collision',False,dict(refusals=26,selectors=64,scopes=65)),
        ('repeatable','basic',True,dict(refusals=2,selectors=2,scopes=3)),
    ]:
        db='bb_scope_guards_'+revision[:7]+'_'+name
        require(re.fullmatch('[a-z0-9_]{1,63}',db),'database-name')
        lab.sql(pg,'postgres',f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE bb_scope_54d1a18_{template}')
        report['databases'].append(db);save()
        before=rows(db);require(before==json.loads((basis/(template+'-after.json')).read_text()),'exact-clone')
        lab.write_json(output/(name+'-before.json'),before)
        # Only this clone gets extra privileges; otherwise ACL refusal could mask
        # the row/statement guards being tested. No original ACL is modified.
        lab.sql(pg,db,'GRANT UPDATE,DELETE,TRUNCATE ON control.acquisition_selectors,control.acquisition_scopes TO bigbrotr_control;')
        result=lab.docker('exec','-i','--env','PGOPTIONS=-c statement_timeout=30000 -c lock_timeout=5000',
            target['Id'],'psql','-X','-A','-t','-q','--no-password','-U','bigbrotr_control','-d',db,
            '-v','ON_ERROR_STOP=1','-v','repeatable='+str(repeatable).lower(),
            '-v','collision='+str(name=='collision').lower(),data=fixture.read_text(),check=False,timeout=45)
        (output/(name+'.stdout')).write_text(result.stdout);(output/(name+'.stderr')).write_text(result.stderr)
        summary=[json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
        # Independent literal document at the exact SQL byte boundary.
        prefix=b'{"version":1,"filters":[{"#t":["';suffix=b'"]}],"history_since":0,"ephemeral":"retain"}'
        count,odd=divmod(2097152-len(prefix)-len(suffix),2)
        document=prefix+b'\\n'*count+b'a'*odd+suffix
        require(len(document)==2097152 and len(json.loads(document)['filters'][0]['#t'][0])<=1048576,'independent-max-document')
        wanted=dict(status='PASS',**expected,new_document_length=2097152 if name=='basic' else None,
            new_document_sha256=hashlib.sha256(document).hexdigest() if name=='basic' else None)
        after=rows(db);lab.write_json(output/(name+'-after.json'),after)
        require(result.returncode==0 and summary==[wanted],'sql-guards-'+name)
        require(after==before,'complete-rollback-'+name)
        require(lab.sql(pg,db,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0','unjoined-sql-backend')
        report['cases'].append(dict(name=name,status='PASS',summary=summary[0],all66_tables_unchanged=True));save()
    for name in ['basic','collision']:
        require(rows('bb_scope_54d1a18_'+name)==json.loads((basis/(name+'-after.json')).read_text()),'retained-basis-after')
    require((basis/'result.json').read_bytes()==original_report,'original-report-changed')
    report['status']='PASS_SCOPED_ACQUISITION_SQL_GUARDS'
except BaseException as error:
    report.update(status='FAIL',failure=type(error).__name__+': '+str(error));raise
finally:
    # No transcript write can bypass this stop. Only the ownership-verified PG
    # is touched; all clones/results remain, including an interrupted outcome.
    errors=[]
    try:
        item=json.loads(lab.docker('inspect',target['Id']).stdout)[0]
        require(item['Config']['Labels'][lab.LABEL]==pg['owner'],'postgres-ownership')
        if item['State']['Running']:lab.docker('stop','--time','10',target['Id'])
    except BaseException as error:
        errors.append('postgres-'+type(error).__name__)
    try:
        after={c['Id']:projection(c) for c in containers()}
    except BaseException as error:
        after={};errors.append('inventory-'+type(error).__name__)
    terminal=after.get(target['Id'])
    report.update(terminal=terminal,cleanup_errors=errors,
        missing_owned=[] if terminal else [target['Id']],
        unrelated_changed=[c['Id'] for c in prior if c['Id']!=target['Id'] and after.get(c['Id'])!=projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_after=shutil.disk_usage(root).free)
    if errors or terminal is None or report['unrelated_changed'] or any(
        terminal['State'][key] for key in ['Running','Pid','OOMKilled','ExitCode']):
        report['status']='INCOMPLETE_LAB_AUDIT'

    save()
require(report['status']=='PASS_SCOPED_ACQUISITION_SQL_GUARDS','incomplete-sql-guards')
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))))
