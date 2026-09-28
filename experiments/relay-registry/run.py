#!/usr/bin/env python3
"""Actual migration/Control adapter and independent PG observations, isolated only."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/control-migrations'))
import lab

root, build, source = map(Path, sys.argv[1:])
assert source == SOURCE
revision = lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD']).stdout.strip()
assert not lab.command(['git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all']).stdout
receipt = json.loads((build/'build-result.json').read_text())
assert receipt['source'] == revision
assert hashlib.sha256((build/'relay_registry_probe').read_bytes()).hexdigest() == receipt['probe_sha256']
report = dict(format='bigbrotr-relay-registry-native/1', source=revision, build=receipt,
              status='RUNNING', cases=[], not_run=['enrollment','route authority','socket acquisition','load','arm64','held/lost COMMIT','stale policy after lock wait','fresh-policy successor','concurrent callers','corrupt overfull bucket'])

def save():
    lab.write_json(root/'registry-result.json',report)

def require(condition, message):
    if not condition: raise RuntimeError(message)

def record(name, condition, **details):
    report['cases'].append(dict(name=name,status='PASS' if condition else 'FAIL',**details)); save()
    require(condition,name)

def table_state(state, db):
    names=json.loads(lab.sql(state,db,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    result={}
    for name in names:
        require(re.fullmatch('[a-z_]+',name),'table-name')
        raw=lab.sql(state,db,f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t")
        result[name]=dict(rows=len(json.loads(raw)),sha256=hashlib.sha256(raw.encode()).hexdigest())
    return result

def prepare(state, case):
    db='bb_registry_'+case
    lab.sql(state,'postgres',f'CREATE DATABASE {db} OWNER bigbrotr_migrator')
    path=lab.config(root,state,db)
    local=root/Path(path).name
    text=lab.change(local.read_text(),'postgres.control','password_ref','file:/lab/secrets/application')
    text,count=re.subn(r'(?ms)^local_roles = \[.*?\]', 'local_roles = ["control"]',text)
    require(count==1,'roles-config')
    local.write_text(text)
    result=lab.migrate(state,'apply',path,target=816)
    record('old-prefix-'+case,result.returncode==0,stdout=result.stdout,stderr=result.stderr)
    old=json.loads(lab.sql(state,db,"SELECT json_agg(json_build_array(migration,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions"))
    baseline=json.loads(subprocess.check_output(['git','-C',str(SOURCE),'show','eb4ba5a:migrations/manifest.json']))['application_order']
    expected=sorted([[m['version'],hashlib.sha256(subprocess.check_output(['git','-C',str(SOURCE),'show','eb4ba5a:migrations/postgres/'+m['name']])).hexdigest()] for m in baseline])
    record('independent-old-ledger-'+case,old==expected,ledger=old)
    lab.sql(state,db,'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    for arguments in [('config','initialize'),('config','activate-initial','--request-id','77'*16,'--expected-revision','1')]:
        result=lab.cli(state,*arguments,'--file',path)
        record('-'.join(arguments[:2])+'-'+case,result.returncode==0,stdout=result.stdout,stderr=result.stderr)
    before=table_state(state,db)
    result=lab.migrate(state,'apply',path,target=3)
    after=table_state(state,db)
    record('upgrade-'+case,result.returncode==0 and all(after[k]==v for k,v in before.items() if k!='schema_versions')
           and set(after)-set(before)=={'relay_endpoints'} and after['relay_endpoints']['rows']==0,
           stdout=result.stdout,stderr=result.stderr,before=before,after=after)
    repeated=lab.migrate(state,'apply',path,target=3)
    record('idempotent-migration-'+case,repeated.returncode==0 and table_state(state,db)==after,
           stdout=repeated.stdout,stderr=repeated.stderr)
    lab.sql(state,db,'GRANT SELECT,INSERT ON control.relay_endpoints TO bigbrotr_control; GRANT EXECUTE ON FUNCTION control.relay_endpoint_candidate(bytea),control.guard_relay_endpoint(),control.require_relay_registration_commit() TO bigbrotr_control;')
    policy=json.loads(lab.sql(state,db,'SELECT document::text FROM control.config_revisions WHERE revision=1'))
    policy_path=root/(case+'-policy.json'); policy_path.write_text(json.dumps(policy,sort_keys=True,separators=(',',':')))
    return db,'/lab/'+policy_path.name

created=False
try:
    lab.setup(root,build,SOURCE,'registry_'+revision[:7]); created=True
    state=lab.load(root); report['server']=state['server']; save()
    for case in ['basic','collision','expired']:
        db,policy=prepare(state,case)
        if case=='collision':
            # Disposable empty DB only. Keep its helper and generated values
            # together for reproducibility; never restore over populated rows.
            lab.sql(state,db,"CREATE OR REPLACE FUNCTION control.relay_endpoint_candidate(url_bytes bytea) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path=pg_catalog AS $$ SELECT decode(repeat('42',32),'hex') $$")
        before=table_state(state,db)
        result=lab.docker('exec',state['client'],'/build/relay_registry_probe',db,
                          '/lab/secrets/application',policy,case,check=False,timeout=90)
        (root/(case+'.stdout')).write_text(result.stdout); (root/(case+'.stderr')).write_text(result.stderr)
        record('actual-adapter-'+case,result.returncode==0,exit_code=result.returncode,stdout=result.stdout,stderr=result.stderr)
        value=json.loads(result.stdout)
        rows=json.loads(lab.sql(state,db,"SELECT coalesce(json_agg(json_build_object('id',encode(endpoint_id,'hex'),'url',encode(url_bytes,'hex'),'candidate',encode(candidate_digest,'hex'),'config',config_revision,'policy',policy_revision,'epoch',owner_epoch,'time',registered_at_us) ORDER BY url_bytes),'[]') FROM control.relay_endpoints"))
        if case=='basic':
            short=['wss://fixture.example/a/~?b=2&a=1','wss://fixture.example/a/~?a=1&b=2','ws://fixture.example/a/~?b=2&a=1','wss://fixture.example/b/~?b=2&a=1']
            long=('wss://fixture.example/'+''.join(hashlib.sha256(n.to_bytes(4,'big')).hexdigest() for n in range(256)))[:8192]
            wanted={url.encode().hex():identity for url,identity in zip(short+[long],value['short_ids']+[value['long_id']],strict=True)}
            ok=len(rows)==5 and all(wanted.get(r['url'])==r['id'] and r['candidate']==hashlib.sha256(bytes.fromhex(r['url'])).hexdigest() and r['config']==r['policy']==r['epoch']==1 for r in rows)
        elif case=='collision':
            wanted={f'wss://fixture.example/collision/{n}'.encode().hex() for n in range(64)}
            ok=len(rows)==64 and {r['url'] for r in rows}==wanted and len({r['id'] for r in rows})==64 and all(r['candidate']=='42'*32 for r in rows)
        else: ok=rows==[]
        after=table_state(state,db)
        changed={k for k in before if before[k]!=after[k]}
        allowed={'owner_epochs','role_instances','relay_endpoints'}
        record('independent-effects-'+case,ok and changed <= allowed,
               endpoints=rows,changed=sorted(changed),before=before,after=after)
    db='bb_registry_basic'
    oversized=lab.docker('exec','-i',state['postgres'],'psql','-X','-A','-t','-q','--no-password',
        '-U','postgres','-d',db,'-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose',
        data='CREATE UNIQUE INDEX fixture_full_url_key ON control.relay_endpoints(url_bytes)',check=False)
    record('full-url-btree-negative-control',oversized.returncode!=0 and '54000' in oversized.stderr,
           exit_code=oversized.returncode,stderr=oversized.stderr)
    before=table_state(state,db)
    lab.sql(state,db,'GRANT UPDATE,DELETE,TRUNCATE ON control.relay_endpoints TO bigbrotr_control')
    fixture=(SOURCE/'experiments/relay-registry/guards.sql').read_text()
    result=lab.docker('exec','-i',state['postgres'],'psql','-X','-A','-t','-q','--no-password',
                     '-U','postgres','-d',db,'-v','ON_ERROR_STOP=1',data=fixture,check=False)
    (root/'guards.stdout').write_text(result.stdout); (root/'guards.stderr').write_text(result.stderr)
    summaries=[json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    record('actual-sql-guards-and-rollback',result.returncode==0 and summaries==[
        dict(status='PASS',expected_refusals=8,uncommitted_rows=6)] and table_state(state,db)==before,
        exit_code=result.returncode,summary=summaries,stderr=result.stderr)
    report['status']='PASS_SCOPED_ENDPOINT_REGISTRY'
except BaseException as error:
    report['status']='FAIL'; report['failure']=str(error)
    raise
finally:
    if created:
        save(); lab.stop(root)
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))))
