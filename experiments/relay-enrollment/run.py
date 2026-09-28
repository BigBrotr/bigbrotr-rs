#!/usr/bin/env python3
"""Actual no-auth enrollment adapter; independent SQL identity/effect oracles."""
import hashlib
import json
from pathlib import Path
import re
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
if receipt['source'] != revision:
    changed=lab.command(['git','-C',str(SOURCE),'diff','--name-only',receipt['source'],revision]).stdout.splitlines()
    assert changed and all(name.startswith(('docs/','experiments/')) for name in changed), changed
    # Reuse exact artifacts only for a fixture/documentation correction. Every
    # product, lock, migration, build input and tool remains byte-identical.

assert hashlib.sha256((build/'relay_enrollment_probe').read_bytes()).hexdigest() == receipt['probe_sha256']
report = dict(format='bigbrotr-relay-enrollment-native/1', source=revision, build=receipt,
              application_source=receipt['source'],status='RUNNING', cases=[], not_run=['held/lost COMMIT', 'expiry during accepting COMMIT',
              'concurrent lock schedules', 'installed HTTP/CLI enrollment', 'action issuer and restrictive drain',
              'signed socket acquisition', 'load and aged state', 'arm64', 'protected/multi-rule/overlay routes'])
def save():lab.write_json(root/'enrollment-result.json', report)
def record(name, condition, **details):
    report['cases'].append(dict(name=name,status='PASS' if condition else 'FAIL',**details));save()
    if not condition:raise RuntimeError(name)
def tables(state,db):
    names=json.loads(lab.sql(state,db,"SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    result={}
    for name in names:
        assert re.fullmatch('[a-z_]+',name)
        raw=lab.sql(state,db,f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM control.{name} t")
        result[name]=dict(rows=len(json.loads(raw)),sha256=hashlib.sha256(raw.encode()).hexdigest())
    return result

created=False
try:
    lab.setup(root,build,SOURCE,'enrollment_'+revision[:7],subnet='172.16.240.0/28');created=True
    state=lab.load(root);report['server']=state['server'];save()
    db='bb_enrollment'
    lab.sql(state,'postgres',f'CREATE DATABASE {db} OWNER bigbrotr_migrator')
    config=lab.config(root,state,db)
    local=root/Path(config).name
    text=lab.change(local.read_text(),'postgres.control','password_ref','file:/lab/secrets/application')
    text,count=re.subn(r'(?ms)^local_roles = \[.*?\]', 'local_roles = ["control"]',text);assert count==1
    local.write_text(text)
    outcome=lab.migrate(state,'apply',config,target=3)
    record('old33-prefix',outcome.returncode==0,stdout=outcome.stdout,stderr=outcome.stderr)
    old=json.loads(lab.sql(state,db,"SELECT json_agg(json_build_array(migration,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions"))
    baseline=json.loads(lab.command(['git','-C',str(SOURCE),'show','981c42e:migrations/manifest.json']).stdout)['application_order']
    expected=sorted([[m['version'],hashlib.sha256(lab.command(['git','-C',str(SOURCE),'show','981c42e:migrations/postgres/'+m['name']]).stdout.encode()).hexdigest()] for m in baseline])
    record('old-ledger-independent-checksums',old==expected,ledger=old)
    lab.sql(state,db,'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    for args in [('config','initialize'),('config','activate-initial','--request-id','77'*16,'--expected-revision','1')]:
        outcome=lab.cli(state,*args,'--file',config)
        record('-'.join(args[:2]),outcome.returncode==0,stdout=outcome.stdout,stderr=outcome.stderr)
    before=tables(state,db)
    outcome=lab.migrate(state,'apply',config,target=19)
    after=tables(state,db)
    added={'relay_access_contexts','network_routes','relay_enrollment_changes','relay_enrollments'}
    record('additive-upgrade-with-retained-content',outcome.returncode==0
           and set(after)-set(before)==added and all(after[k]==v for k,v in before.items() if k!='schema_versions')
           and all(after[k]['rows']==0 for k in added),stdout=outcome.stdout,stderr=outcome.stderr,before=before,after=after)
    outcome=lab.migrate(state,'apply',config,target=19)
    record('reapply-unchanged',outcome.returncode==0 and tables(state,db)==after,stdout=outcome.stdout,stderr=outcome.stderr)
    # Fixed independent Argon2 reference from the earlier credential fixture.
    # The probe must verify the real password and issue a real session; no fake bearer.
    lab.sql(state,db,"""
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'local',0,'fixture',0 FROM control.datasets;
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'alice',true,true,1,1 FROM control.datasets;
        INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash)
            SELECT principal_id,dataset_id,1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),
                decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex') FROM control.local_accounts;
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'policy.change','dataset',dataset_id FROM control.local_accounts;
        GRANT SELECT,INSERT ON control.relay_access_contexts,control.network_routes,control.relay_enrollment_changes TO bigbrotr_control;
        GRANT SELECT,INSERT,UPDATE ON control.relay_enrollments TO bigbrotr_control;
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;
    """)
    policy=json.loads(lab.sql(state,db,'SELECT document::text FROM control.config_revisions WHERE revision=1'))
    (root/'policy.json').write_text(json.dumps(policy,sort_keys=True,separators=(',',':')))
    before=tables(state,db)
    outcome=lab.docker('exec',state['client'],'/build/relay_enrollment_probe',db,'/lab/secrets/application','/lab/policy.json','/lab/secrets/bootstrap',check=False,timeout=120)
    (root/'probe.stdout').write_text(outcome.stdout);(root/'probe.stderr').write_text(outcome.stderr)
    cases=[json.loads(line) for line in outcome.stdout.splitlines() if line.startswith('{')]
    expected_cases=['private-enable','historical-retry-with-roots-removed','changed-key-digest-and-wider-prefix-refused',
                    'withdraw-without-root','old-enable-does-not-reapply','reenable-new-route-generation',
                    'stale-cas-noop-and-wrong-csrf-refused','shared-noauth-context-different-endpoint',
                    'different-visibility-is-another-enrollment',
                    'historical-acceptance-survives-permission-removal-fresh-work-refuses','expired-session-refused',
                    'audit-failure-rolls-back-poisons-and-joins-driver']
    record('actual-session-and-control-adapter',outcome.returncode==0 and [c['case'] for c in cases]==expected_cases and all(c['status']=='PASS' for c in cases),exit_code=outcome.returncode,cases=cases,stderr=outcome.stderr)
    rows=json.loads(lab.sql(state,db,"""
        SELECT json_agg(json_build_object('actor',encode(d.actor_id,'hex'),'key',encode(d.request_key,'hex'),
            'dataset',encode(d.dataset_id,'hex'),'endpoint',encode(d.endpoint_id,'hex'),'access',encode(d.access_scope_id,'hex'),
            'route',encode(d.route_id,'hex'),'domain',encode(a.allocation_domain,'hex'),'ordinal',a.ordinal::text,
            'enabled',d.enabled,'previous',d.expected_generation::text,'generation',d.generation::text,
            'config',c.expected_config_revision::text,'policy',c.policy_revision::text,
            'route_bytes',encode(r.route_bytes,'hex'),'digest',encode(c.request_digest,'hex'),
            'same_xact',d.xmin=c.xmin AND c.xmin=m.xmin,'audit',m.action=c.action AND m.created_at_us=c.accepted_at_us
                AND m.actor_id=c.actor_id AND m.subject_id=c.request_key AND m.dataset_id=c.dataset_id
                AND m.policy_revision=c.policy_revision AND m.config_revision=c.expected_config_revision)
            ORDER BY d.request_key)
        FROM control.relay_enrollment_changes d JOIN control.management_commands c USING(actor_id,request_key)
        JOIN control.management_audit m ON m.audit_id=c.audit_id
        JOIN control.relay_access_contexts a ON a.access_scope_id=d.access_scope_id
        JOIN control.network_routes r ON r.route_id=d.route_id
    """))
    for row in rows:
        data=b'bigbrotr/relay-enrollment-change/1\0'
        for name in ['dataset','actor','key','endpoint','dataset']:data+=bytes.fromhex(row[name])
        data+=b'\0\4'+bytes.fromhex(row['domain'])+int(row['ordinal']).to_bytes(8,'big')+bytes([row['enabled']])
        for value in [row['previous'] or '0',row['config'],row['policy']]:data+=int(value).to_bytes(8,'big')
        data+=bytes.fromhex(row['route_bytes'])
        assert hashlib.sha256(data).hexdigest()==row['digest']
    keyed={int(r['key'][:2],16):r for r in rows}
    record('independent-original-command-values', len(rows)==5
        and all(r['domain']=='37'*16 and r['ordinal']==('8' if int(r['key'][:2],16)==7 else '7')
            and r['route_bytes']==('0104180a020100' if int(r['key'][:2],16) in (1,3) else '00')
            and r['config']==r['policy']=='1' for r in rows))
    record('independent-command-digests-and-atomic-attribution',set(keyed)=={1,3,4,6,7}
        and all(r['same_xact'] and r['audit'] and r['actor']=='10'*16 for r in rows)
        and [(r['enabled'],r['previous'],r['generation']) for r in rows]==[(True,None,'1'),(False,'1','2'),(True,'2','3'),(True,None,'1'),(True,None,'1')],rows=rows)
    current=json.loads(lab.sql(state,db,"SELECT json_agg(json_build_array(encode(endpoint_id,'hex'),encode(access_scope_id,'hex'),encode(route_id,'hex'),generation::text,enabled) ORDER BY endpoint_id,access_scope_id) FROM control.relay_enrollments"))
    wanted=sorted([[keyed[k]['endpoint'],keyed[k]['access'],keyed[k]['route'],keyed[k]['generation'],True] for k in [4,6,7]])
    after=tables(state,db)
    record('independent-retained-contexts-routes-and-generations',current==wanted and after['relay_access_contexts']['rows']==2
        and after['network_routes']['rows']==3 and keyed[1]['access']==keyed[3]['access']==keyed[4]['access']==keyed[6]['access']
        and keyed[7]['access']!=keyed[1]['access'] and keyed[4]['route']==keyed[7]['route']
        and keyed[1]['route']==keyed[3]['route'],current=current)
    allowed={'owner_epochs','role_instances','relay_endpoints','login_buckets','browser_sessions','management_audit','management_commands'}|added
    changed={k for k in before if before[k]!=after[k]}
    record('whole-ledger-effects-are-contained',changed<=allowed,before=before,after=after,changed=sorted(changed))
    before=tables(state,db)
    lab.sql(state,db,'GRANT DELETE,TRUNCATE ON control.relay_access_contexts,control.network_routes,control.relay_enrollment_changes,control.relay_enrollments TO bigbrotr_control')
    outcome=lab.docker('exec','-i',state['postgres'],'psql','-X','-A','-t','-q','--no-password',
        '-U','bigbrotr_control','-d',db,'-v','ON_ERROR_STOP=1',data=(SOURCE/'experiments/relay-enrollment/guards.sql').read_text(),check=False)
    (root/'guards.stdout').write_text(outcome.stdout);(root/'guards.stderr').write_text(outcome.stderr)
    record('missing-details-immutability-and-rollback',outcome.returncode==0
        and 'expected enrollment refusals: 10' in outcome.stderr and tables(state,db)==before,
        exit_code=outcome.returncode,stderr=outcome.stderr)
    cases=[('00',True),('0104100a020000',True),('010420a9fea9fe',True),
           ('010610fd120000000000000000000000000000',True),('',False),('01',False),('0000',False),
           ('01042100000000',False),('0104100a020001',False),('01050000000000',False),
           ('01060000000000000000000000ffff00000000',False),('010610fd120000000000000000000000000001',False)]
    observed=json.loads(lab.sql(state,db,"SELECT json_agg(control.relay_route_valid(decode(value,'hex')) ORDER BY position) FROM (VALUES "
        +','.join("("+str(i)+",'"+value+"')" for i,(value,_) in enumerate(cases))+") c(position,value)"))
    record('independent-binary-route-validation',observed==[expected for _,expected in cases],cases=cases,observed=observed)
    report['status']='PASS_INITIAL_ENROLLMENT_BOUNDARY'
except BaseException as error:
    report['status']='FAIL';report['failure']=str(error);raise
finally:
    if created:save();lab.stop(root)
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))))
