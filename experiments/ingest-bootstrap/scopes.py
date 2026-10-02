"""Installed immutable scope acceptance and explicit additive schema upgrade."""
import hashlib
import json
from pathlib import Path
import shutil
import lab
from installed import messages, native_oracle, policy_setup, set_value, start, stop, terminal, wait_for
from sessions import configuration
from registrations import upgrade as upgrade_registry

CASES = ['scope-upgrade','scope-journey','scope-permission','scope-prerequisite',
         'scope-candidate','scope-digest','scope-schema']
SOURCE = Path(__file__).resolve().parents[2]
CANONICAL = '{"version":1,"filters":[{"kinds":[1,2],"#t":["a","β"]}],"history_since":0,"ephemeral":"retain"}'.encode()


def upgrade(pg, client, output, database, name, rows):
    before = rows(database)
    assert any(r['migration']==25 for r in before['schema_versions'])
    def physical():
        return json.loads(lab.sql(pg,database,"""
            SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='control' AND c.relkind='r'
        """))
    files=physical()
    state=dict(pg,client=client)
    config=lab.config(output,state,database,endpoint='127.0.0.1:5432',boundary='loopback')
    def apply():
        return lab.cli(state,'migrate','apply','--file',config,'--user','bigbrotr_migrator',
            '--password-ref','file:/run/secrets/migrator','--target','26')
    applied=apply()
    (output/(name+'-scope-migration.stdout')).write_text(applied.stdout)
    (output/(name+'-scope-migration.stderr')).write_text(applied.stderr)
    assert applied.returncode==0,applied.stderr
    after=rows(database)
    assert set(after)-set(before)=={'acquisition_scope_commands'} and not after['acquisition_scope_commands']
    assert all(after[t]==before[t] for t in before if t!='schema_versions')
    added=[r for r in after['schema_versions'] if r not in before['schema_versions']]
    assert len(added)==1 and added[0]['migration']==26
    assert added[0]['checksum']=='\\x'+hashlib.sha256((SOURCE/'migrations/postgres/0026_acquisition_scope_commands.sql').read_bytes()).hexdigest()
    assert all(r in after['schema_versions'] for r in before['schema_versions'])
    current_files=physical()
    assert all(current_files[t]==node for t,node in files.items())
    reapplied=apply()
    (output/(name+'-scope-reapply.stdout')).write_text(reapplied.stdout)
    (output/(name+'-scope-reapply.stderr')).write_text(reapplied.stderr)
    assert reapplied.returncode==0 and rows(database)==after and physical()==current_files
    lab.write_json(output/(name+'-scope-upgrade.json'),dict(before=before,after=after,
        physical_before=files,physical_after=current_files))
    lab.sql(pg,database,"""
        GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes,
            control.acquisition_scope_commands TO bigbrotr_control;
        GRANT EXECUTE ON FUNCTION control.acquisition_selector_candidate(bytea),
            control.acquisition_scope_digest(control.acquisition_scope_commands,control.management_commands)
            TO bigbrotr_control;
    """)
    return before,after


def scope_receipt_oracle(state, observations):
    """Bind every successful wire receipt to its exact independently read rows."""
    receipts = [r for r in observations if r['path']=='acquisition-scopes' and r['status'] in (200,202)]
    assert len(receipts)==9
    assert [(r['key'],r['status']) for r in receipts]==[
        ('a1'*16,202),('a1'*16,200),('a2'*16,202),('a3'*16,202),
        ('a1'*16,200),('a2'*16,200),('a3'*16,200),('a1'*16,200),('a1'*16,200)]
    for receipt in receipts:
        key='\\x'+receipt['key']
        command,=[r for r in state['management_commands'] if r['request_key']==key]
        detail,=[r for r in state['acquisition_scope_commands'] if r['request_key']==key]
        assert receipt['body']==dict(format='bigbrotr-acquisition-scope-registration/1',
            command_key=receipt['key'],scope_id=detail['scope_id'][2:],
            accepted_at_us=str(command['accepted_at_us']),created=receipt['status']==202,
            authority='identity-only')


def scope_oracle(state, observations):
    scope_receipt_oracle(state, observations)
    dataset=state['datasets'][0]['dataset_id'];actor=state['local_accounts'][0]
    session=state['browser_sessions'][0];owner=next(r for r in state['owner_epochs'] if r['role']=='control')
    assert len(state['acquisition_selectors'])==len(state['acquisition_scopes'])==1
    selector,=state['acquisition_selectors'];scope,=state['acquisition_scopes']
    endpoint,=state['relay_endpoints'];access,=state['relay_access_contexts']
    assert bytes.fromhex(endpoint['url_bytes'][2:])==b'ws://127.0.0.1:7447/installed-fixture'
    assert bytes.fromhex(selector['document'][2:])==CANONICAL
    assert selector['candidate_digest']=='\\x'+hashlib.sha256(CANONICAL).hexdigest()
    assert scope['selector_id']==selector['selector_id'] and scope['endpoint_id']==endpoint['endpoint_id']
    assert scope['access_scope_id']==access['access_scope_id']
    assert (scope['visibility_kind'],scope['visibility_domain'],scope['visibility_ordinal'])==(4,access['allocation_domain'],2)
    assert scope['visibility_ordinal']==access['ordinal'] and access['dictionary_kind']==4
    assert selector['profile_version']==1
    for row in [selector,scope]:
        assert row['dataset_id']==dataset and row['config_revision']==row['policy_revision']==1
        for stored,current in [('owner_resource_id','resource_id'),('owner_epoch','epoch'),('owner_incarnation','process_incarnation')]:
            assert row[stored]==owner[current]
    assert len(state['acquisition_scope_commands'])==3
    be=lambda n:n.to_bytes(8,'big')
    for nonce in ['a1','a2','a3']:
        key=bytes.fromhex(nonce*16);stored='\\x'+key.hex()
        detail=next(r for r in state['acquisition_scope_commands'] if r['request_key']==stored)
        command=next(r for r in state['management_commands'] if r['request_key']==stored)
        audit=next(r for r in state['management_audit'] if r['audit_id']==command['audit_id'])
        assert detail['scope_id']==scope['scope_id']
        assert detail['dataset_id']==command['dataset_id']==audit['dataset_id']==dataset
        assert detail['actor_id']==command['actor_id']==audit['actor_id']==actor['principal_id']
        assert command['action']==audit['action']=='acquisition.scope.register'
        assert command['expected_config_revision']==command['policy_revision']==1
        assert audit['config_revision']==audit['policy_revision']==1 and audit['outcome']=='accepted'
        assert audit['subject_id']==stored and audit['created_at_us']==command['accepted_at_us']
        assert detail['session_id']==session['session_id'] and detail['owner_role']=='control'
        for field in ['credential_revision','authentication_revision','grant_revision']:
            assert detail['actor_'+field]==actor[field]
        for stored_owner,current in [('owner_resource_id','resource_id'),('owner_epoch','epoch'),('owner_incarnation','process_incarnation')]:
            assert detail[stored_owner]==owner[current]
        assert selector['registered_at_us']<=scope['registered_at_us']<=command['accepted_at_us']
        original=(b'bigbrotr/acquisition-scope-registration/1\0'+bytes.fromhex(dataset[2:])
            +bytes.fromhex(actor['principal_id'][2:])+key+bytes.fromhex(endpoint['endpoint_id'][2:])
            +bytes.fromhex(access['access_scope_id'][2:])+bytes.fromhex(dataset[2:])+b'\0\4'
            +bytes.fromhex(access['allocation_domain'][2:])+be(2)+be(1)+be(1)
            +len(CANONICAL).to_bytes(4,'big')+CANONICAL)
        assert command['request_digest']=='\\x'+hashlib.sha256(original).hexdigest()


def run(pg,client,output,database,name,rows):
    assert name in CASES
    if not any(r['migration']==25 for r in rows(database)['schema_versions']):
        upgrade_registry(pg,client,output,database,name,rows)
    if name!='scope-schema':
        previous,upgraded=upgrade(pg,client,output,database,name,rows)
    if name=='scope-upgrade':
        assert previous['relay_registration_commands'] and previous['relay_enrollment_changes'] and previous['producer_requests']
        return dict(name=name,status='PASS',actual_upgrade=True,populated=True,
            original_rows_xmin_filenodes=True,control_tables=len(upgraded))
    for filename in ['installed_client.py','session_client.py','scope_client.py']:
        if not (output/filename).exists():shutil.copy2(Path(__file__).with_name(filename),output/filename)
    policy_setup(pg,database,name,rows,output,administration=True)
    if name=='scope-journey':
        lab.sql(pg,database,"""INSERT INTO control.principal_grants
            SELECT dataset_id,principal_id,'roles.control','dataset',dataset_id FROM control.local_accounts""")
    if name=='scope-prerequisite':
        lab.sql(pg,database,'REVOKE INSERT ON control.acquisition_scope_commands FROM bigbrotr_control')
    functions={'scope-candidate':'control.acquisition_selector_candidate(bytea)',
        'scope-digest':'control.acquisition_scope_digest(control.acquisition_scope_commands,control.management_commands)'}
    if name in functions:
        signature=functions[name]
        lab.sql(pg,database,f'REVOKE EXECUTE ON FUNCTION {signature} FROM bigbrotr_control')
        assert lab.sql(pg,database,f"SELECT has_function_privilege('bigbrotr_control','{signature}','EXECUTE')")=='f'
    before=rows(database)
    assert not before['owner_epochs'] and not before['relay_endpoints']
    lab.write_json(output/(name+'-before.json'),before)
    state=dict(pg,client=client)
    config=lab.config(output,state,database,endpoint='127.0.0.1:5432',boundary='loopback')
    path=output/Path(config).name
    text=configuration(path.read_text(),before,name)
    rule=[dict(endpoint='ws://127.0.0.1:7447/installed-fixture',cidr='127.0.0.1/32',port=7447)]
    text=set_value(text,'network.private','rules',rule)
    assert text.count(json.dumps(rule))==1
    path.write_text(text.replace(json.dumps(rule),'[{endpoint="ws://127.0.0.1:7447/installed-fixture",cidr="127.0.0.1/32",port=7447}]'))
    validated=lab.cli(state,'config','validate','--file',config)
    (output/(name+'-validate.stdout')).write_text(validated.stdout)
    (output/(name+'-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode==0,validated.stderr
    directory=output/name;directory.mkdir(mode=0o700)
    prefix=start(client,config,output,name)
    first=wait_for(lambda:messages(prefix) or Path(str(prefix)+'.terminal.json').exists(),'scope listener')
    initialized=None
    prerequisite=name not in ('scope-journey','scope-permission')
    if prerequisite:
        if messages(prefix):
            stop(client,prefix);terminal(prefix)
            lab.write_json(output/(name+'-unexpected-after.json'),rows(database))
            raise RuntimeError('missing scope prerequisite bound listener: '+name)
        ended=terminal(prefix)
        assert ended['exit_code']==4
        assert 'initial acquisition scope prerequisites unavailable: Prerequisite' in Path(str(prefix)+'.stderr').read_text()
        lab.docker('exec',client,'python3','-c',
            'import socket; s=socket.socket(); assert s.connect_ex(("127.0.0.1",18081))!=0; s.close()')
    else:
        assert isinstance(first,list) and len(first)==1 and first[0]['state']=='initialized',first
        initialized=first[0]
        assert initialized['listeners'][0]['profile']=='initial-source-commands'
        assert initialized['ready'] is False and initialized['acquisition']=='not-started'
        lab.write_json(output/(name+'-initialized.json'),rows(database))
        enrollment=dict(visibility=dict(initialized['public_scope'],ordinal='2'),
            route={'class':'private','cidr':'127.0.0.1/32'},enabled=True,
            expected_generation=None,expected_config_revision='1',expected_policy_revision='1')
        lab.write_json(directory/'command-input.json',dict(enrollment=enrollment,permitted=name=='scope-journey'))
        wrapper='''import subprocess,sys,json
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 r=subprocess.run(['python3','/lab/session_client.py',str(p),'scope'],stdout=out,stderr=err,timeout=25)
(p/'client-exit.json').write_text(json.dumps({'exit_code':r.returncode}))
'''
        lab.docker('exec','-d',client,'python3','-c',wrapper,'/lab/'+name)
        if name=='scope-journey':
            snapshots=[]
            for stage in ['before-replay','after-replay']:
                wait_for(lambda:(directory/(stage+'.json')).exists() or (directory/'client-exit.json').exists(),stage,15)
                assert (directory/(stage+'.json')).exists(),(directory/'client.stderr').read_text()
                observed=rows(database);lab.write_json(output/(name+'-'+stage+'-sql.json'),observed)
                snapshots.append(observed);(directory/(stage+'.continue')).write_text('observed\n')
            assert all(snapshots[0][t]==snapshots[1][t] for t in snapshots[0] if t!='owner_epochs')
            stable=lambda r:{k:v for k,v in r.items() if k not in ('lease_expires_at_us','xmin')}
            assert [stable(r) for r in snapshots[0]['owner_epochs']]==[stable(r) for r in snapshots[1]['owner_epochs']]
        wait_for((directory/'client-exit.json').exists,'scope HTTP terminal',25)
        assert json.loads((directory/'client-exit.json').read_text())['exit_code']==0,(directory/'client.stderr').read_text()
        stop(client,prefix);ended=terminal(prefix)
        assert ended['exit_code']==0 and not Path(str(prefix)+'.stderr').read_text(),ended
        assert messages(prefix)[-1]['owner_release']=='committed'
    after=rows(database);lab.write_json(output/(name+'-after.json'),after)
    assert len(after['owner_epochs'])==len(after['role_instances'])==2
    assert len(after['producer_requests'])==len(after['producer_outcomes'])==2
    if initialized:
        native_oracle(pg,client,output,database,name,after,initialized)
        assert len(after['browser_sessions'])==1 and after['browser_sessions'][0]['revoked_at_us'] is not None
    else:
        from commits import native_oracle as retained_oracle
        retained_oracle(client,output,name,after,number=6,healthy=True,last_commit=6)
        assert not after['browser_sessions']
    audits=[r for r in after['management_audit'] if r not in before['management_audit']]
    expected=[] if not initialized else ['browser.session.issue','browser.session.revoke','relay.endpoint.register','relay.enrollment.change']
    if name=='scope-journey':
        expected+=['acquisition.scope.register']*3+['relay.enrollment.change']
        result=json.loads((directory/'client-result.json').read_text())
        scope_oracle(after,result['observations'])
        assert len(after['relay_enrollment_changes'])==2
        assert after['relay_enrollments'][0]['generation']==2 and not after['relay_enrollments'][0]['enabled']
    else:
        for table in ['acquisition_selectors','acquisition_scopes','acquisition_scope_commands']:
            if table in before:assert after[table]==before[table]
    assert sorted(r['action'] for r in audits)==sorted(expected)
    assert all(r in after['management_audit'] for r in before['management_audit'])
    allowed={'datasets','owner_epochs','role_instances','topology_generations','topology_streams',
        'source_streams','allocation_domains','producer_slots','producer_epochs','producer_requests',
        'producer_outcomes'}
    if initialized:
        allowed.update({'browser_sessions','login_buckets','management_audit','management_commands',
            'relay_endpoints','relay_registration_commands','relay_enrollments','network_routes',
            'relay_access_contexts','relay_enrollment_changes'})
        assert len(after['relay_endpoints'])==len(after['relay_registration_commands'])==1
        assert len(after['relay_enrollments'])==len(after['network_routes'])==len(after['relay_access_contexts'])==1
        keys={'91'*16,'92'*16}
        if name=='scope-journey':
            keys.update({'93'*16,'a1'*16,'a2'*16,'a3'*16})
            allowed.update({'acquisition_selectors','acquisition_scopes','acquisition_scope_commands'})
        else:
            assert len(after['relay_enrollment_changes'])==1
            assert after['relay_enrollments'][0]['generation']==1 and after['relay_enrollments'][0]['enabled']
        assert all(r in after['management_commands'] for r in before['management_commands'])
        added=[r for r in after['management_commands'] if r not in before['management_commands']]
        assert len(added)==len(keys)
        assert {r['request_key'][2:] for r in added}==keys
    assert {t for t in before if before[t]!=after[t]}<=allowed
    assert lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
    return dict(name=name,status='PASS',actual_cli=True,actual_http=initialized is not None,
        startup_refusal=prerequisite,control_tables=len(after),terminal=ended)
