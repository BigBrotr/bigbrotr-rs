"""Installed finite intent on the original native/Control owner; no dispatch claim."""
import hashlib
import json
from pathlib import Path
import shutil
import lab
from installed import messages, native_oracle, policy_setup, set_value, start, stop, terminal, wait_for
from sessions import configuration
from registrations import upgrade as upgrade_registry
from scopes import upgrade as upgrade_scopes

CASES = ['jobs-journey', 'jobs-detail-privilege', 'jobs-admission-privilege', 'jobs-digest-privilege']
ACTOR_CASES = ['jobs-other-actor']
EXPIRY_CASES = ['jobs-idle-expiry', 'jobs-absolute-expiry']
GAP_CASES = ['jobs-gap-legacy-grants', 'jobs-gap-uncorrected']
EFFECTS = {'jobs', 'acquisition_jobs', 'job_admissions', 'management_commands', 'management_audit'}


def unchanged(before, after, exceptions=()):
    assert set(before)==set(after)
    for table in before:
        if table=='owner_epochs':
            strip=lambda rows:[{k:v for k,v in r.items() if k not in ('lease_expires_at_us','xmin')} for r in rows]
            assert strip(before[table])==strip(after[table]), table
        elif table not in exceptions:
            assert before[table]==after[table], table


def logged_out(before, after):
    unchanged(before, after, ('browser_sessions', 'management_audit'))
    mine = lambda rows: [r for r in rows if r['principal_id']=='\\x'+'10'*16]
    other = lambda rows: [r for r in rows if r['principal_id']!='\\x'+'10'*16]
    assert other(before['browser_sessions']) == other(after['browser_sessions'])
    old, = mine(before['browser_sessions']); new, = mine(after['browser_sessions'])
    assert old['revoked_at_us'] is None and new['revoked_at_us'] >= old['issued_at_us']
    strip = lambda r: {k:v for k,v in r.items() if k not in ('xmin', 'revoked_at_us')}
    assert strip(old) == strip(new)
    assert all(r in after['management_audit'] for r in before['management_audit'])
    audit, = [r for r in after['management_audit'] if r not in before['management_audit']]
    assert audit['created_at_us'] >= new['revoked_at_us']
    assert {k:v for k,v in audit.items() if k not in ('xmin','audit_id','created_at_us')} == dict(
        dataset_id=old['dataset_id'], actor_id=old['principal_id'], action='browser.session.revoke',
        outcome='accepted', config_revision=1, policy_revision=1, subject_id=old['session_id'],
        reason=None, requester_database_role=None, requester_os_uid=None)


def released(before, after, *, initial=True):
    # Only Control retires. Initial Ingest remains registered for explicit recovery.
    assert set(before) == set(after)
    for table in before:
        if table not in ('owner_epochs', 'role_instances'):
            assert before[table] == after[table], table
    count = 2 if initial else 1
    assert len(before['owner_epochs']) == len(after['owner_epochs']) == count
    assert len(before['role_instances']) == len(after['role_instances']) == count
    assert {r['role'] for r in before['owner_epochs']} == ({'control','ingest'} if initial else {'control'})
    for old in before['owner_epochs']:
        new, = [r for r in after['owner_epochs'] if r['resource_id'] == old['resource_id']]
        omit = {'xmin', 'lease_expires_at_us'}
        if old['role'] == 'control':
            assert old['state'] == 'active' and new['state'] == 'released'
            assert old['released_at_us'] is None and new['released_at_us'] >= old['acquired_at_us']
            digest = hashlib.sha256(b'bigbrotr/control-exclusion/1\0' +
                bytes.fromhex(old['dataset_id'][2:]) + bytes.fromhex(old['resource_id'][2:]) +
                int(old['epoch']).to_bytes(8,'big') + bytes.fromhex(old['process_incarnation'][2:])*2).hexdigest()
            assert new['exclusion_method'] == 'storage_fenced' and new['exclusion_digest'] == '\\x'+digest
            omit |= {'state', 'released_at_us', 'exclusion_method', 'exclusion_digest'}
        assert {k:v for k,v in old.items() if k not in omit} == {k:v for k,v in new.items() if k not in omit}
    for old in before['role_instances']:
        new, = [r for r in after['role_instances'] if r['role_instance_id'] == old['role_instance_id']]
        if old['role'] == 'control':
            assert old['retired_at_us'] is None and new['retired_at_us'] >= old['created_at_us']
            omit = {'xmin', 'retired_at_us'}
            assert {k:v for k,v in old.items() if k not in omit} == {k:v for k,v in new.items() if k not in omit}
        else:
            assert old == new


def acceptance(before, after, observations):
    unchanged(before, after, EFFECTS)
    receipt,=[v['body'] for v in observations if v['path']=='jobs' and v['status']==202]
    job,=after['jobs']; detail,=after['acquisition_jobs']; admission,=after['job_admissions']
    assert not any(before[t] for t in ('jobs','acquisition_jobs','job_admissions'))
    command,=[v for v in after['management_commands'] if v not in before['management_commands']]
    audit,=[v for v in after['management_audit'] if v not in before['management_audit']]
    assert all(v in after['management_commands'] for v in before['management_commands'])
    assert all(v in after['management_audit'] for v in before['management_audit'])
    validate_job(after, receipt)


def validate_job(state, receipt, key='b1'*16, *, expected_principal='10'*16):
    job,=state['jobs']; detail,=state['acquisition_jobs']; admission,=state['job_admissions']
    command,=[r for r in state['management_commands'] if r['request_key']=='\\x'+key]
    audit,=[r for r in state['management_audit'] if r['audit_id']==command['audit_id']]
    dataset=state['datasets'][0]['dataset_id']
    assert len(expected_principal) == 32 and len(bytes.fromhex(expected_principal)) == 16
    actor,=[r for r in state['local_accounts'] if r['principal_id']=='\\x'+expected_principal]
    scope,=state['acquisition_scopes']
    session,=[r for r in state['browser_sessions'] if r['session_id']==detail['session_id']]
    assert session['principal_id']==actor['principal_id'] and session['dataset_id']==dataset
    key='\\x'+key; ident='\\x'+receipt['job_id']; accepted=int(receipt['accepted_at_us'])
    preimage=(b'bigbrotr/acquisition-job/1\0'+bytes.fromhex(dataset[2:])+
        bytes.fromhex(actor['principal_id'][2:])+bytes.fromhex(key[2:])+bytes.fromhex(scope['scope_id'][2:])+
        (120000000).to_bytes(8,'big')+(1).to_bytes(8,'big')*2)
    digest='\\x'+hashlib.sha256(preimage).hexdigest()
    clean=lambda r:{k:v for k,v in r.items() if k!='xmin'}
    assert clean(job)==dict(job_id=ident,dataset_id=dataset,principal_id=actor['principal_id'],
        request_key=key,request_digest=digest,kind='acquisition',config_revision=1,policy_revision=1,
        created_at_us=accepted,state='accepted',job_revision=1,checkpoint_revision=0,reason_code=None,
        result_manifest_digest=None,owner_resource_id=None,owner_epoch=None,owner_role='control',
        owner_incarnation=None,completed_at_us=None,input_topology_generation=None,input_complete=True,
        export_job_protocol=None,acquisition_job_protocol=1)
    assert clean(detail)==dict(job_id=ident,dataset_id=dataset,actor_id=actor['principal_id'],request_key=key,
        scope_id=scope['scope_id'],duration_us=120000000,deadline_at_us=accepted+120000000,
        session_id=session['session_id'],actor_credential_revision=actor['credential_revision'],
        actor_authentication_revision=actor['authentication_revision'],actor_grant_revision=actor['grant_revision'])
    assert clean(admission)==dict(job_id=ident,admission_ordinal=1)
    assert clean(command)==dict(dataset_id=dataset,actor_id=actor['principal_id'],request_key=key,
        request_digest=digest,action='job.acquisition.accept',accepted_at_us=accepted,
        expected_config_revision=1,policy_revision=1,audit_id=audit['audit_id'],
        authority_mode=None,document_digest=None)
    assert clean(audit)==dict(dataset_id=dataset,actor_id=actor['principal_id'],audit_id=command['audit_id'],
        action='job.acquisition.accept',outcome='accepted',subject_id=key,created_at_us=accepted,
        config_revision=1,policy_revision=1,reason=None,requester_os_uid=None,requester_database_role=None)
    assert receipt==dict(format='bigbrotr-job-acceptance/1',job_id=ident[2:],command_key=key[2:],
        accepted_at_us=str(accepted),created=True,execution='unobserved',authority='acceptance-only')


def upgrade_gap_schema(pg, client, output, database, name, rows, *, target):
    """Real additive migrations with original row/xmin/filenode preservation."""
    assert target in (27, 28)
    state = dict(pg, client=client)
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    source = Path(__file__).resolve().parents[2]
    def physical():
        return json.loads(lab.sql(pg, database, """
            SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='control' AND c.relkind='r'
        """))
    for version, filename in [(27, '0027_acquisition_abandonment.sql'),
            (28, '0028_acquisition_gap_guard.sql')][:target-26]:
        before = rows(database); files = physical()
        def apply():
            return lab.cli(state, 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator',
                '--target', str(version))
        response = apply()
        lab.write_json(output / f'{name}-migration-{version}.json', dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr))
        assert response.returncode == 0, response.stderr
        after = rows(database); current_files = physical()
        assert set(after) - set(before) == ({'acquisition_gaps'} if version == 27 else set())
        assert not after['acquisition_gaps']
        assert all(after[t] == before[t] for t in before if t != 'schema_versions')
        added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
        assert len(added) == 1 and added[0]['migration'] == version
        assert added[0]['checksum'] == '\\x' + hashlib.sha256((source/'migrations/postgres'/filename).read_bytes()).hexdigest()
        assert all(r in after['schema_versions'] for r in before['schema_versions'])
        assert all(current_files[t] == node for t, node in files.items())
        repeat = apply()
        assert repeat.returncode == 0 and rows(database) == after and physical() == current_files
        lab.write_json(output / f'{name}-upgrade-{version}.json', dict(before=before, after=after,
            physical_before=files, physical_after=current_files, exact_reapply=True))
    # Ordinary acceptance retains its original grants. Recovery's table is not
    # a prerequisite for this consumer, even when the schema exists.
    assert lab.sql(pg, database, "SELECT has_table_privilege('bigbrotr_control', 'control.acquisition_gaps', 'SELECT')") == 'f'


def run(pg,client,output,database,name,rows):
    assert name in CASES + ACTOR_CASES + EXPIRY_CASES + GAP_CASES
    upgrade_registry(pg,client,output,database,name,rows)
    upgrade_scopes(pg,client,output,database,name,rows)
    if name in GAP_CASES:
        upgrade_gap_schema(pg, client, output, database, name, rows,
            target=27 if name == 'jobs-gap-uncorrected' else 28)
    # Explicit consumer grants on this clone. These are not applied migrations.
    lab.sql(pg,database,"""
        GRANT SELECT,INSERT ON control.acquisition_jobs TO bigbrotr_control;
        GRANT EXECUTE ON FUNCTION control.acquisition_command_digest(
            control.acquisition_jobs,control.management_commands) TO bigbrotr_control;
        INSERT INTO control.principal_grants
            SELECT dataset_id,principal_id,a,'dataset',dataset_id
            FROM control.local_accounts CROSS JOIN (VALUES ('roles.control'),('status.read')) v(a);
    """)
    if name == 'jobs-other-actor':
        # Explicit clone fixture identity, not an account-enrollment claim. All
        # ordinary credential/account/session constraints remain enabled.
        lab.sql(pg,database,"""
            BEGIN;
            INSERT INTO control.principals
                SELECT decode(repeat('20',16),'hex'),dataset_id,'local',created_at_us,database_role,local_os_uid
                FROM control.principals WHERE principal_id=decode(repeat('10',16),'hex');
            INSERT INTO control.local_accounts
                SELECT decode(repeat('20',16),'hex'),dataset_id,'bob',false,true,1,1,1
                FROM control.local_accounts WHERE principal_id=decode(repeat('10',16),'hex');
            INSERT INTO control.local_credentials
                SELECT decode(repeat('20',16),'hex'),dataset_id,credential_revision,format,
                    argon_version,memory_kib,passes,lanes,salt,password_hash
                FROM control.local_credentials WHERE principal_id=decode(repeat('10',16),'hex');
            INSERT INTO control.principal_grants
                SELECT dataset_id,principal_id,'status.read','dataset',dataset_id
                FROM control.local_accounts WHERE username='bob';
            COMMIT;
        """)
    policy_setup(pg,database,name,rows,output,administration=True)
    if name in EXPIRY_CASES:
        # Short valid issuance in this clone only. Ordinary session guards and
        # audit constraints run afterwards. Defaults stay8h absolute/30min idle;
        # this tests reads against stored expiry, not configurable login TTL.
        absolute_us = 20_000_000 if name=='jobs-idle-expiry' else 8_000_000
        lab.sql(pg,database,f"""
            CREATE SCHEMA job_expiry_fixture;
            CREATE FUNCTION job_expiry_fixture.shorten() RETURNS trigger
            LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                NEW.expires_at_us := NEW.issued_at_us + {absolute_us};
                NEW.idle_window_us := 8000000;
                NEW.idle_expires_at_us := NEW.issued_at_us + 8000000;
                RETURN NEW;
            END $$;
            CREATE TRIGGER aa_job_expiry_fixture BEFORE INSERT ON control.browser_sessions
                FOR EACH ROW EXECUTE FUNCTION job_expiry_fixture.shorten();
            GRANT USAGE ON SCHEMA job_expiry_fixture TO bigbrotr_control;
            GRANT EXECUTE ON FUNCTION job_expiry_fixture.shorten() TO bigbrotr_control;
        """)
    faults={
        'jobs-detail-privilege':'REVOKE INSERT ON control.acquisition_jobs FROM bigbrotr_control',
        'jobs-admission-privilege':'REVOKE INSERT ON control.job_admissions FROM bigbrotr_control',
        'jobs-digest-privilege':'REVOKE EXECUTE ON FUNCTION control.acquisition_command_digest(control.acquisition_jobs,control.management_commands) FROM bigbrotr_control',
    }
    if name in faults: lab.sql(pg,database,faults[name])
    refuse = name in faults or name == 'jobs-gap-uncorrected'
    before=rows(database);lab.write_json(output/(name+'-before.json'),before)
    state=dict(pg,client=client)
    config=lab.config(output,state,database,endpoint='127.0.0.1:5432',boundary='loopback')
    path=output/Path(config).name
    text=configuration(path.read_text(),before,name)
    rule=[dict(endpoint='ws://127.0.0.1:7447/installed-fixture',cidr='127.0.0.1/32',port=7447)]
    text=set_value(text,'network.private','rules',rule)
    path.write_text(text.replace(json.dumps(rule),'[{endpoint="ws://127.0.0.1:7447/installed-fixture",cidr="127.0.0.1/32",port=7447}]'))
    validated=lab.cli(state,'config','validate','--file',config)
    (output/(name+'-validate.stdout')).write_text(validated.stdout)
    (output/(name+'-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode==0,validated.stderr
    for filename in ('installed_client.py','session_client.py','job_client.py','scope_http_client.py'):
        if not (output/filename).exists(): shutil.copy2(Path(__file__).with_name(filename),output/filename)
    directory=output/name;directory.mkdir(mode=0o700)
    prefix=start(client,config,output,name)
    first=wait_for(lambda:messages(prefix) or Path(str(prefix)+'.terminal.json').exists(),'finite listener')
    initialized=None
    if refuse:
        if messages(prefix):
            stop(client,prefix);terminal(prefix)
            raise RuntimeError('missing finite job prerequisite bound listener: '+name)
        ended=terminal(prefix)
        assert ended['exit_code']==4
        assert 'initial acquisition job prerequisites unavailable: Prerequisite' in Path(str(prefix)+'.stderr').read_text()
        lab.docker('exec',client,'python3','-c','import socket;s=socket.socket();assert s.connect_ex(("127.0.0.1",18081))!=0;s.close()')
    else:
        assert isinstance(first,list) and len(first)==1 and first[0]['state']=='initialized',first
        initialized=first[0]
        assert initialized['ready'] is False and initialized['acquisition']=='not-started'
        lab.write_json(directory/'visibility.json',dict(initialized['public_scope'],ordinal='2'))
        wrapper='''import subprocess,sys,json
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 r=subprocess.run(['python3','/lab/session_client.py',str(p),'jobs'],stdout=out,stderr=err,timeout=75)
(p/'client-exit.json').write_text(json.dumps({'exit_code':r.returncode}))
'''
        lab.docker('exec','-d',client,'python3','-c',wrapper,'/lab/'+name)
        previous=None;accepted=None
        stages = ('before-refusals','before-acceptance','accepted') + (
            ('before-expiry','expired-refusals') if name in EXPIRY_CASES
            else ('replayed','permission-withdrawn','status-withdrawn'))
        for stage in stages:
            wait_for(lambda:(directory/(stage+'.json')).exists() or (directory/'client-exit.json').exists(),stage,18)
            assert (directory/(stage+'.json')).exists(),(directory/'client.stderr').read_text()
            current=rows(database);lab.write_json(output/(name+'-'+stage+'-sql.json'),current)
            observed=json.loads((directory/(stage+'.json')).read_text())
            if stage=='accepted':
                acceptance(previous,current,observed);accepted=current
            elif previous is not None:
                unchanged(previous,current)
            if stage=='before-expiry':
                session,=current['browser_sessions']
                assert session['idle_window_us']==8000000
                assert session['expires_at_us']-session['issued_at_us']==absolute_us
                end=min(session['idle_expires_at_us'],session['expires_at_us'])
                clock=lambda:int(lab.sql(pg,database,
                    'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
                started=clock();assert started<end, 'no live session before expiry wait'
                def expired_time():
                    now=clock()
                    return now if now>=end else None
                observed_now=wait_for(expired_time,'stored session expiry',12)
                if name=='jobs-idle-expiry':
                    assert observed_now<session['expires_at_us']
                else:
                    assert end==session['expires_at_us']
                lab.write_json(output/(name+'-expiry-clock.json'),dict(started_at_us=started,
                    observed_at_us=observed_now,expires_at_us=session['expires_at_us'],
                    idle_expires_at_us=session['idle_expires_at_us']))
            if stage=='expired-refusals':
                # Bound the whole HTTP refusal interval, not only its start:
                # absolute expiry must not accidentally explain the idle case.
                finished=clock();assert finished>=end
                if name=='jobs-idle-expiry': assert finished<session['expires_at_us']
                lab.write_json(output/(name+'-expiry-refusals-clock.json'),dict(
                    observed_at_us=finished,stored_end_us=end,absolute_end_us=session['expires_at_us']))
            if stage in ('replayed','permission-withdrawn'):
                action='roles.control' if stage=='replayed' else 'status.read'
                lab.sql(pg,database,f"""BEGIN;
                    SELECT 1 FROM control.local_accounts WHERE principal_id=decode('{'10'*16}','hex') FOR UPDATE;
                    DELETE FROM control.principal_grants WHERE principal_id=decode('{'10'*16}','hex') AND action='{action}';
                    UPDATE control.local_accounts SET grant_revision=grant_revision+1 WHERE principal_id=decode('{'10'*16}','hex');
                    COMMIT;""")
                written=rows(database);unchanged(current,written,('local_accounts','principal_grants'))
                expected=[r for r in current['principal_grants']
                    if r['action']!=action or r['principal_id']!='\\x'+'10'*16]
                assert written['principal_grants']==expected
                expected_accounts=[dict(r,grant_revision=r['grant_revision']+1)
                    if r['principal_id']=='\\x'+'10'*16 else r for r in current['local_accounts']]
                # Capture ordering includes xmin, so Alice's real UPDATE can
                # reorder these rows. Compare by immutable principal identity;
                # Bob's row, including xmin, must remain byte-for-byte exact.
                clean=lambda rows:{r['principal_id']:({k:v for k,v in r.items() if k!='xmin'}
                    if r['principal_id']=='\\x'+'10'*16 else r) for r in rows}
                assert clean(written['local_accounts'])==clean(expected_accounts)
                lab.write_json(output/(name+'-'+stage+'-writer.json'),written);current=written
            previous=current
            (directory/(stage+'.continue')).write_text('observed\n')
        wait_for((directory/'client-exit.json').exists,'finite client terminal',15)
        assert json.loads((directory/'client-exit.json').read_text())['exit_code']==0,(directory/'client.stderr').read_text()
        before_stop=rows(database);lab.write_json(output/(name+'-before-stop.json'),before_stop)
        logged_out(previous,before_stop)
        stop(client,prefix);ended=terminal(prefix)
        assert ended['exit_code']==0 and not Path(str(prefix)+'.stderr').read_text()
        assert messages(prefix)[-1]['owner_release']=='committed'
    after=rows(database);lab.write_json(output/(name+'-after.json'),after)
    if initialized:
        released(before_stop,after)
        native_oracle(pg,client,output,database,name,after,initialized)
        for table in EFFECTS-{'management_audit'}: assert after[table]==accepted[table],table
        mine,=[r for r in after['browser_sessions'] if r['principal_id']=='\\x'+'10'*16]
        assert mine['revoked_at_us'] is not None
        assert len(after['browser_sessions'])==(2 if name=='jobs-other-actor' else 1)
        actions=['browser.session.issue','browser.session.revoke','relay.endpoint.register',
            'relay.enrollment.change','acquisition.scope.register','job.acquisition.accept']
        if name=='jobs-other-actor': actions.append('browser.session.issue')
        assert sorted(r['action'] for r in after['management_audit'] if r not in before['management_audit'])==sorted(actions)
    else:
        from commits import native_oracle as retained_oracle
        retained_oracle(client,output,name,after,number=6,healthy=True,last_commit=6)
        for table in EFFECTS: assert after[table]==before[table],table
        assert not after['browser_sessions']
    assert lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
    return dict(name=name,status='PASS',actual_cli=True,actual_http=initialized is not None,
        before_bind_refusal=refuse,control_tables=len(after),terminal=ended)
