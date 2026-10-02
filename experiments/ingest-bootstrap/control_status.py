"""Pre0022 ordinary job query compatibility; no archive/export execution claim."""
import json
from pathlib import Path
import shutil
import lab
from initial_jobs import unchanged, logged_out, released
from installed import messages, policy_setup, set_value, stop, terminal, wait_for
from sessions import configuration

CASES = ['export-status-pre0022']


def run(pg,client,output,database,name,rows):
    assert name in CASES
    original=rows(database)
    assert len(original)==64 and not original['jobs'] and not original['owner_epochs']
    versions={r['migration'] for r in original['schema_versions']}
    assert 813 in versions and 22 not in versions and 'acquisition_jobs' not in original
    assert lab.sql(pg,database,"""SELECT count(*) FROM information_schema.columns
        WHERE table_schema='control' AND table_name='jobs' AND column_name='acquisition_job_protocol'""")=='0'
    lab.sql(pg,database,"""INSERT INTO control.principal_grants
        SELECT dataset_id,principal_id,'status.read','dataset',dataset_id FROM control.local_accounts""")
    policy_setup(pg,database,name,rows,output,administration=True)
    before=rows(database);lab.write_json(output/(name+'-before.json'),before)
    state=dict(pg,client=client)
    config=lab.config(output,state,database,endpoint='127.0.0.1:5432',boundary='loopback')
    path=output/Path(config).name
    text=configuration(path.read_text(),before,name)
    path.write_text(set_value(text,'topology','local_roles',['control']))
    validated=lab.cli(state,'config','validate','--file',config)
    (output/(name+'-validate.stdout')).write_text(validated.stdout)
    (output/(name+'-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode==0,validated.stderr
    for filename in ('installed_client.py','session_client.py','control_status_client.py'):
        shutil.copy2(Path(__file__).with_name(filename),output/filename)
    directory=output/name;directory.mkdir(mode=0o700)
    prefix=output/name
    lab.docker('exec','-d',client,'python3','/lab/installed_client.py',config,'/lab/'+name,'control')
    wait_for(lambda:Path(str(prefix)+'.pid.json').exists(),'ordinary Control process')
    try:
        first=wait_for(lambda:messages(prefix) or Path(str(prefix)+'.terminal.json').exists(),'ordinary listener')
        assert isinstance(first,list) and len(first)==1 and first[0]['state']=='idle',first
        assert first[0]['native_stores']==[] and first[0]['export_execution']=='not-selected'
        assert first[0]['listeners'][0]['profile']=='trusted-host-loopback'
        wrapper='''import subprocess,sys,json
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 r=subprocess.run(['python3','/lab/session_client.py',str(p),'export-compat'],stdout=out,stderr=err,timeout=35)
(p/'client-exit.json').write_text(json.dumps({'exit_code':r.returncode}))
'''
        lab.docker('exec','-d',client,'python3','-c',wrapper,'/lab/'+name)
        previous=None
        for stage in ('before-reads','after-reads'):
            wait_for(lambda:(directory/(stage+'.json')).exists() or (directory/'client-exit.json').exists(),stage,18)
            assert (directory/(stage+'.json')).exists(),(directory/'client.stderr').read_text()
            current=rows(database);lab.write_json(output/(name+'-'+stage+'-sql.json'),current)
            if previous is not None:unchanged(previous,current)
            previous=current
            (directory/(stage+'.continue')).write_text('observed\n')
        wait_for((directory/'client-exit.json').exists,'ordinary client terminal',10)
        assert json.loads((directory/'client-exit.json').read_text())['exit_code']==0,(directory/'client.stderr').read_text()
        before_stop=rows(database);lab.write_json(output/(name+'-before-stop.json'),before_stop)
        logged_out(previous,before_stop)
        stop(client,prefix);ended=terminal(prefix)
        assert ended['exit_code']==0 and not Path(str(prefix)+'.stderr').read_text()
        assert messages(prefix)[-1]['release']=='committed'
    finally:
        if not Path(str(prefix)+'.terminal.json').exists():
            stop(client,prefix);terminal(prefix)
    after=rows(database);lab.write_json(output/(name+'-after.json'),after)
    released(before_stop,after,initial=False)
    allowed={'owner_epochs','role_instances','browser_sessions','login_buckets','management_audit'}
    assert {t for t in before if before[t]!=after[t]}<=allowed
    assert after['schema_versions']==original['schema_versions']
    assert not (directory/'store').exists()
    added=[r for r in after['management_audit'] if r not in before['management_audit']]
    assert all(r in after['management_audit'] for r in before['management_audit'])
    assert sorted(r['action'] for r in added)==['browser.session.issue','browser.session.revoke']
    assert lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")=='0'
    return dict(name=name,status='PASS',control_tables=len(after),actual_cli=True,actual_http=True,
        acquisition_schema_absent=True,native_opened=False,terminal=ended)
