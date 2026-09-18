"""Bounded real-PG FK lock schedules against the exact activation executable."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
p = argparse.ArgumentParser(); p.add_argument('--root', type=Path, required=True); p.add_argument('--source', type=Path, required=True)
a = p.parse_args(); root = a.root.resolve(strict=True); source = a.source.resolve(strict=True)
sys.path.insert(0, str(source / 'experiments/control-migrations'))
import lab
from fault_cases import Process

def require(v, message):
    if not v: raise RuntimeError(message)

state = lab.load(root)
revision = lab.command(['git', '-C', str(source), 'rev-parse', 'HEAD']).stdout.strip()
require(revision == state['build']['build_info']['source']['revision'] and str(source) == state['source'], 'source-identity')
require(not lab.command(['git', '-C', str(source), 'status', '--porcelain']).stdout, 'dirty-source')
require(lab.docker('exec',state['client'],'sha256sum','/build/bigbrotr').stdout.split()[0] == state['binary_sha256'], 'binary-identity')
output = root / 'fk-races.json'; require(not output.exists(), 'existing-report')
report = {'format':'bigbrotr-activation-fk-races/1','source':revision,'binary_sha256':state['binary_sha256'],'helper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'status':'RUNNING','cases':[]}

def send(process, text):
    process.process.stdin.write((text + '\n').encode()); process.process.stdin.flush()

def wait_query(database, predicate):
    deadline = time.monotonic() + 4
    while True:
        rows = json.loads(lab.sql(state,database,"SELECT coalesce(json_agg(json_build_object('pid',pid,'user',usename,'state',state,'wait_type',wait_event_type,'wait',wait_event,'application',application_name)),'[]') FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid();"))
        if predicate(rows): return rows
        require(time.monotonic() < deadline, 'schedule-not-observed'); time.sleep(.05)

def psql(database):
    return Process(['docker','exec','-i',state['postgres'],'psql','-X','-A','-t','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d',database], input_pipe=True)

def prepare(name):
    db = 'bb_fk_' + name; lab.sql(state,'postgres',f'CREATE DATABASE {db} OWNER bigbrotr_migrator;')
    path = lab.config(root,state,db); local=root/Path(path).name
    local.write_text(lab.change(local.read_text(),'postgres.control','password_ref','file:/lab/secrets/application'))
    require(lab.migrate(state,'apply',path,target=802).returncode==0,'migration')
    lab.sql(state,db,'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets,control.config_revisions,control.principals,control.management_audit,control.policy_revisions,control.management_commands TO bigbrotr_control; GRANT UPDATE(config_revision,policy_revision) ON control.datasets TO bigbrotr_control;')
    require(lab.cli(state,'config','initialize','--file',path).returncode==0,'initialize')
    return db,path

insert = "INSERT INTO control.role_instances SELECT decode(repeat('77',16),'hex'),dataset_id,'control',1,NULL FROM control.datasets;"
try:
    for outcome in ['commit','rollback']:
        db,path=prepare(outcome); parent=psql(db); app=None
        try:
            send(parent,"SET application_name='activation-fk-parent'; BEGIN; " + insert)
            wait_query(db,lambda rows:any(r['application']=='activation-fk-parent' and r['state']=='idle in transaction' for r in rows))
            app=Process(['docker','exec',state['client'],'/build/bigbrotr','config','activate-initial','--file',path,'--request-id','71'*16,'--expected-revision','1'])
            blocked=wait_query(db,lambda rows:any(r['user']=='bigbrotr_control' and r['wait_type']=='Lock' for r in rows))
            send(parent,outcome.upper()+';\n\\q');require(parent.wait(5)==0,'fixture-transaction')
            code=app.wait(10);require(code==(3 if outcome=='commit' else 0),'cli-outcome')
            if outcome=='commit':require(app.stderr==b'error: initial policy activation refused: ExistingActivity\n' and not app.stdout,'activity-refusal')
            else:require(json.loads(app.stdout)['status']=='activated' and not app.stderr,'rollback-activation')
            counts=json.loads(lab.sql(state,db,"SELECT json_build_object('roles',(SELECT count(*) FROM control.role_instances),'policies',(SELECT count(*) FROM control.policy_revisions),'commands',(SELECT count(*) FROM control.management_commands),'audits',(SELECT count(*) FROM control.management_audit),'head',(SELECT policy_revision FROM control.datasets));"))
            require(counts==({'roles':1,'policies':0,'commands':0,'audits':1,'head':None} if outcome=='commit' else {'roles':0,'policies':1,'commands':1,'audits':2,'head':1}),'durable-effects')
            report['cases'].append({'name':'prior-fk-'+outcome,'status':'PASS','blocked_activity':blocked,'final':counts});lab.write_json(output,report)
        finally:
            # End the exact fixture session before terminating its local wrapper.
            if parent.process.poll() is None:
                send(parent,'ROLLBACK;\n\\q');parent.wait(5)
            parent.stop()
            if app: app.stop()
    # A deliberately weaker lock permits the FK insertion. This demonstrates why
    # ordinary UPDATE / NO KEY UPDATE does not establish the production barrier.
    db,path=prepare('negative');parent=psql(db)
    try:
        send(parent,"SET application_name='activation-fk-parent'; BEGIN; SELECT dataset_id FROM control.datasets FOR NO KEY UPDATE;")
        wait_query(db,lambda rows:any(r['application']=='activation-fk-parent' and r['state']=='idle in transaction' for r in rows))
        lab.sql(state,db,"SET statement_timeout='2s'; "+insert)
        require(lab.sql(state,db,'SELECT count(*) FROM control.role_instances;')=='1','negative-control-did-not-discriminate')
        send(parent,'ROLLBACK;\n\\q');require(parent.wait(5)==0,'negative-terminal')
        result=lab.cli(state,'config','activate-initial','--file',path,'--request-id','71'*16,'--expected-revision','1')
        require(result.returncode==3 and result.stderr=='error: initial policy activation refused: ExistingActivity\n','negative-activity-refused')
        report['cases'].append({'name':'no-key-update-negative-control','status':'PASS','child_insert_committed_while_weaker_parent_lock_held':True})
    finally:
        if parent.process.poll() is None:
            send(parent,'ROLLBACK;\n\\q');parent.wait(5)
        parent.stop()
    require(lab.sql(state,'postgres',"SELECT count(*) FROM pg_stat_activity WHERE application_name='activation-fk-parent' OR usename IN ('bigbrotr_control','bigbrotr_migrator');")=='0','remaining-backends')
    report.update(status='PASS',remaining_backends=0)
except Exception as e:
    report.update(status='FAIL',failure=str(e));raise
finally:lab.write_json(output,report)
print(json.dumps({'status':report['status'],'cases':len(report['cases'])}))
