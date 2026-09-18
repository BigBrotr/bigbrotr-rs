"""Actual existing enrollment/authentication consumers after migration802/activation."""
import argparse,json,sys,hashlib
from pathlib import Path
sys.dont_write_bytecode=True
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);a=p.parse_args();root=a.root.resolve(strict=True);source=a.source.resolve(strict=True)
sys.path.insert(0,str(source/'experiments/control-migrations'));import lab
state=lab.load(root);revision=lab.command(['git','-C',str(source),'rev-parse','HEAD']).stdout.strip()
assert revision==state['build']['build_info']['source']['revision'] and str(source)==state['source']
assert not lab.command(['git','-C',str(source),'status','--porcelain','--untracked-files=all']).stdout
assert lab.docker('exec',state['client'],'sha256sum','/build/bigbrotr').stdout.split()[0]==state['binary_sha256']
output=root/'enrollment-after.json';assert not output.exists()
report={'format':'bigbrotr-post-activation-enrollment/1','status':'RUNNING','source':revision,'binary_sha256':state['binary_sha256'],'helper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
db='bb_activation_old';config='/lab/'+db+'.toml'
try:
 lab.sql(state,db,'GRANT INSERT ON control.local_accounts,control.local_credentials,control.principal_grants,control.owner_challenges TO bigbrotr_control; GRANT UPDATE(state,failed_attempts,consumed_principal,consumed_at_us) ON control.owner_challenges TO bigbrotr_control;')
 baseline=lab.sql(state,db,'SELECT row_to_json(m) FROM control.management_commands m;')
 password=root/'secrets'/'post-activation-password';password.write_text('Independent owner password 2026');password.chmod(0o600)
 issued=lab.cli(state,'access','owner-token','issue','--file',config,'--username','alice','--request-id','81'*16);assert issued.returncode==0
 secret=root/'secrets'/'post-activation-token';secret.write_text(json.loads(issued.stdout)['token']);secret.chmod(0o600)
 enrolled=lab.cli(state,'access','enroll-owner','--file',config,'--challenge-id','81'*16,'--token-ref','file:/lab/secrets/post-activation-token','--password-ref','file:/lab/secrets/post-activation-password');assert enrolled.returncode==0
 principal=json.loads(enrolled.stdout)['principal_id'];before=lab.schema_digest(state,db)
 account=lab.cli(state,'access','me','--file',config,'--username','alice','--password-ref','file:/lab/secrets/post-activation-password');assert account.returncode==0 and not account.stderr
 value=json.loads(account.stdout);dataset=lab.sql(state,db,"SELECT encode(dataset_id,'hex') FROM control.datasets;")
 assert value=={'format':'bigbrotr-local-account/1','dataset_id':dataset,'principal_id':principal,'username':'alice','credential_revision':'1','grant_revision':'1','security_revision':'1','stored_actions':['audit.read','budgets.change','data.read','policy.change','principals.manage','roles.control','status.read'],'resource_kind':'dataset','resource_id':dataset,'runtime_authorization':'not-granted','session':'not-issued'}
 assert lab.schema_digest(state,db)==before
 assert lab.sql(state,db,'SELECT row_to_json(m) FROM control.management_commands m;')==baseline
 counts=json.loads(lab.sql(state,db,"SELECT json_build_object('principals',(SELECT count(*) FROM control.principals),'accounts',(SELECT count(*) FROM control.local_accounts),'credentials',(SELECT count(*) FROM control.local_credentials),'grants',(SELECT count(*) FROM control.principal_grants),'challenges',(SELECT count(*) FROM control.owner_challenges WHERE state='consumed'),'audits',(SELECT count(*) FROM control.management_audit),'policies',(SELECT count(*) FROM control.policy_revisions),'commands',(SELECT count(*) FROM control.management_commands));"))
 assert counts=={'principals':2,'accounts':1,'credentials':1,'grants':7,'challenges':1,'audits':4,'policies':1,'commands':1}
 report.update(status='PASS',database=db,account_observation=value,counts=counts,read_only_dump=before,initial_receipt_unchanged=True)
except Exception:
 report.update(status='FAIL');raise
finally:lab.write_json(output,report)
print(json.dumps({'status':report['status'],'database':db}))
