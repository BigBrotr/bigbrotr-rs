#!/usr/bin/env python3
"""First SIGTERM during actual catalog rollback, after durable grant acceptance.

Run after private_grant_fault_cases.py in the same owner-verified laboratory.
Observe completed Control polls past the original policy lifetime while the
catalog rollback is held; retry the accepted command after graceful restart.
"""
import hashlib
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode=True
from signed_publication_cases import command, event, send, sha, Process


def main(root):
    resources=json.loads((root/'resources.json').read_text());source=root/'source'
    assert Path(__file__).resolve().parents[2]==source
    assert command('git','-C',str(source),'rev-parse','HEAD')==resources['source']
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    runner,pg=resources['container_id'],resources['postgres_id']
    for identity in [runner,pg]:
        c=json.loads(command('docker','inspect',identity))[0]
        assert c['Config']['Labels'][resources['owner_label']]==resources['owner'] and c['State']['Running']
    assert json.loads((root/'private-grant-fault-cases.json').read_text())['status']=='PASS'
    record=dict(source=resources['source'],tree=resources['tree'],status='INCOMPLETE')
    path=root/'private-grant-cleanup-cases.json';assert not path.exists()
    def save():path.write_text(json.dumps(record,indent=2)+'\n')
    def sql(database,query):return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,input=query)
    def control(query):return json.loads(sql('bbpub_control_normal',query))
    def proofs():return control("SELECT json_object_agg(encode(request_key,'hex'),encode(proof_id,'hex')) FROM control.reauthentication_proofs")
    policy_revision=int(sql('bbpub_control_normal',"SELECT policy_revision FROM control.datasets WHERE dataset_id=decode('01010101010101010101010101010101','hex')"))
    def counts():return control("SELECT json_build_object('active_pins',(SELECT count(*) FROM control.snapshot_leases WHERE state='active'),'proofs',(SELECT count(*) FROM control.reauthentication_proofs),'commands',(SELECT count(*) FROM control.management_commands WHERE action='scope-grant.change'),'active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'))")
    def snapshot():
        tables={'management_commands':" WHERE action='scope-grant.change'",'management_audit':" WHERE action='scope-grant.change'",'scope_grant_changes':'','scope_grants':'','reauthentication_proofs':'','local_accounts':'','snapshot_leases':'','archive_snapshot_pins':'','retention_claims':''}
        # Credentials and plaintext session/CSRF values never enter this snapshot.
        return {table:control(f"SELECT jsonb_agg(to_jsonb(t)||jsonb_build_object('row_xmin',t.xmin::text) ORDER BY to_jsonb(t)::text) FROM control.{table} t"+where) for table,where in tables.items()}
    def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    def backend():return control("SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'state',state,'no_transaction',xact_start IS NULL,'committed',trim(query)='COMMIT','query_start',query_start)),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")
    def manager(pid):
        return json.loads(sql('bbpub_normal',f"SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'state',state,'transaction',xact_start IS NOT NULL)),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='CatalogManager' AND pid={int(pid)}"))
    def process(parent):
        script="""import json
from pathlib import Path
parent=int(__import__('sys').argv[1]); path=Path('/proc',str(parent)); children=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  stat=(p/'stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
  if int(fields[1])==parent:children.append(dict(pid=int(p.name),state=fields[0],start_ticks=int(fields[19])))
 except (FileNotFoundError,PermissionError,ProcessLookupError):pass
stat=(path/'stat').read_text();fields=stat[stat.rfind(')')+2:].split()
print(json.dumps(dict(pid=parent,start_ticks=int(fields[19]),state=fields[0],children=children)))
"""
        return json.loads(command('docker','exec',runner,'python3','-c',script,str(parent)))
    proxy=client=None
    try:
        record['proofs_before']=proofs();assert set(record['proofs_before'])=={'d1'*16,'d2'*16}
        proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/native_commit_proxy.py','--mode','hold-capture-rollback'],input_pipe=True)
        ready=record['proxy_ready']=event(proxy,'ready');assert ready['source_sha256']==sha(source/'experiments/archive-engine/native_commit_proxy.py')
        client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/archive-engine/private_grant_fault_client.py','cleanup',str(ready['port']),str(policy_revision)],input_pipe=True)
        submitted=record['submitted']=event(client,'submitted');parent=submitted['parent_pid']
        held=record['held']=event(proxy,'held');assert held['stage']=='capture-rollback'
        record['manager']=manager(held['backend_pid']);assert len(record['manager'])==1 and record['manager'][0]['state']=='idle in transaction' and record['manager'][0]['transaction']
        record['process']=process(parent);assert not record['process']['children'] and record['process']['state'] not in ('Z','X')
        record['before_signal']=counts();assert record['before_signal']==dict(active_pins=0,proofs=1,commands=3,active_control=1)
        before=snapshot();record['rows_before_sha256']=digest(before)
        record['proofs_after']=proofs();assert record['proofs_after']=={'d1'*16:record['proofs_before']['d1'*16]}
        accepted=[row for row in before['management_commands'] if row['request_key']=='\\x'+'d2'*16];assert len(accepted)==1
        detail=[row for row in before['scope_grant_changes'] if row['request_key']=='\\x'+'d2'*16];assert len(detail)==1 and detail[0]['proof_id']=='\\x'+record['proofs_before']['d2'*16]
        assert detail[0]['scope_generation']==3 and detail[0]['resulting_grant_revision']==4
        assert len([row for row in before['management_audit'] if row['audit_id']==accepted[0]['audit_id'] and row['subject_id']=='\\x'+'d2'*16 and row['created_at_us']==accepted[0]['accepted_at_us']])==1
        record['control_before']=backend();assert len(record['control_before'])==1
        original=record['control_before'][0];assert original['state']=='idle' and original['no_transaction'] and original['committed']
        save();send(client,'check\n');record['request_pending']=event(client,'request_pending',timeout=2)
        started=time.monotonic();command('docker','exec',runner,'kill','-TERM',str(parent));send(client,'observe\n')
        record['http_closed']=event(client,'request_closed',timeout=2)
        assert record['http_closed']['response']['closed']=='RemoteDisconnected' and record['http_closed']['response']['request_ms']<5000
        record['completed_polls']=[]
        while time.monotonic()-started<6:
            current=process(parent);assert current['start_ticks']==record['process']['start_ticks'] and current['state'] not in ('Z','X') and not current['children']
            assert manager(held['backend_pid'])==record['manager']
            rows=backend();assert len(rows)==1
            row=rows[0];assert (row['pid'],row['backend_start'])==(original['pid'],original['backend_start'])
            if row['state']=='idle' and row['no_transaction'] and row['committed'] and row['query_start']>original['query_start'] and row['query_start'] not in record['completed_polls']:
                record['completed_polls'].append(row['query_start'])
            time.sleep(.1)
        record['held_after_signal_ms']=round((time.monotonic()-started)*1000,3)
        assert len(record['completed_polls'])>=2
        assert snapshot()==before
        save();send(proxy,'release\n');event(proxy,'released')
        record['client_exit']=client.wait(10);record['client_error']=client.stderr.decode();assert record['client_exit']==0 and not client.stderr and not client.overflow
        record['client_terminal']=event(client,'terminal')
        assert record['client_terminal']['retry']['accepted_at_us']==str(accepted[0]['accepted_at_us'])
        after=snapshot();record['rows_after_sha256']=digest(after);assert after==before
        record['after_retry']=counts();assert record['after_retry']==dict(active_pins=0,proofs=1,commands=3,active_control=0)
        record['password_attempts']=control("SELECT json_object_agg(kind,attempts) FROM control.login_buckets");assert record['password_attempts']==dict(account=5,peer=5)
    except BaseException as error:
        record['failure']=type(error).__name__
        raise
    finally:
        cleanup_errors=[]
        try:
            if proxy is not None:
                try:
                    if proxy.process.poll() is None:send(proxy,'stop\n')
                    record['proxy_exit']=proxy.wait(5);record['proxy_terminal']=event(proxy,'terminal')
                finally:proxy.stop()
        except BaseException as error:cleanup_errors.append(dict(stage='proxy',error=type(error).__name__))
        try:
            if client is not None:
                client.stop()
                record['client_output']=client.stdout.decode();record['client_error']=client.stderr.decode()
        except BaseException as error:cleanup_errors.append(dict(stage='client',error=type(error).__name__))
        try:record['retained_terminal']=counts()
        except BaseException as error:cleanup_errors.append(dict(stage='terminal-sql',error=type(error).__name__))
        record['cleanup_errors']=cleanup_errors;save()
    assert not cleanup_errors and record['proxy_exit']==0 and record['proxy_terminal']['unexpected']==0
    record['status']='PASS';save()
    print(json.dumps(dict(status=record['status'],source=resources['source'])))


if __name__=='__main__':main(Path(sys.argv[1]).resolve())
