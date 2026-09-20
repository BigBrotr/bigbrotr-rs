#!/usr/bin/env python3
"""Actual backend loss, held native LOCK, SIGKILL/reap and a fresh recovery process."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from recovery import SOURCE, command, event, Process, proxy_observation, sha, check_inputs, check_messages
from closing import check_state, expected_native


def check_replacement(old, new):
    """Exact first-Control exclusion; historical Ingest and all other rows retained."""
    bytea = lambda n:'\\x'+(bytes([n])*16).hex()
    control = lambda tables:sorted([r for r in tables['owner_epochs'] if r['role']=='control'], key=lambda r:r['epoch'])
    assert len(old['owner_epochs']) == 2 and len(new['owner_epochs']) == 3
    previous, = control(old)
    retired, active = control(new)
    assert previous['epoch'] == 1 and previous['state'] == 'active'
    assert previous['released_at_us'] is previous['exclusion_method'] is previous['exclusion_digest'] is None
    assert retired['released_at_us'] >= previous['acquired_at_us']
    digest = sha(b'bigbrotr/control-exclusion/1\0'+bytes([1])*16+bytes.fromhex(previous['resource_id'][2:])+(1).to_bytes(8,'big')+bytes([41])*16+bytes([43])*16)
    assert retired == dict(previous, state='released', xmin=retired['xmin'], released_at_us=retired['released_at_us'], exclusion_method='storage_fenced', exclusion_digest='\\x'+digest)
    assert retired['xmin'] != previous['xmin'] and active['xmin'] == retired['xmin']
    assert active['acquired_at_us'] >= retired['released_at_us'] and active['lease_expires_at_us'] == active['acquired_at_us']+60_000_000
    assert active['control_backend_pid'] > 0 and active['control_backend_pid'] != previous['control_backend_pid']
    assert active['control_backend_start'] != previous['control_backend_start']
    assert active == dict(previous, epoch=2, xmin=retired['xmin'], role_instance_id=bytea(33), process_incarnation=bytea(43), acquired_at_us=active['acquired_at_us'], lease_expires_at_us=active['lease_expires_at_us'], control_backend_pid=active['control_backend_pid'], control_backend_start=active['control_backend_start'])
    roles = lambda tables:{r['role_instance_id']:r for r in tables['role_instances']}
    a,b=roles(old),roles(new)
    assert set(a)=={bytea(31),bytea(62)} and set(b)==set(a)|{bytea(33)}
    assert b[bytea(62)]==a[bytea(62)]
    assert b[bytea(31)]['retired_at_us'] >= a[bytea(31)]['created_at_us']
    assert b[bytea(31)] == dict(a[bytea(31)], xmin=active['xmin'], retired_at_us=b[bytea(31)]['retired_at_us'])
    assert b[bytea(33)]['created_at_us'] <= active['acquired_at_us']
    assert b[bytea(33)] == dict(a[bytea(31)], role_instance_id=bytea(33), xmin=active['xmin'], created_at_us=b[bytea(33)]['created_at_us'])
    assert [r for r in old['owner_epochs'] if r['role']=='ingest'] == [r for r in new['owner_epochs'] if r['role']=='ingest']
    for table in old:
        if table not in ['role_instances','owner_epochs']:assert old[table]==new[table],table


def main(root, tag):
    resources=json.loads((root/'resources.json').read_text())
    revision=command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all')
    work=root/'out/recovery-probe-process'
    ready=json.loads((work/'ready.json').read_text())
    assert ready['baseline_product']==resources['source'] and ready['status']=='READY'
    assert not command('git','-C',str(SOURCE),'diff',ready['source'],revision,'--','apps','crates','migrations','tools','Cargo.lock','Cargo.toml','rust-toolchain.toml')
    assert sha((work/'ingest-bootstrap-probe').read_bytes())==ready['sha256']
    for identity in [resources['container_id'],resources['postgres_id']]:
        c=json.loads(command('docker','inspect',identity))[0]
        assert c['State']['Running'] and not c['State']['OOMKilled'] and c['Config']['Labels']['org.bigbrotr.publication']==resources['owner']
    assert tag.isalnum() and 1<=len(tag)<=8
    target=root/('process-recovery-'+tag+'.json');assert not target.exists()
    report=dict(status='INCOMPLETE',source=resources['source'],harness_revision=revision,artifacts=ready,processes={},sql=[],process_output_limit=512*1024)
    def save():target.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db,query):
        observation=dict(database=db,sql_sha256=sha(query.encode()),status='INCOMPLETE');report['sql'].append(observation);save()
        try:
            r=subprocess.run(['docker','exec','-i',resources['postgres_id'],'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input=query,text=True,capture_output=True,timeout=15)
            observation.update(status='RETURNED',exit=r.returncode,stdout=r.stdout,stderr=r.stderr);r.check_returncode();return r.stdout.strip()
        finally:save()
    binary=['docker','exec','-i',resources['container_id'],'/out/recovery-probe-process/ingest-bootstrap-probe']
    def run(label,args):
        observation=report['processes'][label]=dict(exit=None,remote_exclusion='UNKNOWN');save()
        p=Process(binary+args,limit=report['process_output_limit'])
        try:observation['exit']=p.wait(30)
        finally:
            p.stop();observation.update(stdout=p.stdout.decode(),stderr=p.stderr.decode(),overflow=p.overflow);save()
        return observation
    db, directory='bbprocess_'+tag,'/out/process-recovery-'+tag
    save();sql('postgres','CREATE DATABASE '+db)
    setup=run('setup',['setup-14',db,'/secrets/postgres','unused']);assert setup['exit']==0 and not setup['stderr']
    sql(db,"CREATE FUNCTION public.fixture_abort_original_copy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback of original event outcome copy'; END $$; CREATE TRIGGER zz_fixture_abort_original_copy AFTER INSERT ON control.producer_outcomes FOR EACH ROW WHEN (NEW.recorder_epoch=1 AND NEW.request_sequence=2) EXECUTE FUNCTION public.fixture_abort_original_copy();")
    command('docker','exec',resources['container_id'],'mkdir','-m','700',directory)
    holder=None
    try:
        holder=Process(['docker','exec','-i',resources['container_id'],'python3','-B','/out/recovery-probe-process/source/experiments/producer-requests/held_old_owner.py',binary[-1],db,'/secrets/bigbrotr_control',directory+'/db','event-recovery-hold-old','old-native-held'],input_pipe=True,limit=128*1024)
        held=report['old_held']=event(holder,'old-held');save()
        owner=report['old_control']=json.loads(sql(db,"SELECT to_jsonb(o) FROM control.owner_epochs o WHERE role='control' AND epoch=1"))
        pid,start=owner['control_backend_pid'],owner['control_backend_start']
        assert type(pid) is int and pid>0 and all(c in '0123456789-:+.T' for c in start)
        assert sql(db,f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid={pid} AND backend_start='{start}'::timestamptz")=='t'
        deadline=time.monotonic()+5
        while int(sql(db,f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND backend_start='{start}'::timestamptz")):
            assert time.monotonic()<deadline;time.sleep(.02)
        holder.process.stdin.write(b'observe\n');holder.process.stdin.flush()
        still=report['old_still_held']=event(holder,'old-still-held');save()
        assert {k:v for k,v in held.items() if k!='event'}=={k:v for k,v in still.items() if k!='event'}
        assert held['executable_sha256']==ready['sha256']
        refused=run('held_attempt',['recovery-process-held',db,'/secrets/bigbrotr_control',directory+'/db'])
        assert refused['exit']==0 and not refused['stderr'] and not refused['overflow']
        # Observe again after the refused native open, excluding a stale lock
        # observation or accidentally released fcntl lock in the real old child.
        holder.process.stdin.write(b'observe\n');holder.process.stdin.flush()
        with holder.condition:
            deadline=time.monotonic()+5
            while sum(json.loads(x).get('event')=='old-still-held' for x in bytes(holder.stdout).split(b'\n')[:-1])<2:
                assert time.monotonic()<deadline;holder.condition.wait(.02)
        last=[json.loads(x) for x in bytes(holder.stdout).split(b'\n')[:-1] if b'old-still-held' in x][-1]
        report['old_after_refusal']=last;assert last==still
        holder.process.stdin.write(b'kill\n');holder.process.stdin.flush()
        joined=report['old_joined']=event(holder,'old-joined');assert holder.wait(5)==0
        assert joined['exit']==-9 and joined['joined'] and not joined['stderr']
        report['holder']=proxy_observation(holder);save()
    finally:
        if holder:holder.stop();report['holder']=proxy_observation(holder)
        save()
    recovered=run('recover',['recovery-process-resume',db,'/secrets/bigbrotr_control',directory+'/db'])
    control=report['control']=json.loads(sql(db,"SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'bindings',(SELECT coalesce(jsonb_agg(to_jsonb(b)),'[]'::jsonb) FROM control.producer_input_bindings b))"))
    dumped=run('dump',['dump',directory+'/db']);assert dumped['exit']==0 and not dumped['stderr']
    report['native']=json.loads(dumped['stdout']);wanted=expected_native(False,True)
    names=report['table_names']=json.loads(sql(db,"SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    report['remaining_backends']=int(sql(db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    files={p.name:p.read_bytes() for p in (root/directory.lstrip('/')/'db/producer-inputs-v1').iterdir()}
    originals=report['originals']={name:dict(bytes=len(value),sha256=sha(value)) for name,value in files.items()}
    log=subprocess.check_output(['docker','logs','--tail','1000',resources['postgres_id']],text=True,stderr=subprocess.STDOUT,timeout=15)
    report['seed_failure']=dict(control_backend_pid=pid,control_backend_start=start,lines=[line for line in log.splitlines() if '['+str(pid)+']' in line]);save()
    assert report['remaining_backends']==0 and recovered['exit']==0 and not recovered['stderr'] and not recovered['overflow']
    assert report['native']['rows']==[dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in wanted]
    check_inputs(control,files,wanted,'normal')
    seed=[json.loads(x) for x in joined['stdout'].splitlines()]
    attempt=[json.loads(x) for x in refused['stdout'].splitlines()]
    resumed=[json.loads(x) for x in recovered['stdout'].splitlines()]
    one=lambda messages,label:next(m for m in messages if m['event']==label)
    old=one(seed,'old-native-held')['state']
    assert one(attempt,'process-before')['state']==one(attempt,'held-open-refused')['state']==one(resumed,'process-before')['state']==old
    assert one(attempt,'held-open-refused')['error']=='EngineOpen'
    assert len({held['pid'],one(attempt,'recovery-process')['pid'],one(resumed,'recovery-process')['pid']})==3
    report['checked']=check_messages(seed+resumed,'normal',names,originals,control,report['seed_failure'])
    before=one(resumed,'closing-before')['state']
    check_replacement(check_state(old,expected_native(False,False),names,originals),check_state(before,expected_native(False,False),names,originals))
    assert before['sequence']==old['sequence']
    report.update(status='PASS',scope='same native directory after backend loss and positively joined SIGKILL; no mid-FFI or power loss')
    save();print(json.dumps(dict(status='PASS',**report['checked'])))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True);parser.add_argument('--run-tag',required=True)
    args=parser.parse_args();main(args.root.resolve(strict=True),args.run_tag)
