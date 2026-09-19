#!/usr/bin/env python3
"""Actual producer barrier, fresh Control and interrupted native/PG transitions."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/archive-engine'))
sys.path.insert(0, str(SOURCE/'experiments/ingest-bootstrap'))
from signed_publication_cases import command, execute, event, proxy_observation
from fault_cases import Process
from event_oracle import expected_event
from existing_outcome import check_all_outcome_copies
from import_originals import check as check_originals


def sha(value): return hashlib.sha256(value).hexdigest()
def compact(value): return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def expected_native(empty, closed):
    wanted = expected_event(bytes([1])*16, True, 1)[0]
    if empty:
        wanted = [r for r in wanted if r[0] == 'default' and r[1] == b'\x01\x01']
    if closed:
        key = b'\x01\x31'+bytes([66])*16+(1).to_bytes(8,'big')
        wanted = [r for r in wanted if (r[0],r[1]) != ('receipts',key)]
        value = b'\x02\x31'+(0 if empty else 2).to_bytes(8,'big')+bytes(16)+b'\x01'
        wanted.append(('receipts',key,value)); wanted.sort()
    return wanted


def check_state(state, wanted, names, originals):
    rows = [dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in wanted]
    assert state['native_rows'] == len(rows) and state['native_sha256'] == sha(compact(rows))
    assert [r[0] for r in state['control_tables']] == names
    assert state['control_sha256'] == sha(compact(state['control_tables']))
    assert state['originals'] == [[name,value] for name,value in sorted(originals.items())]
    tables = {name:json.loads(value) for name,value in state['control_tables']}
    assert all('xmin' in row for rows in tables.values() for row in rows)
    return tables


def same_window(before, after, pg_closes, recovered=False):
    for name in before:
        if name == 'producer_epochs':
            assert len(before[name]) == len(after[name]) == 1
            old, new = before[name][0].copy(), after[name][0].copy()
            if pg_closes:
                assert old.pop('state') == 'open' and new.pop('state') == 'closing'
                assert old.pop('xmin') != new.pop('xmin')
            assert old == new
        elif recovered and name in ('role_instances','owner_epochs'):
            # A new Control must not impersonate the failed one; target Ingest
            # identity/state/xmin remain exact. Other claim changes are checked
            # against their literal role/epoch/incarnation below.
            assert [r for r in before[name] if r['role']=='ingest'] == [r for r in after[name] if r['role']=='ingest']
        else:
            assert before[name] == after[name], name


def main(root, tag, scenario):
    resources = json.loads((root/'resources.json').read_text())
    revision = command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all')
    assert not command('git','-C',str(SOURCE),'diff',resources['source'],revision,'--','apps','crates','migrations','Cargo.lock','Cargo.toml','rust-toolchain.toml')
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['status']=='READY' and ready['source']==resources['source']
    assert sha((root/'out/native-tests/ingest-bootstrap-probe').read_bytes()) == ready['ingest_probe']
    for identity in [resources['container_id'],resources['postgres_id']]:
        owned = json.loads(command('docker','inspect',identity))[0]
        assert owned['Config']['Labels']['org.bigbrotr.publication']==resources['owner']
        assert owned['State']['Running'] and not owned['State']['OOMKilled']
    assert tag.isalnum() and 1 <= len(tag) <= 8
    target = root/('closing-'+tag+'.json'); assert not target.exists()
    report = dict(status='INCOMPLETE',source=resources['source'],harness_revision=revision,
                  artifacts=ready,scenario=scenario,processes={},sql=[])
    def save(): target.write_text(json.dumps(report,indent=2)+'\n')
    def sql(db, text):
        observation=dict(database=db,sql_sha256=sha(text.encode()),status='INCOMPLETE',remote_exclusion='UNKNOWN')
        report['sql'].append(observation);save()
        try:
            result=subprocess.run(['docker','exec','-i',resources['postgres_id'],'psql','-XqAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',db], input=text,text=True,capture_output=True,timeout=30)
            observation.update(status='RETURNED',exit=result.returncode,stdout=result.stdout,stderr=result.stderr)
            result.check_returncode(); return result.stdout.strip()
        finally: save()
    binary=['docker','exec','-i',resources['container_id'],'/out/native-tests/ingest-bootstrap-probe']
    def run(label,args):
        observation=report['processes'][label]={};save()
        try:execute(binary+args,45,observation)
        finally:save()
        return observation
    db,directory='bbclosing_'+tag,'/out/closing-'+tag
    save();sql('postgres','CREATE DATABASE '+db)
    setup=run('setup',['setup-14',db,'/secrets/postgres','unused'])
    assert setup['exit']==0 and not setup['stderr']
    if scenario in ['rollback','late-sql']:
        action = "RAISE EXCEPTION 'fixture rollback after native sync'" if scenario=='rollback' else 'PERFORM pg_sleep(3)'
        sql(db,f"CREATE FUNCTION public.fixture_abort_closing() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('application_name')='bigbrotr-closing-fault' THEN {action}; END IF; RETURN NEW; END $$; CREATE TRIGGER zz_fixture_abort_closing AFTER UPDATE ON control.producer_epochs FOR EACH ROW WHEN (OLD.state='open' AND NEW.state='closing') EXECUTE FUNCTION public.fixture_abort_closing();")
    command('docker','exec',resources['container_id'],'mkdir','-m','700',directory)
    lost=scenario in ['before-forward','after-complete']
    proxy=None
    try:
        if lost:
            proxy=Process(['docker','exec','-i',resources['container_id'],'python3','-B','/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432','--listen-port','6543','--mode',scenario,'--commit-number','2','--deadline-seconds','45'],limit=65536)
            event(proxy,'ready')
        result=run('close',['event-closing-'+('commit-lost' if lost else scenario),db,'/secrets/bigbrotr_control',directory+'/db'])
        if proxy:
            assert proxy.wait(5)==0
            report['proxy']=proxy_observation(proxy);save()
            fault=json.loads(report['proxy']['stdout'].splitlines()[-1])
            assert fault['event']=='receipt' and fault['commits_seen']==2 and fault['faults']==1
            assert fault['server_commit_complete']==(2 if scenario=='after-complete' else 1)
            assert fault['client_commit_complete']==1
    finally:
        if proxy:proxy.stop();report['proxy']=proxy_observation(proxy)
        save()
    # Capture durable effects even when the application asserts/fails.
    control=report['control']=json.loads(sql(db,"SELECT jsonb_build_object('requests',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY request_sequence),'[]'::jsonb) FROM control.producer_requests r),'outcomes',(SELECT coalesce(jsonb_agg(to_jsonb(o) ORDER BY request_sequence),'[]'::jsonb) FROM control.producer_outcomes o),'bindings',(SELECT coalesce(jsonb_agg(to_jsonb(b)),'[]'::jsonb) FROM control.producer_input_bindings b),'producer',(SELECT jsonb_agg(to_jsonb(p)) FROM control.producer_epochs p),'streams',(SELECT jsonb_agg(to_jsonb(s)) FROM control.source_streams s))"));save()
    dump=run('dump',['dump',directory+'/db']);assert dump['exit']==0 and not dump['stderr']
    report['native']=json.loads(dump['stdout']);save()
    empty=scenario=='empty';negative=scenario in ['root','metadata','policy','expired']
    interrupted=scenario in ['rollback','late-sql'] or lost
    wanted=expected_native(empty,not negative)
    assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in report['native']['rows']]==wanted
    spool=root/directory.lstrip('/')/'db/producer-inputs-v1'
    files={p.name:p.read_bytes() for p in spool.iterdir()} if spool.exists() else {}
    originals=report['originals']={name:dict(bytes=len(value),sha256=sha(value)) for name,value in files.items()};save()
    if empty:
        assert not files and control['requests']==control['outcomes']==[]
    else:
        check_all_outcome_copies(control,wanted,request_count=2,outcome_count=2)
        check_originals(control,files,wanted,2,repetitions=1)
    names=json.loads(sql(db,"SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    report['remaining_client_backends']=int(sql(db,'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'));save()
    assert report['remaining_client_backends']==0
    assert result['exit']==0 and not result['stderr']
    messages=[json.loads(line) for line in result['stdout'].splitlines()]
    one=lambda label:next(m['state'] for m in messages if m['event']==label)
    before=one('closing-before');bt=check_state(before,expected_native(empty,False),names,originals)
    after=one('closing-interrupted' if negative else 'closing-after');at=check_state(after,wanted,names,originals)
    same_window(bt,at,not negative and scenario!='pg-first',recovered=interrupted)
    assert after['sequence']==before['sequence']+int(not negative)
    if interrupted:
        mid=one('closing-interrupted');mt=check_state(mid,wanted,names,originals)
        same_window(bt,mt,scenario=='after-complete')
        assert mid['sequence']==before['sequence']+1
    successes=[m for m in messages if m['event']=='producer-closed']
    assert len(successes)==(0 if negative else 1 if interrupted else 2)
    for m in successes:
        assert m['issued']==(0 if empty else 2) and m['historical_control_epoch']==1
        assert m['current_control_epoch']==(3 if interrupted else 2)
    assert len(at['producer_epochs'])==1
    assert at['producer_epochs'][0]['reserved_through_request']==(0 if empty else 2)
    assert at['producer_epochs'][0]['state']==('open' if negative else 'closing')
    assert len(at['source_streams'])==1 and at['source_streams'][0]['state']=='open'
    for name in ['producer_checkpoints','producer_input_bindings','import_record_closures','import_checkpoints','jobs']:
        assert at[name]==[]
    report['checked']=dict(native_rows=len(wanted),control_tables=len(names),originals=len(files),
                           before_sequence=before['sequence'],after_sequence=after['sequence'],successes=len(successes))
    report['status']='PASS';save();print(json.dumps(dict(status='PASS',scenario=scenario,**report['checked'])))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--run-tag',required=True)
    parser.add_argument('--scenario',choices=['normal','empty','pg-first','old-lease','root','metadata','policy','expired','rollback','late-sql','before-forward','after-complete'],required=True)
    args=parser.parse_args();main(args.root.resolve(strict=True),args.run_tag,args.scenario)
