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


def check_recovery_control(before, after):
    bytea = lambda n: '\\x'+(bytes([n])*16).hex()
    owners = lambda tables: {r['epoch']:r for r in tables['owner_epochs'] if r['role']=='control'}
    old, new = owners(before), owners(after)
    assert set(old)=={1,2} and set(new)=={1,2,3}
    assert len(before['owner_epochs'])==3 and len(after['owner_epochs'])==4
    assert new[1]==old[1] and old[1]['state']=='released'
    for epoch, role, incarnation in [(1,31,41),(2,33,43),(3,34,44)]:
        row=new[epoch]
        assert row['dataset_id']==bytea(1) and row['resource_id']==old[1]['resource_id']
        assert row['role_instance_id']==bytea(role) and row['process_incarnation']==bytea(incarnation)
        assert row['control_fence_version']==1 and row['lease_expires_at_us']==row['acquired_at_us']+60_000_000
    retired, active = new[2],new[3]
    assert old[2]['state']=='active' and retired['state']=='released' and active['state']=='active'
    assert old[2]['released_at_us'] is old[2]['exclusion_method'] is old[2]['exclusion_digest'] is None
    assert retired['released_at_us']>=old[2]['acquired_at_us'] and retired['xmin']!=old[2]['xmin']
    exclusion=(b'bigbrotr/control-exclusion/1\0'+bytes([1])*16+bytes.fromhex(old[2]['resource_id'][2:])
               +(2).to_bytes(8,'big')+bytes([43])*16+bytes([44])*16)
    assert retired==dict(old[2],state='released',xmin=retired['xmin'],released_at_us=retired['released_at_us'],
                        exclusion_method='storage_fenced',exclusion_digest='\\x'+sha(exclusion))
    assert active['acquired_at_us']>=retired['released_at_us']
    assert active['control_backend_pid']>0 and active['control_backend_pid'] not in [old[n]['control_backend_pid'] for n in [1,2]]
    assert active['control_backend_start'] not in [old[n]['control_backend_start'] for n in [1,2]]
    assert active==dict(old[2],epoch=3,xmin=active['xmin'],role_instance_id=bytea(34),process_incarnation=bytea(44),
                       acquired_at_us=active['acquired_at_us'],lease_expires_at_us=active['lease_expires_at_us'],
                       control_backend_pid=active['control_backend_pid'],control_backend_start=active['control_backend_start'])
    assert active['xmin']==retired['xmin']
    roles=lambda tables:{r['role_instance_id']:r for r in tables['role_instances']}
    old_roles,new_roles=roles(before),roles(after)
    assert set(old_roles)=={bytea(31),bytea(33),bytea(62)} and set(new_roles)==set(old_roles)|{bytea(34)}
    assert len(before['role_instances'])==3 and len(after['role_instances'])==4
    for role in [31,62]: assert new_roles[bytea(role)]==old_roles[bytea(role)]
    prior, retired_role, current = old_roles[bytea(33)],new_roles[bytea(33)],new_roles[bytea(34)]
    assert prior['retired_at_us'] is None and retired_role['retired_at_us']>=prior['created_at_us']
    assert retired_role==dict(prior,xmin=active['xmin'],retired_at_us=retired_role['retired_at_us'])
    assert prior['xmin']!=active['xmin'] and current['created_at_us']<=active['acquired_at_us']
    assert current==dict(prior,role_instance_id=bytea(34),xmin=active['xmin'],created_at_us=current['created_at_us'])

def same_window(before, after, pg_closes, recovered=False):
    if recovered: check_recovery_control(before, after)
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
            # identity/state/xmin remain exact. check_recovery_control checks
            # every Control row against the exact allowed transition above.
            assert [r for r in before[name] if r['role']=='ingest'] == [r for r in after[name] if r['role']=='ingest']
        else:
            assert before[name] == after[name], name


def main(root, tag, scenario, fixture_build=False):
    resources = json.loads((root/'resources.json').read_text())
    revision = command('git','-C',str(SOURCE),'rev-parse','HEAD')
    assert not command('git','-C',str(SOURCE),'status','--porcelain','--untracked-files=all')
    fixture_exception = [':!apps/bigbrotr/examples/support/ingest_closing.rs'] if fixture_build else []
    assert not command('git','-C',str(SOURCE),'diff',resources['source'],revision,'--','apps','crates','migrations','Cargo.lock','Cargo.toml','rust-toolchain.toml',*fixture_exception)
    location = 'out/closing-probe' if fixture_build else 'out/native-tests'
    ready = json.loads((root/location/'ready.json').read_text())
    assert ready['status']=='READY' and ready['baseline_product' if fixture_build else 'source']==resources['source']
    if fixture_build:
        # The rebuilt artifact may have a later Python-only observer. Every
        # actual Rust fixture/product/build input must still match its source.
        assert not command('git','-C',str(SOURCE),'diff',ready['source'],revision,'--',
                           'apps','crates','migrations','Cargo.lock','Cargo.toml','rust-toolchain.toml','tools')
    assert sha((root/location/'ingest-bootstrap-probe').read_bytes()) == ready['sha256' if fixture_build else 'ingest_probe']
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
    binary=['docker','exec','-i',resources['container_id'],'/'+location+'/ingest-bootstrap-probe']
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
        if scenario=='after-complete':
            assert mt['producer_epochs']==at['producer_epochs'], 'committed PG retry rewrote producer row'
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
    parser.add_argument('--fixture-build',action='store_true')
    args=parser.parse_args();main(args.root.resolve(strict=True),args.run_tag,args.scenario,args.fixture_build)
