#!/usr/bin/env python3
"""Deliberately omit actual positive claim release; recovery COMMIT must refuse."""
import argparse
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
from signed_publication_cases import command, execute, sha, Process, event, send, proxy_observation


def main(root):
    source=Path(__file__).resolve().parents[2]
    state=json.loads((root/'resources.json').read_text());assert source==root/'negative-source'
    revision=command('git','-C',str(source),'rev-parse','HEAD')
    assert not command('git','-C',str(source),'status','--porcelain','--untracked-files=all')
    changed=command('git','-C',str(source),'diff','--name-only',state['source'],revision).splitlines()
    assert all(p.startswith('docs/') or p in ('tests/fixtures/storage/archive-pin-recovery-constraints.sql',
        'experiments/archive-engine/pin_recovery_schema_cases.py','experiments/archive-engine/pin_recovery_claim_negative.py',
        'experiments/archive-engine/native_commit_proxy.py') for p in changed)
    ready=json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source']==state['source'] and sha(root/'out/native-tests/archive-publication-probe')==ready['publisher']
    assert json.loads((root/'pin-recovery-cases.json').read_text())['status']=='PASS'
    runner,pg=state['container_id'],state['postgres_id']
    for identity in [runner,pg]:
        c=json.loads(command('docker','inspect',identity))[0]
        assert c['State']['Running'] and not c['State']['OOMKilled'] and c['Config']['Labels']['org.bigbrotr.publication']==state['owner']
    directory=root/'out/pin-recovery-negative';directory.mkdir(mode=0o700)
    # The container cannot see the new Git checkout; export this exact verified
    # workload file into its existing lab output mount. Production stays fixed.
    proxy_path=directory/'proxy.py';proxy_path.write_bytes((source/'experiments/archive-engine/native_commit_proxy.py').read_bytes())
    assert sha(proxy_path)==sha(source/'experiments/archive-engine/native_commit_proxy.py')
    report_path=root/'pin-recovery-negative.json';assert not report_path.exists()
    report=dict(status='INCOMPLETE',source=state['source'],harness=revision,changed_paths=changed,
        publisher_sha256=ready['publisher'],proxy_sha256=sha(proxy_path))
    def save():report_path.write_text(json.dumps(report,indent=2)+'\n')
    def sql(text,db='bbpub_control_normal'):
        return command('docker','exec','-i',pg,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,input=text)
    def rows():
        return {n:json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM (SELECT *,xmin::text AS xmin FROM control."+n+")t"))
            for n in ['snapshot_leases','archive_snapshot_pins','retention_claims','archive_pin_recoveries']}
    def fixture(label,action,port):
        request=dict(case=label,operation=0xa4,control_port=port,action=action,ttl_ms=10000,wait=False,cancel=False)
        path=directory/(label+'.json');path.write_text(json.dumps(request)+'\n')
        observed=report[label]={};save()
        try:
            execute(['docker','exec','--env','LD_LIBRARY_PATH=/out/archive-worker/lib',runner,
                '/out/native-tests/archive-publication-probe','normal','pin','/out/pin-recovery-negative/'+path.name],75,observed)
        finally:save()
        assert observed['exit']==0 and not observed['stderr'] and not observed['overflow']
        result=json.loads(observed['stdout'].splitlines()[-1]);assert result['drivers_joined']
        return result
    proxy=None;save()
    try:
        initial=report['initial']=rows()
        seed=fixture('claim-negative-seed','register',5432);assert seed['outcome']=='registered' and seed['receipt']['created']
        before=report['before']=rows();identifier='\\x'+'a4'*16
        for table in ['snapshot_leases','archive_snapshot_pins','retention_claims']:
            assert len([r for r in before[table] if r['lease_id']==identifier])==1
            assert [r for r in before[table] if r['lease_id']!=identifier]==initial[table]
        assert before['archive_pin_recoveries']==initial['archive_pin_recoveries']
        claim=next(r for r in before['retention_claims'] if r['lease_id']==identifier)
        assert [claim[k] for k in ['dataset_id','topology_generation','shard_id','owner_epoch','first_sequence','last_sequence','purpose','state']]==['\\x'+'01'*16,0,0,1,1,10,'reader','active']
        proxy=Process(['docker','exec','-i',runner,'python3','-B','/out/pin-recovery-negative/proxy.py',
            '--target','control-pin-recovery','--mode','omit-recovery-claim-release'],input_pipe=True)
        started=report['proxy_ready']=event(proxy,'ready');assert started['source_sha256']==report['proxy_sha256']
        result=report['result']=fixture('claim-negative-recover','recover',started['port'])
        assert result['outcome']=='CommitUnknown' and result['commit_started'] and result['ordinary_reacquired']
        assert result['receipt'] is None
        send(proxy,'stop\n');assert proxy.wait(5)==0 and not proxy.stderr and not proxy.overflow
        terminal=report['proxy_terminal']=event(proxy,'terminal');report['proxy']=proxy_observation(proxy)
        assert [terminal[k] for k in ['unexpected','data_queries','data_commits','server_complete','client_complete','faults','pin_claim_injections']]==[0,1,1,0,0,0,1]
        assert terminal['pin_rejections']==['incomplete-recovery']
        proxy.stop();proxy=None
        report['after']=rows();assert report['after']==before
        report['remaining_pg_clients']=json.loads(sql("SELECT coalesce(json_agg(pid),'[]') FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()",'postgres'))
        assert report['remaining_pg_clients']==[]
        report['status']='PASS'
    finally:
        if proxy:
            try:send(proxy,'stop\n');proxy.wait(5)
            finally:proxy.stop();report['proxy_incomplete']=proxy_observation(proxy)
        save()
    print(json.dumps(dict(status=report['status'],source=state['source'],harness=revision)))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('root',type=Path)
    main(parser.parse_args().root.resolve())
