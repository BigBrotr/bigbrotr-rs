#!/usr/bin/env python3
"""Independently bind restored-authority resume to the original signed unit."""
import copy
import hashlib
import json
import time
from signed_publication_cases import Process, command, event, send, sha
from signed_publication_oracle import EVENT


def run(root, state, sql, expected_manifest, revoked):
    runner=state['container_id'];db='bbpub_control_normal'
    result=dict(status='INCOMPLETE',source=state['source'],output_hashes={})
    path=root/'resume-output-cases.json';assert not path.exists()
    client=None
    def save():path.write_text(json.dumps(result,indent=2)+'\n')
    def snapshot():
        tables=['jobs','job_input_bounds','export_jobs','export_execution_units','artifacts','artifact_files','snapshot_leases','archive_snapshot_pins','retention_claims','export_resume_commands','management_commands','management_audit']
        return {t:json.loads(sql(db,f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{t} t")) for t in tables}
    job=revoked['acceptance']['receipt']['job_id']
    def target(rows):return next(v['row'] for v in rows['jobs'] if v['row']['job_id']=='\\x'+job)
    def exchange(instruction,event_name):
        send(client,instruction+'\n');v=event(client,event_name);result[event_name]=v;save();return v
    def output():
        directory=root/'out/signed_normal/export-destination'/artifact
        raw=(directory/'events.jsonl').read_bytes();manifest=(directory/'manifest.json').read_bytes()
        assert raw==signed and json.loads(manifest)==expected
        return {name:dict(sha256=sha(directory/name),bytes=(directory/name).stat().st_size,inode=(directory/name).stat().st_ino) for name in ['events.jsonl','manifest.json']}
    try:
        assert revoked['status']=='PASS' and revoked['backends']=={db:0,'bbpub_normal':0}
        native=['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/resume_output_client.py']
        result['migration']=json.loads(command(*native,'migrate'))
        sql(db,"GRANT SELECT,INSERT ON control.export_resume_commands TO bigbrotr_control; GRANT EXECUTE ON FUNCTION control.export_resume_digest(control.export_resume_commands,control.management_commands) TO bigbrotr_control;")
        before=result['before']=snapshot();save()
        assert target(before)['state']=='paused' and target(before)['job_revision']==4
        # Migration must preserve the exact installed revocation result.
        canonical=lambda rows:sorted(json.dumps(v,sort_keys=True) for v in rows)
        for table,rows in revoked['terminal'].items():assert canonical(rows)==canonical(before[table]),table
        unit=next(v for v in before['export_execution_units'] if v['row']['job_id']=='\\x'+job)
        export_pin=next(v['row']['pin_lease_id'] for v in before['export_jobs'] if v['row']['job_id']=='\\x'+job)
        assert next(v['row']['state'] for v in before['snapshot_leases'] if v['row']['lease_id']==export_pin)=='active'
        artifact=unit['row']['artifact_id'][2:]
        signed=(json.dumps({k:EVENT[k] for k in ['id','pubkey','created_at','kind','tags','content','sig']},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        assert len(signed)==399
        expected=copy.deepcopy(expected_manifest);expected.update(artifact_id=artifact,job_id=job)
        expected['selection']['event_id']=EVENT['id'];expected['accepted_revisions']['policy']='3'
        expected['files'][0].update(bytes='399',rows='1',sha256=hashlib.sha256(signed).hexdigest())
        result['files_before']=output();save()
        client=Process([*native,job],input_pipe=True)
        result['ready']=event(client,'ready');save();assert result['ready']['denied']==403
        denied=snapshot()
        for table in before.keys()-{'management_audit'}:assert denied[table]==before[table],table
        # Login audit is the only new row before grant restoration.
        assert len(denied['management_audit'])==len(before['management_audit'])+1
        assert all(x in denied['management_audit'] for x in before['management_audit'])
        exchange('restore','restored')
        restored=result['after_restore']=snapshot();save()
        for table in ['jobs','job_input_bounds','export_jobs','export_execution_units','artifacts','artifact_files','export_resume_commands']:
            assert restored[table]==before[table],table
        accepted=exchange('resume','resumed')['receipt']
        end=time.monotonic()+12
        while True:
            after=snapshot();row=target(after)
            if row['state']=='succeeded':
                completed=next(v['row'] for v in after['artifacts'] if v['row']['job_id']=='\\x'+job)
                reader=completed['verification_pin_id']
                if all(next(v['row']['state'] for v in after[t] if v['row']['lease_id']==reader)=='released' for t in ['snapshot_leases','retention_claims']):break
            assert row['state'] in ['queued','running','succeeded'],row
            assert time.monotonic()<end,'resumed signed completion deadline'
            time.sleep(.05)
        result['completed']=after;save()
        assert row['job_revision']==7 and row['checkpoint_revision']==1 and row['reason_code'] is None
        assert len(after['jobs'])==len(before['jobs'])
        for table in ['job_input_bounds','export_jobs','export_execution_units']:assert after[table]==before[table],table
        assert all(v in after['jobs'] for v in before['jobs'] if v['row']['job_id']!='\\x'+job)
        inventory=[v['row'] for v in after['artifacts'] if v not in before['artifacts']]
        files=[v['row'] for v in after['artifact_files'] if v not in before['artifact_files']]
        assert len(inventory)==len(files)==1
        for table in ['artifacts','artifact_files']:
            assert all(v in after[table] for v in before[table]),table
        a,f=inventory[0],files[0]
        assert a['artifact_id']==f['artifact_id']=='\\x'+artifact and a['job_id']=='\\x'+job
        assert a['manifest_sha256']==row['result_manifest_digest'] and a['completed_at_us']==row['completed_at_us']
        assert f['file_id']=='events' and f['relative_path']=='events.jsonl' and f['row_count']==1 and f['byte_count']==399 and f['sha256']=='\\x'+hashlib.sha256(signed).hexdigest()
        found=result['files_after']=output()
        assert a['manifest_sha256']=='\\x'+found['manifest.json']['sha256'] and a['manifest_bytes']==found['manifest.json']['bytes'] and a['total_bytes']==399+a['manifest_bytes'] and a['file_count']==1
        for n in found:assert found[n]['sha256']==result['files_before'][n]['sha256']
        for table in ['snapshot_leases','archive_snapshot_pins','retention_claims']:
            assert all(v in after[table] for v in before[table]),table
            assert len(after[table])==len(before[table])+2,table # grant allocation and export reader
            assert all(v in after[table] for v in restored[table]),table
            assert len(after[table])==len(restored[table])+1,table
        reader=a['verification_pin_id']
        lease,pin,claim=[next(v['row'] for v in after[t] if v not in restored[t]) for t in ['snapshot_leases','archive_snapshot_pins','retention_claims']]
        assert lease['lease_id']==pin['lease_id']==claim['lease_id']==reader
        original=next(v['row'] for v in before['export_jobs'] if v['row']['job_id']=='\\x'+job)
        assert lease['dataset_id']==pin['dataset_id']==claim['dataset_id']==original['dataset_id']
        assert lease['consumer_id']==pin['consumer_id']==original['catalog_id']
        assert lease['frontier_revision']==pin['frontier_revision']==original['frontier_revision']
        assert lease['topology_generation']==claim['topology_generation']==target(before)['input_topology_generation']
        bounds=[v['row'] for v in before['job_input_bounds'] if v['row']['job_id']=='\\x'+job]
        assert len(bounds)==1 and (claim['shard_id'],claim['owner_epoch'],claim['first_sequence'],claim['last_sequence'])==(bounds[0]['shard_id'],bounds[0]['owner_epoch'],1,bounds[0]['last_complete_sequence'])==(0,1,1,10)
        assert claim['purpose']==pin['purpose']=='reader' and claim['release_reason']=='reader_joined'
        owner=result['ready']['owner']
        assert pin['issuer_resource']=='\\x'+owner['owner_resource_id'] and pin['issuer_epoch']==int(owner['owner_epoch']) and pin['issuer_incarnation']=='\\x'+owner['process_incarnation']
        assert next(v['row']['state'] for v in after['snapshot_leases'] if v['row']['lease_id']==reader)=='released'
        assert next(v['row']['purpose'] for v in after['archive_snapshot_pins'] if v['row']['lease_id']==reader)=='reader'
        assert next(v['row']['state'] for v in after['retention_claims'] if v['row']['lease_id']==reader)=='released'
        commands=[v['row'] for v in after['management_commands'] if v['row']['request_key']=='\\x'+'f2'*16]
        assert len(commands)==1 and str(commands[0]['accepted_at_us'])==accepted['accepted_at_us']
        assert accepted==dict(format='bigbrotr-export-resume/1',command_key='f2'*16,job_id=job,job_revision='5',accepted_at_us=str(commands[0]['accepted_at_us']),created=True,execution='unobserved',authority='acceptance-only')
        assert len(after['export_resume_commands'])==1
        assert len(after['management_commands'])==len(restored['management_commands'])+1
        for table in ['management_commands','management_audit']:
            assert len(after[table])==len(restored[table])+1
            assert all(v in after[table] for v in restored[table]),table
        c=commands[0];d=after['export_resume_commands'][0]['row']
        audit=next(v['row'] for v in after['management_audit'] if v['row']['audit_id']==c['audit_id'])
        original=next(v['row'] for v in before['export_jobs'] if v['row']['job_id']=='\\x'+job)
        assert c['dataset_id']==d['dataset_id']==audit['dataset_id']==original['dataset_id']
        assert c['actor_id']==d['actor_id']==audit['actor_id']==original['actor_id']
        assert c['request_key']==d['request_key']==audit['subject_id']=='\\x'+'f2'*16
        assert d['job_id']=='\\x'+job and d['expected_job_revision']==4
        assert c['action']==audit['action']=='job.export.resume' and audit['outcome']=='accepted'
        assert c['expected_config_revision']==audit['config_revision']==1 and c['policy_revision']==audit['policy_revision']==4
        assert c['accepted_at_us']==audit['created_at_us']
        payload=b'bigbrotr/export-resume/1\0'+b''.join(bytes.fromhex(d[k][2:]) for k in ['dataset_id','actor_id','request_key','job_id'])+b''.join(n.to_bytes(8,'big') for n in [4,1,4])
        assert c['request_digest']=='\\x'+hashlib.sha256(payload).hexdigest()
        exchange('retry','retried');assert snapshot()==after and output()==found
        restarted=exchange('restart','restarted');time.sleep(1.5)
        assert int(restarted['owner']['owner_epoch'])==int(result['ready']['owner']['owner_epoch'])+1
        assert snapshot()==after and output()==found
        exchange('stop','terminal');assert client.wait(10)==0 and not client.stderr and not client.overflow
        client.stop();client=None
        result['terminal']=snapshot();assert result['terminal']==after
        result['backends']={d:int(sql(d,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) for d in [db,'bbpub_normal']}
        assert result['backends']=={db:0,'bbpub_normal':0}
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        result['output_hashes']={str((root/'out/signed_normal/export-destination'/artifact/n).relative_to(root/'out')):m['sha256'] for n,m in found.items()}
        result['status']='PASS';return result
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        if client:
            try:
                client.stop()
            except BaseException as error:
                result['cleanup_error']=type(error).__name__
                result['status']='INCOMPLETE'
            finally:
                result['client_output']=client.stdout.decode();result['client_error']=client.stderr.decode()
        save()
