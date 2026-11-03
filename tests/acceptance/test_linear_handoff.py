"""Source-only discriminants for the single declared zero-query handoff."""
import copy
import importlib.util
import json
import tempfile
import sys
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from test_linear_capped_adoption import ROOT,ACCEPTED,adoption_fault_fixture,current42_head,head_digest,native_reference,fixture,projection
from test_linear_capped_adoption import root_configuration_report
from cases import first_launch_handoff_oracle as check
from cases import first_launch_handoff as case
from cases import first_launch_capped_oracle as base
from cases import first_launch_restart as source
from cases import first_launch_quantum_oracle as quantum
from installed_family_science import NetworkScience
from transition_replay import native_rows
from initial_succession_oracle import exclusion,successor_rows
from installed_family_science import decode


def sync_fault_diagnostic(**changes):
    value=dict(format='bigbrotr-initial-sync-diagnostic/1',scope='caller-progress-not-commit-outcome',
        timing='monotonic-offset-microseconds-saturating',stage='quantum-plan',reason='operation-error',
        frame_ordinal=None,request_sequence=None,last_delivered_admission_sequence=None,
        authority_checked_us=2,entered_us=10,returned_us=20,captured_us=21,renewal_timeout_us=100,
        authority_remaining_us=200,due_remaining_us=60,maximum_us=60,renewal_due_us=62,
        parent_deadline_us=62,dispatch_deadline_us=62,outer_deadline_us=62)
    value.update(changes)
    return value


def sync_fault_stderr(value=None,error='Quantum(CommitUnknown)'):
    value=sync_fault_diagnostic() if value is None else value
    return 'error: initial Sync unavailable: '+error+'; sync_dispatch='+json.dumps(value)+'\n'


def zero_fixture(root):
    original,sealed,before,state,report,events,wires,first,_,lower,upper,fault=adoption_fault_fixture(root,'after-complete')
    manifest,handoff,activated=base.adoption_activated_native(before,state,first)
    domains=[source.only(state['allocation_domains'],owner_epoch=2,dictionary_kind=k)['allocation_domain'] for k in range(1,10)]
    model=SimpleNamespace(ds=source.only(state['datasets']),slot='\\x'+report['producer_slot_id'],
        legacy=dict(previous_manifest='\\x'+first['manifest'].hex()),inputs={},native=copy.deepcopy(activated))
    science=NetworkScience(model,dict(epoch=2,topology=1,manifest=manifest,domains=domains),acknowledge=True,
        new_event=events[2],endpoint='ws://127.0.0.1:7447/installed-fixture',previous_event=events[1],previous_coordinates=(3,4,13,14,15,16))
    science.attribution(lower+100_000)
    current=source.only(state['owner_epochs'],role='control',epoch=4)
    ingest=source.only(state['owner_epochs'],role='ingest',epoch=2)
    ingest.update(lease_expires_at_us=lower+50_000+60_000_000,xmin='135')
    common=dict(dataset_id=current['dataset_id'],producer_slot_id=model.slot,producer_epoch=2)
    original_input= science.inputs[1];receipt=science.receipts[1]
    nulls=dict.fromkeys(('relay_endpoint_id','relay_access_scope_id','relay_enrollment_generation','relay_route_id','acquisition_scope_id','acquisition_job_id',*source.PROGRESS_FIELDS))
    state['producer_requests'].append(dict(common,manifest_sha256='\\x'+source.sha(manifest),request_sequence=1,request_digest='\\x'+source.sha(original_input),
        input_bytes=len(original_input),input_profile=1,monitor_attempt_id=None,monitor_resolution=None,issuer_role='control',
        issuer_resource=current['resource_id'],issuer_epoch=current['epoch'],issuer_incarnation=current['process_incarnation'],
        **nulls,reserved_at_us=lower+101_000,xmin='140'))
    state['producer_outcomes'].append(dict(common,manifest_sha256='\\x'+source.sha(manifest),request_sequence=1,request_digest='\\x'+source.sha(original_input),
        outcome_kind=1,outcome_bytes='\\x'+receipt.hex(),outcome_sha256='\\x'+source.sha(receipt),policy_epoch=1,
        recorder_role='control',recorder_resource=current['resource_id'],recorder_epoch=current['epoch'],recorder_incarnation=current['process_incarnation'],
        recorded_at_us=lower+102_000,xmin='141'))
    source.only(state['producer_epochs'],producer_epoch=2).update(reserved_through_request=1,xmin='140')
    deps=[dict(common,source_shard=ref[1],source_epoch=ref[2],source_sequence=ref[3],payload_digest='\\x'+digest.hex(),xmin='141')
        for ref,digest in quantum.receipt_refs(receipt)]
    head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(manifest),checkpoint_revision=1,complete_through_request=1,
        progress_jobs=0,dependency_count=5,issuer_resource=current['resource_id'],issuer_epoch=current['epoch'],issuer_incarnation=current['process_incarnation'],
        committed_at_us=lower+102_000,xmin='141',checkpoint_digest='\\x'+'00'*32)
    head['checkpoint_digest']='\\x'+head_digest(head,[],deps,lambda row:native_reference(row,science.values)).hex()
    state['producer_checkpoints'].append(current42_head(head,initial=False));state['producer_checkpoint_dependencies']+=deps
    activation=source.only(state['initial_ingest_activations'])
    started=dict(state='activated',producer_epoch='2',dataset_id=current['dataset_id'][2:],producer_slot_id=model.slot[2:],
        owner_resource_id=ingest['resource_id'][2:],process_incarnation=ingest['process_incarnation'][2:],
        config_revision='1',policy_revision='1',config_sha256='ab'*32,policy_sha256='ab'*32,
        activated_at_us=str(activation['activated_at_us']),acquisition='finite-initial-successor',ready=False)
    return before,state,first,started,wires[:1],events,model.native,[original_input],lower,upper,fault


def journey_fixture(root):
    before,zero,first,started,wires,events,dump,inputs,lower,upper,fault=zero_fixture(root)
    science=check.zero_query_cut(before,zero,first,started,wires,events,dump,inputs,lower,upper,fault)
    job=zero['jobs'][0];slot='\\x'+started['producer_slot_id'];ds=job['dataset_id']
    def control(state,epoch,at,tx):
        previous=max((r for r in state['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        row=dict(previous,epoch=epoch,role_instance_id='\\x'+bytes([0x40+epoch]).hex()*16,
            process_incarnation='\\x'+bytes([0x50+epoch]).hex()*16,acquired_at_us=at+100,lease_expires_at_us=at+250_000+60_000_000,
            state='released',released_at_us=at+500_000,exclusion_method='storage_fenced',exclusion_digest=None,
            control_backend_pid=7500+epoch,control_backend_start='successor-'+str(epoch),xmin=str(tx+4))
        row['exclusion_digest']=exclusion(row,row['process_incarnation'])
        if previous['state']=='active':
            previous.update(state='released',released_at_us=at+10,exclusion_method='storage_fenced',exclusion_digest=exclusion(previous,row['process_incarnation']),xmin=str(tx))
            source.only(state['role_instances'],role_instance_id=previous['role_instance_id']).update(retired_at_us=at+20,xmin=str(tx))
        state['owner_epochs'].append(row)
        state['role_instances'].append(dict(dataset_id=ds,role='control',role_instance_id=row['role_instance_id'],created_at_us=at+50,retired_at_us=at+500_001,xmin=str(tx+4)))
        return row
    sealed=copy.deepcopy(zero);at=upper+1000;sealer=control(sealed,5,at,200)
    digest='\\x'+source.closure(science['manifest'],slot,2,science['inputs'],science['receipts'],4,science['handoff'])
    source.only(sealed['producer_epochs'],producer_epoch=2).update(state='closing',xmin='201')
    source.only(sealed['source_streams'],owner_epoch=2).update(state='closed',closed_at_us=at+200_000,last_complete_sequence=4,closure_digest=digest,xmin='202')
    head=source.only(zero['producer_checkpoints'],producer_epoch=2);old=zero['acquisition_continuations'][0]
    sealed['acquisition_continuations'].append(dict(old,continuation_id='\\x'+'ee'*16,previous_job_revision=3,previous_continuation=old['continuation_id'],
        previous_owner_resource=job['owner_resource_id'],previous_owner_epoch=job['owner_epoch'],previous_owner_incarnation=job['owner_incarnation'],
        producer_epoch=2,manifest_sha256='\\x'+source.sha(science['manifest']),source_sequence=4,source_digest=digest,checkpoint_revision=1,
        complete_through_request=1,checkpoint_sha256=head['checkpoint_digest'],sealing_resource=sealer['resource_id'],sealing_epoch=5,
        sealing_incarnation=sealer['process_incarnation'],sealed_at_us=at+300_000,**dict.fromkeys(base.ADOPTION_FIELDS),xmin='203'))
    retired=copy.deepcopy(sealed);at2=at+1_000_000;retirer=control(retired,6,at2,210);retirement_at=at2+300_000
    source.only(retired['producer_epochs'],producer_epoch=2).update(state='retired',retired_at_us=retirement_at,closure_digest=digest,xmin='213')
    ingest=source.only(retired['owner_epochs'],role='ingest',epoch=2)
    ingest.update(state='released',released_at_us=retirement_at,exclusion_method='storage_fenced',exclusion_digest=digest,xmin='213')
    source.only(retired['role_instances'],role_instance_id=ingest['role_instance_id']).update(retired_at_us=retirement_at,xmin='213')
    for row in retired['allocation_domains']:
        if row['owner_epoch']==2:row.update(state='closed',closed_at_us=retirement_at,closure_digest=digest,xmin='213')
    report=dict(format='bigbrotr-ingest-recovery/1',state='activated-acquisition-sealed',dataset_id=ds[2:],owner_resource_id=ingest['resource_id'][2:],
        producer_slot_id=slot[2:],producer_epoch='2',issued_through='1',reserved_through='1',resolved_requests='1',
        source_closure=dict(last_complete_sequence='4',sha256=digest[2:]),native='closed',owner_release='committed',ready=False,
        acquisition_continuation=dict(sealed_jobs='1',execution='not-authorized'))
    reports=[report,dict({k:v for k,v in report.items() if k!='acquisition_continuation'},state='activated-initial-retired',
        ingest_retirement=dict(allocation_domains='9',acquisition_gaps='0',owner_release='committed',native_acknowledged='1',native_retired='0'))]
    bounds=[(at,at+600_000),(at2,at2+600_000)]
    after=copy.deepcopy(retired);begin=at2+1_000_000;current=control(after,7,begin,220);current.update(released_at_us=begin+3_000_000,lease_expires_at_us=begin+2_000_000+60_000_000)
    source.only(after['role_instances'],role_instance_id=current['role_instance_id']).update(retired_at_us=begin+3_000_001)
    created=begin+10_000;inc='\\x'+'af'*16;role='\\x'+'ae'*16;domains=['\\x'+bytes([180+k]).hex()*16 for k in range(1,10)]
    manifest=bytearray(science['manifest']);manifest[2:4]=source.be(3,2);manifest[26:34]=source.be(2);manifest[54:62]=source.be(3);manifest[62:78]=source.raw(inc)
    for k,domain in enumerate(domains):manifest[106+k*18:122+k*18]=source.raw(domain)
    manifest=bytes(manifest);handoff=(b'\x02\x0a\x01'+source.raw(slot)+source.be(2)+source.be(1)+source.be(4)+source.raw(digest)
        +bytes.fromhex(source.sha(science['handoff']))+source.be(266,2)+science['manifest']+source.be(266,2)+manifest)
    after['owner_epochs'].append(dict(ingest,epoch=3,role_instance_id=role,process_incarnation=inc,acquired_at_us=created,lease_expires_at_us=begin+2_000_000+60_000_000,
        state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin='226'))
    after['role_instances'].append(dict(dataset_id=ds,role='ingest',role_instance_id=role,created_at_us=created,retired_at_us=None,xmin='221'))
    source2=source.only(retired['source_streams'],owner_epoch=2);producer2=source.only(retired['producer_epochs'],producer_epoch=2)
    after['source_streams'].append(dict(source2,owner_epoch=3,owner_incarnation=inc,state='open',opened_at_us=created,closed_at_us=None,last_complete_sequence=None,closure_digest=None,xmin='221'))
    after['producer_epochs'].append(dict(producer2,producer_epoch=3,producer_role_instance_id=role,producer_incarnation=inc,owner_epoch=3,owner_incarnation=inc,
        state='open',reserved_through_request=3,opened_at_us=created,retired_at_us=None,closure_digest=None,xmin='303'))
    for k,domain in enumerate(domains,1):
        old_domain=source.only(retired['allocation_domains'],owner_epoch=2,dictionary_kind=k)
        after['allocation_domains'].append(dict(old_domain,allocation_domain=domain,owner_epoch=3,owner_incarnation=inc,state='active',created_at_us=created,closed_at_us=None,closure_digest=None,xmin='221'))
    after['topology_generations'].append(dict(dataset_id=ds,generation=2,previous_generation=1,created_at_us=created,sealed=True,xmin='221'))
    after['topology_streams'] += [dict(dataset_id=ds,topology_generation=2,shard_id=0,owner_epoch=e,handoff_last_complete_sequence=cut,xmin='221') for e,cut in ((1,17),(2,4),(3,None))]
    after['datasets'][0].update(topology_generation=2,xmin='221')
    prior=retired['initial_ingest_successions'][0]
    after['ingest_successions']=[dict(prior,target_epoch=3,previous_epoch=2,terminal_kind=1,target_incarnation=inc,target_role_instance_id=role,producer_epoch=3,
        previous_topology=1,target_topology=2,issued_through=1,last_complete_sequence=4,closure_digest=digest,previous_manifest='\\x'+science['manifest'].hex(),
        target_manifest='\\x'+manifest.hex(),previous_manifest_sha256='\\x'+source.sha(science['manifest']),target_manifest_sha256='\\x'+source.sha(manifest),
        handoff='\\x'+handoff.hex(),handoff_sha256='\\x'+source.sha(handoff),parent_handoff_sha256='\\x'+source.sha(science['handoff']),created_at_us=created,
        lease_expires_at_us=created+5_000_000,prepared_by_resource=current['resource_id'],prepared_by_epoch=7,prepared_by_incarnation=current['process_incarnation'],preparation_transaction='221',xmin='221')]
    intent=after['ingest_successions'][0]
    after['ingest_activations']=[dict({k:intent[k] for k in ('dataset_id','owner_resource_id','target_epoch','target_incarnation','target_role_instance_id','producer_slot_id','producer_epoch','target_manifest_sha256','handoff_sha256')},
        activated_by_resource=current['resource_id'],activated_by_epoch=7,activated_by_incarnation=current['process_incarnation'],activated_at_us=created+100,activation_transaction='222',xmin='222')]
    seal=source.only(after['acquisition_continuations'],producer_epoch=2)
    seal.update(target_epoch=3,target_manifest_sha256='\\x'+source.sha(manifest),activation_sha256='\\x'+source.sha(handoff),adopting_resource=current['resource_id'],adopting_epoch=7,
        adopting_incarnation=current['process_incarnation'],adopted_job_revision=4,adopted_at_us=created+200,adoption_transaction='223',xmin='223')
    after['jobs'][0].update(job_revision=4,owner_resource_id=current['resource_id'],owner_epoch=7,owner_incarnation=current['process_incarnation'],xmin='223')
    all_wires=fixture(root)[6];shift=begin+1_000_000-all_wires[1]['connected_at_us']
    for wire in all_wires[1:]:
        for key in ('connected_at_us','request_at_us','sent_at_us','closed_at_us'):wire[key]+=shift
    closed=check.closed_attribution(science,slot)
    values=successor_rows({(r['cf'],bytes.fromhex(r['key'])):bytes.fromhex(r['value']) for r in closed['rows']},manifest,source.raw(slot),handoff,repeatable=True)
    model=SimpleNamespace(ds=after['datasets'][0],slot=slot,legacy=dict(previous_manifest='\\x'+first['manifest'].hex()),inputs={},
        native=dict(event='dump-state',sequence=119,rows=[dict(cf=cf,key=key.hex(),value=v.hex()) for (cf,key),v in sorted(values.items())]))
    encoder=NetworkScience(model,dict(epoch=3,topology=2,manifest=manifest,domains=domains),acknowledge=True,new_event=events[2],endpoint='ws://127.0.0.1:7447/installed-fixture',previous_event=events[1],previous_coordinates=(3,4,13,14,15,16))
    encoder.network(True,all_wires[1]['connected_at_us']-10,all_wires[2]['sent_at_us']+10,all_wires[2]['sent_at_us']+50,1,
        (all_wires[1]['sent_at_us']+10,all_wires[1]['sent_at_us']+50,1),historical_first=True)
    full=dict(native=[(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in model.native['rows']],inputs=list(encoder.inputs.values()),receipts=list(encoder.receipts.values()),manifest=manifest,handoff=handoff)
    template=fixture(root)[3]['acquisition_quanta'][1:]
    for n,row in enumerate(template,2):
        wire=all_wires[n-1];effects=[] if n==5 else [(events[1 if n==2 else 2],2 if n<4 else 5,n if n<4 else 0,quantum.receipt_refs(full['receipts'][1 if n==2 else 2]))]
        records=b''.join(source.be(1,2)+source.be(1)+bytes.fromhex(source.sha(quantum.wire_json(e)))+source.be(c,2)+source.be(request)+source.be(e['created_at'])+b'\x01'+bytes.fromhex(e['id']) for e,c,request,refs in effects)
        refs=quantum.reference_bytes([ref for e,c,request,refs in effects for ref in refs])
        row.update(producer_epoch=3,manifest_sha256='\\x'+source.sha(manifest),issuer_resource=current['resource_id'],issuer_epoch=7,issuer_incarnation=current['process_incarnation'],
            created_at_us=wire['connected_at_us']-1,terminal_received_at_us=wire['sent_at_us']+200,completed_at_us=wire['closed_at_us']+10,effect_records='\\x'+records.hex(),dependencies='\\x'+refs.hex(),
            charged_bytes=1024+len(source.raw(row['request_predicate']))+len(source.raw(row['terminal_frame']))+len(records)+256*(len(refs)//72))
        after['acquisition_quanta'].append(row)
    projection(after,full,events,all_wires,3)
    final_head=source.only(after['producer_checkpoints'],producer_epoch=3);progress=source.only(after['acquisition_progress'],producer_epoch=3)
    for n in (2,3):
        wire=all_wires[n-1];item=full['inputs'][n-1];event=events[1 if n==2 else 2]
        p=dict(progress,progress_revision=n-1,classified_events=n-1,successful_events=n-1,request_id='\\x'+wire['request'][1].encode().hex(),
            input_sha256='\\x'+source.sha(quantum.wire_json(event)),intent_digest='\\x'+source.sha(item),classification=2,mutation_request=n,
            observed_at_us=decode(item.split(b'\0',1)[1])[3][5][0])
        refs={tuple(ref):digest for r in full['receipts'][:n] for ref,digest in quantum.receipt_refs(r)}
        deps=[dict(dataset_id=ds,producer_slot_id=slot,producer_epoch=3,source_shard=ref[1],source_epoch=ref[2],source_sequence=ref[3],payload_digest='\\x'+digest.hex(),xmin='1') for ref,digest in sorted(refs.items())]
        head={k:v for k,v in final_head.items() if not k.startswith('prefix_')};head.update(checkpoint_revision=n,complete_through_request=n,dependency_count=len(deps))
        after['acquisition_quanta'][n-1]['checkpoint_sha256']='\\x'+head_digest(head,[p],deps,lambda row:native_reference(row,encoder.values)).hex()
    for n in (4,5):after['acquisition_quanta'][n-1]['checkpoint_sha256']=final_head['checkpoint_digest']
    started3=dict(state='activated',producer_epoch='3',dataset_id=ds[2:],producer_slot_id=slot[2:],owner_resource_id=ingest['resource_id'][2:],
        process_incarnation=inc[2:],role_instance_id=role[2:],prepared_at_us=str(created),lease_expires_at_us=str(created+5_000_000),
        config_revision='1',policy_revision='1',activated_at_us=str(created+100),acquisition='finite-successor',ready=False)
    return zero,sealed,retired,science,bounds,reports,after,started3,all_wires,events,first,model.native,full['inputs'],begin,begin+3_100_000


class HandoffControls(unittest.TestCase):
    def test_first_plan_fault_requires_ordinary_sync_error_and_preserves_late_typed_errors(self):
        for changes in ({},{'returned_us':70,'captured_us':71}):
            value=sync_fault_diagnostic(**changes)
            self.assertEqual(check.zero_query_fault_error('',sync_fault_stderr(value)),value)
        startup='error: initial native batch unavailable: Quantum(CommitUnknown)\n'
        base.adoption_fault_error('',startup)
        with self.assertRaises(AssertionError):check.zero_query_fault_error('',startup)
        with self.assertRaises(AssertionError):base.adoption_fault_error('',sync_fault_stderr())

    def test_first_plan_fault_rejects_wrong_error_output_and_malformed_diagnostics(self):
        valid=sync_fault_stderr();prefix=valid.split('sync_dispatch=',1)[0]+'sync_dispatch='
        for stdout,stderr in (('{}\n',valid),('',valid+'extra\n'),('','extra\n'+valid),('',valid.rstrip('\n')),
                ('',valid+valid),('',prefix+'{}\n'),('',prefix+'[]\n'),('',prefix+'null\n'),('',prefix+'{\n'),
                ('',sync_fault_stderr(error='Quantum(Backend)')),('',sync_fault_stderr(error='Native(CommitUnknown)')),
                ('',prefix+json.dumps(sync_fault_diagnostic())[:-1]+',"stage":"quantum-plan"}\n')):
            with self.subTest(stdout=stdout,stderr=stderr),self.assertRaises((AssertionError,ValueError)):
                check.zero_query_fault_error(stdout,stderr)
        value=sync_fault_diagnostic()
        for key in value:
            missing=dict(value);missing.pop(key)
            with self.subTest(missing=key),self.assertRaises(AssertionError):check.zero_query_fault_error('',sync_fault_stderr(missing))
        for key,item in (('stage','quantum-terminal'),('reason','deadline'),('format','bigbrotr-initial-sync-diagnostic/2'),
                ('scope','commit-outcome'),('timing','utc'),('frame_ordinal',0),('request_sequence',1),
                ('last_delivered_admission_sequence',1),('entered_us',True),('returned_us',None),('captured_us',19),
                ('authority_checked_us',11),('entered_us',21),('renewal_due_us',-1),('maximum_us',0),
                ('maximum_us',61),('authority_remaining_us',119),('outer_deadline_us',63),('extra',1)):
            with self.subTest(field=key),self.assertRaises(AssertionError):
                check.zero_query_fault_error('',sync_fault_stderr(sync_fault_diagnostic(**{key:item})))

    def test_epoch3_initialized_and_settlement_share_one_derived_stage_budget(self):
        for exhausted in (False,True):
            with self.subTest(exhausted=exhausted),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=object.__new__(case.ZeroQueryHandoffRuntime);runtime.root=root;runtime.home=root/'peer';runtime.home.mkdir()
                runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
                runtime.handoff_direct_document=case.digest(runtime.config);runtime.handoff_root_ttl=5_000_000
                runtime.remaining_budget=Mock(return_value=50);runtime.clock=Mock(side_effect=[100,200])
                runtime.launch=Mock(return_value=dict(id='epoch3'));started=dict(state='activated',producer_epoch='3',acquisition='finite-successor',ready=False)
                (runtime.home/'commands-finished.json').write_text('[]\n');(runtime.home/'client.stderr').write_text('')
                case.save(runtime.home/'client-result.json',dict(status='PASS',handoff='zero-query',gate_seconds=110,job_status=dict(job_revision='4')))
                case.save(runtime.home/'capped-adoption-peer-complete.json',dict(requests=5,listening=True,listener_closed=False))
                runtime.wait=Mock(side_effect=[[started],True,True]);runtime.wires=Mock();runtime.capture=Mock(return_value=({},{}))
                runtime.stop_container=Mock(return_value=dict(State=dict(ExitCode=0)));runtime.backend_absence=Mock()
                runtime.client=Mock();runtime.client.poll.return_value=None;runtime.client.wait.return_value=0;runtime.processes=[runtime.client]
                clocks=[0,1_000_000_000,51_000_000_000] if exhausted else [0,1_000_000_000,31_000_000_000,32_000_000_000,33_000_000_000]
                with patch.object(case.time,'CLOCK_BOOTTIME',7,create=True),patch.object(case.time,'clock_gettime_ns',side_effect=clocks),patch.object(case,'stop_process'):
                    if exhausted:
                        with self.assertRaisesRegex(ValueError,'stage exhausted'):runtime.run_epoch3(root)
                    else:runtime.run_epoch3(root)
                self.assertEqual(runtime.wait.call_args_list[0].args[2],49)
                if exhausted:
                    self.assertEqual(runtime.wait.call_count,1);runtime.capture.assert_not_called();runtime.client.wait.assert_not_called()
                else:
                    self.assertEqual(runtime.wait.call_args_list[1].args[2],18)
                    self.assertTrue((runtime.home/'adoption-old-cut.continue').exists())
                    self.assertTrue((runtime.home/'commands-finished.continue').exists())

    def test_short_epoch2_recovery_keeps_the_actual_root_bound_and_complete_census(self):
        for failure in (None,'authority','native'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);zero,sealed,retired,science,bounds,reports,*_=journey_fixture(root)
                runtime=object.__new__(case.ZeroQueryHandoffRuntime);runtime.root=root;runtime.state=root/'state'
                runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
                runtime.handoff_zero=zero;runtime.handoff_attribution=science
                slot=source.only(zero['producer_epochs'],producer_epoch=2)['producer_slot_id'];runtime.initialized=dict(producer_slot_id=slot[2:])
                runtime.handoff_input_files={'originals':'retained'};runtime.files=Mock(return_value=runtime.handoff_input_files);runtime.wires=Mock()
                runtime.clock=Mock(side_effect=[value for pair in bounds for value in pair])
                def finite(label,*args):
                    if label=='handoff-root-config':
                        value=root_configuration_report(case.digest(runtime.config));(root/'bootstrap').mkdir();case.save(root/'bootstrap/handoff-root-config.stdout',value);return value
                    return reports[0 if label=='seal-zero-query-initial' else 1]
                runtime.finite=Mock(side_effect=finite);runtime.capture=Mock(side_effect=[(sealed,{}),(retired,{})]);runtime.backend_absence=Mock()
                closed=check.closed_attribution(science,slot)
                if failure=='native':closed['sequence']=113
                clocks=[0,5_000_000_000] if failure=='authority' else [0,1_000_000_000,2_000_000_000,3_000_000_000]
                with patch.object(case.time,'CLOCK_BOOTTIME',7,create=True),patch.object(case.time,'clock_gettime_ns',side_effect=clocks),patch.object(case.native,'census',return_value=closed):
                    if failure is None:runtime.recover_epoch2(root)
                    else:
                        with self.assertRaises((AssertionError,ValueError)):runtime.recover_epoch2(root)
                if failure=='authority':runtime.capture.assert_not_called()
                elif failure is None:
                    self.assertEqual(runtime.handoff_closed['sequence'],114)
                    self.assertEqual(runtime.finite.call_args_list[1].args,('seal-zero-query-initial','ingest','recover','--file','/etc/bigbrotr/instance.toml','--seal-activated-acquisition'))
                    self.assertEqual(runtime.finite.call_args_list[2].args[-1],'--retire-activated-initial')

    def test_checkpoint_epoch_extension_is_explicit_and_keeps_old_defaults_closed(self):
        from installed_progress_context_oracle import current_head_core
        with tempfile.TemporaryDirectory() as temporary:
            _,state,*_=zero_fixture(Path(temporary))
            head=source.only(state['producer_checkpoints'],producer_epoch=2)
            current_head_core(head,prefix_profile=False)
            later=dict(head,producer_epoch=3)
            with self.assertRaises(AssertionError):current_head_core(later,prefix_profile=False)
            self.assertEqual(current_head_core(later,prefix_profile=False,producer_epoch=3)['producer_epoch'],3)
            for selected in (True,0,1,256):
                with self.subTest(epoch=selected),self.assertRaises(AssertionError):current_head_core(later,prefix_profile=False,producer_epoch=selected)
            with self.assertRaises(AssertionError):current_head_core(later,prefix_profile=True,producer_epoch=3)

    def test_only_explicit_handoff_replaces_expiry_wait_with_real_fencing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);original,sealed,retired,_,_,_,_,first,*_=adoption_fault_fixture(root,'before-forward')
            old=source.only(original['owner_epochs'],role='control')
            for snapshot in (original,sealed,retired):
                source.only(snapshot['owner_epochs'],role='control',epoch=old['epoch'])['lease_expires_at_us']=ACCEPTED+70_000_000
            with self.assertRaises(AssertionError):base.adoption_recovery_prefix(original,sealed,retired,first)
            base.adoption_recovery_prefix(original,sealed,retired,first,initial_exclusion='exclusive-before-expiry')
            runtime=object.__new__(case.ZeroQueryHandoffRuntime);runtime.root=root;runtime.quantum_fault='plan-before'
            runtime.resources=dict(quantum_proxy=dict(status='joined'));runtime.backend_absence=Mock();runtime.wait_control_expiry=Mock()
            case.save(root/'native-observer-terminal.json',dict(state=dict(ExitCode=0,Pid=0,Running=False,OOMKilled=False)))
            runtime.control_lease_observation=Mock(return_value=dict(active_unexpired=1))
            runtime.wait_adoption_owner_exclusion(root);runtime.wait_control_expiry.assert_not_called();runtime.backend_absence.assert_called_once()
            runtime.control_lease_observation.return_value=dict(active_unexpired=0)
            with self.assertRaises(ValueError):runtime.wait_adoption_owner_exclusion(root)

    def test_long_epoch2_serve_joins_real_fault_and_attribution_before_recovery(self):
        from installed import set_value
        for failure in (None,'counter','pid','native','root-change','extra-report','startup-error','sync-stage','sync-extra'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);before,state,first,started,wires,events,dump,inputs,lower,upper,fault=zero_fixture(root)
                upper=lower+31_000_000
                for owner in state['owner_epochs']:
                    if owner['state']=='active':owner['lease_expires_at_us']=lower+30_000_000+60_000_000
                runtime=object.__new__(case.ZeroQueryHandoffRuntime);runtime.root=root;runtime.home=root/'peer';runtime.home.mkdir()
                runtime.source=ROOT;runtime.state=root/'state';runtime.config=root/'instance.toml'
                runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n');runtime.set_value=set_value
                runtime.adoption_before=before;runtime.adoption_sealed=before;runtime.adoption_retired=before;runtime.adoption_first=first
                runtime.initialized=dict(producer_slot_id=started['producer_slot_id']);runtime.events=events;runtime.inputs={}
                runtime.remaining_budget=Mock(return_value=50);runtime.clock=Mock(side_effect=[lower,upper])
                runtime.resources=dict(quantum_proxy=dict(status='joined'),handoff_proxy=dict(status='running'))
                runtime.handoff_proxy_binding=dict(mode='before-forward',target='control-quantum-plan',commit_ordinal=1,frame_limit=131072,connection_limit=64,wire_limit=64*1024*1024)
                def start(*args):
                    runtime.config.write_text(set_value(runtime.config.read_text(),'postgres.control','endpoint','127.0.0.1:12345'))
                    runtime.handoff_route=Mock()
                runtime.start_commit_proxy=Mock(side_effect=start)
                def finite(*args):
                    value=root_configuration_report(case.digest(runtime.config));(root/'bootstrap').mkdir()
                    case.save(root/'bootstrap/handoff-zero-root-config.stdout',value)
                    if failure=='root-change':runtime.config.write_text(runtime.config.read_text()+'\n#changed\n')
                    return value
                runtime.finite=Mock(side_effect=finite);runtime.launch=Mock(return_value=dict(id='epoch2'))
                runtime.proxy_event=Mock(return_value=dict(fault,backend_pid=1 if failure=='pid' else fault['backend_pid']))
                runtime.docker=Mock(return_value=SimpleNamespace(stdout='4\n'))
                runtime.inspect=Mock(return_value=dict(Id='epoch2',State=dict(ExitCode=4,Pid=0,Running=False,OOMKilled=False)))
                def logs(entry,output):
                    stderr=sync_fault_stderr(sync_fault_diagnostic(stage='quantum-terminal')) if failure=='sync-stage' else sync_fault_stderr()
                    if failure=='startup-error':stderr='error: initial native batch unavailable: Quantum(CommitUnknown)\n'
                    if failure=='sync-extra':stderr+='extra\n'
                    (output/'serve.stderr').write_text(stderr)
                    return [started,started] if failure=='extra-report' else [started]
                runtime.logs=Mock(side_effect=logs)
                terminal=dict(runtime.handoff_proxy_binding,event='terminal',unexpected=0,frames=10,connections=1,bytes=100,
                    data_queries=1,data_commits=1,matched_commits=1,skipped_commits=0,selected_commits=1,selected_server_complete=0,
                    selected_client_complete=0,server_complete=0,client_complete=0,faults=0 if failure=='counter' else 1)
                def stop():
                    runtime.resources['handoff_proxy']['status']='joined';case.save(root/'handoff-proxy-terminal.json',terminal)
                runtime.stop_handoff_proxy=Mock(side_effect=stop);runtime.backend_absence=Mock();runtime.capture=Mock(return_value=(state,{}))
                directory=runtime.state/'data/ingest/producer-inputs-v1';directory.mkdir(parents=True)
                (directory/f"{started['producer_slot_id']}-0000000000000002-0000000000000001.cbor").write_bytes(inputs[0])
                runtime.files=Mock(return_value={'attribution':'exact'});runtime.wires=Mock(return_value=wires)
                runtime.client=Mock(pid=42);runtime.client.poll.return_value=None;runtime.owned_listeners=Mock(return_value=['listener']);runtime.loopback_listener=Mock()
                if failure=='native':dump['sequence']=91
                def census(*args,**kwargs):
                    runtime.stop_handoff_proxy.assert_called_once();runtime.backend_absence.assert_called_once()
                    runtime.recover_epoch2.assert_not_called();return dump
                def recover(folder):
                    self.assertEqual(runtime.handoff_attribution['inputs'],inputs)
                    self.assertTrue((folder/'serve-authority.json').exists())
                    self.assertEqual(case.digest(folder/'instance-epoch2.toml'),case.load(folder/'serve-authority.json')['root_config_document_sha256'])
                    self.assertEqual(runtime.resources['handoff_proxy']['status'],'joined')
                    self.assertEqual(runtime.handoff_attribution['dump']['sequence'],113)
                runtime.recover_epoch2=Mock(side_effect=recover);runtime.run_epoch3=Mock()
                with patch.object(base,'adoption_recovery_prefix'),patch.object(case.native,'census',side_effect=census):
                    if failure is None:self.assertTrue(runtime.after_adoption_retirement(root,{}))
                    else:
                        with self.assertRaises((AssertionError,ValueError)):runtime.after_adoption_retirement(root,{})
                if failure is None:
                    runtime.recover_epoch2.assert_called_once();runtime.run_epoch3.assert_called_once()
                    self.assertEqual(case.load(root/'zero-query-handoff/serve-authority.json')['root_authorization_ttl_us'],5_000_000)
                else:runtime.recover_epoch2.assert_not_called();runtime.run_epoch3.assert_not_called()

    def test_zero_query_epoch_still_requires_an_exact_second_seal_and_retirement(self):
        with tempfile.TemporaryDirectory() as temporary:
            zero,sealed,retired,science,bounds,reports,*_=journey_fixture(Path(temporary))
            def verify(a=sealed,b=retired):check.epoch2_recovery(zero,a,b,science,bounds,reports)
            verify()
            self.assertEqual(source.only(sealed['acquisition_continuations'],producer_epoch=2)['last_quantum_number'],1)
            for table,index,key,value in (('acquisition_continuations',0,'xmin','999'),('acquisition_continuations',1,'previous_continuation',None),
                    ('acquisition_continuations',1,'last_quantum_number',0),('acquisition_continuations',1,'checkpoint_revision',0),
                    ('jobs',0,'job_revision',4),('producer_requests',5,'xmin','999'),('source_streams',1,'last_complete_sequence',0)):
                changed=copy.deepcopy(sealed);changed[table][index][key]=value
                with self.subTest(table=table,key=key),self.assertRaises((AssertionError,ValueError)):verify(a=changed)
            for table,index,key,value in (('producer_epochs',1,'state','open'),('owner_epochs',4,'state','active'),
                    ('allocation_domains',9,'closure_digest','\\x'+'ff'*32),('acquisition_continuations',1,'sealed_at_us',bounds[1][0])):
                changed=copy.deepcopy(retired);changed[table][index][key]=value
                with self.subTest(retirement=table,key=key),self.assertRaises((AssertionError,ValueError)):verify(b=changed)

    def test_epoch3_requires_real_lineage_atomic_revision4_and_independently_encoded_science(self):
        with tempfile.TemporaryDirectory() as temporary:
            _,_,retired,attribution,_,_,after,started,wires,events,first,dump,inputs,lower,upper=journey_fixture(Path(temporary))
            def verify(state=after,native=dump,originals=inputs):
                return check.final_rows(retired,state,first,attribution,started,wires,events,native,originals,lower,upper,preparation_ttl_us=5_000_000)
            verify();self.assertEqual(dump['sequence'],167)
            for table,index,key,value in (('jobs',0,'job_revision',3),('jobs',0,'xmin','999'),
                    ('acquisition_continuations',0,'sealed_at_us',lower),('acquisition_continuations',1,'adoption_transaction','999'),
                    ('acquisition_continuations',1,'previous_continuation',None),('ingest_successions',0,'terminal_kind',2),
                    ('ingest_successions',0,'parent_handoff_sha256','\\x'+'ff'*32),('ingest_activations',0,'target_epoch',2),
                    ('ingest_successions',0,'lease_expires_at_us',int(started['prepared_at_us'])+60_000_000),
                    ('producer_requests',5,'request_digest','\\x'+'ff'*32),('source_streams',1,'xmin','999'),
                    ('producer_epochs',1,'reserved_through_request',0),('acquisition_quanta',1,'producer_epoch',2),
                    ('owner_epochs',8,'lease_expires_at_us',upper)):
                changed=copy.deepcopy(after);changed[table][index][key]=value
                with self.subTest(table=table,key=key),self.assertRaises((AssertionError,ValueError)):verify(state=changed)
            changed=copy.deepcopy(after);changed['acquisition_quanta'].append(copy.deepcopy(changed['acquisition_quanta'][-1]))
            with self.assertRaises((AssertionError,ValueError)):verify(state=changed)
            with self.assertRaises(AssertionError):verify(native=dict(dump,sequence=166))
            with self.assertRaises(AssertionError):verify(originals=inputs[:-1])

    def test_zero_query_cut_authenticates_one_real_attribution_and_all_native_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,state,first,started,wires,events,dump,inputs,lower,upper,fault=zero_fixture(Path(temporary))
            def verify(rows=state,native=dump,originals=inputs,selected=fault):
                return check.zero_query_cut(before,rows,first,started,wires,events,native,originals,lower,upper,selected)
            science=verify();self.assertEqual(dump['sequence'],113);self.assertEqual(len(science['inputs']),1)
            self.assertEqual(len(quantum.receipt_refs(science['receipts'][0])),5)
            mutations=[('jobs',0,'job_revision',2),('jobs',0,'xmin','999'),('acquisition_continuations',0,'adoption_transaction','999'),
                ('producer_requests',5,'acquisition_job_id',state['jobs'][0]['job_id']),('producer_requests',5,'input_profile',2),
                ('producer_outcomes',5,'outcome_kind',2),('producer_checkpoints',1,'checkpoint_revision',0),
                ('producer_epochs',1,'reserved_through_request',0),('owner_epochs',4,'lease_expires_at_us',upper)]
            for table,index,key,value in mutations:
                changed=copy.deepcopy(state);changed[table][index][key]=value
                with self.subTest(table=table,key=key),self.assertRaises((AssertionError,ValueError)):verify(rows=changed)
            for table in ('acquisition_quanta','producer_requests','producer_checkpoint_dependencies'):
                changed=copy.deepcopy(state);changed[table].append(copy.deepcopy(changed[table][-1]))
                with self.subTest(extra=table),self.assertRaises((AssertionError,ValueError)):verify(rows=changed)
            for changed in (dict(dump,sequence=91),dict(dump,rows=dump['rows'][:-1])):
                with self.assertRaises((AssertionError,ValueError,StopIteration)):verify(native=changed)
            with self.assertRaises(AssertionError):verify(originals=[])
            with self.assertRaises(AssertionError):verify(selected=dict(fault,backend_pid=1))
            closed=check.closed_attribution(science,'\\x'+started['producer_slot_id'])
            self.assertEqual(closed['sequence'],114)
            self.assertEqual(sum(a!=b for a,b in zip(dump['rows'],closed['rows'])),1)
            with self.assertRaises(AssertionError):check.closed_attribution(science,'\\x'+started['producer_slot_id'],dump)

    def test_two_stability_intervals_must_fit_both_original_job_and_unchanged_client_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);before,*_=zero_fixture(root)
            for kind in ('config','policy'):
                before[kind+'_revisions'][0]['document']['settings']['runtime.pressure_stable_duration']=30_000_000
            before['policy_revisions'][0]['config_revision']=1
            runtime=object.__new__(case.ZeroQueryHandoffRuntime);runtime.adoption_retired=before
            runtime.clock=Mock(return_value=ACCEPTED+5_000_000)
            self.assertEqual(runtime.remaining_budget(root,2),50)
            value=case.load(root/'observation-budget.json');self.assertEqual(value['needed_us'],80_000_000)
            self.assertEqual(value['unchanged_client_deadline_at_us'],ACCEPTED+110_000_000)
            runtime.clock.return_value=ACCEPTED+35_000_000
            with self.assertRaisesRegex(ValueError,'cannot fit'):runtime.remaining_budget(root,2)
            self.assertFalse(case.load(root/'observation-budget.json')['fits'])
            runtime.clock.return_value=ACCEPTED+5_000_000
            before['policy_revisions'][0]['document']['settings']['runtime.pressure_stable_duration']=1
            with self.assertRaises(AssertionError):runtime.remaining_budget(root,2)

    def test_selector_excludes_competing_profiles_before_creating_evidence(self):
        spec=importlib.util.spec_from_file_location('handoff_runner_control',ROOT/'tests/acceptance/run.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        common=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption','--linear-quantum-fault','plan-before']
        with tempfile.TemporaryDirectory() as temporary:
            tail=['--linear-quantum-handoff','zero-query','--source-root',str(ROOT),'--evidence',str(Path(temporary)/'accepted')]
            with patch.object(sys,'argv',common+tail),patch.object(sys,'stdout'),patch('cases.first_launch.first_launch') as launched:
                self.assertEqual(runner.main(),2)
            self.assertEqual(launched.call_args.args[0].linear_quantum_handoff,'zero-query')
        for prefix in (['run.py','p03-linear-quantum'],common[:-1]+['plan-after'],common+['--linear-quantum-seal-fault','before-forward'],
                common+['--linear-quantum-seal-retry','exact'],common+['--linear-quantum-adoption-fault','after-complete'],
                common+['--linear-quantum-authority-refusal','actor-grant'],common+['--linear-quantum-integrity-refusal','workflow-effect'],
                common+['--linear-quantum-trace-successor-sql']):
            with tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'refused'
                with patch.object(sys,'argv',prefix+['--linear-quantum-handoff','zero-query','--source-root',str(ROOT),'--evidence',str(evidence)]),\
                        patch.object(sys,'stderr'),self.assertRaises(SystemExit):runner.main()
                self.assertFalse(evidence.exists())

    def test_ordinal_one_proxy_is_distinct_and_untriggered_cleanup_never_claims_the_fault(self):
        from installed import set_value
        with tempfile.TemporaryDirectory() as temporary:
            runtime=object.__new__(case.ZeroQueryHandoffRuntime);runtime.root=Path(temporary);runtime.source=ROOT
            runtime.config=runtime.root/'instance.toml';runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n')
            runtime.set_value=set_value;runtime.resources=dict(quantum_proxy=dict(status='joined',pid=42));runtime.inputs={};runtime.record_resources=Mock()
            runtime.route=Mock();old=runtime.route;route=Mock();route.process.pid=43;runtime.proxy_process=Mock(return_value=route)
            runtime.proxy_event=Mock(side_effect=lambda *args:dict(runtime.handoff_proxy_binding,event='ready',port=12345))
            runtime.start_commit_proxy('handoff','before-forward','control-quantum-plan')
            self.assertIs(runtime.route,old);self.assertIs(runtime.handoff_route,route);self.assertEqual(runtime.resources['quantum_proxy']['pid'],42)
            value=dict(runtime.handoff_proxy_binding,event='terminal',unexpected=0,frames=10,connections=1,bytes=100,
                data_queries=1,data_commits=1,matched_commits=1,skipped_commits=0,selected_commits=1,selected_server_complete=0,
                selected_client_complete=0,server_complete=0,client_complete=0,faults=1)
            runtime.validate_handoff_fault_terminal(value)
            for key,item in (('commit_ordinal',2),('selected_server_complete',1),('faults',0),('matched_commits',2),('data_commits',0)):
                with self.subTest(key=key),self.assertRaises(ValueError):runtime.validate_handoff_fault_terminal(dict(value,**{key:item}))
            untriggered=dict(value,frames=0,bytes=0,connections=0,**{k:0 for k in ('faults','data_queries','data_commits','matched_commits','selected_commits')})
            runtime.validate_handoff_proxy(untriggered,'terminal')
            with self.assertRaises(ValueError):runtime.validate_handoff_fault_terminal(untriggered)


if __name__=='__main__':unittest.main()
