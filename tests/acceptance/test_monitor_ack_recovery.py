"""Literal fault/recovery boundaries; no product or PostgreSQL engine execution."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import Mock,patch

from cases.first_launch import Runtime
from cases import first_launch_index_oracle as inventory
from cases import first_launch_monitor_ack_recovery as case
from cases import first_launch_monitor_ack_recovery_oracle as oracle
from cases import first_launch_passive_oracle as passive
from test_passive_index import native_fixture
from test_passive_monitor import encoder,decode,retained_fixture
from installed import set_value


def fixture():
    dump,old,initialized,receipts=native_fixture()
    state={name:old.get(name,[]) for name in inventory.CONTROL_TABLES}
    initialized.update(dataset_id='01'*16,owner_resource_id='dd'*16)
    control=dict(role='control',resource_id='\\x'+'51'*16,epoch=1,process_incarnation='\\x'+'52'*16,
        dataset_id='\\x'+'01'*16,role_instance_id='\\x'+'53'*16,state='active',acquired_at_us=1,
        lease_expires_at_us=1000,released_at_us=None,exclusion_method=None,exclusion_digest=None,
        control_fence_version=1,control_backend_pid=7331,control_backend_start='2026-11-02T00:00:00+00:00',xmin='1')
    ingest=dict(role='ingest',resource_id='\\x'+'dd'*16,epoch=1,role_instance_id='\\x'+'e1'*16,
        dataset_id='\\x'+'01'*16,state='active',xmin='2')
    state['owner_epochs']=[control,ingest]
    state['role_instances']=[dict(role=r['role'],role_instance_id=r['role_instance_id'],dataset_id=r['dataset_id'],
        retired_at_us=None,created_at_us=1,xmin=r['xmin']) for r in state['owner_epochs']]
    state['producer_epochs']=[dict(producer_slot_id='\\x'+'44'*16,producer_epoch=1,
        dataset_id='\\x'+'01'*16,state='open',reserved_through_request=4,xmin='400')]
    manifest=state['producer_requests'][0]['manifest_sha256']
    state['producer_checkpoints']=[dict(dataset_id='\\x'+'01'*16,producer_slot_id='\\x'+'44'*16,producer_epoch=1,
        checkpoint_revision=4,complete_through_request=4,progress_format=1,progress_jobs=0,dependency_count=10,
        manifest_sha256=manifest,checkpoint_digest='\\x'+'ac79a894d24aaccec13adf1dbc5f317557a35f6ee2b746d200ffb37fe753fbe9',
        issuer_resource=control['resource_id'],issuer_epoch=1,issuer_incarnation=control['process_incarnation'],xmin='404')]
    # Frozen progress digest independently checked with Node's Buffer/crypto;
    # it does not call the checkpoint oracle to choose an expected digest.
    state['producer_checkpoint_dependencies']=[dict(dataset_id='\\x'+source[0].hex(),producer_slot_id='\\x'+'44'*16,
        producer_epoch=1,source_shard=source[1],source_epoch=source[2],source_sequence=source[3],
        payload_digest='\\x'+digest.hex(),xmin='404' if source[3]==10 else '403')
        for receipt in receipts for source,digest in receipt[4]]
    for row in state['producer_outcomes']:row['xmin']=str(400+row['request_sequence'])
    head=next(row for row in dump['rows'] if row['cf']=='receipts' and row['key'].startswith('0131'))
    head['value']='0131'+''.join(n.to_bytes(8,'big').hex() for n in (4,3,0))
    originals=retained_fixture()[6]
    selection=retained_fixture()[4]
    return state,dump,initialized,receipts,originals,selection


def recovered_fixture(before, *, retry=False):
    after=copy.deepcopy(before)
    if not retry:after['producer_epochs'][0].update(state='closing',xmin='405')
    previous=max((r for r in after['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    if previous['state']!='released':
        previous.update(state='released',released_at_us=499,exclusion_method='storage_fenced',exclusion_digest='\\x'+'ab'*32,xmin='499')
        role=passive.only(after['role_instances'],role_instance_id=previous['role_instance_id'])
        role.update(retired_at_us=499,xmin='499')
    epoch=previous['epoch']+1
    current=dict(previous,epoch=epoch,process_incarnation='\\x'+f'{epoch+160:02x}'*16,
        role_instance_id='\\x'+f'{epoch+176:02x}'*16,acquired_at_us=500+epoch,
        lease_expires_at_us=1000+epoch,released_at_us=550+epoch,control_backend_pid=7331+epoch,xmin=str(550+epoch))
    after['owner_epochs'].append(current)
    after['role_instances'].append(dict(role='control',role_instance_id=current['role_instance_id'],dataset_id=current['dataset_id'],
        retired_at_us=550+epoch,created_at_us=500+epoch,xmin=str(550+epoch)))
    return after


class MonitorAckRecoveryControls(unittest.TestCase):
    def test_retained_measurement_decoder_preserves_boolean_and_signed_canonical_types(self):
        value=[False,True,None,-1,-(2**63),0,1,2**64-1]
        self.assertEqual(decode(encoder.cbor(value)),value)
        self.assertIs(type(decode(b'\xf4')),bool)
        self.assertIs(type(decode(b'\x00')),int)
        for raw,expected in ((b'\xf4',False),(b'\xf5',True),(b'\x00',0),(b'\x01',1),(b'\x37',-24),(b'\x38\x18',-25)):
            actual=decode(raw);self.assertEqual(actual,expected);self.assertIs(type(actual),type(expected))
        for raw in (b'\x38\x00',b'\xf8\x14',b'\x3b'+(2**63).to_bytes(8,'big'),b'\xff',b'\xf9\x00\x00',b'\xc0\x00',b'\x00\x00'):
            with self.subTest(raw=raw.hex()),self.assertRaises(AssertionError):decode(raw)

    def test_actual_checkpoint_and_known_terminal_history_bind_the_original_attempt(self):
        state,dump,initialized,receipts,originals,selection=fixture()
        oracle.eligible(state)
        self.assertEqual(oracle.terminal(state,initialized,selection,originals,encoder.cbor,decode),receipts)
        passive.native_ack(dump,state,initialized,receipts,encoder.cbor,phase='reply-lost')
        for change in ('attempt','context','request-binding','resolution','head3','dependency','outcome-xmin','unknown'):
            changed=copy.deepcopy(state);inputs=list(originals)
            if change=='attempt':changed['measurement_dispatches'][0]['attempt_id']='\\x'+'ab'*16
            elif change=='context':changed['measurement_dispatches'][0]['context_bytes']='\\x'+'ab'*294
            elif change=='request-binding':changed['producer_requests'][3]['monitor_attempt_id']='\\x'+'ab'*16
            elif change=='resolution':changed['producer_requests'][3]['monitor_resolution']=True
            elif change=='head3':changed['producer_checkpoints'][0]['complete_through_request']=3
            elif change=='dependency':changed['producer_checkpoint_dependencies'].pop()
            elif change=='outcome-xmin':changed['producer_outcomes'][3]['xmin']='999'
            else:
                decoded=decode(inputs[3].split(b'\0',1)[1]);decoded[3][3][4]=[0]
                inputs[3]=b'bigbrotr/producer-request/1\0'+encoder.cbor(decoded)
            with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                oracle.terminal(changed,initialized,selection,inputs,encoder.cbor,decode)

    def test_recovery_preserves_all_history_and_only_closes_the_original_producer(self):
        before,*_=fixture();after=recovered_fixture(before)
        oracle.recovered(before,after)
        oracle.recovered(after,recovered_fixture(after,retry=True),retry=True)
        for table in ('measurement_dispatches','producer_requests','producer_outcomes','producer_checkpoints',
                'producer_checkpoint_dependencies','allocation_domains','source_streams','browser_sessions'):
            changed=copy.deepcopy(after)
            if changed[table]:changed[table][0]['xmin']='corrupt'
            else:changed[table].append(dict(invented=True))
            with self.subTest(table=table),self.assertRaises(AssertionError):oracle.recovered(before,changed)
        for change in ('ingest-released','producer-retired','new-control-active','rewrite-original-control'):
            changed=copy.deepcopy(after)
            if change=='ingest-released':passive.only(changed['owner_epochs'],role='ingest')['state']='released'
            elif change=='producer-retired':changed['producer_epochs'][0]['state']='retired'
            elif change=='new-control-active':changed['owner_epochs'][-1]['state']='active'
            else:changed['owner_epochs'][0]['process_incarnation']='\\x'+'ab'*16
            with self.subTest(change=change),self.assertRaises(AssertionError):oracle.recovered(before,changed)
        for change in ('released','retired-role','closing-producer'):
            altered=copy.deepcopy(before)
            if change=='released':passive.only(altered['owner_epochs'],role='ingest')['state']='released'
            elif change=='retired-role':passive.only(altered['role_instances'],role='ingest')['retired_at_us']=99
            else:altered['producer_epochs'][0]['state']='closing'
            with self.subTest(change=change),self.assertRaises(AssertionError):oracle.eligible(altered)

    def test_physical_ACK_phase_and_exact_retry_cannot_launder_native_changes(self):
        state,before,initialized,receipts,*_=fixture();after=copy.deepcopy(before)
        head=next(r for r in after['rows'] if r['cf']=='receipts' and r['key'].startswith('0131'))
        head['value']='0231'+''.join(n.to_bytes(8,'big').hex() for n in (4,4,0))+'01';after['sequence']+=2
        passive.native_ack(after,state,initialized,receipts,encoder.cbor,phase='recovered')
        oracle.native_transition(before,after,initialized)
        oracle.native_transition(after,copy.deepcopy(after),initialized,retry=True)
        for phase in ('admitted','reply-lost'):
            with self.assertRaises(AssertionError):passive.native_ack(after,state,initialized,receipts,encoder.cbor,phase=phase)
        for change in ('science','input-receipt','pending','sequence','head'):
            altered=copy.deepcopy(after)
            if change=='sequence':altered['sequence']+=1
            else:
                prefix={'science':'0120','input-receipt':'0130','pending':'0121','head':'0131'}[change]
                next(r for r in altered['rows'] if r['key'].startswith(prefix))['value']+='00'
            with self.subTest(change=change),self.assertRaises(AssertionError):
                passive.native_ack(altered,state,initialized,receipts,encoder.cbor,phase='recovered')
                oracle.native_transition(before,altered,initialized)
        with self.assertRaises(AssertionError):oracle.native_transition(before,after,initialized,retry=True)

    def test_only_a_matching_server_complete_loss_and_exact_installed_report_pass(self):
        state,_,initialized,*_=fixture()
        wire=dict(event='fault',target='control-measurement-checkpoint',mode='after-complete',armed=True,
            data_queries=1,data_commits=1,server_complete=1,client_complete=0,faults=1,measurement_updates_complete=1,
            measurement_identity_sha256=hashlib.sha256(bytes.fromhex('01'*16+'44'*16)).hexdigest(),backend_pid=7331)
        oracle.fault(wire,initialized['dataset_id'],initialized['producer_slot_id'])
        for field,value in (('mode','before-forward'),('server_complete',0),('client_complete',1),('data_queries',2),
                ('measurement_updates_complete',0),('backend_pid',0),('measurement_identity_sha256','ab'*32)):
            with self.subTest(field=field),self.assertRaises(AssertionError):oracle.fault(dict(wire,**{field:value}),initialized['dataset_id'],initialized['producer_slot_id'])
        report=dict(format='bigbrotr-ingest-recovery/1',state='positive-outcomes-resolved',dataset_id=initialized['dataset_id'],
            owner_resource_id=initialized['owner_resource_id'],producer_slot_id=initialized['producer_slot_id'],producer_epoch='1',
            issued_through='4',reserved_through='4',resolved_requests='4',native='closed',owner_release='committed',ready=False)
        oracle.completion([report],initialized)
        for records in ([],[report,report],[dict(report,owner_release='unknown')],[dict(report,resolved_requests='3')]):
            with self.assertRaises(AssertionError):oracle.completion(records,initialized)

    def test_new_journey_launches_only_initial_monitor_then_plain_installed_recovery(self):
        runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.launch=Mock(return_value='owner')
        with tempfile.TemporaryDirectory() as temporary:
            runtime.root=Path(temporary);runtime.state=runtime.root/'instance';runtime.state.mkdir()
            runtime.home=runtime.root/'archive-export';runtime.home.mkdir()
            runtime.config=runtime.state/'instance.toml'
            runtime.config.write_text((Path(__file__).resolve().parents[2]/'config/recipes/acceptance.toml').read_text())
            runtime.inputs={};runtime.set_value=set_value
            with patch.object(Runtime,'prepare_state'):
                runtime.prepare_state()
            declared=tomllib.loads(runtime.config.read_text())
            self.assertEqual(declared['topology']['local_roles'],['control','ingest','monitor','archive'])
            self.assertFalse(declared['archive']['reader']['enabled'])
            self.assertEqual(runtime.inputs['initial_recipe_sha256'],case.digest(runtime.config))
        self.assertEqual(runtime.passive_launch(),'owner')
        runtime.launch.assert_called_once_with('monitor-checkpoint-reply-loss','app','serve','--file',
            '/etc/bigbrotr/instance.toml','--roles','control,ingest,monitor','--initialize-ingest',
            '--monitor-once','/var/lib/bigbrotr/monitor-once.json')
