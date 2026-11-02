"""Literal fault/recovery boundaries; no product or PostgreSQL engine execution."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from cases.first_launch import Runtime,first_launch
from cases import first_launch_index_oracle as inventory
from cases import first_launch_monitor_ack_recovery as case
from cases import first_launch_monitor_ack_recovery_oracle as oracle
from cases import first_launch_passive_oracle as passive
from test_passive_index import native_fixture
from test_passive_monitor import encoder,decode,retained_fixture,sample
from bootstrap_oracle import cbor as wire_cbor
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
    # Independent synthetic clock schedule, including prior faulted Controls.
    # A repeated fault must not put exclusion before that owner's acquisition
    # or before its already-committed repair timestamps.
    observed=[499]
    for table,fields in (('owner_epochs',('acquired_at_us','released_at_us')),
            ('role_instances',('created_at_us','retired_at_us')),
            ('producer_outcomes',('recorded_at_us',)),('producer_checkpoints',('committed_at_us',))):
        observed.extend(row[field] for row in after[table] for field in fields if type(row.get(field)) is int)
    now=max(observed)+1
    if previous['state']!='released':
        previous.update(state='released',released_at_us=now,exclusion_method='storage_fenced',exclusion_digest='\\x'+'ab'*32,xmin='499')
        role=passive.only(after['role_instances'],role_instance_id=previous['role_instance_id'])
        role.update(retired_at_us=now,xmin='499')
    epoch=previous['epoch']+1
    acquired=max(500+epoch,now+1);released=acquired+50
    current=dict(previous,epoch=epoch,process_incarnation='\\x'+f'{epoch+160:02x}'*16,
        role_instance_id='\\x'+f'{epoch+176:02x}'*16,acquired_at_us=acquired,
        lease_expires_at_us=acquired+500,released_at_us=released,control_backend_pid=7331+epoch,xmin=str(550+epoch))
    after['owner_epochs'].append(current)
    after['role_instances'].append(dict(role='control',role_instance_id=current['role_instance_id'],dataset_id=current['dataset_id'],
        retired_at_us=released,created_at_us=acquired,xmin=str(550+epoch)))
    return after


def native_outcome_fixture():
    state,dump,initialized,receipts,originals,selection=fixture()
    frozen=json.loads((Path(__file__).resolve().parents[2]/'tests/fixtures/model/passive-monitor-recovery.json').read_text())
    for outcome in state['producer_outcomes']:outcome['recorded_at_us']=70+outcome['request_sequence']
    state['producer_outcomes'].pop()
    state['producer_checkpoint_dependencies'].pop()
    state['producer_checkpoints'][0].update(checkpoint_revision=3,complete_through_request=3,
        dependency_count=9,checkpoint_digest='\\x'+frozen['expected'][1]['sha256'],committed_at_us=80,xmin='403',
        prefix_profile=True,prefix_prepared_bytes=None,prefix_active_generation=None,prefix_active_sha256=None)
    return state,dump,initialized,receipts,originals,selection


def repaired_fixture(before):
    after=recovered_fixture(before)
    control=after['owner_epochs'][-1]
    original=fixture()[0]['producer_outcomes'][-1]
    outcome=dict(original,recorder_resource=control['resource_id'],recorder_epoch=control['epoch'],
        recorder_incarnation=control['process_incarnation'],recorded_at_us=control['acquired_at_us']+28,xmin='610')
    after['producer_outcomes'].append(outcome)
    after['producer_checkpoints'][0].update(checkpoint_revision=4,complete_through_request=4,
        dependency_count=10,checkpoint_digest='\\xac79a894d24aaccec13adf1dbc5f317557a35f6ee2b746d200ffb37fe753fbe9',
        committed_at_us=control['acquired_at_us']+29,issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],
        issuer_incarnation=control['process_incarnation'],xmin='610')
    receipt=decode(passive.raw(outcome['outcome_bytes']).split(b'\0',1)[1])
    after['producer_checkpoint_dependencies'].append(dict(after['producer_checkpoint_dependencies'][-1],
        source_sequence=10,payload_digest='\\x'+receipt[4][0][1].hex(),xmin='610'))
    return after


def rewrite_native_fact(dump, receipts, change):
    """Self-consistent actual bytes use the separate existing wire encoder."""
    row=next(r for r in dump['rows'] if r['cf']=='facts' and r['key'].startswith('0120') and r['key'].endswith('000000000000000a'))
    fields=decode(bytes.fromhex(row['value'])[34:].split(b'\0',1)[1]);change(fields)
    encoded=b'bigbrotr/fact/1\0'+wire_cbor(fields);digest=hashlib.sha256(encoded).digest()
    row['value']=(b'\x01\x20'+digest+encoded).hex()
    receipt=copy.deepcopy(receipts[3]);receipt[4][0][1]=digest;receipt[5][-1][1]=digest
    native=next(r for r in dump['rows'] if r['cf']=='receipts' and r['key'].startswith('0130') and r['key'].endswith('0000000000000004'))
    native['value']=(b'bigbrotr/receipt/1\0'+wire_cbor(receipt)).hex()
    pending=next(r for r in dump['rows'] if r['cf']=='facts' and r['key'].startswith('0121') and r['key'].endswith('000000000000000a'))
    batch=hashlib.sha256(b'bigbrotr/admission-batch/1\0'+wire_cbor([bytes.fromhex('01'*16),0,1,10,10,receipt[4]])).digest()
    pending['value']=(b'\x01\x21'+(10).to_bytes(8,'big')+batch).hex()
    return receipt


def uncertain_repair_fixture(before, *, committed):
    after=repaired_fixture(before) if committed else recovered_fixture(before)
    control=after['owner_epochs'][-1]
    control.update(state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None)
    passive.only(after['role_instances'],role_instance_id=control['role_instance_id'])['retired_at_us']=None
    return after


def closing_native_fixture(dump, *, acknowledged=3, writes=1):
    after=copy.deepcopy(dump)
    head=next(row for row in after['rows'] if row['cf']=='receipts' and row['key'].startswith('0131'))
    head['value']='0231'+''.join(n.to_bytes(8,'big').hex() for n in (4,acknowledged,0))+'01'
    after['sequence']+=writes
    return after


class RepairFaultControls(unittest.TestCase):
    def test_two_uncertain_repairs_then_direct_ack_and_retry_preserve_each_literal_boundary(self):
        initial,dump,initialized,receipts,originals,selection=native_outcome_fixture()
        rollback=uncertain_repair_fixture(initial,committed=False)
        committed=uncertain_repair_fixture(rollback,committed=True)
        direct=recovered_fixture(committed,retry=True);retry=recovered_fixture(direct,retry=True)
        oracle.repair_rollback(initial,rollback,initialized,receipts)
        oracle.before_terminal(rollback,initialized,selection,originals,receipts,encoder.cbor,decode)
        oracle.repair_commit_loss(rollback,committed,initialized,receipts,encoder.cbor)
        self.assertEqual(oracle.repaired_terminal(committed,initialized,selection,originals,receipts,encoder.cbor,decode),receipts)
        oracle.repair_ack(committed,direct,initialized,receipts,encoder.cbor)
        oracle.repair_ack(direct,retry,initialized,receipts,encoder.cbor)
        closed=closing_native_fixture(dump);confirmed=closing_native_fixture(closed,acknowledged=4)
        manifest=passive.raw(initial['producer_requests'][0]['manifest_sha256'],32)
        passive.native_physical(closed,initialized,receipts,encoder.cbor,manifest,phase='closing-unacknowledged')
        passive.native_ack(closed,committed,initialized,receipts,encoder.cbor,phase='closing-unacknowledged')
        oracle.native_repair_fault(dump,closed,initialized,committed=False)
        oracle.native_repair_fault(closed,copy.deepcopy(closed),initialized,committed=True)
        passive.native_ack(confirmed,direct,initialized,receipts,encoder.cbor,phase='recovered')
        oracle.native_repair_ack(closed,confirmed,initialized)
        oracle.native_transition(confirmed,copy.deepcopy(confirmed),initialized,retry=True)
        self.assertEqual([len(s['owner_epochs']) for s in (initial,rollback,committed,direct,retry)],[2,3,4,5,6])
        for table in ('producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies'):
            self.assertEqual(committed[table],direct[table]);self.assertEqual(direct[table],retry[table])

    def test_rollback_refuses_partial_PG_effects_and_invented_release_or_replaced_custody(self):
        initial,_,initialized,receipts,*_=native_outcome_fixture();rollback=uncertain_repair_fixture(initial,committed=False)
        for change in ('head','dependency','outcome','producer-reopen','ingest','released-control','retired-role'):
            altered=copy.deepcopy(rollback)
            if change=='head':altered['producer_checkpoints'][0]['xmin']='999'
            elif change=='dependency':altered['producer_checkpoint_dependencies'][0]['xmin']='999'
            elif change=='outcome':altered['producer_outcomes'].append(fixture()[0]['producer_outcomes'][-1])
            elif change=='producer-reopen':altered['producer_epochs'][0]['state']='open'
            elif change=='ingest':passive.only(altered['owner_epochs'],role='ingest')['xmin']='999'
            elif change=='released-control':altered['owner_epochs'][-1]['state']='released'
            else:altered['role_instances'][-1]['retired_at_us']=550
            with self.subTest(change=change),self.assertRaises(AssertionError):oracle.repair_rollback(initial,altered,initialized,receipts)
        # The approved successful plain repair keeps its stricter released-owner contract.
        with self.assertRaises(AssertionError):oracle.repaired(initial,repaired_fixture(initial)|{'owner_epochs':rollback['owner_epochs']},initialized,receipts,encoder.cbor)

    def test_lost_committed_reply_requires_atomic_fresh_repair_without_laundering_previous_outcomes(self):
        initial,_,initialized,receipts,*_=native_outcome_fixture();rollback=uncertain_repair_fixture(initial,committed=False)
        committed=uncertain_repair_fixture(rollback,committed=True)
        for change in ('head-xmin','outcome4-xmin','dependency10-xmin','prior-outcome','prior-dependency',
                'old-recorder','released-control','producer-xmin'):
            altered=copy.deepcopy(committed)
            if change=='head-xmin':altered['producer_checkpoints'][0]['xmin']='999'
            elif change=='outcome4-xmin':altered['producer_outcomes'][3]['xmin']='999'
            elif change=='dependency10-xmin':altered['producer_checkpoint_dependencies'][9]['xmin']='999'
            elif change=='prior-outcome':altered['producer_outcomes'][0]['xmin']='999'
            elif change=='prior-dependency':altered['producer_checkpoint_dependencies'][0]['xmin']='999'
            elif change=='old-recorder':altered['producer_outcomes'][3]['recorder_epoch']=2
            elif change=='released-control':altered['owner_epochs'][-1]['state']='released'
            else:altered['producer_epochs'][0]['xmin']='999'
            with self.subTest(change=change),self.assertRaises(AssertionError):oracle.repair_commit_loss(rollback,altered,initialized,receipts,encoder.cbor)
        direct=recovered_fixture(committed,retry=True)
        for table in ('producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies'):
            altered=copy.deepcopy(direct);altered[table][-1]['xmin']='999'
            with self.subTest(table=table),self.assertRaises(AssertionError):oracle.repair_ack(committed,altered,initialized,receipts,encoder.cbor)

    def test_unknown_repair_never_grants_ACK_and_fixed_native_write_deltas_are_discriminating(self):
        initial,dump,initialized,receipts,*_=native_outcome_fixture();closed=closing_native_fixture(dump)
        manifest=passive.raw(initial['producer_requests'][0]['manifest_sha256'],32)
        for altered in (dump,closing_native_fixture(dump,acknowledged=4)):
            with self.assertRaises(AssertionError):passive.native_physical(altered,initialized,receipts,encoder.cbor,manifest,phase='closing-unacknowledged')
        for extra in (-1,1):
            wrong=copy.deepcopy(closed);wrong['sequence']+=extra
            with self.assertRaises(AssertionError):oracle.native_repair_fault(dump,wrong,initialized,committed=False)
        for change in ('write','head','receipt','fact'):
            wrong=copy.deepcopy(closed)
            if change=='write':wrong['sequence']+=1
            else:
                prefix={'head':'0131','receipt':'0130','fact':'0120'}[change]
                next(row for row in wrong['rows'] if row['key'].startswith(prefix))['value']+='00'
            with self.subTest(change=change),self.assertRaises(AssertionError):oracle.native_repair_fault(closed,wrong,initialized,committed=True)
        acknowledged=closing_native_fixture(closed,acknowledged=4)
        with self.assertRaises(AssertionError):oracle.native_transition(closed,acknowledged,initialized)  # Plain case requires close+ACK, two writes.
        with self.assertRaises(AssertionError):oracle.native_repair_ack(closed,closed,initialized)

    def test_fault_variant_keeps_closed_stage_order_and_failure_stops_later_recoveries(self):
        root=Path(__file__).resolve().parents[2]
        expected=['first_launch_'+name for name in case.REPAIR_FAULT_STAGES]
        with tempfile.TemporaryDirectory() as directory:
            import subprocess,sys
            directory=Path(directory)
            result=subprocess.run([sys.executable,'-B',str(root/'tests/acceptance/run.py'),'p04-monitor-native-outcome-faults',
                '--source-root',str(root),'--candidate-manifest',str(directory/'absent'),'--evidence',str(directory/'evidence')],capture_output=True,timeout=5)
            report=json.loads((directory/'evidence/result.json').read_text())
            self.assertEqual((result.returncode,report['result']),(2,'NOT_RUN'))
            self.assertEqual([r['name'] for r in report['checks']],expected)
            self.assertFalse(list((directory/'evidence/commands').iterdir()))
        for failure in (None,'rollback_repair','lose_repair_commit'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as directory:
                args=SimpleNamespace(case='p04-monitor-native-outcome-faults',evidence=Path(directory),source_root=root);checks=[]
                with patch.object(case,'MonitorAckRecoveryRuntime') as runtime:
                    if failure:getattr(runtime.return_value,failure).side_effect=ValueError('unproven repair boundary')
                    first_launch(args,checks,{})
                    called=[call[0] for call in runtime.return_value.method_calls]
                    if failure:
                        runtime.return_value.recover_ack.assert_not_called();runtime.return_value.retry_ack.assert_not_called()
                        failed=5 if failure=='rollback_repair' else 6
                        self.assertEqual([r['result'] for r in checks],['PASS']*failed+['FAIL']+['NOT_RUN']*(8-failed)+['PASS'])
                    else:
                        self.assertEqual([r['result'] for r in checks],['PASS']*10)
                        self.assertEqual([name for name in called if name in ('checkpoint_reply_loss','rollback_repair','lose_repair_commit','recover_ack','retry_ack')],
                            ['checkpoint_reply_loss','rollback_repair','lose_repair_commit','recover_ack','retry_ack'])

    def test_faulted_installed_recovery_uses_same_selector_and_positive_custody_before_census(self):
        for committed in (False,True):
            initial,dump,initialized,receipts,originals,selection=native_outcome_fixture()
            rollback=uncertain_repair_fixture(initial,committed=False)
            before=rollback if committed else initial
            after=uncertain_repair_fixture(before,committed=committed)
            closed=closing_native_fixture(dump)
            old_dump=closed if committed else dump
            with self.subTest(committed=committed),tempfile.TemporaryDirectory() as directory:
                runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.repair_faults=runtime.native_outcome=True
                runtime.root=Path(directory);runtime.home=runtime.root/'peer';runtime.home.mkdir()
                runtime.state=runtime.root/'instance';runtime.config=runtime.root/'instance.toml';runtime.config.write_text('original')
                runtime.direct_recipe='original';runtime.source_unchanged=Mock();runtime.backend_absence=Mock()
                runtime.client=Mock();runtime.client.poll.return_value=None
                runtime.before_recovery=initial;runtime.before_dump=dump;runtime.after_recovery=before;runtime.after_dump=old_dump
                runtime.initialized=initialized;runtime.receipts=receipts;runtime.original_inputs=originals;runtime.selection=selection
                runtime.original_files={};runtime.originals=Mock(return_value=originals);runtime.files=Mock(return_value={})
                runtime.catalog=[];runtime.capture=Mock(return_value=(after,[]));runtime.cbor=encoder.cbor;runtime.decode=decode
                runtime.start_checkpoint_proxy=Mock();runtime.select_checkpoint=Mock(return_value='selected')
                runtime.checkpoint_fault=Mock(return_value=dict(backend_pid=after['owner_epochs'][-1]['control_backend_pid']))
                runtime.joined_unknown=Mock(return_value=[]);runtime.launch=Mock(return_value=dict(id='fresh'))
                runtime.fault_proxy=Mock();runtime.finish_fault_proxy=Mock()
                calls=Mock()
                for name in ('backend_absence','start_checkpoint_proxy','select_checkpoint','launch','checkpoint_fault','joined_unknown','finish_fault_proxy','capture'):
                    calls.attach_mock(getattr(runtime,name),name)
                with patch.object(case.native,'census',return_value=closed) as census:
                    calls.attach_mock(census,'census');runtime.repair_fault(committed=committed)
                    label='monitor-outcome-commit-loss' if committed else 'monitor-outcome-rollback';output=runtime.root/label
                    runtime.launch.assert_called_once_with(label,'app','ingest','recover','--file','/etc/bigbrotr/instance.toml')
                    runtime.start_checkpoint_proxy.assert_called_once_with(output,'after-complete' if committed else 'before-forward')
                    self.assertLessEqual(runtime.joined_unknown.call_args.kwargs['timeout'],20)
                    self.assertIn('deadline',runtime.checkpoint_fault.call_args.kwargs)
                    census.assert_called_once_with(runtime,output,expected_writer_exit=4,writer_terminal=output/'terminal-container.json')
                    runtime.finish_fault_proxy.assert_called_once_with(output,failed=False)
                    names=[call[0] for call in calls.method_calls]
                    self.assertEqual(names[:7],['backend_absence','start_checkpoint_proxy','select_checkpoint','launch','checkpoint_fault','joined_unknown','finish_fault_proxy'])
                    self.assertLess(names.index('backend_absence',1),names.index('capture'));self.assertLess(names.index('capture'),names.index('census'))
                    self.assertEqual(runtime.config.read_text(),'original')
                    verdict=json.loads((output/'independent-verdict.json').read_text())
                    self.assertEqual((verdict['product_result'],verdict['checkpoint'],verdict['native_acknowledged'],verdict['native_writes']),
                        ('CommitUnknown',4 if committed else 3,3,0 if committed else 1))

    def test_direct_followthrough_and_exact_retry_preserve_the_committed_repair(self):
        initial,dump,initialized,receipts,originals,selection=native_outcome_fixture()
        rollback=uncertain_repair_fixture(initial,committed=False);committed=uncertain_repair_fixture(rollback,committed=True)
        closed=closing_native_fixture(dump);acknowledged=closing_native_fixture(closed,acknowledged=4)
        with tempfile.TemporaryDirectory() as directory:
            runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.repair_faults=runtime.native_outcome=True
            runtime.root=Path(directory);runtime.home=runtime.root/'peer';runtime.home.mkdir();runtime.state=runtime.root/'instance'
            runtime.source_unchanged=Mock();runtime.backend_absence=Mock();runtime.client=Mock();runtime.client.poll.return_value=None
            runtime.after_recovery=committed;runtime.after_dump=closed;runtime.initialized=initialized;runtime.receipts=receipts
            runtime.original_inputs=originals;runtime.selection=selection;runtime.original_files={};runtime.cbor=encoder.cbor;runtime.decode=decode
            runtime.originals=Mock(return_value=originals);runtime.files=Mock(return_value={});runtime.catalog=[]
            runtime.resources=dict(containers=[dict(id='fresh')]);runtime.inspect=Mock(return_value=dict(Id='fresh',State=dict(Pid=0)))
            report=dict(format='bigbrotr-ingest-recovery/1',state='positive-outcomes-resolved',dataset_id=initialized['dataset_id'],
                owner_resource_id=initialized['owner_resource_id'],producer_slot_id=initialized['producer_slot_id'],producer_epoch='1',
                issued_through='4',reserved_through='4',resolved_requests='4',native='closed',owner_release='committed',ready=False)
            runtime.finite=Mock(return_value=report)
            for retry in (False,True):
                after=recovered_fixture(runtime.after_recovery,retry=True);runtime.capture=Mock(return_value=(after,[]))
                with patch.object(case.native,'census',return_value=copy.deepcopy(acknowledged)):
                    runtime.recovery(retry=retry)
                label='monitor-outcome-retry' if retry else 'monitor-outcome-recovery'
                runtime.finite.assert_called_with(label,'ingest','recover','--file','/etc/bigbrotr/instance.toml',timeout=30)
                self.assertEqual(runtime.after_recovery,after)
                for table in ('producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies'):
                    self.assertEqual(committed[table],after[table])

    def test_fault_owner_join_requires_honest_CommitUnknown_and_positive_process_exit(self):
        for change in (None,'wait-zero','exit-zero','still-running','oom','delivery-only'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                runtime=object.__new__(case.MonitorAckRecoveryRuntime)
                state=dict(ExitCode=4,Pid=0,Running=False,OOMKilled=False)
                if change=='exit-zero':state['ExitCode']=0
                if change=='still-running':state.update(Running=True,Pid=123)
                if change=='oom':state['OOMKilled']=True
                runtime.inspect=Mock(return_value=dict(Id='fresh',State=state))
                runtime.docker=Mock(side_effect=[SimpleNamespace(stdout='0' if change=='wait-zero' else '4'),
                    SimpleNamespace(stdout='',stderr='DeliveryUnknown' if change=='delivery-only' else 'producer operation unavailable: CommitUnknown')])
                if change:
                    with self.assertRaisesRegex(ValueError,'explicit uncertainty'):runtime.joined_unknown(dict(id='fresh'),Path(directory))
                else:self.assertEqual(runtime.joined_unknown(dict(id='fresh'),Path(directory)),[])

    def test_repair_selector_waits_share_one_fixed_command_deadline(self):
        runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.fault_proxy=Mock();runtime.proxy_event=Mock()
        with self.assertRaisesRegex(ValueError,'repair command fixture deadline'):
            runtime.checkpoint_fault(Path('/unused'),'selected','before-forward',deadline=0)
        runtime.proxy_event.assert_not_called()


class NativeOutcomeRecoveryControls(unittest.TestCase):
    def checked(self, values, *, before=100, after=200):
        state,dump,initialized,receipts,originals,selection=values
        _,_,_,wire,event,_=sample()
        return oracle.native_outcome(state,dump,initialized,selection,originals,encoder.cbor,decode,wire,event,before,after)

    def test_independent_node_receipt_and_literal_C3_C4_vectors_bind_both_phases(self):
        values=native_outcome_fixture();state,dump,initialized,expected,*_=values
        actual=self.checked(values)
        frozen=json.loads((Path(__file__).resolve().parents[2]/'tests/fixtures/model/passive-monitor-recovery.json').read_text())
        self.assertEqual([r['name'] for r in frozen['expected']],['original-monitor-receipt4','generic-C3','generic-C4','original-monitor-input4'])
        self.assertEqual(b'bigbrotr/receipt/1\0'+encoder.cbor(actual[3]),bytes.fromhex(frozen['expected'][0]['preimage_hex']))
        self.assertEqual(values[4][3],bytes.fromhex(frozen['expected'][3]['preimage_hex']))
        self.assertEqual(actual[3][2].hex(),frozen['expected'][3]['sha256'])
        self.assertEqual(actual,expected)
        self.assertEqual(passive.checkpoint_before_terminal(state,initialized,actual[:3])['checkpoint_sha256'],frozen['expected'][1]['sha256'])
        old,*_=fixture()
        self.assertEqual(passive.checkpoint(old,initialized,actual)['checkpoint_sha256'],frozen['expected'][2]['sha256'])
        # The known C4/A3 oracle stays strict; neither phase borrows a fake SQL row.
        with self.assertRaises((AssertionError,ValueError)):
            oracle.terminal(state,initialized,values[5],values[4],encoder.cbor,decode)
        with self.assertRaises(AssertionError):
            passive.native_ack(dump,state,initialized,actual,encoder.cbor,phase='reply-lost')
        with self.assertRaises(AssertionError):self.checked(fixture())

    def test_observed_native_timestamp_is_typed_bounded_and_not_the_terminal_clock(self):
        for recorded in (True,2**63,-(2**63),99,201):
            values=native_outcome_fixture();rewrite_native_fact(values[1],values[3],lambda f:f.__setitem__(4,recorded))
            with self.subTest(recorded=recorded),self.assertRaises(AssertionError):
                self.checked(values,before=0 if type(recorded) is bool else 100)
        values=native_outcome_fixture();expected=rewrite_native_fact(values[1],values[3],lambda f:f.__setitem__(4,107))
        self.assertEqual(self.checked(values)[3],expected)  # Original terminal UTC is108, not a bound on this separate sample.
        for recorded in (-1,-(2**63)):
            values=native_outcome_fixture();expected=rewrite_native_fact(values[1],values[3],lambda f:f.__setitem__(4,recorded))
            self.assertEqual(self.checked(values,before=recorded)[3],expected)
        for before,after in ((True,200),(100,False),(201,100),(-(2**63)-1,200),(100,2**63)):
            with self.subTest(window=(before,after)),self.assertRaises(AssertionError):self.checked(values,before=before,after=after)

    def test_self_consistent_native_fact_alterations_do_not_choose_the_expectation(self):
        for field,value in ((0,[bytes.fromhex('01'*16),0,1,9]),(1,'dictionary'),(2,2),
                (3,[bytes.fromhex('01'*16),4,bytes([4])*16,1])):
            values=native_outcome_fixture();rewrite_native_fact(values[1],values[3],lambda f:f.__setitem__(field,value))
            with self.subTest(field=field),self.assertRaises(AssertionError):self.checked(values)
        values=native_outcome_fixture()
        rewrite_native_fact(values[1],values[3],lambda f:f[5][4][1][0][3].__setitem__(2,0))
        with self.assertRaises(AssertionError):self.checked(values)

    def test_original_scientific_fields_and_full_input_cannot_be_laundered(self):
        for change in ('profile','auth','counter','extra','wrong-boolean','original-bytes'):
            values=list(native_outcome_fixture());state,dump,initialized,receipts,originals,selection=values
            originals=list(originals);request=decode(originals[3].split(b'\0',1)[1]);measurement=request[3][3]
            if change=='profile':measurement[2][13]=65
            elif change=='auth':measurement[4][1][0][2][4]=True
            elif change=='counter':measurement[4][1][0][3][2]=0
            elif change=='extra':measurement.append(None)
            elif change=='wrong-boolean':measurement[4][1][0][2][3]=1
            originals[3]=b'bigbrotr/producer-request/1\0'+wire_cbor(request)+(b'\0' if change=='original-bytes' else b'')
            state['producer_requests'][3].update(request_digest='\\x'+hashlib.sha256(originals[3]).hexdigest(),input_bytes=len(originals[3]))
            values[4]=originals
            with self.subTest(change=change),self.assertRaises((AssertionError,TypeError,ValueError)):self.checked(values)

    def test_exact_PG_C3_reservations_dependencies_and_native_originals_are_required(self):
        for change in ('checkpoint4','revision4','extra-dependency','changed-dependency','missing-outcome',
                'reservation3','input4-manifest','input4-dataset','outcome-dataset','producer-dataset',
                'producer-slot','dispatch','dispatch-owner','request-role','native-receipt','native-closure'):
            values=native_outcome_fixture();state,dump,*_=values
            if change=='checkpoint4':state['producer_checkpoints'][0]['complete_through_request']=4
            elif change=='revision4':state['producer_checkpoints'][0]['checkpoint_revision']=4
            elif change=='extra-dependency':state['producer_checkpoint_dependencies'].append(dict(state['producer_checkpoint_dependencies'][-1],source_sequence=10))
            elif change=='changed-dependency':state['producer_checkpoint_dependencies'][0]['payload_digest']='\\x'+'ab'*32
            elif change=='missing-outcome':state['producer_outcomes'].pop()
            elif change=='reservation3':state['producer_epochs'][0]['reserved_through_request']=3
            elif change=='input4-manifest':state['producer_requests'][3]['manifest_sha256']='\\x'+'ab'*32
            elif change=='input4-dataset':state['producer_requests'][3]['dataset_id']='\\x'+'ab'*16
            elif change=='outcome-dataset':state['producer_outcomes'][0]['dataset_id']='\\x'+'ab'*16
            elif change=='producer-dataset':state['producer_epochs'][0]['dataset_id']='\\x'+'ab'*16
            elif change=='producer-slot':state['producer_epochs'][0]['producer_slot_id']='\\x'+'ab'*16
            elif change=='dispatch':state['measurement_dispatches'][0]['context_bytes']='\\x'+'ab'*294
            elif change=='dispatch-owner':state['measurement_dispatches'][0]['issuer_incarnation']='\\x'+'ab'*16
            elif change=='request-role':state['producer_requests'][3]['issuer_role']='ingest'
            else:
                row=next(r for r in dump['rows'] if r['cf']=='receipts' and r['key'].startswith('0130') and r['key'].endswith('0000000000000004'))
                if change=='native-receipt':row['value']+='00'
                else:
                    value=decode(bytes.fromhex(row['value']).split(b'\0',1)[1]);value[5].pop()
                    row['value']=(b'bigbrotr/receipt/1\0'+wire_cbor(value)).hex()
            with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):self.checked(values)

    def test_missing_outcome_repair_is_atomic_fresh_Control_and_exact_retry_is_read_only(self):
        before,dump,initialized,receipts,originals,selection=native_outcome_fixture();after=repaired_fixture(before)
        oracle.repaired(before,after,initialized,receipts,encoder.cbor)
        self.assertEqual(oracle.repaired_terminal(after,initialized,selection,originals,receipts,encoder.cbor,decode),receipts)
        retry=recovered_fixture(after,retry=True)
        oracle.repaired(after,retry,initialized,receipts,encoder.cbor,retry=True)
        self.assertEqual(oracle.repaired_terminal(retry,initialized,selection,originals,receipts,encoder.cbor,decode),receipts)
        native=copy.deepcopy(dump);next(r for r in native['rows'] if r['key'].startswith('0131'))['value']='0231'+''.join(n.to_bytes(8,'big').hex() for n in (4,4,0))+'01';native['sequence']+=2
        passive.native_ack(native,after,initialized,receipts,encoder.cbor,phase='recovered')
        oracle.native_transition(dump,native,initialized)
        oracle.native_transition(native,copy.deepcopy(native),initialized,retry=True)
        self.assertNotEqual(after['producer_epochs'][0]['xmin'],after['producer_checkpoints'][0]['xmin'])

    def test_repair_refuses_rewritten_history_wrong_authority_and_nonatomic_new_rows(self):
        before,_,initialized,receipts,originals,selection=native_outcome_fixture();after=repaired_fixture(before)
        for change in ('outcome-old-xmin','dependency-old-xmin','head-xmin','outcome4-xmin','dependency10-xmin',
                'old-recorder','request-issuer','dispatch','prefix-base','extra-state','missing-outcome4','rewritten-input'):
            changed=copy.deepcopy(after)
            if change=='outcome-old-xmin':changed['producer_outcomes'][0]['xmin']='999'
            elif change=='dependency-old-xmin':changed['producer_checkpoint_dependencies'][0]['xmin']='999'
            elif change=='head-xmin':changed['producer_checkpoints'][0]['xmin']='999'
            elif change=='outcome4-xmin':changed['producer_outcomes'][3]['xmin']='999'
            elif change=='dependency10-xmin':changed['producer_checkpoint_dependencies'][9]['xmin']='999'
            elif change=='old-recorder':
                changed['producer_outcomes'][3]['recorder_epoch']=1;changed['producer_outcomes'][3]['recorder_incarnation']='\\x'+'52'*16
                changed['producer_checkpoints'][0].update(issuer_epoch=1,issuer_incarnation='\\x'+'52'*16)
            elif change=='request-issuer':changed['producer_requests'][3]['issuer_epoch']=2
            elif change=='dispatch':changed['measurement_dispatches'][0]['xmin']='new'
            elif change=='prefix-base':changed['producer_checkpoints'][0]['prefix_active_generation']=1
            elif change=='extra-state':changed['jobs'].append({})
            elif change=='missing-outcome4':changed['producer_outcomes'].pop()
            else:changed['producer_requests'][3]['request_digest']='\\x'+'00'*32
            with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                oracle.repaired(before,changed,initialized,receipts,encoder.cbor)
                oracle.repaired_terminal(changed,initialized,selection,originals,receipts,encoder.cbor,decode)
        for table in ('producer_checkpoints','producer_checkpoint_dependencies','producer_outcomes'):
            changed=recovered_fixture(after,retry=True);changed[table][-1]['xmin']='999'
            with self.subTest(retry_table=table),self.assertRaises(AssertionError):
                oracle.repaired(after,changed,initialized,receipts,encoder.cbor,retry=True)

    def test_before_forward_fault_is_explicit_and_never_becomes_known_checkpoint_completion(self):
        wire=dict(event='fault',target='control-measurement-checkpoint',mode='before-forward',armed=True,
            data_queries=1,data_commits=1,server_complete=0,client_complete=0,faults=1,measurement_updates_complete=1,
            measurement_identity_sha256=hashlib.sha256(bytes.fromhex('01'*16+'44'*16)).hexdigest(),backend_pid=7331)
        oracle.fault_before_forward(wire,'01'*16,'44'*16)
        with self.assertRaises(AssertionError):oracle.fault(wire,'01'*16,'44'*16)
        for field,value in (('mode','after-complete'),('server_complete',1),('client_complete',1),('data_queries',2),
                ('measurement_updates_complete',True),('backend_pid',0),('measurement_identity_sha256','00'*32)):
            with self.subTest(field=field),self.assertRaises(AssertionError):oracle.fault_before_forward(dict(wire,**{field:value}),'01'*16,'44'*16)

    def test_new_case_missing_candidate_and_fault_failure_preserve_not_run_stages(self):
        root=Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            directory=Path(directory)
            import subprocess,sys
            result=subprocess.run([sys.executable,'-B',str(root/'tests/acceptance/run.py'),'p04-monitor-native-outcome-recovery',
                '--source-root',str(root),'--candidate-manifest',str(directory/'absent'),'--evidence',str(directory/'evidence')],capture_output=True,timeout=5)
            report=json.loads((directory/'evidence/result.json').read_text())
            self.assertEqual((result.returncode,report['result']),(2,'NOT_RUN'))
            self.assertEqual([r['name'] for r in report['checks']],['first_launch_'+s for s in case.NATIVE_OUTCOME_STAGES])
            self.assertFalse(list((directory/'evidence/commands').iterdir()))
        with tempfile.TemporaryDirectory() as directory:
            args=SimpleNamespace(case='p04-monitor-native-outcome-recovery',evidence=Path(directory),source_root=root);checks=[]
            with patch.object(case,'MonitorAckRecoveryRuntime') as runtime:
                runtime.return_value.checkpoint_reply_loss.side_effect=ValueError('unproven C3')
                first_launch(args,checks,{})
                runtime.return_value.recover_ack.assert_not_called();runtime.return_value.retry_ack.assert_not_called()
            self.assertEqual([r['result'] for r in checks],['PASS']*4+['FAIL','NOT_RUN','NOT_RUN','PASS'])

    def test_new_case_retains_the_same_deployment_and_selects_before_forward_before_source_work(self):
        source=Path(__file__).resolve().parents[2];path=source/'experiments/archive-engine/native_commit_proxy.py'
        ready=dict(source_sha256=case.digest(path),armed=False,upstream_host='127.0.0.1',upstream_port=15432,
            port=43210,mode='before-forward',target='control-measurement-checkpoint',profile='retained-catch-up',
            frame_limit=131072,connection_limit=64,connection_limit_scope='lifetime',wire_limit=64*1024*1024,lifetime_seconds=120)
        for field,value in ((None,None),('mode','after-complete'),('frame_limit',20000),
                ('connection_limit_scope','active'),('lifetime_seconds',180)):
            with self.subTest(field=field),tempfile.TemporaryDirectory() as temporary:
                runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.native_outcome=True
                runtime.source=source;runtime.root=Path(temporary);runtime.config=runtime.root/'instance.toml'
                original=(source/'config/recipes/acceptance.toml').read_text();runtime.config.write_text(original)
                runtime.set_value=set_value;proxy=Mock();proxy.process.pid=123
                runtime.proxy_process=Mock(return_value=proxy)
                runtime.proxy_event=Mock(return_value=ready if field is None else dict(ready,**{field:value}))
                runtime.finish_fault_proxy=Mock();runtime.passive_begin=Mock(side_effect=RuntimeError('ready accepted; no source launched'))
                with self.assertRaisesRegex(RuntimeError if field is None else ValueError,
                        'ready accepted' if field is None else 'wrong deferred checkpoint proxy'):
                    runtime.checkpoint_reply_loss()
                runtime.proxy_process.assert_called_once_with([case.sys.executable,'-B',str(path),'--upstream-port','15432',
                    '--mode','before-forward','--target','control-measurement-checkpoint','--profile','retained-catch-up','--defer-arm'],input_pipe=True)
                self.assertEqual(runtime.passive_begin.call_count,int(field is None))
                runtime.finish_fault_proxy.assert_called_once_with(runtime.root/'passive-native-outcome-loss',failed=True)
                self.assertEqual(runtime.config.read_text(),original)
        with tempfile.TemporaryDirectory() as temporary:
            runtime=object.__new__(case.MonitorAckRecoveryRuntime);runtime.native_outcome=True
            runtime.root=Path(temporary);runtime.state=runtime.root/'instance';runtime.state.mkdir()
            runtime.home=runtime.root/'archive-export';runtime.home.mkdir();runtime.config=runtime.state/'instance.toml'
            runtime.config.write_text((source/'config/recipes/acceptance.toml').read_text());runtime.inputs={};runtime.set_value=set_value
            with patch.object(Runtime,'prepare_state'):runtime.prepare_state()
            self.assertEqual(json.loads((runtime.home/'monitor-ack-recovery.json').read_text()),dict(
                format='bigbrotr-monitor-ack-recovery-peer/1',boundary='checkpoint-before-forward',observed_seconds=120))
            self.assertEqual(tomllib.loads(runtime.config.read_text())['topology']['local_roles'],['control','ingest','monitor','archive'])
            runtime.launch=Mock(return_value='owner');runtime.passive_launch()
            runtime.launch.assert_called_once_with('monitor-native-outcome-loss','app','serve','--file',
                '/etc/bigbrotr/instance.toml','--roles','control,ingest,monitor','--initialize-ingest',
                '--monitor-once','/var/lib/bigbrotr/monitor-once.json')
            self.assertEqual(runtime.recovery_label(retry=False),'monitor-outcome-recovery')
            self.assertEqual(runtime.recovery_label(retry=True),'monitor-outcome-retry')


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

    def test_proxy_finalizer_retains_diagnostics_and_preserves_primary_failure(self):
        for failed,code,timeout in ((False,0,False),(True,3,False),(False,3,False),(True,-15,True)):
            with self.subTest(failed=failed,code=code,timeout=timeout),tempfile.TemporaryDirectory() as temporary:
                runtime=object.__new__(case.MonitorAckRecoveryRuntime)
                runtime.resources={'cleanup_errors':[]};runtime.proxy_send=Mock()
                proxy=Mock();proxy.process.poll.return_value=None;proxy.process.returncode=code
                terminal=dict(event='terminal',unexpected=int(code!=0),frames=20001 if code==3 else 0,
                    failure_kinds={'traffic-budget':1} if code==3 else {})
                proxy.stdout=(json.dumps(terminal)+'\n').encode();proxy.stderr=b'';proxy.overflow=False
                proxy.wait.side_effect=TimeoutError() if timeout else None;proxy.wait.return_value=code
                runtime.proxy_event=Mock(return_value=terminal);runtime.fault_proxy=proxy
                output=Path(temporary)
                original=AssertionError('primary fixture event deadline')
                if failed:
                    with self.assertRaises(AssertionError) as caught:
                        try:raise original
                        finally:runtime.finish_fault_proxy(output,failed=True)
                    self.assertIs(caught.exception,original)
                elif code:
                    with self.assertRaisesRegex(ValueError,'checkpoint proxy did not terminate cleanly'):
                        runtime.finish_fault_proxy(output,failed=False)
                else:runtime.finish_fault_proxy(output,failed=False)
                runtime.proxy_send.assert_called_once_with(proxy,'stop\n');proxy.wait.assert_called_once_with(5)
                proxy.stop.assert_called_once_with()
                self.assertIsNone(runtime.fault_proxy)
                self.assertEqual(json.loads((output/'proxy-terminal.json').read_text()),dict(exit_code=code,overflow=False))
                self.assertEqual((output/'proxy.stdout').read_bytes(),proxy.stdout)
                self.assertEqual(bool(runtime.resources['cleanup_errors']),bool(code))
                if not timeout:self.assertEqual(json.loads((output/'proxy-report.json').read_text()),terminal)

    def test_driver_explicitly_selects_and_checks_the_retained_proxy_envelope_before_source_work(self):
        source=Path(__file__).resolve().parents[2]
        path=source/'experiments/archive-engine/native_commit_proxy.py'
        ready=dict(source_sha256=case.digest(path),armed=False,upstream_host='127.0.0.1',upstream_port=15432,port=43210,
            mode='after-complete',target='control-measurement-checkpoint',profile='retained-catch-up',
            frame_limit=131072,connection_limit=64,connection_limit_scope='lifetime',wire_limit=64*1024*1024,lifetime_seconds=120)
        for field,value in ((None,None),('profile','default'),('frame_limit',20000),('connection_limit',16),
                ('connection_limit_scope','active'),('wire_limit',128*1024*1024),('lifetime_seconds',180),
                ('mode','before-forward'),('target','control-producer-progress'),('armed',True),('upstream_host','127.0.0.2')):
            with self.subTest(field=field),tempfile.TemporaryDirectory() as temporary:
                runtime=object.__new__(case.MonitorAckRecoveryRuntime)
                runtime.source=source;runtime.root=Path(temporary);runtime.config=runtime.root/'instance.toml'
                original=(source/'config/recipes/acceptance.toml').read_text();runtime.config.write_text(original)
                runtime.set_value=set_value;proxy=Mock();proxy.process.pid=123
                runtime.proxy_process=Mock(return_value=proxy)
                runtime.proxy_event=Mock(return_value=ready if field is None else dict(ready,**{field:value}))
                runtime.finish_fault_proxy=Mock()
                runtime.passive_begin=Mock(side_effect=RuntimeError('ready accepted; stop before source work'))
                with self.assertRaisesRegex(RuntimeError if field is None else ValueError,
                        'ready accepted' if field is None else 'wrong deferred checkpoint proxy'):
                    runtime.checkpoint_reply_loss()
                runtime.proxy_process.assert_called_once_with([case.sys.executable,'-B',str(path),'--upstream-port','15432',
                    '--mode','after-complete','--target','control-measurement-checkpoint','--profile','retained-catch-up','--defer-arm'],input_pipe=True)
                self.assertEqual(runtime.passive_begin.call_count,int(field is None))
                runtime.finish_fault_proxy.assert_called_once_with(runtime.root/'passive-checkpoint-reply-loss',failed=True)
                self.assertEqual(runtime.config.read_text(),original)
