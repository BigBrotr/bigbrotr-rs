"""Portable false-PASS controls; connected Rust/SQL execution is deliberately absent."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cases.first_launch import first_launch
from cases import first_launch_monitor_ack_recovery as case
from cases import first_launch_monitor_ack_recovery_oracle as oracle
from cases import first_launch_monitor_outcome_seed as seed
from cases import first_launch_passive_oracle as passive
from test_monitor_ack_recovery import native_outcome_fixture, recovered_fixture, repaired_fixture
from test_passive_monitor import encoder


def refused_schedule():
    before,_,initialized,receipts,*_=native_outcome_fixture()
    closed=recovered_fixture(before)
    borrowed=recovered_fixture(closed,retry=True)
    invalid=recovered_fixture(borrowed,retry=True)
    owner=invalid['owner_epochs'][-1]
    owner.update(state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None)
    passive.only(invalid['role_instances'],role_instance_id=owner['role_instance_id'])['retired_at_us']=None
    return before,closed,borrowed,invalid,initialized,receipts


def sql_fixture():
    expected={name:'23514' for name in '''valid-before foreign-context wrong-kind zero-domain missing-byte
        profile ceiling owner expired pending-other-id closed-permits-new refusal-immutable immutable-owner
        immutable-context delete-intent valid-after evidence-before missing-dispatch unknown-dispatch
        mixed-acquisition changed-generation changed-route missing-resolution closed-evidence-refused
        evidence-noio-exclusion evidence-after closing-valid-before closing-dataset closing-slot closing-epoch
        closing-zero-source closing-zero-sequence closing-valid-after capacity-deps-16384 capacity-deps-16385
        capacity-bytes-exact capacity-bytes-plus-one deferred-historical deferred-current'''.split()}
    expected.update({name:'ZX001' for name in ('valid-before','valid-after','closed-permits-new',
        'evidence-before','evidence-after','closing-valid-before','closing-valid-after',
        'capacity-deps-16384','capacity-bytes-exact','deferred-current')})
    expected.update(expired='BB001',**{'pending-other-id':'BB001','closing-slot':'P0002','closing-epoch':'P0002'})
    catalog=[dict(table=table,trigger=trigger,type=kind,enabled='O',security_definer=False,
        deferrable=deferred,initially_deferred=deferred) for table,trigger,kind,deferred in (
            ('producer_checkpoints','producer_checkpoint_guard',31,False),
            ('producer_checkpoints','zzzz_monitor_late_issuer',19,False),
            ('producer_checkpoint_dependencies','producer_checkpoint_dependency_guard',7,False),
            ('producer_checkpoint_dependencies','zzz_producer_checkpoint_dependencies_complete',5,True))]
    observations=dict(catalog=catalog,closing=dict(runtime_user='bigbrotr_control',session_user='bigbrotr_control',
        backend_pid=100,retained_bytes=300000,jobs=0,dependencies=9,c4_digest='ab'*32))
    for name,code in (('deferred-historical','23514'),('deferred-current','ZX001')):
        observations[name]=dict(updated=1,late_hook_invocations=1,stored_complete_c4=True,retained_validated=True,
            sqlstate=code,message='Control owner belongs to another backend or is inactive' if code=='23514' else 'positive fixture rollback',
            exception_context='PL/pgSQL function control.require_control_owner\nSQL statement\nPL/pgSQL function control.require_producer_progress_children',
            runtime_user='bigbrotr_control',session_user='bigbrotr_control',backend_pid=100)
    return expected,observations


class ConnectedGuardControls(unittest.TestCase):
    def test_close_borrowed_and_invalid_phases_keep_original_c3_without_claiming_an_extra_owner(self):
        before,closed,borrowed,invalid,initialized,receipts=refused_schedule()
        for old,new,phase in ((before,closed,'close'),(closed,borrowed,'borrowed-backend'),(borrowed,invalid,'invalid-context')):
            oracle.guard_refused(old,new,initialized,receipts,phase=phase)
        after=repaired_fixture(invalid)
        oracle.guard_resolved(invalid,after,initialized,receipts,encoder.cbor)
        acknowledged=recovered_fixture(after,retry=True)
        oracle.repair_ack(after,acknowledged,initialized,receipts,encoder.cbor)
        oracle.repair_ack(acknowledged,recovered_fixture(acknowledged,retry=True),initialized,receipts,encoder.cbor)

    def test_refusals_reject_copied_outcome_head_dependency_or_original_input_effects(self):
        _,closed,borrowed,_,initialized,receipts=refused_schedule()
        for table in ('producer_requests','producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies','measurement_dispatches'):
            changed=copy.deepcopy(borrowed);changed[table][0]['xmin']='999'
            with self.subTest(table=table),self.assertRaises(AssertionError):
                oracle.guard_refused(closed,changed,initialized,receipts,phase='borrowed-backend')
        changed=copy.deepcopy(borrowed);changed['owner_epochs'].append(copy.deepcopy(changed['owner_epochs'][-1]))
        with self.assertRaises(AssertionError):oracle.guard_refused(closed,changed,initialized,receipts,phase='borrowed-backend')

    def test_failed_context_session_cannot_pass_as_released_or_reset(self):
        _,_,borrowed,invalid,initialized,receipts=refused_schedule()
        released=recovered_fixture(borrowed,retry=True)
        with self.assertRaises(AssertionError):oracle.guard_refused(borrowed,released,initialized,receipts,phase='invalid-context')
        changed=copy.deepcopy(invalid);changed['producer_outcomes'].append(copy.deepcopy(changed['producer_outcomes'][-1]))
        with self.assertRaises(AssertionError):oracle.guard_refused(borrowed,changed,initialized,receipts,phase='invalid-context')

    def test_explicit_corruption_baseline_keeps_every_other_column_and_table(self):
        _,closed,*_=refused_schedule()
        closed['measurement_dispatches'][0]['xmin']='800'
        changed=copy.deepcopy(closed);row=changed['measurement_dispatches'][0]
        context=bytearray(passive.raw(row['context_bytes'],294));context[228]^=128
        row.update(context_bytes='\\x'+context.hex(),xmin='900')
        oracle.guard_setup(closed,changed,bytes(context))
        for change in ('other-column','other-table','wrong-context','same-xmin'):
            bad=copy.deepcopy(changed)
            if change=='other-column':bad['measurement_dispatches'][0]['profile_version']=2
            elif change=='other-table':bad['producer_requests'][0]['xmin']='900'
            elif change=='wrong-context':bad['measurement_dispatches'][0]['context_bytes']='\\x'+'00'*294
            else:bad['measurement_dispatches'][0]['xmin']=closed['measurement_dispatches'][0]['xmin']
            with self.subTest(change=change),self.assertRaises(AssertionError):oracle.guard_setup(closed,bad,bytes(context))

    def test_sql_matrix_requires_exact_codes_positive_pairs_complete_phase_and_actual_owning_error(self):
        rows,observations=sql_fixture();oracle.guard_sql_results(rows,observations,'ab'*32,300000,100)
        for change in ('missing-result','wrong-code','missing-positive','retained-charge','early-failure','partial-head',
                'wrong-error','wrong-context','wrong-backend','disabled-trigger','late-order','deferral-mode'):
            actual=copy.deepcopy(rows);seen=copy.deepcopy(observations)
            if change=='missing-result':actual.pop('closing-zero-source')
            elif change=='wrong-code':actual['closing-slot']='23514'
            elif change=='missing-positive':actual['capacity-deps-16384']='23514'
            elif change=='retained-charge':seen['closing']['retained_bytes']+=1
            elif change=='early-failure':seen['deferred-historical']['updated']=0
            elif change=='partial-head':seen['deferred-historical']['stored_complete_c4']=False
            elif change=='wrong-error':seen['deferred-historical']['message']='other check violation'
            elif change=='wrong-context':seen['deferred-historical']['exception_context']='guard_producer_checkpoint -> require_control_owner'
            elif change=='wrong-backend':seen['deferred-current']['backend_pid']=101
            elif change=='disabled-trigger':seen['catalog'][0]['enabled']='D'
            elif change=='late-order':seen['catalog'][0]['trigger']='zzzzz_after_fixture'
            else:seen['catalog'][-1]['initially_deferred']=False
            with self.subTest(change=change),self.assertRaises(AssertionError):
                oracle.guard_sql_results(actual,seen,'ab'*32,300000,100)

    def test_actual_fixture_receipt_binds_two_ordinary_runtime_backends_and_transport_grants(self):
        result=dict(format='bigbrotr-monitor-connected-guards-result/1',status='PASS',phase='borrowed-backend',
            evidence=dict(runtime_user='bigbrotr_control',session_user='bigbrotr_control',superuser=False,owns_objects=False,
                replication_role='origin',owner_backend=100,borrowed_backend=101,control_data_bytes=8388608,borrowed_data_bytes=8388608))
        oracle.guard_fixture_result(result,'borrowed-backend')
        for field,value in (('runtime_user','postgres'),('superuser',True),('owns_objects',True),('replication_role','replica'),
                ('borrowed_backend',100),('owner_backend',True),('control_data_bytes',0),('borrowed_data_bytes',0)):
            changed=copy.deepcopy(result);changed['evidence'][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):oracle.guard_fixture_result(changed,'borrowed-backend')

    def test_product_source_fence_exempts_only_the_attributed_sql_fixture(self):
        import hashlib
        with tempfile.TemporaryDirectory() as temporary:
            source=Path(temporary).resolve()
            owner=source/seed.OWNER;owner.parent.mkdir(parents=True)
            owner.write_bytes(b'unchanged runtime owner'+seed.HOOK.encode())
            private=source/seed.FIXTURE;private.write_bytes(b'private compiled test')
            sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
            baseline={seed.OWNER:sha(owner),seed.FIXTURE:sha(private),
                'experiments/relay-source/monitor_guards.sql':'old','Cargo.lock':'locked'}
            current=dict(baseline,**{'experiments/relay-source/monitor_guards.sql':'new'})
            seed.parent_sources(dict(files_sha256=current),dict(files_sha256=baseline),source)
            for name in ('migrations/postgres/0044_measurement_dispatches.sql','experiments/relay-source/other.sql'):
                changed=dict(current,**{name:'unreviewed'})
                with self.subTest(name=name),self.assertRaises(ValueError):
                    seed.parent_sources(dict(files_sha256=changed),dict(files_sha256=baseline),source)

    def test_guard_mount_target_is_explicit_and_keeps_the_existing_two_read_only_inputs(self):
        entry=dict(service='app',monitor_guard_phase='invalid-context',monitor_outcome_seed=dict(binary='/fixture',request='/request'))
        info=dict(Mounts=[dict(Destination='/fixture/seed',RW=False),dict(Destination='/fixture/request.json',RW=False)],
            Config=dict(Entrypoint=['/fixture/seed'],Cmd=[seed.GUARD_TEST,'--ignored','--exact','--nocapture','--test-threads=1'],
                Env=['BIGBROTR_MONITOR_OUTCOME_SEED=/fixture/request.json']))
        self.assertEqual(set(seed.mounts(info,entry)),{'/fixture/seed','/fixture/request.json'})
        for phase in ('other',''):
            with self.assertRaises(ValueError):seed.mounts(info,dict(entry,monitor_guard_phase=phase))
        info['Config']['Cmd'][0]=seed.TEST
        with self.assertRaises(ValueError):seed.mounts(info,entry)

    def test_case_requires_explicit_corruption_and_missing_build_cannot_start_engines(self):
        root=Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as temporary:
            home=Path(temporary).resolve()
            args=[sys.executable,'-B',str(root/'tests/acceptance/run.py'),'p04-monitor-connected-guards',
                '--source-root',str(root),'--candidate-manifest',str(home/'missing'),'--evidence',str(home/'evidence')]
            refused=subprocess.run(args,capture_output=True,timeout=5)
            self.assertEqual(refused.returncode,2);self.assertFalse((home/'evidence').exists())
            result=subprocess.run(args+['--monitor-guard-corruption','dispatch-clock'],capture_output=True,timeout=5)
            report=json.loads((home/'evidence/result.json').read_text())
            self.assertEqual((result.returncode,report['result']),(2,'NOT_RUN'))
            self.assertEqual([row['name'] for row in report['checks']],['first_launch_'+name for name in case.CONNECTED_GUARD_STAGES])
            self.assertFalse(list((home/'evidence/commands').iterdir()))

    def test_invalid_phase_failure_keeps_positive_resolver_and_installed_ack_not_run(self):
        root=Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            args=SimpleNamespace(case='p04-monitor-connected-guards',evidence=Path(directory),source_root=root);checks=[]
            with patch.object(case,'MonitorAckRecoveryRuntime') as runtime:
                runtime.return_value.guard_invalid_context.side_effect=ValueError('missing actual InvalidContext refusal')
                first_launch(args,checks,{})
                runtime.return_value.guard_resolve.assert_not_called()
                runtime.return_value.recover_ack.assert_not_called();runtime.return_value.retry_ack.assert_not_called()
            self.assertEqual([row['result'] for row in checks],['PASS']*7+['FAIL']+['NOT_RUN']*3+['PASS'])
