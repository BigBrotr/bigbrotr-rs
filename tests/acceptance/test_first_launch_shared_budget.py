"""Literal scheduling/ownership refusal controls, without product or engines."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from cases import first_launch as base
from cases import first_launch_index_oracle as index
from cases import first_launch_shared_budget as case
from cases import first_launch_shared_budget_oracle as check
from test_first_launch import ROOT, arguments
from test_first_launch_archive_outage import preservation_fixture
from test_first_launch_index import set_value


def trace():
    units = dict(data_bytes=100, items=5, native_bytes=0, control_bytes=2)
    held = dict(incarnation=f'{20:032x}', sequence='7', kind='publication', state='actual-retained-operation-owner')
    denial = dict(format=check.FORMAT, scope='operator-observation-only', dataset_id=f'{1:032x}',
        control_resource_id=f'{2:032x}', control_epoch='1', control_incarnation=f'{20:032x}',
        control_role_instance_id=f'{12:032x}', transition='deferred', record_limit=128,
        wait_id='3', role='sync', timing='monotonic-microseconds-from-capacity-observer-start',
        wait_started_us=1000, observed_us=1000, wait_elapsed_us=0, budget_domain='application',
        budget_observation='after-atomic-refusal-not-the-failing-snapshot',
        requested=units, charged=dict(data_bytes=80,items=3,native_bytes=0,control_bytes=1),
        limits=dict(data_bytes=150,items=10,native_bytes=10,control_bytes=20),
        retained_index_at_denial=held, retained_index_now=copy.deepcopy(held),
        local_joined_consumed_through='6', denied_index_locally_joined_and_consumed=False,
        remote_query_unknown=False)
    admission = copy.deepcopy(denial)
    admission.update(transition='admitted',observed_us=3000,wait_elapsed_us=2000,
        budget_observation='after-whole-operation-reservation',charged=copy.deepcopy(units),
        retained_index_now=None,local_joined_consumed_through='7',denied_index_locally_joined_and_consumed=True)
    return denial, admission


def pending_fixture(*, accepted=False, role='sync'):
    """Only scheduling/table transition structure; signed science is separate."""
    prefix, _, _ = preservation_fixture()
    raw = lambda n: '\\x'+f'{n:032x}'
    prefix['producer_requests'] = [dict(request_sequence=n, request_digest=raw(50+n),producer_epoch=1,xmin='20') for n in (1,2)]
    prefix['producer_outcomes'] = copy.deepcopy(prefix['producer_requests'])
    prefix['archive_attempts'] = [dict(ordinal=n,xmin='20',attempt_id=raw(70+n),worker_incarnation=raw(60+n),
        dataset_id=raw(1),catalog_id=raw(4),expected_predecessor=n-1,schema_version=1,topology_generation=0,
        persistence_protocol=1,issuer_role='control',issuer_resource=raw(2),issuer_epoch=1,
        issuer_incarnation=raw(20),coordinator_incarnation=raw(20),coordinator_binding_sha256='\\x'+'aa'*32,
        issued_at_us=200+n) for n in (1,2)]
    prefix['projection_plans'] = [dict(frontier_revision=n,source_first=check.THROUGH[n-1]+1,
        source_last=check.THROUGH[n],sealed=True,xmin='20') for n in (1,2)]
    prefix['projection_chunks'] = [dict(chunk_id=raw(100+n),state='committed',xmin='20') for n in range(20)]
    prefix['consumer_frontiers'] = [dict(consumer_id=raw(c),frontier_revision=n,last_complete_sequence=check.THROUGH[n],xmin='20')
        for c in (4,5) for n in (1,2)]
    prefix['consumer_frontier_revisions'] = copy.deepcopy(prefix['consumer_frontiers'])
    prefix['archive_frontier_snapshots'] = [dict(original=n) for n in (1,2)]
    prefix['archive_frontier_batches'] = [dict(original=n) for n in range(3)]
    current = copy.deepcopy(prefix)
    current['jobs'] = [dict(job_id=raw(40),kind='acquisition',state='accepted' if accepted else 'queued',
        job_revision=1 if accepted else 2,checkpoint_revision=0,completed_at_us=None,
        principal_id=raw(41),owner_epoch=None if accepted else 1,owner_incarnation=None if accepted else raw(20),
        owner_resource_id=None if accepted else raw(2),owner_role='control',xmin='30')]
    current['acquisition_jobs'] = [dict(job_id=raw(40),actor_id=raw(41),scope_id=raw(42),xmin='30')]
    current['acquisition_scopes'] = [dict(scope_id=raw(42),xmin='30')]
    for table in ('acquisition_selectors','acquisition_scope_commands','job_admissions'):
        current[table] = [dict(added=1,xmin='30')]
    for table in ('management_commands','management_audit'):
        current[table] += [dict(added=n,xmin='30') for n in (1,2)]
    count = 2 if accepted else 3
    if count == 3:
        for table in ('producer_requests','producer_outcomes'):
            current[table].append(dict(request_sequence=3,request_digest=raw(53),producer_epoch=1,xmin='30'))
    current['producer_epochs'][0].update(reserved_through_request=count,xmin='30')
    current['producer_checkpoints'][0].update(complete_through_request=count,checkpoint_revision=count,xmin='30')
    denial, admission = trace()
    denial['role'] = admission['role'] = role
    if accepted:
        for value in (denial, admission):
            value['retained_index_at_denial']['kind'] = 'readiness-plan'
        denial['retained_index_now']['kind'] = 'readiness-plan'
    else:
        current['projection_plans'].append(dict(frontier_revision=3,source_first=4,source_last=7,sealed=True,xmin='30'))
        current['projection_chunks'].extend(dict(chunk_id=raw(100+n),state='prepared',xmin='30',
            committed_at_us=None,engine_receipt_digest=None) for n in range(20,30))
        for table in ('projection_plan_chunks','projection_chunk_bounds'):
            current[table] += [dict(added=n,xmin='30') for n in range(10)]
    return prefix,current,denial,admission,f'{40:032x}'


class SharedBudgetControls(unittest.TestCase):
    def test_fixed_recipe_changes_only_payload_and_refuses_a_replaced_declaration(self):
        self.assertEqual(case.APPLICATION_PAYLOAD_BYTES,905969664)
        with tempfile.TemporaryDirectory() as directory:
            runtime=object.__new__(case.SharedBudgetRuntime)
            runtime.config=Path(directory)/'instance.toml';runtime.inputs={};runtime.set_value=set_value
            original='[runtime]\nrenewal_timeout = "5s"\n[runtime.processes.application]\npayload_bytes = "2GiB"\nnative_bytes = "512MiB"\n[archive]\nmax_publish_delay = "1s"\n'
            runtime.config.write_text(original)
            with patch.object(case.RestartIndexRuntime,'prepare_state'):
                runtime.prepare_state()
            value=tomllib.loads(runtime.config.read_text());before=tomllib.loads(original)
            self.assertEqual(value['runtime']['processes']['application'].pop('payload_bytes'),'905969664B')
            before['runtime']['processes']['application'].pop('payload_bytes')
            self.assertEqual(value,before)
            self.assertEqual(runtime.inputs['initial_recipe_sha256'],case.digest(runtime.config))
            with patch.object(case.RestartIndexRuntime,'initial_launch',return_value='original launch') as launch:
                self.assertEqual(runtime.initial_launch(),'original launch')
                runtime.config.write_text(set_value(runtime.config.read_text(),'runtime.processes.application','payload_bytes','2GiB'))
                with self.assertRaises(ValueError):runtime.initial_launch()
                launch.assert_called_once()

    def test_original_pair_requires_exact_owner_wait_join_and_actual_held_identity(self):
        prefix,state,denied,admitted,job = pending_fixture()
        check.denial(denied,prefix); check.admitted(denied,admitted)
        # A post-refusal snapshot need not itself reconstruct the earlier deficit.
        later = copy.deepcopy(denied); later['charged']['data_bytes'] = 0
        check.denial(later,prefix)
        for change in (
            lambda v:v.update(control_incarnation='ff'*16),
            lambda v:v.update(retained_index_now=None),
            lambda v:v['retained_index_now'].update(sequence='8'),
            lambda v:v['retained_index_at_denial'].update(kind='installation'),
            lambda v:v.update(local_joined_consumed_through='7'),
            lambda v:v.update(remote_query_unknown=True),
            lambda v:v.update(budget_domain='native'),
            lambda v:v.update(budget_observation='atomic-failing-state'),
        ):
            changed=copy.deepcopy(denied);change(changed)
            with self.subTest(change=change),self.assertRaises((AssertionError,TypeError)):
                check.denial(changed,prefix)
        for change in (
            lambda v:v.update(wait_id='4'), lambda v:v.update(role='archive'),
            lambda v:v.update(control_epoch='2'), lambda v:v['requested'].update(data_bytes=99),
            lambda v:v.update(local_joined_consumed_through='6'),
            lambda v:v.update(denied_index_locally_joined_and_consumed=False),
            lambda v:v.update(remote_query_unknown=True),
            lambda v:v.update(retained_index_now=denied['retained_index_now']),
            lambda v:v['charged'].update(data_bytes=99),
            lambda v:v.update(observed_us=30_001_000, wait_elapsed_us=30_000_000),
        ):
            changed=copy.deepcopy(admitted);change(changed)
            with self.subTest(change=change),self.assertRaises(AssertionError):check.admitted(denied,changed)

    def test_timestamp_truncation_has_only_one_microsecond_allowance(self):
        denied,admitted=trace()
        admitted['observed_us']+=1
        check.admitted(denied,admitted)
        admitted['observed_us']+=1
        with self.assertRaises(AssertionError):check.admitted(denied,admitted)

    def test_complete_lifecycle_refuses_missing_duplicate_replaced_or_incomplete_evidence(self):
        denied,admitted=trace()
        encode=lambda rows:'\n'.join(json.dumps(row) for row in rows)+'\n'
        first=dict(state='initialized')
        self.assertEqual(case.records(encode([first,denied,admitted])),[denied,admitted])
        incomplete={key:denied[key] for key in check.COMMON};incomplete['transition']='incomplete'
        for rows in ([denied,admitted],[first,admitted],[first,denied,denied],[first,denied,incomplete]):
            with self.subTest(rows=rows),self.assertRaises(ValueError):case.records(encode(rows))
        with self.assertRaises(ValueError):case.records(encode([first])+(' '*4096)+'\n')

    def test_pending_work_preserves_all_other_tables_and_accepts_only_real_job_states(self):
        for accepted,role in ((True,'sync'),(False,'sync'),(False,'archive')):
            prefix,state,denied,_,job=pending_fixture(accepted=accepted,role=role)
            result=check.pending(prefix,state,denied,job)
            self.assertEqual(result['job_state'],'accepted' if accepted else 'queued')
        prefix,state,denied,_,job=pending_fixture()
        changes=[lambda s:s['datasets'][0].update(dataset_id='wrong'),
            lambda s:s['jobs'][0].update(state='running'),lambda s:s['jobs'][0].update(owner_epoch=2),
            lambda s:s['jobs'][0].update(checkpoint_revision=1),lambda s:s['producer_requests'][0].update(xmin='999'),
            lambda s:s['producer_outcomes'].pop(),lambda s:s['producer_checkpoints'][0].update(manifest_sha256='wrong'),
            lambda s:s['archive_attempts'].append(dict(ordinal=3)),
            lambda s:s['projection_plans'][-1].update(source_last=8),
            lambda s:s['projection_chunks'][-1].update(state='committed'),
            lambda s:s['index_frontier_handoffs'].append(dict(synthetic='handoff')),
            lambda s:s['owner_epochs'][0].update(process_incarnation='wrong')]
        for change in changes:
            mutated=copy.deepcopy(state);change(mutated)
            with self.subTest(change=change),self.assertRaises(AssertionError):check.pending(prefix,mutated,denied,job)
        prefix,state,denied,_,job=pending_fixture(accepted=True)
        denied['role']='archive'
        with self.assertRaises(AssertionError):check.pending(prefix,state,denied,job)

    def test_follow_through_allows_only_initial_claim_and_prepared_chunk_commit(self):
        for accepted in (False,True):
            _,pending,_,_,_=pending_fixture(accepted=accepted)
            final=copy.deepcopy(pending)
            final['jobs'][0].update(state='queued',job_revision=2,owner_epoch=1,
                owner_incarnation='\\x'+f'{20:032x}',owner_resource_id='\\x'+f'{2:032x}')
            if accepted:final['jobs'][0]['xmin']='40'
            for row in final['projection_chunks']:
                if row['state']=='prepared':row.update(state='committed',committed_at_us=4000,engine_receipt_digest='proof',xmin='40')
            check.preserved(pending,final)
            for table,field,value in (('jobs','principal_id','other'),('projection_chunks','chunk_id','other'),
                    ('producer_outcomes','request_digest','other'),('role_instances','retired_at_us',4000),
                    ('consumers','current_topology_generation',1),('producer_checkpoints','manifest_sha256','other')):
                mutated=copy.deepcopy(final);mutated[table][0][field]=value
                with self.subTest(accepted=accepted,table=table),self.assertRaises((AssertionError,ValueError)):check.preserved(pending,mutated)

    def test_new_archive_publication_is_bound_to_the_same_control_and_coordinator(self):
        prefix,state,denied,_,job=pending_fixture()
        consumer=index.only(state['consumers'],consumer_role='archive');consumer['current_revision']=3
        third=dict(state['archive_attempts'][1],ordinal=3,expected_predecessor=2,
            attempt_id='\\x'+f'{73:032x}',worker_incarnation='\\x'+f'{63:032x}',xmin='30')
        state['archive_attempts'].append(third)
        state['archive_frontier_snapshots'].append(dict(added=3))
        state['archive_frontier_batches'] += [dict(added=n) for n in range(3)]
        for table in ('consumer_frontiers','consumer_frontier_revisions'):
            state[table].append(dict(consumer_id=consumer['consumer_id'],frontier_revision=3,last_complete_sequence=7,xmin='30'))
        check.pending(prefix,state,denied,job)
        for key,value in (('issuer_epoch',2),('coordinator_incarnation','\\x'+'ff'*16),
                ('coordinator_binding_sha256','\\x'+'bb'*32),('worker_incarnation',third['coordinator_incarnation'])):
            changed=copy.deepcopy(state);changed['archive_attempts'][-1][key]=value
            with self.subTest(key=key),self.assertRaises(AssertionError):check.pending(prefix,changed,denied,job)

    def test_terminal_archive_renewal_is_explicit_and_cannot_change_other_fields(self):
        _,live,after=preservation_fixture()
        archive=index.only(after['owner_epochs'],role='archive');archive.update(lease_expires_at_us=30000,xmin='90')
        case.terminal.terminal_preservation(live,after,7000,archive_renewal=True)
        with self.assertRaises(AssertionError):case.terminal.terminal_preservation(live,after,7000)
        archive['lease_expires_at_us']=9999
        with self.assertRaises(AssertionError):case.terminal.terminal_preservation(live,after,7000,archive_renewal=True)
        archive['lease_expires_at_us']=30000
        archive['process_incarnation']='different'
        with self.assertRaises(AssertionError):case.terminal.terminal_preservation(live,after,7000,archive_renewal=True)

    def test_driver_requires_raw_denial_before_one_capture_and_unpause_before_matching_admission(self):
        # Scheduling/cleanup control only. Fake commands cannot prove an installed
        # operation, backend, resource grant or independent signed science.
        for capture_fails in (False,True):
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory);output=root/'initial';output.mkdir()
                prefix,state,denied,admitted,job=pending_fixture()
                owner=index.only(state['owner_epochs'],role='control',state='active')
                for selected in (prefix,state):
                    index.only(selected['owner_epochs'],role='control',state='active').update(
                        control_backend_start='2026-01-01T00:00:00+00:00',control_backend_pid=123)
                (root/'submitted.json').write_text(json.dumps([dict(path='jobs',body=dict(job_id=job))]))
                (output/'before-control.json').write_text('{}')
                runtime=object.__new__(case.SharedBudgetRuntime)
                runtime.home=root;runtime.state=root;runtime.prefix=prefix;runtime.capacity_prefix=[]
                runtime.client=object();runtime.ids=dict(catalog='ac');runtime.enrolled=dict(principal_id='ab'*16)
                runtime.ch_entry=dict(id='owned-engine');runtime.custody_before=dict(original='custody')
                runtime.wait=lambda function,*args:function()
                events=[]
                def observed(*args):
                    events.append('denial' if not events else 'admission')
                    return [denied] if len(events)==1 else [denied,admitted]
                runtime.capacity_records=Mock(side_effect=observed)
                def capture(*args):
                    events.append('capture')
                    if capture_fails:raise AssertionError('original capture failure')
                    return state,{}
                runtime.capture=Mock(side_effect=capture)
                runtime.current_owner_view=lambda value,setup:value
                runtime.files=lambda path:{str(n):dict(original=n) for n in range(3)}
                runtime.custody=lambda:runtime.custody_before
                runtime.inspect=lambda entry:dict(State=dict(Running=True,Paused=True))
                runtime.sql=lambda *args:json.dumps([dict(resource_id=owner['resource_id'][2:],epoch='1',
                    incarnation=owner['process_incarnation'][2:],role_instance_id=owner['role_instance_id'][2:],
                    pid=123,started='2026-01-01T00:00:00+00:00',observed_at_us=500)])
                runtime.unpause_clickhouse=Mock(side_effect=lambda:events.append('unpause'))
                queued=Mock()
                with patch.object(case,'APPLICATION_PAYLOAD_BYTES',150), \
                        patch.dict(sys.modules,{'installed_sync':SimpleNamespace(validate_queued=queued)}), \
                        patch.object(base.Runtime,'initial_archive_observation',side_effect=lambda *args:events.append('publication')):
                    if capture_fails:
                        with self.assertRaisesRegex(AssertionError,'original capture failure'):
                            runtime.initial_archive_observation(dict(id='owner'),output)
                        self.assertEqual(events,['denial','capture'])
                        runtime.unpause_clickhouse.assert_not_called()
                        with patch.object(case.IndexRuntime,'cleanup'):
                            runtime.cleanup()
                        runtime.unpause_clickhouse.assert_called_once()
                    else:
                        runtime.initial_archive_observation(dict(id='owner'),output)
                        self.assertEqual(events,['denial','capture','unpause','admission','publication'])
                        queued.assert_called_once()
                    runtime.capture.assert_called_once()

    def test_failure_dispatch_keeps_following_stages_not_run_and_always_cleans(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);args=arguments(root);args.case='p01-first-launch-shared-budget'
            checks=[]
            with patch.object(case,'SharedBudgetRuntime') as runtime:
                runtime.return_value.initial.side_effect=AssertionError('missing actual held denial')
                base.first_launch(args,checks,{})
                runtime.return_value.cleanup.assert_called_once()
                runtime.return_value.verify_index.assert_not_called()
                runtime.return_value.ordinary.assert_not_called()
            states={r['name']:r['result'] for r in checks}
            self.assertEqual(states['first_launch_signed_acquisition'],'FAIL')
            self.assertEqual(states['first_launch_index_publication'],'NOT_RUN')
            self.assertEqual(states['first_launch_cleanup'],'PASS')

    def test_missing_candidate_never_starts_an_engine(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            result=subprocess.run([sys.executable,str(ROOT/'tests/acceptance/run.py'),'p01-first-launch-shared-budget',
                '--source-root',str(ROOT),'--candidate-manifest',str(root/'missing'),
                '--evidence',str(root/'out')],capture_output=True,text=True,timeout=5)
            report=json.loads((root/'out/result.json').read_text())
            self.assertEqual(result.returncode,2)
            self.assertEqual(report['result'],'NOT_RUN')
            self.assertFalse(list((root/'out/commands').iterdir()))


if __name__ == '__main__':
    unittest.main()
