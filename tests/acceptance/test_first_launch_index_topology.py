"""Literal successor-input controls; no product, Docker or engine execution."""
import copy
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from cases import first_launch as base
from cases import first_launch_index_oracle as index
from cases import first_launch_index_topology as case
from cases import first_launch_index_topology_oracle as topology
from cases import first_launch_restart as restart
from test_first_launch import arguments
from test_first_launch_index import schema, ROOT, metadata_fixture
from test_first_launch_restart import fixture, publication_fixture


def recovery_fixture():
    """Literal typed history; commitments are test bytes, not product output."""
    binding=tuple(bytes.fromhex(value*size) for value,size in
        [('11',16),('22',16),('33',16),('44',16),('55',32),('66',32)])+(1,)
    encoded=lambda value:'\\x'+value.hex()
    expected=[None,'47c2a63a5be1b4e246032c6cdb5398b2a77f49fe78f84922caf5a740d032c5bb',
              '5eb01119fa9d7e0957e4ca694df20482b123dd36ad0a626bf9efd609d42f08d7']
    owners=[dict(epoch=n,process_incarnation=encoded(bytes([n])*16),role_instance_id=str(n),
        state='released' if n<3 else 'active',exclusion_method='storage_fenced' if n<3 else None,
        exclusion_digest='\\x'+expected[n] if n<3 else None,xmin=str(n+1)) for n in range(1,4)]
    generation=dict(dataset_id=encoded(binding[0]),owner_resource_id=encoded(binding[1]),generation_id=encoded(binding[2]))
    sessions=[dict(owner_epoch=1)]
    controls=[]
    for n,head in ((2,4),(3,8)):
        controls.append(dict(role='control',resource_id='control',epoch=n,process_incarnation=owners[n-1]['process_incarnation']))
        sessions.append(dict(owner_epoch=n,previous_epoch=n-1,recovered_revision=head,
            recovered_plan=encoded(bytes([head])*32),recovered_handoff=None,recovered_pending_plan=None,
            recovery_digest='\\x'+expected[n-1],xmin=str(n),resource_id=encoded(binding[1]),
            generation_id=encoded(binding[2]),dataset_id=encoded(binding[0]),owner_role='index',
            owner_incarnation=owners[n-1]['process_incarnation'],issuer_resource='control',issuer_role='control',
            issuer_epoch=n,issuer_incarnation=owners[n-1]['process_incarnation']))
    state=dict(projection_generations=[generation],index_owner_sessions=sessions,owner_epochs=controls,
        role_instances=[dict(role_instance_id=str(n),xmin=str(n+1),retired_at_us=100+n) for n in (1,2)])
    handoff=dict(handoff_digest='\\x'+'93e6288fd8a0f5fbd46932a47483376e3bdb3952f3be25ad9764337130bf3586',
                 owner_incarnation=owners[1]['process_incarnation'],committed_at_us=123)
    commitments=[(n,bytes([n])*32,bytes([n+16])*32) for n in (1,2,3,4,6,7,8)]
    return state,binding,owners,commitments,handoff


class IndexTopologyControls(unittest.TestCase):
    def test_each_owner_keeps_a_distinct_backend_observation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=case.TopologyIndexRuntime(arguments(root),{})
            with patch.object(case.IndexRuntime,'backend_absence') as observe:
                runtime.number=2;runtime.backend_absence(root)
                runtime.number=3;runtime.backend_absence(root)
            self.assertEqual([call.args[0] for call in observe.call_args_list],
                             [root/'backend-absence-2',root/'backend-absence-3'])
            self.assertTrue((root/'backend-absence-2').is_dir())
            self.assertTrue((root/'backend-absence-3').is_dir())

    def test_epoch1_profiles_refuse_any_bridge_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);state,_,_=metadata_fixture(root)
            index.validate_metadata(state,root,complete=True)
            state['index_frontier_handoffs'].append(dict(frontier_revision=5))
            with self.assertRaises(AssertionError):index.validate_metadata(state,root,complete=True)

    def test_only_index_head_changes_and_other_consumer_history_is_exact(self):
        before=dict(consumers=[dict(consumer_id='index',consumer_role='index',created_at_us=1,current_revision=4,
            current_topology_generation=0,xmin='1'),dict(consumer_id='archive',consumer_role='archive')],
            consumer_frontier_revisions=[dict(consumer_id='archive',frontier_revision=8)],
            consumer_frontiers=[dict(consumer_id='archive',frontier_revision=8,last_complete_sequence=10)])
        after=copy.deepcopy(before);after['consumers'][0].update(current_revision=8,current_topology_generation=1,xmin='2')
        topology.preserve_consumer_identity(before,after,'index')
        for table in before:
            changed=copy.deepcopy(after);changed[table].append(dict(consumer_id='unexpected'))
            with self.subTest(extra=table),self.assertRaises(AssertionError):topology.preserve_consumer_identity(before,changed,'index')
            changed=copy.deepcopy(after);changed[table][-1]['changed']=True
            with self.subTest(mutation=table),self.assertRaises(AssertionError):topology.preserve_consumer_identity(before,changed,'index')
        changed=copy.deepcopy(after);changed['consumers'][0]['created_at_us']=2
        with self.assertRaises(AssertionError):topology.preserve_consumer_identity(before,changed,'index')

    def test_literal_handoff_commits_binding_succession_and_both_cuts(self):
        _,binding,_,_,_=recovery_fixture()
        actual=topology.handoff_digest(binding,bytes.fromhex('77'*16),bytes.fromhex('88'*32))
        self.assertEqual(actual.hex(),'93e6288fd8a0f5fbd46932a47483376e3bdb3952f3be25ad9764337130bf3586')
        for position in range(7):
            changed=list(binding);changed[position]=0 if position==6 else bytes(len(binding[position]))
            with self.subTest(binding=position):
                self.assertNotEqual(topology.handoff_digest(changed,bytes.fromhex('77'*16),bytes.fromhex('88'*32)),actual)
        self.assertNotEqual(topology.handoff_digest(binding,bytes(16),bytes.fromhex('88'*32)),actual)
        self.assertNotEqual(topology.handoff_digest(binding,bytes.fromhex('77'*16),bytes(32)),actual)

    def test_recovery_commits_bridge_before_later_batches_and_checks_atomic_fence(self):
        state,binding,owners,commitments,handoff=recovery_fixture()
        topology.recovery_sessions(state,binding,owners,commitments,handoff)
        mutations=(
            lambda s,o,c,h:s['index_owner_sessions'][2].update(recovered_handoff=h['handoff_digest']),
            lambda s,o,c,h:s['index_owner_sessions'][2].update(recovered_plan=None),
            lambda s,o,c,h:s['index_owner_sessions'][1].update(recovered_pending_plan='\\x'+'00'*32),
            lambda s,o,c,h:s['index_owner_sessions'][2].update(issuer_incarnation='\\x'+'00'*16),
            lambda s,o,c,h:s['role_instances'][1].update(xmin='changed'),
            lambda s,o,c,h:o[1].update(exclusion_method='lease_expired'),
            lambda s,o,c,h:o[1].update(exclusion_digest='\\x'+'00'*32),
            lambda s,o,c,h:c.__setitem__(4,(6,bytes(32),c[4][2])),
            lambda s,o,c,h:c.__setitem__(4,(6,c[4][1],bytes(32))),
            lambda s,o,c,h:h.update(handoff_digest='\\x'+'00'*32),
            lambda s,o,c,h:h.update(owner_incarnation='\\x'+'00'*16),
            lambda s,o,c,h:h.update(committed_at_us=124),
        )
        for number,change in enumerate(mutations):
            changed,own,chunks,bridge=copy.deepcopy((state,owners,commitments,handoff));change(changed,own,chunks,bridge)
            with self.subTest(mutation=number),self.assertRaises((AssertionError,ValueError)):
                topology.recovery_sessions(changed,binding,own,chunks,bridge)

    def test_report_distinguishes_bridge_creation_retry_and_no_readiness(self):
        generation=dict(dataset_id='\\x'+'11'*16,generation_id='\\x'+'33'*16,
            storage_incarnation='\\x'+'44'*16,table_identity_digest='\\x'+'55'*32)
        cut=dict(topology_generation=1,bounds=[dict(shard_id=0,owner_epoch=1,last_complete_sequence=12),
                                               dict(shard_id=0,owner_epoch=2,last_complete_sequence=10)])
        report=dict(format='bigbrotr-initial-index-catch-up/2',state='complete',
            **{k:v[2:] for k,v in generation.items()},previous_owner_epoch=1,owner_epoch=2,
            published_revision_before=4,published_revision_after=8,source_epoch=2,source_through=10,published_through=10,
            source_cut=cut,published_cut=cut,index_handoff_revision=5,published_handoff=True,recovered_pending=False,
            new_batches=3,ready=False,acknowledgement='not-performed',ingest_retirement='not-performed',
            topology_transition='existing-ingest-handoff-verified')
        topology.catch_up_report(report,generation,retry=False)
        retried=dict(report,previous_owner_epoch=2,owner_epoch=3,published_revision_before=8,published_handoff=False,new_batches=0)
        topology.catch_up_report(retried,generation,retry=True)
        for field,value in [('ready',True),('acknowledgement','performed'),('source_epoch',1),
                ('index_handoff_revision',None),('new_batches',True),('published_handoff',False),('recovered_pending',True)]:
            with self.subTest(field=field),self.assertRaises(AssertionError):
                topology.catch_up_report(dict(report,**{field:value}),generation,retry=False)
        with self.assertRaises(AssertionError):topology.catch_up_report(report,generation,retry=True)
        changed=copy.deepcopy(report);changed['source_cut']['bounds'][0]['last_complete_sequence']=11
        with self.assertRaises(AssertionError):topology.catch_up_report(changed,generation,retry=False)

    def test_topology_dispatch_preserves_original_failure_and_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);args=arguments(root);args.case='p01-first-launch-index-topology';checks=[]
            with patch.object(case,'TopologyIndexRuntime') as runtime:
                runtime.return_value.index_epoch2_catch_up.side_effect=AssertionError('original handoff refusal')
                runtime.return_value.cleanup.side_effect=ValueError('cleanup refusal')
                base.first_launch(args,checks,{})
                runtime.return_value.restart_retry.assert_called_once()
                runtime.return_value.index_epoch2_retry.assert_not_called()
                runtime.return_value.restart_history.assert_not_called()
            self.assertEqual(base.load(root/'failure.json')['reason'],'original handoff refusal')
            self.assertEqual([c['name'] for c in checks],['first_launch_'+n for n in case.STAGES])
            result={c['name']:c['result'] for c in checks}
            self.assertEqual(result['first_launch_index_epoch2_catch_up'],'FAIL')
            self.assertEqual(result['first_launch_index_epoch2_exact_retry'],'NOT_RUN')
            self.assertEqual(result['first_launch_successor_history'],'NOT_RUN')
            self.assertEqual(result['first_launch_cleanup'],'FAIL')

    def test_missing_candidate_never_launches_an_engine(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence=Path(temporary)/'result'
            result=subprocess.run([sys.executable,str(ROOT/'tests/acceptance/run.py'),'p01-first-launch-index-topology',
                '--source-root',str(ROOT),'--candidate-manifest',str(Path(temporary)/'missing'),
                '--evidence',str(evidence)],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,2,result.stderr)
            report=base.load(evidence/'result.json')
            self.assertEqual(report['counts'],dict(collected=15,executed=1,failed=0,skipped=0,not_run=14))
            self.assertFalse(list((evidence/'commands').iterdir()))

    def test_literal_tag_postings_keep_first_values_deduplicate_and_order_utf8(self):
        from bootstrap_oracle import cbor
        from installed_sync_oracle import decode
        science=copy.deepcopy(fixture()[3].first)
        # Synthetic mapping control, not a newly signed acceptance input.
        tags=[['t','first','ignored'],['x'],['t','first'],['p','ö'],['p','a'],
              ['1','no'],['ab','no'],['é','no'],['x',''],['p','é'],['A','z'],['t','second']]
        for batch in science['batches']:
            for fact in batch['facts']:
                value=decode(bytes.fromhex(fact['preimage_hex'])[16:])
                if value[1]=='event-payload':
                    value[5][4]=tags
                    fact['preimage_hex']=(b'bigbrotr/fact/1\0'+cbor(value)).hex()
        counts=list(index.COUNTS);counts[7]=7
        rows=index.expected_rows(science,bytes.fromhex('66'*16),schema,counts=tuple(counts))
        self.assertEqual(rows['events'][0][11],tags)
        self.assertEqual([(r[6],r[-2],r[-1]) for r in rows['event_tag_index']],
                         [(0,'A','z'),(1,'p','a'),(2,'p','é'),(3,'p','ö'),
                          (4,'t','first'),(5,'t','second'),(6,'x','')])

    def test_existing_signed_successor_adds_one_tag_and_preserves_original_body(self):
        old,new,_,_,science=fixture()
        rows=index.expected_rows(science,bytes.fromhex('66'*16),schema,counts=topology.COUNTS)
        posting,=rows['event_tag_index']
        self.assertEqual(posting[1],[bytes.fromhex('22'*16),0,2,6])
        self.assertEqual(posting[6:],[0,bytes.fromhex(new['id']),'t','allowed'])
        self.assertEqual(new['tags'],[['t','allowed']])
        self.assertEqual([(r[7].hex(),r[11],r[12]) for r in rows['events']],
                         [(old['id'],old['tags'],old['content']),(new['id'],new['tags'],new['content'])])
        self.assertEqual([r[1][2:] for r in rows['event_receive_facts']],[[1,11],[2,8]])

    def test_successor_physical_comparison_refuses_changed_or_duplicated_posting(self):
        science=fixture()[4];generation='\\x'+'66'*16
        rows=index.expected_rows(science,index.raw(generation,16),schema,counts=topology.COUNTS)
        actual={name:[{key:index.json_value(ty,value) for (key,ty),value in zip(schema.COMMON+columns,row)}
                      for row in rows[name]] for name,columns,_ in schema.FAMILIES}
        topology.physical_rows(actual,science,generation,schema)
        changed=copy.deepcopy(actual);changed['event_tag_index'][0]['tag_value']='wrong'
        with self.assertRaises(AssertionError):topology.physical_rows(changed,science,generation,schema)
        changed=copy.deepcopy(actual);changed['event_tag_index']*=2
        with self.assertRaises(AssertionError):topology.physical_rows(changed,science,generation,schema)
        changed=copy.deepcopy(actual);changed['event_tag_index']=[]
        with self.assertRaises(AssertionError):topology.physical_rows(changed,science,generation,schema)

    def test_archive_check_selects_its_consumer_and_still_rejects_its_wrong_bound(self):
        science=fixture()[4];state,census=publication_fixture(science)
        state['consumers'].append(dict(consumer_id='\\x'+'88'*16,consumer_role='index',
                                      current_revision=6,current_topology_generation=1))
        state['consumer_frontiers'].append(dict(consumer_id='\\x'+'88'*16,frontier_revision=6,
            topology_generation=1,shard_id=0,owner_epoch=2,last_complete_sequence=4))
        arguments=dict(revision=6,snapshot=8,topology=1,cuts=[(1,12),(2,10)])
        restart.publication(state,census,science,**arguments)
        state['consumer_frontiers'][0]['last_complete_sequence']=13
        with self.assertRaises(AssertionError):restart.publication(state,census,science,**arguments)


if __name__=='__main__':unittest.main()
