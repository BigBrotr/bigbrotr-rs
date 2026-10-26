"""Independent literal/mutation controls; no product, Docker or engine run."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cases import first_launch as base
from cases import first_launch_index as case
from cases import first_launch_index_oracle as check
from test_first_launch_restart import fixture
from installed import set_value

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('index_schema_control', ROOT/'experiments/clickhouse-frontier/initial_schema.py')
schema = importlib.util.module_from_spec(spec); spec.loader.exec_module(schema)


def metadata_fixture(root):
    """Synthetic complete SQL shape for refusal controls, not a product PASS.

    The scientific source is the existing literal fixture. Two separate fixed
    metadata vectors below check the encoder, rather than deriving their hashes
    from this fixture's encoder calls.
    """
    science = fixture()[3].first
    prefix = '\\x'
    ds, resource, generation, incarnation, storage = ('22'*16,'55'*16,'66'*16,'77'*16,'88'*16)
    table = 'ad1b0478a2f12cac320ffc2b92723cd4cd1f019d2661fddfa2deb1a7af813106'
    identity = dict(format='bigbrotr-local-index-custody/1',root='/var/lib/bigbrotr/index',
        dataset=ds,resource=resource,generation=generation,storage_incarnation=storage,
        table_identity_digest=table,provenance='local_first_witness')
    journal = root/'.bigbrotr-index-custody'; journal.mkdir()
    path = journal/'identity.json'; path.write_text(json.dumps(identity)); path.chmod(0o600)
    custody = hashlib.sha256(path.read_bytes()).digest()
    path = journal/'tables.json'; path.write_text(json.dumps(dict(format='bigbrotr-index-tables/1',
        server_version=schema.VERSION,database=case.DATABASE,table_uuids=[r['uuid'] for r in schema.inventory()]))); path.chmod(0o600)
    own = dict(resource_id=prefix+resource,epoch=1,dataset_id=prefix+ds,role='index',role_instance_id=prefix+'99'*16,
               process_incarnation=prefix+incarnation,index_publication_protocol=1)
    control = dict(role='control',resource_id=prefix+'aa'*16,epoch=9,process_incarnation=prefix+incarnation)
    gen = dict(dataset_id=prefix+ds,owner_resource_id=prefix+resource,generation_id=prefix+generation,
        storage_incarnation=prefix+storage,table_identity_digest=prefix+table,custody_binding_digest=prefix+custody.hex(),
        index_publication_protocol=1,provenance='local_first_witness',owner_epoch=1,owner_incarnation=prefix+incarnation,owner_role='index')
    session = dict(dataset_id=prefix+ds,resource_id=prefix+resource,generation_id=prefix+generation,owner_epoch=1,
        owner_role='index',owner_incarnation=prefix+incarnation,issuer_role='control',issuer_resource=control['resource_id'],
        issuer_epoch=9,issuer_incarnation=prefix+incarnation,previous_epoch=None,recovery_digest=None,
        recovered_plan=None,recovered_pending_plan=None,recovered_revision=0)
    state = {name:[] for name in (*check.INDEX_TABLES,'consumers','consumer_frontiers','consumer_frontier_revisions')}
    state.update(datasets=[dict(dataset_id=prefix+ds)],owner_epochs=[control,own],
        role_instances=[dict(role_instance_id=own['role_instance_id'],role='index',dataset_id=prefix+ds,retired_at_us=None)],
        projection_generations=[gen],index_owner_sessions=[session],
        consumers=[dict(consumer_id=prefix+resource,consumer_role='index',dataset_id=prefix+ds,
            current_revision=4,current_topology_generation=0,xmin='104')])
    binding = tuple(bytes.fromhex(n) for n in (ds,resource,generation,storage,table))+(custody,1)
    values = check.expected_rows(science,bytes.fromhex(generation),schema)
    actual = {name:[{key:check.json_value(ty,value) for (key,ty),value in zip(schema.COMMON+cols,row)}
                   for row in values[name]] for name,cols,_ in schema.FAMILIES}
    for n,batch in enumerate(science['batches'],1):
        first,last=batch['first'],batch['last']; chunks=[]
        cut=[bytes.fromhex(ds),0,[[0,1,last]]]
        for target in check.TARGETS:
            rows=[r for r in values[target] if first<=r[1][3]<=last]
            rows_digest=check.digest('projection-chunk',[bytes.fromhex(generation),target,1,cut,0,[[r[1],[target,0],r] for r in rows]])
            chunks.append((len(rows),rows_digest,rows_digest))
        digest=check.metadata_digest(binding,n-1,(0,[(0,1,first-1)]),(0,[(0,1,last)]),
            (bytes.fromhex(batch['admission_digest']),0,1,first,last),chunks)
        engine=check.engine_digest(binding[4],digest,chunks); at=10000+n
        plan=dict(generation_id=prefix+generation,plan_digest=prefix+digest.hex(),dataset_id=prefix+ds,
            consumer_id=prefix+resource,frontier_revision=n,expected_revision=n-1,topology_generation=0,source_shard=0,
            source_owner_epoch=1,source_first=first,source_last=last,source_batch_digest=prefix+batch['admission_digest'],
            origin_epoch=1,origin_incarnation=prefix+incarnation,sealed=True)
        state['projection_plans'].append(plan)
        for target,(count,chunk,rows_digest) in zip(check.TARGETS,chunks):
            state['projection_plan_chunks'].append(dict(generation_id=prefix+generation,plan_digest=plan['plan_digest'],
                target=target,chunk_id=prefix+chunk.hex(),partition_key='all',rows_digest=prefix+rows_digest.hex()))
            state['projection_chunks'].append(dict(generation_id=prefix+generation,chunk_id=prefix+chunk.hex(),
                dataset_id=prefix+ds,storage_incarnation=prefix+storage,table_identity_digest=prefix+table,target=target,
                target_schema_version=1,topology_generation=0,chunk_ordinal=n,row_count=count,owner_resource_id=prefix+resource,
                owner_epoch=1,owner_role='index',owner_incarnation=prefix+incarnation,invalid_reason=None,state='committed',
                committed_at_us=at,engine_receipt_digest=prefix+engine.hex(),xmin=str(100+n)))
            state['projection_chunk_bounds'].append(dict(generation_id=prefix+generation,chunk_id=prefix+chunk.hex(),
                dataset_id=prefix+ds,topology_generation=0,shard_id=0,owner_epoch=1,last_complete_sequence=last))
        common=dict(consumer_id=prefix+resource,frontier_revision=n,dataset_id=prefix+ds,topology_generation=0,xmin=str(100+n))
        state['index_frontier_plans'].append(dict(common,generation_id=prefix+generation,plan_digest=plan['plan_digest'],
            owner_epoch=1,owner_incarnation=prefix+incarnation,engine_receipt_digest=prefix+engine.hex(),committed_at_us=at))
        state['consumer_frontier_revisions'].append(dict(common,consumer_role='index',previous_revision=n-1 if n>1 else None,
            owner_resource_id=prefix+resource,owner_epoch=1,owner_incarnation=prefix+incarnation,sealed=True,created_at_us=at))
        state['consumer_frontiers'].append(dict(common,shard_id=0,owner_epoch=1,last_complete_sequence=last,
            attestation_kind='projection-chunk',attestation_digest=plan['plan_digest']))
    return state,actual,science


class InitialIndexControls(unittest.TestCase):
    def test_live_roles_require_literal_cut_freshness_identity_and_all_capabilities(self):
        state = dict(
            datasets=[dict(dataset_id='\\x'+'22'*16,config_revision=1,policy_revision=1)],
            owner_epochs=[dict(role='control',state='active',process_incarnation='\\x'+'77'*16)],
            projection_generations=[dict(generation_id='\\x'+'66'*16,storage_incarnation='\\x'+'88'*16)])
        cut = dict(dataset_id='22'*16,topology_generation='0',
            bounds=[dict(shard_id='0',owner_epoch='1',last_complete_sequence='12')])
        clock = dict(observed_at_us='123456789',age_us='1000')
        value = dict(format='bigbrotr-role-status/1',authority='observation-only',observed_at_us='123456789',
            identity=dict(dataset_id='22'*16,process_incarnation='77'*16,config_revision='1',policy_revision='1'),
            source=dict(cut=cut,freshness='fresh',time=clock,reason='current'),
            roles=[dict(role=role,state='ready' if role=='control' else 'idle',reason='current')
                for role in ('control','ingest','sync','archive','index')],
            capabilities=[dict(capability=kind,state='ready',reason='current',freshness='fresh',time=clock,
                published=dict(revision='4',cut=cut) if kind in ('archive_publication','index_publication') else None,
                generation='66'*16 if kind=='index_publication' else None,
                storage_incarnation='88'*16 if kind=='index_publication' else None)
                for kind in ('signed_event_admission','finite_acquisition','archive_publication','index_publication')])
        check.live_roles(value,state,123456000,123457000)
        for change in (
            lambda v:v['identity'].update(process_incarnation='99'*16),
            lambda v:v['source'].update(cut=None),
            lambda v:v['source']['cut']['bounds'][0].update(last_complete_sequence='11'),
            lambda v:v['source']['cut']['bounds'][0].update(last_complete_sequence=12),
            lambda v:v['source']['time'].update(age_us='30000000'),
            lambda v:v['capabilities'][-1].update(state='building'),
            lambda v:v['capabilities'][-1].update(storage_incarnation='99'*16),
            lambda v:v['capabilities'][-1].update(freshness='stale'),
            lambda v:v['capabilities'][-1]['published'].update(revision='3'),
            lambda v:v['capabilities'].pop(),
            lambda v:v['roles'].pop(),
            lambda v:v.update(pending_bytes='0'),
        ):
            changed=copy.deepcopy(value); change(changed)
            with self.subTest(change=change),self.assertRaises(AssertionError):
                check.live_roles(changed,state,123456000,123457000)
        for timestamp in ('0','123455999','123457001'):
            changed=copy.deepcopy(value)
            changed['observed_at_us']=timestamp
            changed['source']['time']['observed_at_us']=timestamp
            for cap in changed['capabilities']:
                cap['time']['observed_at_us']=timestamp
            with self.subTest(timestamp=timestamp),self.assertRaises(AssertionError):
                check.live_roles(changed,state,123456000,123457000)

        invalidated = copy.deepcopy(value)
        invalidated['roles'][-1].update(state='degraded',reason='dependency_unavailable')
        invalidated['capabilities'][-1].update(state='unavailable',reason='dependency_unavailable')
        check.live_roles(invalidated,state,123456000,123457000,invalidated=True)
        with self.assertRaises(AssertionError):
            check.live_roles(invalidated,state,123456000,123457000)
        for change in (
            lambda v:v['capabilities'][-1].update(state='ready'),
            lambda v:v['capabilities'][-1].update(state='building',reason='verification_in_flight'),
            lambda v:v['capabilities'][-1].update(state='stale',reason='verification_stale'),
            lambda v:v['capabilities'][-1].update(reason='remote_outcome_unknown'),
            lambda v:v['capabilities'][-1].update(published=None),
            lambda v:v['capabilities'][-1].update(time=None),
            lambda v:v['capabilities'][-1].update(generation='99'*16),
            lambda v:v['capabilities'][2].update(state='unavailable'),
            lambda v:v['roles'][0].update(state='degraded'),
            lambda v:v['roles'][-1].update(state='idle',reason='current'),
            lambda v:v['source']['cut']['bounds'][0].update(last_complete_sequence='13'),
            lambda v:v['capabilities'][-1]['time'].update(observed_at_us='123456788'),
        ):
            changed=copy.deepcopy(invalidated); change(changed)
            with self.subTest(negative_change=change),self.assertRaises((AssertionError,TypeError)):
                check.live_roles(changed,state,123456000,123457000,invalidated=True)
        # The failed physical check retains the last cut even when its age
        # crosses30s. Staleness by itself never passes this negative oracle.
        invalidated['capabilities'][-1].update(freshness='stale',
            time=dict(clock,age_us='30000000'))
        check.live_roles(invalidated,state,123456000,123457000,invalidated=True)

    def test_physical_content_conflict_is_exact_and_preserves_the_original_oracle(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); state,rows,science=metadata_fixture(root)
            check.publication(state,state,rows,science,schema,root)
            corrupted=copy.deepcopy(rows)
            corrupted['events'].append(dict(corrupted['events'][0],content=check.CONFLICTING_CONTENT))
            check.content_conflict(rows,corrupted)
            corrupted['events'].reverse()  # Equal ORDER BY keys have no tie order.
            check.content_conflict(rows,corrupted)
            with self.assertRaises(AssertionError):
                check.publication(state,state,corrupted,science,schema,root)
            for change in (
                lambda r:r.update(events=copy.deepcopy(rows['events'])),
                lambda r:r.update(events=copy.deepcopy(rows['events'])*2),
                lambda r:r['events'].pop(),
                lambda r:r['events'][0].update(content='different corruption'),
                lambda r:r['events'][0].update(discriminator=1),
                lambda r:r['dictionaries'].pop(),
                lambda r:r.update(event_tag_index=[dict(unexpected=True)]),
            ):
                changed=copy.deepcopy(corrupted); change(changed)
                with self.subTest(change=change),self.assertRaises(AssertionError):
                    check.content_conflict(rows,changed)
            check.publication(state,state,rows,science,schema,root)
            state.update(config_revisions=[dict(revision=1)],policy_revisions=[dict(revision=1)],
                browser_sessions=[dict(id='fixture',idle_expires_at_us=300)])
            check.unchanged_publication(state,copy.deepcopy(state))
            for table,field,value in [('consumers','current_revision',5),
                    ('projection_chunks','rows_digest','forged'),('datasets','config_revision',2),
                    ('browser_sessions','idle_expires_at_us',301)]:
                changed=copy.deepcopy(state); changed[table][0][field]=value
                with self.subTest(table=table),self.assertRaises(AssertionError):
                    check.unchanged_publication(state,changed)
        self.assertEqual(case.content_conflict_sql(schema),
            'INSERT INTO serving_fixture.events (`projection_generation`,`source`,`record_schema_version`,'
            '`visibility_scope`,`recorded_at`,`payload_digest`,`discriminator`,`event_id`,`pubkey`,'
            '`created_at`,`kind`,`tags`,`content`,`event_ordinal`,`body_digest`) SELECT '
            '`projection_generation`,`source`,`record_schema_version`,`visibility_scope`,`recorded_at`,'
            '`payload_digest`,`discriminator`,`event_id`,`pubkey`,`created_at`,`kind`,`tags`,'
            "'independent physical readiness corruption',`event_ordinal`,`body_digest` "
            'FROM serving_fixture.events LIMIT 1')

    def test_all_zero_chunk_digests_match_existing_independent_row_vectors(self):
        fixture()  # Load the maintained independent canonical encoder.
        golden=json.loads((ROOT/'apps/bigbrotr/src/storage/clickhouse/rows/fixtures/golden.json').read_text())
        cut=[bytes([0x11])*16,17,[[7,2,100],[7,3,5]]]
        for target in check.TARGETS:
            with self.subTest(target=target):
                self.assertEqual(check.digest('projection-chunk',[bytes([0x33])*16,target,1,cut,0,[]]).hex(),
                    golden['targets'][target]['empty_digest'])

    def test_metadata_encoding_matches_both_existing_independent_literal_vectors(self):
        chunks=[(int(i==0),bytes([30+i])*32,bytes([50+i])*32) for i in range(10)]
        for mode,wanted in [(1,'aa7f2eef19d03e4e3704714e90e400c10b75bddce94485682b45452adac61e98'),
                            (0,'4321a18b1a112a16a2d899e6da618dd6abfc7c67bfab89c2aecfcb02df094900')]:
            binding=tuple(bytes([i])*16 for i in range(1,5))+(bytes([5])*32,bytes([6])*32,mode)
            actual=check.metadata_digest(binding,0,(0,[(0,1,0),(7,1,0)]),(0,[(0,1,0),(7,1,2)]),
                (bytes.fromhex('2f9a7d60f00046f394e093e0b697dd064b7629e21f77486193e07236dec92e74'),7,1,1,2),chunks)
            self.assertEqual(actual.hex(),wanted)

    def test_frozen_signed_fields_and_explicit_zero_family(self):
        science=fixture()[3].first
        rows=check.expected_rows(science,bytes([0x66])*16,schema)
        self.assertEqual([len(rows[n]) for n in check.TARGETS],[8,1,1,1,1,1,1,0,1,1])
        self.assertEqual(max(sum(batch['first'] <= row[1][3] <= batch['last'] for row in rows[target])
            for batch in science['batches'] for target in check.TARGETS),4)
        self.assertEqual(check.BATCH_MAX_ROWS,4)
        self.assertEqual(rows['events'][0][7].hex(),'8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849')
        self.assertEqual(rows['events'][0][9:13],[1,1,[],'import then network'])
        self.assertEqual(rows['events'][0][-1].hex(),'1e05f420946c84c3ee8b178daf05dd564c9904c7ed18f7ab99874aec878337fb')
        self.assertEqual(rows['event_eligibility_facts'][0][10:],[4000,'network',None])
        self.assertEqual(rows['event_receive_facts'][0][8],3000)
        self.assertEqual(rows['dictionaries'][0][13],'public')

    def test_changed_fields_missing_zero_outcomes_and_unrelated_proofs_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); state,rows,science=metadata_fixture(root)
            self.assertEqual(check.publication(state,state,rows,science,schema,root)['committed_chunks'],40)
            for change in [lambda s:s['projection_plan_chunks'].pop(),lambda s:s['projection_chunks'][0].update(row_count=True),
                    lambda s:s['projection_plans'][0].update(source_first=2),lambda s:s['index_owner_sessions'][0].update(issuer_incarnation='\\x'+'00'*16),
                    lambda s:s['index_frontier_plans'][0].update(engine_receipt_digest='\\x'+'00'*32),
                    lambda s:s['consumer_frontiers'][-1].update(last_complete_sequence=13),
                    lambda s:s['projection_chunks'][0].update(xmin='999')]:
                changed=copy.deepcopy(state); change(changed)
                with self.subTest(change=change),self.assertRaises(AssertionError): check.publication(changed,changed,rows,science,schema,root)
            for target,column,value in [('events','content','forged'),('event_eligibility_facts','sig','00'*64),
                    ('event_receive_facts','received_at',3001),('dictionaries','descriptor_digest','00'*32)]:
                changed=copy.deepcopy(rows); changed[target][0][column]=value
                with self.subTest(target=target),self.assertRaises(AssertionError): check.publication(state,state,changed,science,schema,root)
            changed=copy.deepcopy(rows); changed['event_tag_index']=[dict(unexpected=True)]
            with self.assertRaises(AssertionError): check.publication(state,state,changed,science,schema,root)
            changed=copy.deepcopy(state); changed['owner_epochs'].append(dict(role='search'))
            with self.assertRaises(AssertionError): check.without_index_owner(changed,root)

    def test_current_consumer_head_shares_only_its_publication_transaction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); state,_,_=metadata_fixture(root)
            self.assertEqual([r['xmin'] for r in state['index_frontier_plans']],['101','102','103','104'])
            self.assertEqual(state['consumers'][0]['xmin'],'104')
            check.validate_metadata(state,root,complete=True)
            for wrong in ('103','999'):
                changed=copy.deepcopy(state); changed['consumers'][0]['xmin']=wrong
                with self.subTest(xmin=wrong),self.assertRaises(AssertionError):
                    check.validate_metadata(changed,root,complete=True)

    def test_recipe_preserves_original_and_separates_initializer(self):
        original=(ROOT/'config/recipes/acceptance.toml').read_text()
        parsed=tomllib.loads(case.recipe(original,set_value))
        self.assertFalse(tomllib.loads(original)['indexer']['enabled'])
        self.assertEqual(parsed['topology']['local_roles'],['control','ingest','sync','archive','index'])
        self.assertEqual(parsed['indexer']['batch_max_rows'],4)
        self.assertEqual(parsed['clickhouse']['initialize_password_ref'],'file:/run/index-initialize/password')
        self.assertEqual(parsed['clickhouse']['write_password_ref'],'file:/run/secrets/index-writer')
        self.assertTrue(parsed['runtime']['processes']['clickhouse']['enabled'])
        self.assertTrue(parsed['archive']['publisher']['enabled'])

    def test_both_stored_profiles_must_accept_the_small_batch(self):
        document=dict(format='bigbrotr-product-policy/2',settings={'indexer.enabled':True,
            'indexer.batch_max_rows':4,'sync.provenance':'local_first_witness'})
        state=dict(datasets=[dict(config_revision=1,policy_revision=1)],
            config_revisions=[dict(revision=1,document=document,document_digest='recorded')],
            policy_revisions=[dict(revision=1,config_revision=1,document=copy.deepcopy(document),document_digest='recorded')])
        check.accepted_profile(state)
        for key,value in [('indexer.enabled',False),('indexer.batch_max_rows',4.0),('indexer.batch_max_rows',100)]:
            changed=copy.deepcopy(state)
            for table in ('config_revisions','policy_revisions'): changed[table][0]['document']['settings'][key]=value
            with self.subTest(key=key,value=value),self.assertRaises(AssertionError): check.accepted_profile(changed)
        state['config_revisions'][0]['document']['settings']['indexer.enabled']=False
        with self.assertRaises(AssertionError): check.accepted_profile(state)

    def test_schema_census_rejects_extra_columns_changed_types_or_foreign_uuid(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); metadata_fixture(root)
            inventory=sorted(schema.inventory(),key=lambda r:r['name'])
            tables=[dict(name=r['name'],uuid=r['uuid'],engine_full=r['engine_full']) for r in inventory]
            columns=[dict(table=r['name'],name=name,type=ty,position=i+1,default_kind='',default_expression='')
                for r in inventory for i,(name,ty) in enumerate(r['columns'])]
            self.assertEqual(len(columns),160)
            check.schema_metadata(schema,root,tables,columns)
            for change in [lambda r:r.append(copy.deepcopy(r[0])),lambda r:r[0].update(type='String'),
                    lambda r:r[0].update(default_expression='1')]:
                changed=copy.deepcopy(columns); change(changed)
                with self.subTest(change=change),self.assertRaises(AssertionError): check.schema_metadata(schema,root,tables,changed)
            tables[0]['uuid']='00000000-0000-0000-0000-0000000000ff'
            with self.assertRaises(AssertionError): check.schema_metadata(schema,root,tables,columns)

    def test_clickhouse_custody_refuses_wrong_owner_mount_image_and_limits(self):
        runtime=SimpleNamespace(owner='owner',project='project',state=Path('/owned/state'),
            inputs=dict(runtime_images=dict(clickhouse=dict(id='sha256:'+64*'a'))))
        entry=dict(name='project-clickhouse',id=64*'b')
        info=dict(Name='/project-clickhouse',Id=entry['id'],Image=runtime.inputs['runtime_images']['clickhouse']['id'],
            Config=dict(User=f'{os.geteuid()}:{os.getegid()}',Labels={base.LABEL:'owner',
                'com.docker.compose.project':'project','com.docker.compose.service':'clickhouse'}),
            Mounts=[dict(Type='bind',Source=str(runtime.state/'clickhouse'/name),Destination=destination,RW=name!='private')
                for name,destination in [('data','/var/lib/clickhouse'),('logs','/var/log/clickhouse-server'),('private','/run/ch-private')]],
            HostConfig=dict(NetworkMode='host',PortBindings={},Memory=4*1024**3,MemorySwap=4*1024**3,
                NanoCpus=2_000_000_000,PidsLimit=256,ReadonlyRootfs=True,Privileged=False,CapAdd=None,CapDrop=['ALL'],
                SecurityOpt=['no-new-privileges'],Tmpfs={'/tmp':'rw,nosuid,nodev,noexec,size=64m'},RestartPolicy=dict(Name='no')))
        case.check_clickhouse(info,entry,runtime)
        for change in [lambda r:r.update(Id=64*'c'),lambda r:r.update(Image='foreign'),
                lambda r:r['Config']['Labels'].update({base.LABEL:'foreign'}),
                lambda r:r['Mounts'][0].update(Source='/unrelated'),lambda r:r['Mounts'][-1].update(RW=True),
                lambda r:r['Mounts'].append(dict(Type='volume',Destination='/foreign',Source='/foreign')),
                lambda r:r['HostConfig'].update(Memory=8*1024**3),lambda r:r['HostConfig'].update(Privileged=True),
                lambda r:r['HostConfig'].update(SecurityOpt=[])]:
            changed=copy.deepcopy(info); change(changed)
            with self.subTest(change=change),self.assertRaises(ValueError): case.check_clickhouse(changed,entry,runtime)

    def test_index_failure_preserves_capture_stage_and_unreached_export(self):
        from test_first_launch import arguments
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); args=arguments(root); args.case='p01-first-launch-index'
            checks=[]
            with patch.object(case,'IndexRuntime') as runtime:
                runtime.return_value.verify_index.side_effect=AssertionError('foreign physical proof')
                base.first_launch(args,checks,{})
                runtime.return_value.cleanup.assert_called_once()
                runtime.return_value.ordinary.assert_not_called()
            selected={r['name']:r['result'] for r in checks}
            self.assertEqual(selected['first_launch_signed_acquisition'],'PASS')
            self.assertEqual(selected['first_launch_index_publication'],'FAIL')
            self.assertEqual(selected['first_launch_ordinary_history_export'],'NOT_RUN')
            self.assertEqual(selected['first_launch_cleanup'],'PASS')
            self.assertEqual(json.loads((root/'failure.json').read_text())['reason'],'foreign physical proof')

    def test_missing_candidate_is_not_run_and_never_starts_docker(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence=Path(temporary)/'result'
            result=subprocess.run([sys.executable,str(ROOT/'tests/acceptance/run.py'),'p01-first-launch-index',
                '--source-root',str(ROOT),'--candidate-manifest',str(Path(temporary)/'missing'),
                '--evidence',str(evidence)],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,2,result.stderr)
            report=json.loads((evidence/'result.json').read_text())
            self.assertEqual(report['result'],'NOT_RUN')
            self.assertEqual(report['counts']['collected'],8)
            self.assertFalse(list((evidence/'commands').iterdir()))

    def test_initializer_mount_is_refused_for_ordinary_app_and_cleanup_is_ordered(self):
        from test_first_launch import FirstLaunchControls,arguments
        state=Path('/owned/instance'); entry,info=FirstLaunchControls().container(state)
        info['Mounts'].append(dict(Type='bind',Destination='/run/index-initialize',Source=str(state/'secrets/index-initialize'),RW=False))
        with self.assertRaises(ValueError): base.check_container(info,entry,'owner','project',state)
        entry['service']='index-initialize'; info['Config']['Labels']['com.docker.compose.service']='index-initialize'
        base.check_container(info,entry,'owner','project',state)
        info['Mounts'][-1]['RW']=True
        with self.assertRaises(ValueError): base.check_container(info,entry,'owner','project',state)
        with tempfile.TemporaryDirectory() as temporary:
            runtime=case.IndexRuntime(arguments(Path(temporary)),{})
            runtime.resources['containers']=[dict(service=s) for s in ['database','clickhouse','app','index-initialize']]
            with patch.object(base.Runtime,'cleanup') as cleanup:
                runtime.cleanup(); cleanup.assert_called_once()
            self.assertEqual([r['service'] for r in runtime.resources['containers']][:2],['app','index-initialize'])


if __name__ == '__main__': unittest.main()
