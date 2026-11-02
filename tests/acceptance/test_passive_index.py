"""Passive Index scientific/metadata refusal controls; no product or engines."""
import copy
import hashlib
import importlib.util
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
from cases import first_launch_index as engine
from cases import first_launch_index_oracle as common
from cases import first_launch_passive_index as case
from cases import first_launch_passive_index_oracle as oracle
from cases import first_launch_passive_oracle as passive
from test_passive_monitor import sample, retained_fixture, encoder, decode
from bootstrap_oracle import cbor as bootstrap_cbor

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('passive_index_schema_test',ROOT/'experiments/clickhouse-frontier/initial_schema.py')
schema = importlib.util.module_from_spec(spec);spec.loader.exec_module(schema)


def literal_measurement_row():
    """All semantic columns, independent of the projection builder."""
    r=lambda kind,n=1:[bytes.fromhex('01'*16),kind,bytes([kind])*16,n]
    return [bytes.fromhex('66'*16),[bytes.fromhex('01'*16),0,1,10],1,r(4,3),109,
        bytes.fromhex('4522a214e60794e362dd633d59223278667ad98c90905346c889dea69694b334'),0,
        bytes([3])*16,0,r(1),r(2),r(6),r(3),r(5),
        [1,300000000,15000000,10000000,1,1,1,1,1,1,0,2097152,262144,64,32,3,
         4194304,65536,2000000,9,4096,64,0,4000000],
        [r(2,2),1,None,0],2,None,[1,0,None,101],[[102,1],None,[103,2]],
        [[104,3],[bytes.fromhex('8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849'),
            [105,4],[106,5]],[107,6],True,False,False],
        [3,3,1,480,472,160],[108,7],[0,None,None,None],1]


def source_fixture():
    document,report,refs,wire,event,measured = sample()
    state,initialized,report,refs,selection,measurement,originals,receipts = retained_fixture()
    initialized['public_scope'] = dict(refs['visibility_scope_id'],ordinal='1')
    state['owner_epochs'].append(dict(role='ingest',epoch=1,acquired_at_us=20))
    state['projection_generations']=[dict(generation_id='\\x'+'66'*16)]
    dataset=bytes.fromhex('01'*16)
    scope=passive.reference(refs['visibility_scope_id']);public=[*scope[:3],1]
    entries=[([*public[:3],n],1,'visibility',1,{'class':label}) for n,label in ((1,'public'),(2,'private'))]
    entries += [(scope,1,'visibility',1,{'class':'private'})]
    entries += [tuple(v[2]) for v in decode(originals[2].split(b'\0',1)[1])[3][2]]
    facts=[[[dataset,0,1,n],'dictionary',1,public if n<=2 else scope,20 if n<=3 else 30,list(value)]
        for n,value in enumerate(entries,1)]+[measured]
    for receipt,(first,last) in zip(receipts,oracle.RANGES):
        receipt[4]=[[fact[0],hashlib.sha256(b'bigbrotr/fact/1\0'+encoder.cbor(fact)).digest()]
            for fact in facts[first-1:last]]
    actual=dict(dictionaries=[dict(source=[dataset.hex(),0,1,n],discriminator=0,recorded_at=20 if n<=3 else 30)
        for n in range(1,10)])
    args=(state,initialized,refs,originals,receipts,document,measurement,
        b'bigbrotr/fact/1\0'+encoder.cbor(measured),actual,10,40,schema,encoder.cbor,decode)
    return args,report,selection


def accepted(state):
    settings={'indexer.enabled':True,'indexer.batch_max_rows':6,'indexer.insert_workers':1,
        'indexer.query_memory':268435456,'sync.enabled':False,'sync.provenance':'local_first_witness',
        'monitor.enabled':True,'monitor.checks':['open','read'],'monitor.store_checks':['open','read'],
        'monitor.check_timeout':15000000,'monitor.retries':1}
    document=dict(format='bigbrotr-product-policy/3',settings=settings)
    state['datasets']=[dict(dataset_id='\\x'+'01'*16,config_revision=1,policy_revision=1)]
    state['config_revisions']=[dict(revision=1,document=copy.deepcopy(document),document_digest='\\x'+'42'*32)]
    state['policy_revisions']=[dict(revision=1,config_revision=1,document=copy.deepcopy(document),document_digest='\\x'+'42'*32)]


def publication_fixture(root):
    args,_,_=source_fixture();state=args[0]
    science,rows=oracle.expected(*args)
    accepted(state)
    dataset,resource,generation,storage,incarnation=('01'*16,'55'*16,'66'*16,'88'*16,'77'*16)
    table='f043fc18a0785c737aaa2ebd0e3ba944d2feb2c6345b90eac684cdbe9777ffa0'
    identity=dict(format='bigbrotr-local-index-custody/1',root='/var/lib/bigbrotr/index',dataset=dataset,
        resource=resource,generation=generation,storage_incarnation=storage,table_identity_digest=table,provenance='local_first_witness')
    journal=root/'.bigbrotr-index-custody';journal.mkdir()
    path=journal/'identity.json';path.write_text(json.dumps(identity));path.chmod(0o600)
    custody=hashlib.sha256(path.read_bytes()).digest()
    path=journal/'tables.json';path.write_text(json.dumps(dict(format='bigbrotr-index-tables/2',
        server_version=schema.VERSION,database=engine.DATABASE,table_uuids=[r['uuid'] for r in schema.inventory()])));path.chmod(0o600)
    owner=dict(role='index',epoch=1,state='active',resource_id='\\x'+resource,dataset_id='\\x'+dataset,
        process_incarnation='\\x'+incarnation,role_instance_id='\\x'+'99'*16,index_publication_protocol=1)
    control=dict(role='control',epoch=1,resource_id='\\x'+'51'*16,process_incarnation='\\x'+incarnation)
    state['owner_epochs']=[control,state['owner_epochs'][1],dict(role='archive'),owner]
    state['role_instances']=[dict(role_instance_id=owner['role_instance_id'],role='index',dataset_id='\\x'+dataset,retired_at_us=None)]
    gen=dict(dataset_id='\\x'+dataset,owner_resource_id=owner['resource_id'],generation_id='\\x'+generation,
        storage_incarnation='\\x'+storage,table_identity_digest='\\x'+table,custody_binding_digest='\\x'+custody.hex(),
        index_publication_protocol=1,provenance='local_first_witness',owner_epoch=1,owner_role='index',owner_incarnation=owner['process_incarnation'])
    state['projection_generations']=[gen]
    state['index_owner_sessions']=[dict(dataset_id=gen['dataset_id'],resource_id=owner['resource_id'],generation_id=gen['generation_id'],
        owner_epoch=1,owner_role='index',owner_incarnation=owner['process_incarnation'],issuer_role='control',
        issuer_resource=control['resource_id'],issuer_epoch=1,issuer_incarnation=control['process_incarnation'],previous_epoch=None,
        recovery_digest=None,recovered_plan=None,recovered_handoff=None,recovered_pending_plan=None,recovered_revision=0)]
    consumer=dict(consumer_role='index',consumer_id=owner['resource_id'],dataset_id=gen['dataset_id'],
        current_revision=4,current_topology_generation=0,xmin='104')
    state['consumers'].append(consumer)
    for name in common.INDEX_TABLES:
        if name not in ('projection_generations','index_owner_sessions'):state[name]=[]
    archive_id=state['consumers'][0]['consumer_id']
    state['consumer_frontiers']=[dict(consumer_id=archive_id,frontier_revision=n,shard_id=0,owner_epoch=1,last_complete_sequence=last)
        for n,(_,last) in enumerate(oracle.RANGES,1)]
    state['consumer_frontier_revisions']=[dict(consumer_id=archive_id,consumer_role='archive',frontier_revision=n) for n in range(1,5)]
    binding=tuple(bytes.fromhex(identity[n]) for n in ('dataset','resource','generation','storage_incarnation','table_identity_digest'))+(custody,1)
    for number,(batch,(first,last)) in enumerate(zip(science['batches'],oracle.RANGES),1):
        chunks=[]
        for target in common.TARGETS:
            values=[r for r in rows[target] if first<=r[1][3]<=last]
            digest=common.digest('projection-chunk',[binding[2],target,2,[binding[0],0,[[0,1,last]]],0,
                [[r[1],[target,0],r] for r in values]])
            chunks.append((len(values),digest,digest))
        digest=common.metadata_digest(binding,number-1,(0,[(0,1,first-1)]),(0,[(0,1,last)]),
            (bytes.fromhex(batch['admission_digest']),0,1,first,last),chunks)
        plan='\\x'+digest.hex();engine_digest='\\x'+common.engine_digest(binding[4],digest,chunks).hex()
        common_row=dict(dataset_id=gen['dataset_id'],generation_id=gen['generation_id'],consumer_id=consumer['consumer_id'],
            frontier_revision=number,topology_generation=0)
        state['projection_plans'].append(dict(common_row,plan_digest=plan,expected_revision=number-1,source_shard=0,
            source_owner_epoch=1,source_first=first,source_last=last,source_batch_digest='\\x'+batch['admission_digest'],
            origin_epoch=1,origin_incarnation=owner['process_incarnation'],sealed=True))
        for target,(count,chunk,_) in zip(common.TARGETS,chunks):
            key='\\x'+chunk.hex()
            state['projection_plan_chunks'].append(dict(generation_id=gen['generation_id'],plan_digest=plan,target=target,
                chunk_id=key,partition_key='all',rows_digest=key))
            state['projection_chunks'].append(dict(generation_id=gen['generation_id'],chunk_id=key,dataset_id=gen['dataset_id'],
                storage_incarnation=gen['storage_incarnation'],table_identity_digest=gen['table_identity_digest'],target=target,
                target_schema_version=2,topology_generation=0,chunk_ordinal=number,row_count=count,owner_resource_id=owner['resource_id'],
                owner_epoch=1,owner_role='index',owner_incarnation=owner['process_incarnation'],invalid_reason=None,state='committed',
                engine_receipt_digest=engine_digest,committed_at_us=10000+number,xmin=str(100+number)))
            state['projection_chunk_bounds'].append(dict(generation_id=gen['generation_id'],chunk_id=key,
                dataset_id=gen['dataset_id'],topology_generation=0,shard_id=0,owner_epoch=1,last_complete_sequence=last))
        state['index_frontier_plans'].append(dict(common_row,plan_digest=plan,engine_receipt_digest=engine_digest,
            owner_epoch=1,owner_incarnation=owner['process_incarnation'],committed_at_us=10000+number,xmin=str(100+number)))
        state['consumer_frontier_revisions'].append(dict(consumer_id=consumer['consumer_id'],dataset_id=gen['dataset_id'],
            frontier_revision=number,consumer_role='index',topology_generation=0,owner_epoch=1,owner_resource_id=owner['resource_id'],
            owner_incarnation=owner['process_incarnation'],sealed=True,previous_revision=number-1 if number>1 else None,
            created_at_us=10000+number,xmin=str(100+number)))
        state['consumer_frontiers'].append(dict(consumer_id=consumer['consumer_id'],dataset_id=gen['dataset_id'],
            frontier_revision=number,topology_generation=0,shard_id=0,owner_epoch=1,last_complete_sequence=last,
            attestation_kind='projection-chunk',attestation_digest=plan,xmin=str(100+number)))
    actual={name:[{k:common.json_value(t,v) for (k,t),v in zip(schema.COMMON+columns,row)} for row in rows[name]]
        for name,columns,_ in schema.FAMILIES}
    return state,science,rows,actual


class PassiveIndexControls(unittest.TestCase):
    def test_recipe_and_launch_use_real_index_initializer_and_six_row_policy(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=object.__new__(case.PassiveIndexRuntime)
            runtime.root=Path(temporary);runtime.state=runtime.root/'instance'
            runtime.source=runtime.bundle=ROOT;runtime.inputs={};runtime.restart_case=False
            from installed import set_value
            runtime.set_value=set_value
            runtime.prepare_state()
            result=tomllib.loads(runtime.config.read_text())
            self.assertEqual(result['topology']['local_roles'],['control','ingest','monitor','archive','index'])
            self.assertEqual(result['indexer']['batch_max_rows'],6)
            self.assertTrue(result['indexer']['enabled']);self.assertFalse(result['sync']['enabled'])
            self.assertFalse(result['archive']['reader']['enabled'])
            original=tomllib.loads((ROOT/'config/recipes/acceptance.toml').read_text())
            self.assertEqual(result['runtime']['processes']['application'],original['runtime']['processes']['application'])
            self.assertEqual(result['runtime']['processes']['clickhouse']['memory_max_bytes'],'4GiB')
            runtime.launch=Mock(return_value='owned')
            self.assertEqual(runtime.passive_launch(),'owned')
            runtime.launch.assert_called_once_with('passive-monitor-index','index-initialize','serve','--file',
                '/etc/bigbrotr/instance.toml','--roles','control,ingest,monitor,archive,index','--initialize-ingest',
                '--initialize-index','--monitor-once','/var/lib/bigbrotr/monitor-once.json')

    def test_missing_candidate_has_exact_passive_stages_without_engine_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            result=subprocess.run([sys.executable,'-B',str(ROOT/'tests/acceptance/run.py'),'p04-passive-monitor-index',
                '--source-root',str(ROOT),'--candidate-manifest',str(root/'missing'),'--evidence',str(root/'evidence')],
                capture_output=True,text=True,timeout=5)
            report=json.loads((root/'evidence/result.json').read_text())
            self.assertEqual((result.returncode,report['result']),(2,'NOT_RUN'))
            self.assertEqual([r['name'] for r in report['checks']],['first_launch_'+s for s in case.STAGES])
            self.assertFalse(list((root/'evidence/commands').iterdir()))

    def test_failed_passive_intake_cannot_mark_index_or_inspection_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            args=SimpleNamespace(case='p04-passive-monitor-index',evidence=Path(temporary),source_root=ROOT)
            checks=[]
            with patch.object(case,'PassiveIndexRuntime') as runtime:
                runtime.return_value.passive_measurement.side_effect=ValueError('unknown outcome')
                base.first_launch(args,checks,{})
                runtime.return_value.restarted_measurement_inspection.assert_not_called()
                runtime.return_value.cleanup.assert_called_once()
            self.assertEqual([r['result'] for r in checks],['PASS']*4+['FAIL','NOT_RUN','PASS'])

    def test_literal_compound_row_preserves_nested_presence_and_all_zero_families(self):
        args,_,_=source_fixture();science,rows=oracle.expected(*args)
        self.assertEqual([len(rows[t]) for t in common.TARGETS],[9,0,0,0,0,0,0,0,0,0,1])
        row,=rows['measurement_facts']
        self.assertEqual(row,literal_measurement_row())
        self.assertEqual([(b['first'],b['last']) for b in science['batches']],[(1,2),(3,3),(4,9),(10,10)])

    def test_bootstrap_boolean_and_signed_bytes_preserve_old_unsigned_encodings(self):
        for value,wanted in ((False,'f4'),(True,'f5'),(0,'00'),(1,'01'),(23,'17'),(24,'1818'),
                (255,'18ff'),(256,'190100'),(65535,'19ffff'),(65536,'1a00010000'),
                (4294967296,'1b0000000100000000'),(18446744073709551615,'1bffffffffffffffff'),
                (-1,'20'),(-24,'37'),(-25,'3818'),(-256,'38ff'),(-257,'390100'),
                (-(2**63),'3b7fffffffffffffff')):
            with self.subTest(value=value,type=type(value)):
                self.assertEqual(bootstrap_cbor(value),bytes.fromhex(wanted))
        self.assertEqual(bootstrap_cbor([False,True,0,1]),bytes.fromhex('84f4f50001'))
        with self.assertRaises(AssertionError):bootstrap_cbor(-(2**63)-1)
        with self.assertRaises(ValueError):bootstrap_cbor(2**64)

    def test_measurement_chunk_matches_frozen_independent_node_bytes_and_digest(self):
        # Reproduce the frozen preimage with:
        # node experiments/identity-contract/reference.mjs tests/fixtures/model/passive-index-chunk.json
        fixture=json.loads((ROOT/'tests/fixtures/model/passive-index-chunk.json').read_text())
        self.assertEqual([v['name'] for v in fixture['vectors']],
            ['passive-measurement-chunk','passive-measurement-negative-utc-fact','passive-measurement-negative-utc-chunk'])
        self.assertEqual(len(fixture['expected']),3)
        for vector,result in zip(fixture['vectors'],fixture['expected']):
            value=encoder.unpack(vector['value'])
            with self.subTest(vector=vector['name']):
                self.assertEqual(vector['name'],result['name'])
                self.assertEqual(b'bigbrotr/'+vector['domain'].encode()+b'/1\0'+bootstrap_cbor(value),
                    bytes.fromhex(result['preimage_hex']))
                self.assertEqual(common.digest(vector['domain'],value).hex(),result['sha256'])
        value=encoder.unpack(fixture['vectors'][0]['value'])
        row=literal_measurement_row();source=row[1]
        self.assertEqual(value,[row[0],'measurement_facts',2,[source[0],0,[[0,1,10]]],0,
            [[source,['measurement_facts',0],row]]])
        # These complete bytes also bind the mapper used by the actual passive
        # publication oracle, rather than just a separately passing scalar test.
        args,_,_=source_fixture();_,rows=oracle.expected(*args)
        self.assertEqual(rows['measurement_facts'],[row])

        # A signed UTC fixture is also a valid complete A05 fact, not just a
        # negative integer embedded in an otherwise inconsistent row.
        document,report,refs,wire,event,_=sample()
        fact=document['fact'];fact['recorded_at']='-11'
        fact['payload_digest']=report['measurement']['payload_digest']=fixture['expected'][1]['sha256']
        observed=fact['payload']['observed']
        observed['execution']['observed_at']='-19'
        for moment in (observed['open']['tcp_connected'],observed['open']['websocket_opened'],
                observed['read']['request_flushed'],observed['read']['first_valid_event']['received'],
                observed['read']['first_valid_event']['verified'],observed['read']['eose'],fact['payload']['ended']):
            moment['observed_at']=str(int(moment['observed_at'])-120)
        measurement,encoded=passive.scientific(document,report,refs,wire,event,-20,-10,encoder.cbor)
        self.assertEqual(encoded,bytes.fromhex(fixture['expected'][1]['preimage_hex']))
        negative_args=list(args)
        negative_args[4][3][4][0][1]=bytes.fromhex(fixture['expected'][1]['sha256'])
        negative_args[5:8]=[document,measurement,encoded]
        _,negative_rows=oracle.expected(*negative_args)
        negative_chunk=encoder.unpack(fixture['vectors'][2]['value'])
        self.assertEqual(negative_rows['measurement_facts'],[negative_chunk[5][0][2]])
        self.assertEqual(negative_chunk[5][0][2][5],bytes.fromhex(fixture['expected'][1]['sha256']))

    def test_dictionary_time_and_original_input_cannot_supply_forged_science(self):
        args,_,_=source_fixture()
        for change in [lambda a:a[8]['dictionaries'][3].update(recorded_at=41),
                lambda a:a[8]['dictionaries'][3].update(recorded_at=True),
                lambda a:a[3].__setitem__(2,a[3][2]+b'\0'),
                lambda a:a[4][2][4][0].__setitem__(1,bytes(32))]:
            bad=copy.deepcopy(args[:11])+args[11:];change(bad)
            with self.assertRaises((AssertionError,ValueError,TypeError)):oracle.expected(*bad)

    def test_complete_publication_requires_every_typed_row_and_all44_outcomes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);state,science,rows,actual=publication_fixture(root)
            oracle.publication(state,root,science,rows,schema);oracle.physical(actual,rows,schema)
            for change in [lambda s:s['projection_chunks'].pop(),
                    lambda s:s['projection_chunks'][-1].update(target_schema_version=1),
                    lambda s:s['projection_chunks'][-1].update(row_count=0),
                    lambda s:s['projection_chunks'][-1].update(state='prepared'),
                    lambda s:s['projection_chunks'][-1].update(engine_receipt_digest='\\x'+'00'*32),
                    lambda s:s['projection_chunk_bounds'][-1].update(last_complete_sequence=9),
                    lambda s:s['projection_plans'][-1].update(source_last=12),
                    lambda s:s['consumer_frontiers'][-1].update(attestation_kind='diagnostic'),
                    lambda s:s['index_owner_sessions'][0].update(recovered_revision=4),
                    lambda s:s['consumers'].append(dict(consumer_role='index',consumer_id='foreign')),
                    lambda s:s['consumer_frontiers'].append(dict(consumer_id='foreign'))]:
                bad=copy.deepcopy(state);change(bad)
                with self.assertRaises((AssertionError,ValueError,KeyError)):oracle.publication(bad,root,science,rows,schema)
            for change in [lambda r:r['measurement_facts'].clear(),
                    lambda r:r['measurement_facts'].append(copy.deepcopy(r['measurement_facts'][0])),
                    lambda r:r['measurement_facts'][0].update(observed_open=[]),
                    lambda r:r['measurement_facts'][0]['observed_open'][0].__setitem__(1,[[None,None]]),
                    lambda r:r['measurement_facts'][0]['observed_read'][0].__setitem__(3,False),
                    lambda r:r['measurement_facts'][0].update(source=['01'*16,0,1,9]),
                    lambda r:r['events'].append(dict(event_id='foreign'))]:
                bad=copy.deepcopy(actual);change(bad)
                with self.assertRaises((AssertionError,ValueError)):oracle.physical(bad,rows,schema)

    def test_four_row_or_mismatched_policy_refuses_before_publication(self):
        state={};accepted(state);oracle.accepted_profile(state)
        for value in (4,True,6.0):
            bad=copy.deepcopy(state)
            for t in ('config_revisions','policy_revisions'):bad[t][0]['document']['settings']['indexer.batch_max_rows']=value
            with self.assertRaises(AssertionError):oracle.accepted_profile(bad)
        state['policy_revisions'][0]['document']['settings']['monitor.enabled']=False
        with self.assertRaises(AssertionError):oracle.accepted_profile(state)

    def test_admitted_diagnostic_requires_real_fourth_pg_outcome_before_wait(self):
        state,initialized,report,refs,selection,*_=retained_fixture();accepted(state)
        with tempfile.TemporaryDirectory() as temporary:
            runtime=object.__new__(case.PassiveIndexRuntime)
            runtime.home=Path(temporary);(runtime.home/'relay-complete.json').write_text('{"connections":1}')
            runtime.report,runtime.initialized,runtime.selection=report,initialized,selection
            runtime.decode=decode
            runtime.capture=Mock(return_value=(state,{}))
            runtime.passive_source_completed(runtime.home)
            state['producer_outcomes'].pop()
            with self.assertRaises((AssertionError,ValueError)):runtime.passive_source_completed(runtime.home)


if __name__=='__main__':unittest.main()
