"""Portable false-PASS controls for the current connected history oracle.

Synthetic metadata traces exercise the verifier only; they mint no product
capability and are not PostgreSQL/native qualification.
"""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ORACLE = load('historical_fixed_oracle', 'experiments/ingest-bootstrap/oracle.py')
with patch.dict(sys.modules, dict(lab=SimpleNamespace(), oracle=ORACLE)):
    HISTORY = load('historical_integrity_oracle', 'experiments/ingest-bootstrap/historical.py')


class HistoricalScienceControls(unittest.TestCase):
    def test_declared_initial_database_labels_are_normalized_before_sql(self):
        for label in ('history-original','history-edited','history-mismatch'):
            with self.subTest(label=label):
                self.assertEqual(HISTORY.current_database_name('bb_policy_','hi_'+label),
                                 'bb_policy_hi_'+label.replace('-','_'))
        self.assertEqual(HISTORY.current_database_name('bb_policy_','v3_managed'),'bb_policy_v3_managed')
        for name in HISTORY.current_case_names():
            self.assertRegex(HISTORY.current_database_name('bb_hi_',name),r'^[a-z_][a-z0-9_]{0,62}$')

    def test_unsafe_or_oversized_database_names_refuse_before_sql(self):
        for prefix,label in [('bb_policy_','Upper'),('bb_policy_',"x'; DROP DATABASE postgres;--"),
                             ('bb_policy_','with space'),('bb_policy_','é'),('bb_policy_','x'*64),
                             ('',''),('1','invalid')]:
            with self.subTest(label=label),self.assertRaises(AssertionError):
                HISTORY.current_database_name(prefix,label)

    def test_actual_seed_starts_with_the_qualified_independent_bootstrap_oracle(self):
        rows,receipt,_ = ORACLE.expected(bytes([1])*16,True)
        expected = [dict(cf=cf,key=key.hex(),value=value.hex()) for cf,key,value in rows if cf=='facts']
        with patch.dict(sys.modules, dict(oracle=ORACLE)):
            current = HISTORY.current_science(bytes([1])*16)
            inputs = HISTORY.current_originals(bytes([1])*16)
        self.assertEqual([row for row in current if row['key'] in {r['key'] for r in expected}],expected)
        original = inputs['42'*16+'-0000000000000001-0000000000000001.cbor']
        self.assertIn(bytes.fromhex(original['sha256']),receipt)
        self.assertEqual(len(current),5)  # three facts and exactly two markers
        self.assertEqual(len(inputs),2)

    def test_successor_science_requires_the_declared_coordinates(self):
        target = dict(reference=dict(dataset_id='01'*16,dictionary_kind=4,
            allocation_domain='22'*16,ordinal='1'))
        target.update({'class':'history-successor','recorded_at':789})
        with patch.dict(sys.modules, dict(oracle=ORACLE)):
            base = HISTORY.current_science(bytes([1])*16)
            current = HISTORY.current_science(bytes([1])*16,target=target)
            self.assertEqual([r for r in current if r in base],base)
            self.assertEqual(len(current),7)
            for key,value in [('dataset_id','02'*16),('dictionary_kind',3),('ordinal','2')]:
                changed=copy.deepcopy(target)
                changed['reference'][key]=value
                with self.subTest(key=key),self.assertRaises(AssertionError):
                    HISTORY.current_science(bytes([1])*16,target=changed)

    def test_declared_current_batch_has_distinct_bounded_database_names(self):
        names=HISTORY.current_case_names()
        self.assertEqual(len(names),38)
        self.assertEqual(len(set(names)),len(names))
        for phase in ('activation','ack'):
            self.assertIn('private-'+phase+'-positive-distinct-original-pair',names)
            self.assertIn('private-'+phase+'-pair-mismatch-recomputed-digest',names)
        self.assertIn('current-history-swapped-original-digest',names)
        for name in names:
            self.assertLessEqual(len('bb_hi_'+name.replace('-','_')),63)


class HistoricalRevisionControls(unittest.TestCase):
    def setUp(self):
        document=json.loads((ROOT/'tests/fixtures/configuration/policy-v3.json').read_text())
        document['settings']['ingest.producer_outstanding_requests']=2
        self.before=dict(datasets=[dict(config_revision=1,policy_revision=1)],
            config_revisions=[dict(revision=1,previous_revision=None,document=copy.deepcopy(document))],
            policy_revisions=[dict(revision=1,previous_revision=None,config_revision=1,document=copy.deepcopy(document))])

    def check_chain(self,before,planned):
        rows={name:copy.deepcopy(before[name]) for name in ('config_revisions','policy_revisions')}
        for table,_,patch in planned: rows[table].append(patch)
        for table,values in rows.items():
            for row in values:
                self.assertEqual(row['previous_revision'],None if row['revision']==1 else row['revision']-1)
                if row['revision']>1:
                    self.assertIn(row['previous_revision'],[r['revision'] for r in values])
                if table=='policy_revisions':
                    self.assertIn(row['config_revision'],[r['revision'] for r in rows['config_revisions']])
        for _,_,patch in planned:
            canonical=json.dumps(patch['document'],sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
            self.assertEqual(patch['document_digest'],'\\x'+hashlib.sha256(b'bigbrotr/product-policy/3\0'+canonical).hexdigest())
        return rows

    def test_unequal_heads_retain_complete_intermediate_policy_chains(self):
        original=copy.deepcopy(self.before)
        planned=HISTORY.current_pair_rows(self.before)
        self.assertEqual([(t,p['revision'],p['previous_revision'],p.get('config_revision')) for t,_,p in planned],
            [('config_revisions',2,1,None),('policy_revisions',2,1,1),('policy_revisions',3,2,1),
             ('policy_revisions',4,3,1),('policy_revisions',5,4,2)])
        rows=self.check_chain(self.before,planned)
        self.assertEqual(self.before,original)
        for row in rows['policy_revisions'][1:4]:
            self.assertEqual(row['document'],original['policy_revisions'][0]['document'])
        before=dict(rows,datasets=[dict(config_revision=2,policy_revision=5)])
        next_rows=HISTORY.current_pair_rows(before,3,8,retention=60,pending_limit=2*1024**3)
        self.assertEqual([(p['revision'],p['previous_revision'],p['config_revision'])
                          for t,_,p in next_rows if t=='policy_revisions'],[(6,5,2),(7,6,2),(8,7,3)])
        self.check_chain(before,next_rows)

    def test_mismatched_intake_pair_still_has_a_valid_adjacent_policy_revision(self):
        planned=HISTORY.current_pair_rows(self.before,policy_revision=2,mismatched=True)
        self.assertEqual([(t,p['revision'],p['previous_revision'],p['config_revision']) for t,_,p in planned],
                         [('policy_revisions',2,1,1)])
        self.check_chain(self.before,planned)
        self.assertNotEqual(planned[0][2]['document']['settings']['ingest.request_receipt_retention'],
                            self.before['config_revisions'][0]['document']['settings']['ingest.request_receipt_retention'])


def rotation(stage):
    epoch,old_role,old_inc,next_role,next_inc=(1,31,41,33,43) if stage=='seed' else (3,211,221,212,222)
    ident=lambda n:'\\x'+bytes([n]*16).hex()
    original=dict(dataset_id=ident(1),resource_id=ident(90),epoch=epoch,role='control',
        role_instance_id=ident(old_role),process_incarnation=ident(old_inc),acquired_at_us=100,
        lease_expires_at_us=60_000_100,state='active',released_at_us=None,
        exclusion_method=None,exclusion_digest=None,control_fence_version=1,
        control_backend_pid=101,control_backend_start='original-start',xmin='5')
    role=dict(role_instance_id=ident(old_role),dataset_id=ident(1),role='control',
              created_at_us=90,retired_at_us=None,xmin='4')
    tables=dict(owner_epochs=[original],role_instances=[role],source_streams=[dict(state='closed')],
        producer_slots=[dict(registered_by_resource_id=ident(90),registered_by_epoch=epoch,
                             registered_by_incarnation=ident(old_inc))],initial_ingest_activations=[])
    if stage=='ack': tables['initial_ingest_activations']=[dict(activated_by_resource=ident(90),
        activated_by_epoch=epoch,activated_by_incarnation=ident(old_inc))]
    digest=hashlib.sha256(b'bigbrotr/control-exclusion/1\0'+bytes([1]*16)+bytes([90]*16)
        +epoch.to_bytes(8,'big')+bytes([old_inc]*32)).hexdigest()
    released=copy.deepcopy(tables)
    released['owner_epochs'][0].update(state='released',released_at_us=150,exclusion_method='storage_fenced',
                                       exclusion_digest='\\x'+digest,xmin='6')
    released['role_instances'][0].update(retired_at_us=155,xmin='6')
    current=copy.deepcopy(released)
    current['owner_epochs'].append(dict(original,epoch=epoch+1,role_instance_id=ident(next_role),
        process_incarnation=ident(next_inc),acquired_at_us=200,lease_expires_at_us=60_000_200,
        control_backend_pid=102,control_backend_start='fresh-start',xmin='8'))
    current['role_instances'].append(dict(role,role_instance_id=ident(next_role),created_at_us=190,xmin='8'))
    def snapshot(suffix,rows):
        return dict(event='history-integrity-state',stage=stage+'-'+suffix,rows=[],
            state=dict(control_tables=[(k,json.dumps(v)) for k,v in rows.items()],
                       native_rows=0,native_sha256='unchanged',sequence=17,originals=[]))
    events=[snapshot('before-control-release',tables),
        dict(event='history-integrity-control',stage=stage,resource=ident(90)[2:],epoch=epoch,
             role=ident(old_role)[2:],incarnation=ident(old_inc)[2:],released=True,driver_joined=True,backend_absent=True),
        snapshot('after-control-release',released),snapshot('after-control-acquire',current)]
    return events,next_role,next_inc


class HistoricalLifecycleControls(unittest.TestCase):
    def test_both_rotations_preserve_science_and_the_actual_original_attribution(self):
        for stage in ('seed','ack'):
            events,role,incarnation=rotation(stage)
            HISTORY.verify_control_rotation(events,stage,role,incarnation)

    def test_missing_join_false_fence_stale_owner_and_unrelated_effects_refuse(self):
        for stage in ('seed','ack'):
            events,role,incarnation=rotation(stage)
            mutations=[]
            joined=copy.deepcopy(events);joined[1]['driver_joined']=False;mutations.append(joined)
            backend=copy.deepcopy(events);backend[1]['backend_absent']=False;mutations.append(backend)
            for field,value in [('state','active'),('exclusion_digest','\\x'+'00'*32)]:
                changed=copy.deepcopy(events)
                rows=dict(changed[2]['state']['control_tables'])
                owners=json.loads(rows['owner_epochs']);owners[0][field]=value
                changed[2]['state']['control_tables']=[(k,json.dumps(owners) if k=='owner_epochs' else v) for k,v in rows.items()]
                mutations.append(changed)
            unrelated=copy.deepcopy(events);unrelated[3]['state']['control_tables'].append(('extra','[]'));mutations.append(unrelated)
            native=copy.deepcopy(events);native[3]['state']['sequence']+=1;mutations.append(native)
            stale=copy.deepcopy(events)
            rows=dict(stale[3]['state']['control_tables'])
            owners=json.loads(rows['owner_epochs']);owners[-1]['epoch']=owners[0]['epoch']
            stale[3]['state']['control_tables']=[(k,json.dumps(owners) if k=='owner_epochs' else v) for k,v in rows.items()]
            mutations.append(stale)
            for changed in mutations:
                with self.subTest(stage=stage,events=changed),self.assertRaises(AssertionError):
                    HISTORY.verify_control_rotation(changed,stage,role,incarnation)

    def test_all_nine_namespaces_add_no_science_and_omission_cannot_pass(self):
        dataset=bytes([1]*16)
        domains=[(n,bytes([60+n]*16)) for n in range(1,10)]
        full,_,_=ORACLE.expected(dataset,True,namespaces=domains)
        old,_,_=ORACLE.expected(dataset,True)
        self.assertEqual([r for r in full if r[0]=='facts'],[r for r in old if r[0]=='facts'])
        native=dict(rows=[dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in full])
        tables=dict(allocation_domains=[dict(dictionary_kind=n,allocation_domain='\\x'+d.hex(),owner_epoch=1,
            dataset_id='\\x'+dataset.hex(),owner_resource_id='\\x'+bytes([61]*16).hex()) for n,d in domains])
        HISTORY.verify_current_domains(native,tables)
        missing=copy.deepcopy(tables);del missing['allocation_domains'][0]
        with self.assertRaises(AssertionError): HISTORY.verify_current_domains(native,missing)
        wrong=copy.deepcopy(tables);wrong['allocation_domains'][0]['allocation_domain']='\\x'+'ff'*16
        with self.assertRaises(AssertionError): HISTORY.verify_current_domains(native,wrong)


def trace(phase='activation',fault=None):
    config=lambda revision,digest:dict(revision=revision,document_digest='\\x'+digest*64)
    rows=dict(datasets=[dict(config_revision=3,policy_revision=8)],
        config_revisions=[config(2,'a'),config(3,'b')],
        policy_revisions=[dict(config(5,'c'),config_revision=2),dict(config(8,'d'),config_revision=3)])
    before=dict(event='history-integrity-state',stage='before-action',
        state=dict(control_tables=[(k,json.dumps(v)) for k,v in rows.items()]))
    entry=lambda digest,valid=True:dict(event='history-integrity-policy',digest=digest*64,valid=valid)
    events=[before,entry('b'),entry('d'),entry('a',fault!='config-body-stale-digest')]
    if fault!='config-body-stale-digest':
        events.append(entry('c',fault not in ('policy-body-stale-digest','missing-unrelated-recomputed-digest')))
    if phase=='activation' and fault is None: events += [entry('b'),entry('d')]
    events.append(dict(event='history-integrity-state',stage='after-action'))
    return events,before


class HistoricalTraceControls(unittest.TestCase):
    def test_own_original_and_current_digest_pairs_are_distinct(self):
        for phase in ('activation','ack'):
            for fault in HISTORY.CURRENT_PRIVATE_FAULTS:
                events,before=trace(phase,fault)
                HISTORY.verify_policy_trace(events,before,phase,fault,original_policy=5)

    def test_missing_restore_swapped_digest_and_wrong_result_cannot_pass(self):
        events,before=trace()
        mutations=[]
        missing=copy.deepcopy(events); del missing[3:5]; mutations.append(missing)
        swapped=copy.deepcopy(events); swapped[3]['digest']=swapped[4]['digest']; mutations.append(swapped)
        stale=copy.deepcopy(events); stale[4]['valid']=False; mutations.append(stale)
        missing_delivery=copy.deepcopy(events); del missing_delivery[-2]; mutations.append(missing_delivery)
        for changed in mutations:
            with self.subTest(trace=changed),self.assertRaises(AssertionError):
                HISTORY.verify_policy_trace(changed,before,'activation',None,original_policy=5)

    def test_invalid_original_cannot_validate_or_deliver_current_authority(self):
        events,before=trace(fault='policy-body-stale-digest')
        changed=copy.deepcopy(events)
        changed[-2]['valid']=True
        with self.assertRaises(AssertionError):
            HISTORY.verify_policy_trace(changed,before,'activation','policy-body-stale-digest',original_policy=5)
        extra=copy.deepcopy(events)
        extra.insert(-1,dict(event='history-integrity-policy',digest='b'*64,valid=True))
        with self.assertRaises(AssertionError):
            HISTORY.verify_policy_trace(extra,before,'activation','policy-body-stale-digest',original_policy=5)


class HistoricalActivationControls(unittest.TestCase):
    def test_activation_must_copy_the_whole_retained_intent(self):
        intent=dict(dataset_id='01'*16,owner_resource_id='3d'*16,target_epoch=2,target_incarnation='a1'*16,
            target_role_instance_id='a2'*16,producer_slot_id='42'*16,producer_epoch=2,
            previous_manifest_sha256='11'*32,target_manifest_sha256='22'*32,handoff_sha256='33'*32,
            prepared_by_resource='aa'*16,prepared_by_epoch=2,prepared_by_incarnation='bb'*16,
            config_revision=3,policy_revision=8,config_digest='44'*32,policy_digest='55'*32)
        active=dict(intent,activated_by_resource=intent['prepared_by_resource'],activated_by_epoch=2,
            activated_by_incarnation=intent['prepared_by_incarnation'],activated_at_us=15,
            activation_transaction='9',xmin='9')
        rows=dict(initial_ingest_activations=[],initial_ingest_successions=[dict(intent,created_at_us=10,lease_expires_at_us=20)],
            owner_epochs=[dict(resource_id=intent['prepared_by_resource'],epoch=2,lease_expires_at_us=25)])
        state=dict(native_rows=7,native_sha256='66'*32,sequence=17,originals=[])
        before=dict(state=dict(state,control_tables=[(k,json.dumps(v)) for k,v in rows.items()]))
        def after(value):
            return dict(state=dict(state,control_tables=[(k,json.dumps([value] if k=='initial_ingest_activations' else v)) for k,v in rows.items()]))
        HISTORY.verify_activation(before,after(active))
        for key,value in [('config_revision',2),('handoff_sha256','00'*32),('activated_by_epoch',1),('activated_at_us',20)]:
            with self.subTest(key=key),self.assertRaises(AssertionError):
                HISTORY.verify_activation(before,after(dict(active,**{key:value})))
