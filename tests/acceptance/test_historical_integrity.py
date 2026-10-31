"""Portable false-PASS controls for the current connected history oracle.

Synthetic metadata traces exercise the verifier only; they mint no product
capability and are not PostgreSQL/native qualification.
"""
import copy
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
