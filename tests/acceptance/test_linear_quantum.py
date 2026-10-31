"""Independent quantum evidence/refusal controls; no product or engine runs."""
import copy
import hashlib
import json
import unittest
from cases import first_launch_quantum_oracle as check
from installed_sync_client import quantum_status


def fixture(*,pending=False):
    bx=lambda digit:'\\x'+digit*32
    predicate=b'["REQ","000000000000000000000000000000000000000000000000",{"kinds":[1],"since":0,"until":86399,"limit":3}]'
    ident='ab'*24;frame=json.dumps(['EOSE',ident],separators=(',',':')).encode()
    job=dict(dataset_id=bx('1'),job_id=bx('2'),created_at_us=200_000_999_999,
        owner_resource_id=bx('3'),owner_epoch=1,owner_incarnation=bx('4'))
    row=dict(quantum_id=bx('5'),quantum_profile=1,quantum_number=1,dataset_id=bx('1'),job_id=bx('2'),scope_id=bx('6'),
        producer_slot_id=bx('7'),producer_epoch=1,issuer_role='control',issuer_resource=bx('3'),
        issuer_epoch=1,issuer_incarnation=bx('4'),interval_lower=0,interval_upper=86400,horizon=200000,
        request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+hashlib.sha256(predicate).hexdigest(),
        maximum_dependencies=192,checkpoint_before=3,terminal_kind=None if pending else 'eose',
        completed_at_us=None if pending else 200_002_000_000,dependencies=None if pending else '\\x',
        effect_records=None if pending else '\\x',terminal_frame=None if pending else '\\x'+frame.hex(),
        request_id=None if pending else '\\x'+ident.encode().hex(),terminal_sequence=None if pending else 1,
        retained_events=None if pending else 0,classified_prefix=None if pending else 0,
        checkpoint_after=None if pending else 3,continuation_upper=None if pending else 86400,
        result_class=None if pending else 'linear_observed',gap_reason=None,xmin='11')
    row['charged_bytes']=(1024+len(predicate)+2097152+3072+192*256 if pending else 1024+len(predicate)+len(frame))
    state=dict(jobs=[job],acquisition_jobs=[dict(job_id=job['job_id'],scope_id=row['scope_id'])],
        acquisition_quanta=[row],producer_requests=[],producer_checkpoints=[])
    wire=dict(number=1,request=['REQ',ident,dict(kinds=[1],since=0,until=86399,limit=3)],frames=['EOSE'])
    return state,wire


class LinearQuantumControls(unittest.TestCase):
    def test_empty_terminal_has_charged_metadata_and_no_fabricated_effect_or_dependency(self):
        state,wire=fixture();check.shape(state);check.journey(state,[wire],1,{})
        self.assertEqual(state['acquisition_quanta'][0]['charged_bytes'],1189)
        self.assertEqual(state['producer_requests'],[])
        for field,value in [('charged_bytes',1188),('dependencies','\\x'+'00'*72),
                ('retained_events',1),('predicate_sha256','\\x'+'00'*32),('continuation_upper',0)]:
            changed=copy.deepcopy(state);changed['acquisition_quanta'][0][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):check.journey(changed,[wire],1,{})

    def test_pending_reservation_survives_observation_and_cannot_be_refunded_or_relabelled(self):
        state,_=fixture(pending=True);check.shape(state)
        self.assertEqual(state['acquisition_quanta'][0]['charged_bytes'],2150506)
        for field,value in [('charged_bytes',1024),('horizon',200001),('issuer_epoch',2),('quantum_number',2)]:
            changed=copy.deepcopy(state);changed['acquisition_quanta'][0][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):check.shape(changed)
        terminal,_=fixture();check.transition(state,terminal)
        changed=copy.deepcopy(terminal);changed['acquisition_quanta'][0]['scope_id']='\\x'+'88'*16
        with self.assertRaises(AssertionError):check.transition(state,changed)
        changed=copy.deepcopy(terminal);changed['acquisition_quanta'][0]['xmin']='12'
        with self.assertRaises(AssertionError):check.transition(terminal,changed)

    def test_wholly_preexisting_dependency_union_is_exact_and_deduplicated(self):
        ds=bytes.fromhex('11'*16)
        first=[[ds,0,1,7],bytes.fromhex('aa'*32)]
        second=[[ds,0,1,8],bytes.fromhex('bb'*32)]
        expected=bytes.fromhex('11'*16+'0000000000000000'+'0000000000000001'+'0000000000000007'+'aa'*32+
            '11'*16+'0000000000000000'+'0000000000000001'+'0000000000000008'+'bb'*32)
        self.assertEqual(check.reference_bytes([second,first,first]),expected)
        self.assertNotEqual(check.reference_bytes([second]),expected)
        self.assertNotEqual(check.reference_bytes([]),expected)

    def test_commit_proxy_targets_are_exact_and_do_not_match_reads_or_other_work(self):
        import importlib.util
        from pathlib import Path
        path=Path(__file__).resolve().parents[2]/'experiments/archive-engine/native_commit_proxy.py'
        spec=importlib.util.spec_from_file_location('linear_commit_proxy_control',path)
        proxy=importlib.util.module_from_spec(spec);spec.loader.exec_module(proxy)
        for target,query in [('control-quantum-plan',b'INSERT INTO control.acquisition_quanta(quantum_id,quantum_number,dataset_id)'),
                ('control-quantum-terminal',b'UPDATE control.acquisition_quanta SET terminal_kind=$2,terminal_frame=$3')]:
            self.assertTrue(proxy.control_target_query(target,query))
            for other in (b'SELECT * FROM control.acquisition_quanta',query.replace(b'acquisition_quanta',b'acquisition_progress'),
                    b'UPDATE control.jobs SET state=paused',b'INSERT INTO control.producer_requests('):
                self.assertFalse(proxy.control_target_query(target,other))

    def test_status_preserves_pending_and_rejects_false_coverage_or_hidden_proof_fields(self):
        status=dict(format='bigbrotr-linear-quantum-status/1',quantum_number='1',scope_id='66'*16,
            lower='0',upper='86400',horizon='200000',result='pending',terminal=None,gap=None,
            continuation_upper=None,retained_events=None,classified_prefix=None,coverage='pending')
        observed=dict(linear_quantum=status);quantum_status(observed,'66'*16,'200000999999');self.assertEqual(observed,{})
        for field,value in [('coverage','complete'),('result','linear_observed'),('horizon','200001'),('dependencies',[])]:
            changed=dict(status,**{field:value})
            with self.subTest(field=field),self.assertRaises((AssertionError,TypeError)):
                quantum_status(dict(linear_quantum=changed),'66'*16,'200000999999')
