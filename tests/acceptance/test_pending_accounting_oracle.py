"""Independent current raw-accounting vectors; no native/runtime qualification."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ORACLE = load('pending_bytes_oracle', 'experiments/ingest-bootstrap/oracle.py')
with patch.dict(sys.modules, dict(oracle=ORACLE, bootstrap_oracle=ORACLE)):
    EVENTS = load('pending_events_oracle', 'experiments/ingest-bootstrap/event_oracle.py')
    RELAY = load('pending_relay_oracle', 'experiments/relay-source/oracle.py')
    OBSERVATIONS = load('pending_observation_oracle', 'experiments/acquisition-scope/observations.py')


def counter(rows):
    value, = [value for cf, key, value in rows if cf == 'default' and key == b'\x01\x0b']
    return value


class PendingAccountingVectors(unittest.TestCase):
    def test_installed_current_vectors_cover_preparation_ack_off_hole_and_retained_inputs(self):
        # Import only the maintained portable fixture modules, never product codecs.
        import test_first_launch_restart
        from installed_sync_oracle import expected, decode
        from installed_retained_transport import corpus
        dataset=bytes.fromhex('22'*16);domains={k:bytes([100+k])*16 for k in range(1,10)}
        state=dict(datasets=[dict(dataset_id='\\x'+dataset.hex(),policy_revision=1)],
            owner_epochs=[dict(role='ingest',resource_id='\\x'+'aa'*16,
                process_incarnation='\\x'+'bb'*16,acquired_at_us=1000)],
            allocation_domains=[dict(dictionary_kind=k,allocation_domain='\\x'+v.hex()) for k,v in domains.items()])
        public=dict(dataset_id=dataset.hex(),dictionary_kind=4,allocation_domain=domains[4].hex(),ordinal='1')
        report=dict(owner_resource_id='aa'*16,producer_slot_id='33'*16,recovery_generation='44'*16,
            topology_generation='0',public_scope=public,private_scope=dict(public,ordinal='3'),dictionary_count=3)
        events=corpus(ROOT/'tests/fixtures/acquisition/retained-requests.json')
        self.assertEqual(events[0],json.loads((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_text()))
        # Literal hashlib/BE64 vectors from this frozen input vocabulary. These
        # hashes cover every original frame, including its explicit length.
        vectors=[
            (True,False,0,False,False,2228,7,36,'e28da2d49177ebbe5f00e36709de9015fb0ac5e4c5bf0406514d5c7f7f87a9df'),
            (False,False,0,False,False,1225,4,24,'41840612e833a761809d81e8bf3a36c1478b3a1fdc9007e9d18482c806ef9ef4'),
            (True,True,0,False,False,3842,12,50,'d525b9a7b01e62ff6cf11f238553caf16aa5d106cd80120d4ffd7f595669a06f'),
            (False,True,0,False,False,2449,8,36,'7f5c203cbf31cd3d6e676407de348ffcb7b45c21a52c759a99c164b662953272'),
            (True,True,2,False,True,7094,22,76,'9f88521c0a724b8949e11eec7b1104ddaf50db7348a830714d6f814f863ece1a'),
            (True,True,3,True,True,8724,27,89,'8dac522fac2bd2a571714400057a30237e956e50f621aa2d51bda4e6fe02b41f'),
            (True,True,60,False,True,101824,312,830,'4d287423705db65e73016793b531efd5cc064c12f71fd449a3f6a45eb5107cd4')]
        for provenance,received,count,hole,ack,pending,source,size,input_hash in vectors:
            extras=[]
            for n,event in enumerate(events[1:count+1],1):
                body=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],event['kind'],event['tags'],event['content']]
                extras.append((event,3000+n*100,4000+n*100,1,
                    hashlib.sha256(b'bigbrotr/body/1\0'+ORACLE.cbor(body)).hexdigest()))
            with self.subTest(provenance=provenance,received=received,count=count,hole=hole,ack=ack):
                rows,inputs,receipts=expected(state,report,events[0],2000,3000 if received else None,
                    4000 if received else None,provenance=provenance,additional_events=extras,
                    preparation_hole=hole,acknowledge=ack,retained_boundary=count==60)
                self.assertEqual(counter(rows),bytes.fromhex('010b3cfb57fcc843137cd18039ca041e38c8b993dba9ab366e9670541bcbb2d1ad32')
                    +pending.to_bytes(8,'big')+source.to_bytes(8,'big'))
                self.assertEqual(len(rows),size)
                self.assertEqual(sum(len(key)+len(value) for cf,key,value in rows if cf=='facts'),pending)
                self.assertEqual(hashlib.sha256(b''.join(len(raw).to_bytes(8,'big')+raw for raw in inputs)).hexdigest(),input_hash)
                for raw in inputs+receipts:
                    encoded=raw.split(b'\0',1)[1]
                    self.assertEqual(ORACLE.cbor(decode(encoded)),encoded)
        before,inputs,receipts=expected(state,report,events[0],2000,3000,4000)
        acked,acked_inputs,acked_receipts=expected(state,report,events[0],2000,3000,4000,acknowledge=True)
        self.assertEqual((counter(before),inputs,receipts),(counter(acked),acked_inputs,acked_receipts))
        #12 facts: key38 + value prefix/digest34 + literal preimage; four80B markers.
        lengths=[155,156,156,230,183,221,383,180,222,244,210,318]
        self.assertEqual([len(v)-34 for cf,k,v in before if cf=='facts' and k[:2]==b'\x01\x20'],lengths)
        self.assertEqual(sum(lengths)+12*72+4*80,3842)

    def test_fresh_exact_three_records_and_manifest_binding(self):
        rows, _, _ = ORACLE.expected(bytes([1]) * 16, False)
        source = b'\x01\x02' + bytes([1])*16 + bytes(4) + (1).to_bytes(8, 'big')
        self.assertEqual([key for cf, key, _ in rows], [b'\x01\x01', source, b'\x01\x0b'])
        self.assertEqual(rows[1][2], b'\x01\x02' + bytes(8))
        manifest = rows[0][2]
        self.assertEqual(manifest[:10].hex(), '01010001000200010001')
        self.assertEqual(counter(rows), b'\x01\x0b' + hashlib.sha256(manifest).digest() + bytes(16))

    def test_current_handoff_has_five_exact_puts_and_refuses_invalid_prior_counter(self):
        from test_first_launch_restart import fixture
        from cases.first_launch_restart import closure
        from initial_succession_oracle import successor_rows
        old,_,manifest,encoded,_=fixture()
        first=encoded.first;slot=bytes.fromhex(encoded.initial_report['producer_slot_id'])
        previous={(cf,key):value for cf,key,value in first['native']}
        initial=previous['default',b'\x01\x01'];marker=('receipts',b'\x01\x31'+slot+(1).to_bytes(8,'big'))
        previous[marker]=b'\x02\x31'+previous[marker][2:]+b'\x01'
        closed=bytes.fromhex(closure(initial,'\\x'+slot.hex(),1,first['inputs'],first['receipts'],12))
        handoff=(b'\x01\x0a'+slot+(1).to_bytes(8,'big')+(4).to_bytes(8,'big')+(12).to_bytes(8,'big')
            +closed+len(initial).to_bytes(2,'big')+initial+len(manifest).to_bytes(2,'big')+manifest)
        result=successor_rows(previous,manifest,slot,handoff)
        changed={key for key,value in result.items() if previous.get(key)!=value}
        self.assertEqual(changed,{('default',b'\x01\x01'),('default',b'\x01\x0b'),
            ('default',b'\x01\x0a'+initial[10:26]+initial[34:50]+(1).to_bytes(8,'big')),
            ('default',b'\x01\x02'+initial[10:26]+bytes(4)+(2).to_bytes(8,'big')),
            ('receipts',b'\x01\x31'+slot+(2).to_bytes(8,'big'))})
        self.assertEqual(result['default',b'\x01\x0b'][34:42],previous['default',b'\x01\x0b'][34:42])
        self.assertEqual(result['default',b'\x01\x0b'][42:],bytes(8))
        self.assertEqual({k:v for k,v in previous.items() if k[0]=='facts'},
            {k:v for k,v in result.items() if k[0]=='facts'})
        for offset in (None,2,34,42):
            bad=dict(previous)
            if offset is None:del bad['default',b'\x01\x0b']
            else:
                value=bytearray(bad['default',b'\x01\x0b']);value[offset]^=1
                bad['default',b'\x01\x0b']=bytes(value)
            with self.subTest(counter_offset=offset),self.assertRaises(AssertionError):
                successor_rows(bad,manifest,slot,handoff)

    def test_bootstrap_charges_whole_facts_and_one_marker(self):
        rows, _, _ = ORACLE.expected(bytes([1]) * 16, True)
        self.assertEqual(len(rows), 13)
        self.assertEqual(int.from_bytes(counter(rows)[34:42], 'big'), 533)
        self.assertEqual(int.from_bytes(counter(rows)[42:50], 'big'), 2)
        self.assertEqual(sum(len(key) + len(value) for cf, key, value in rows if cf == 'facts'), 533)

    def test_signed_import_corpus_preserves_exact_charge_across_new_event(self):
        before, _, _ = EVENTS.expected_event(bytes([1]) * 16, False)
        after, _, _ = EVENTS.expected_event(bytes([1]) * 16, True)
        self.assertEqual(int.from_bytes(counter(before)[34:42], 'big'), 862)
        self.assertEqual(int.from_bytes(counter(after)[34:42], 'big'), 1839)
        self.assertEqual(int.from_bytes(counter(after)[42:50], 'big'), 6)

    def test_receipt_and_index_copies_are_not_a_second_pending_charge(self):
        rows, _, _ = ORACLE.expected(bytes([1]) * 16, True)
        scientific = [(cf, key, value) for cf, key, value in rows
                      if cf == 'facts' or key in (b'\x01\x01', b'\x01\x0b') or key[:2] == b'\x01\x02']
        self.assertEqual(counter(ORACLE.accounted(scientific)), counter(rows))

    def test_relay_receive_and_dictionary_observation_vectors_include_accounting(self):
        for committed, wanted, source in [(False, 848, 3), (True, 2072, 7)]:
            rows, _ = RELAY.expected_source(bytes([1])*16, committed)
            self.assertEqual(int.from_bytes(counter(rows)[34:42], 'big'), wanted)
            self.assertEqual(int.from_bytes(counter(rows)[42:50], 'big'), source)
        for witness, private, wanted, source in [(False, False, 848, 3),
                                                (True, False, 1838, 6), (True, True, 1838, 6)]:
            rows, _, _ = OBSERVATIONS.expected(bytes([1])*16, witness, private, ORACLE.cbor)
            self.assertEqual(int.from_bytes(counter(rows)[34:42], 'big'), wanted)
            self.assertEqual(int.from_bytes(counter(rows)[42:50], 'big'), source)

    def test_handoff_counter_preserves_predecessor_charge_and_rebinds_source_zero(self):
        rows, _, _ = ORACLE.expected(bytes([1]) * 16, True)
        old = counter(rows)
        manifest = bytearray(rows[0][2])
        manifest[2:4] = (2).to_bytes(2, 'big')
        manifest[54:62] = (2).to_bytes(8, 'big')
        rows = [(cf, key, bytes(manifest) if key == b'\x01\x01' else value) for cf, key, value in rows]
        rows.append(('default', b'\x01\x02' + manifest[10:26] + manifest[50:62], b'\x01\x02' + bytes(8)))
        current = counter(ORACLE.accounted(rows))
        self.assertEqual(current[34:42], old[34:42])
        self.assertEqual(current[42:50], bytes(8))
        self.assertEqual(current[2:34], hashlib.sha256(manifest).digest())
        self.assertNotEqual(current[2:34], old[2:34])

    def test_current_crash_vector_retains_the_frozen_counterless_original(self):
        current = json.loads((ROOT / 'tests/fixtures/ingest/staged-bootstrap-crash-accounted.json').read_text())
        historical = json.loads((ROOT / 'tests/fixtures/ingest/staged-bootstrap-crash.json').read_text())
        self.assertEqual(current['manifest_value_schema'], 2)
        self.assertEqual(len(current['before']), 3)
        self.assertEqual(len(historical['before']), 1)
        self.assertEqual(current['request_hex'], historical['request_hex'])
        old_science = [row for row in historical['after'] if row[0] == 'facts']
        self.assertEqual([row for row in current['after'] if row[0] == 'facts'], old_science)
