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
    def test_fresh_exact_three_records_and_manifest_binding(self):
        rows, _, _ = ORACLE.expected(bytes([1]) * 16, False)
        source = b'\x01\x02' + bytes([1])*16 + bytes(4) + (1).to_bytes(8, 'big')
        self.assertEqual([key for cf, key, _ in rows], [b'\x01\x01', source, b'\x01\x0b'])
        self.assertEqual(rows[1][2], b'\x01\x02' + bytes(8))
        manifest = rows[0][2]
        self.assertEqual(manifest[:10].hex(), '01010001000200010001')
        self.assertEqual(counter(rows), b'\x01\x0b' + hashlib.sha256(manifest).digest() + bytes(16))

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
