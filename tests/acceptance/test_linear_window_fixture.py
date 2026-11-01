"""Frozen independent inputs for window progression and native-binding refusals."""
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'tests/fixtures/acquisition/linear-window-events.json'
SHA256 = 'b6ae647fbaa641c8bb2c4ae7dbcf12987a8b3bd01cd14eecbf15476c344111cc'
spec = importlib.util.spec_from_file_location(
    'linear_window_fixture_crypto', ROOT / 'experiments/identity-contract/signed_fixture.py')
crypto = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crypto)


class LinearWindowFixture(unittest.TestCase):
    def setUp(self):
        self.data = json.loads(PATH.read_bytes())
        self.events = {row['name']: row['event'] for row in self.data['events']}

    def test_declared_input_identity_and_boundaries(self):
        self.assertEqual(hashlib.sha256(PATH.read_bytes()).hexdigest(), SHA256)
        self.assertEqual(self.data['format'], 'bigbrotr-linear-window-events/1')
        self.assertEqual([(row['name'], row['event']['created_at']) for row in self.data['events']], [
            ('same-second-alternate', 23), ('island-first', 172801), ('island-second', 172801)])
        self.assertEqual(len({event['id'] for event in self.events.values()}), 3)
        for event in self.events.values():
            self.assertEqual(event['kind'], 1)
        selector = json.loads((ROOT / 'tests/fixtures/acquisition/selector-events.json').read_bytes())
        original, = [row['event'] for row in selector['events'] if row['name'] == 'included']
        alternate = self.events['same-second-alternate']
        self.assertEqual((alternate['pubkey'], alternate['created_at'], alternate['tags']),
                         (original['pubkey'], original['created_at'], original['tags']))
        self.assertNotEqual(alternate['id'], original['id'])
        self.assertEqual(self.events['island-first']['created_at'], self.events['island-second']['created_at'])
        self.assertNotEqual(self.events['island-first']['id'], self.events['island-second']['id'])

    def test_independent_group_equation_and_corruptions(self):
        # Authoring used libsecp256k1. This maintained, independent equation
        # checker uses only the standard library in portable CI.
        for row in self.data['events']:
            event = row['event']
            with self.subTest(event=row['name']):
                canonical = crypto.canonical(event)
                message = hashlib.sha256(canonical).digest()
                public = bytes.fromhex(event['pubkey'])
                signature = bytes.fromhex(event['sig'])
                self.assertEqual(canonical.hex(), row['canonical_hex'])
                self.assertEqual(message.hex(), event['id'])
                self.assertTrue(crypto.equation_verify(message, public, signature))
                changed = hashlib.sha256(crypto.canonical(dict(event, content=event['content'] + '!'))).digest()
                self.assertFalse(crypto.equation_verify(changed, public, signature))
                self.assertFalse(crypto.equation_verify(message, public, bytes(64)))


if __name__ == '__main__':
    unittest.main()
