"""Current policy-runner artifact admission before any engine setup."""
import hashlib
import importlib.util
import json
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'experiments/policy-versions/run.py'
SPEC = importlib.util.spec_from_file_location('policy_schema_inventory', RUNNER.with_name('schema_inventory.py'))
SCHEMA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCHEMA)


class EngineBoundaryReached(Exception):
    pass


class PolicySchemaControls(unittest.TestCase):
    def test_current_inventory_covers_the_complete_declared_catalog(self):
        inventory = SCHEMA.declared_inventory(ROOT)
        self.assertEqual(inventory['target'], 42)
        self.assertEqual(inventory['applied_versions'][-1], 42)
        self.assertEqual(len(inventory['applied_versions']), 58)
        self.assertEqual(len(inventory['tables']), 82)
        self.assertIn('acquisition_progress', inventory['tables'])
        self.assertIn('producer_checkpoint_dependencies', inventory['tables'])
        self.assertIn('index_frontier_handoffs', inventory['tables'])
        SCHEMA.require_tables(inventory['tables'], inventory)

    def test_same_count_unexpected_relation_cannot_replace_a_declared_table(self):
        inventory = SCHEMA.declared_inventory(ROOT)
        altered = sorted(set(inventory['tables']) - {'producer_checkpoints'} | {'unexpected_lab_table'})
        self.assertEqual(len(altered), len(inventory['tables']))
        with self.assertRaisesRegex(AssertionError, 'missing=.*producer_checkpoints.*unexpected=.*unexpected_lab_table'):
            SCHEMA.require_tables(altered, inventory)

    def test_historical_target_is_refused_as_the_current_schema(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            (source / 'migrations').mkdir()
            manifest = json.loads((ROOT / 'migrations/manifest.json').read_text())
            order = manifest['application_order']
            manifest['application_order'] = order[:next(i for i, entry in enumerate(order)
                                                        if entry['version'] == 19) + 1]
            (source / 'migrations/manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(AssertionError, 'schema profile changed'):
                SCHEMA.declared_inventory(source)


class PolicyArtifactControls(unittest.TestCase):
    def run_preflight(self, tampered):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            run = home / 'new-owned-run'
            build = home / 'build'
            build.mkdir()
            original = b'independent-current-probe-fixture'
            (build / 'ingest_bootstrap_probe').write_bytes(original)
            (build / 'build-result.json').write_text(json.dumps(dict(
                source='a' * 40, probe_sha256=hashlib.sha256(original).hexdigest())))
            if tampered:
                (build / 'ingest_bootstrap_probe').write_bytes(original + b' changed after receipt')
            lab = SimpleNamespace(
                command=Mock(side_effect=lambda args: SimpleNamespace(
                    stdout='a' * 40 + '\n' if args[-1] == 'HEAD' else '')),
                setup=Mock(side_effect=EngineBoundaryReached),
                docker=Mock(side_effect=AssertionError('engine call before preflight')),
            )
            modules = dict(lab=lab, oracle=SimpleNamespace(expected=Mock()))
            expected = AssertionError if tampered else EngineBoundaryReached
            with patch.object(sys, 'argv', [str(RUNNER), str(run), str(build)]), \
                    patch.object(sys, 'path', list(sys.path)), patch.dict(sys.modules, modules):
                if tampered:
                    with self.assertRaisesRegex(expected, 'current ingest probe differs'):
                        runpy.run_path(str(RUNNER), run_name='__main__')
                else:
                    with self.assertRaises(expected):
                        runpy.run_path(str(RUNNER), run_name='__main__')
            self.assertEqual(lab.setup.call_count, 0 if tampered else 1)
            lab.docker.assert_not_called()
            self.assertFalse(run.exists(), 'preflight must not create a run or engine state')

    def test_changed_probe_is_refused_before_engine_setup(self):
        self.run_preflight(True)

    def test_matching_current_probe_reaches_the_setup_boundary(self):
        self.run_preflight(False)
