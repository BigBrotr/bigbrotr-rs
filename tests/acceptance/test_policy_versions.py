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
        self.assertEqual(inventory['target'], 44)
        self.assertEqual(inventory['applied_versions'][-1], 44)
        self.assertEqual(len(inventory['applied_versions']), 60)
        self.assertEqual(len(inventory['inputs_sha256']), 61)
        self.assertEqual(len(inventory['tables']), 85)
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
    def run_preflight(self, tampered=False, *, product_tampered=False, changes=None, subnet=None, historical=False, quota=False):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            run = home / 'new-owned-run'
            build = home / 'build'
            build.mkdir()
            original = b'independent-current-probe-fixture'
            product = b'independent-current-product-fixture'
            (build / 'ingest_bootstrap_probe').write_bytes(original)
            (build / 'bigbrotr').write_bytes(product)
            (build / 'build-result.json').write_text(json.dumps(dict(
                source='a' * 40, probe_sha256=hashlib.sha256(original).hexdigest(),
                binary_sha256=hashlib.sha256(product).hexdigest())))
            if tampered:
                (build / 'ingest_bootstrap_probe').write_bytes(original + b' changed after receipt')
            if product_tampered:
                (build / 'bigbrotr').write_bytes(product + b' changed after receipt')
            def command(args):
                if args[-1] == 'HEAD':
                    return SimpleNamespace(stdout=('a' if changes is None else 'b') * 40 + '\n')
                return SimpleNamespace(stdout=changes if 'diff' in args else '')
            lab = SimpleNamespace(
                command=Mock(side_effect=command),
                setup=Mock(side_effect=EngineBoundaryReached),
                docker=Mock(side_effect=AssertionError('engine call before preflight')),
            )
            modules = dict(lab=lab, oracle=SimpleNamespace(expected=Mock()))
            reason = 'current ingest probe differs' if tampered else \
                'current product differs' if product_tampered else None
            permitted = {'experiments/policy-versions/run.py', 'experiments/policy-versions/README.md',
                         'tests/acceptance/test_policy_versions.py'}
            if historical:
                permitted |= {'experiments/ingest-bootstrap/historical.py',
                              'tests/acceptance/test_historical_integrity.py'}
            if quota:
                permitted |= {'experiments/policy-versions/connected_quota.py',
                              'tests/acceptance/test_connected_quota.py'}
            if changes and any(path not in permitted for path in changes.splitlines()):
                reason = 'built product source changed beyond policy harness'
            argv = [str(RUNNER), str(run), str(build)]
            if historical: argv.append('--historical-integrity')
            if quota: argv.append('--connected-quota')
            if subnet is not None:
                argv += ['--subnet', subnet]
            with patch.object(sys, 'argv', argv), \
                    patch.object(sys, 'path', list(sys.path)), patch.dict(sys.modules, modules):
                if reason:
                    with self.assertRaisesRegex(AssertionError, reason):
                        runpy.run_path(str(RUNNER), run_name='__main__')
                else:
                    with self.assertRaises(EngineBoundaryReached):
                        runpy.run_path(str(RUNNER), run_name='__main__')
            self.assertEqual(lab.setup.call_count, 0 if reason else 1)
            if not reason:
                self.assertEqual(lab.setup.call_args.kwargs['subnet'], subnet or '172.16.240.48/28')
            if changes is not None:
                diff, = [call.args[0] for call in lab.command.call_args_list if 'diff' in call.args[0]]
                self.assertEqual(diff[4:], ['--name-only','--no-renames','a'*40,'b'*40,'--'])
            lab.docker.assert_not_called()
            self.assertFalse(run.exists(), 'preflight must not create a run or engine state')

    def test_changed_probe_is_refused_before_engine_setup(self):
        self.run_preflight(True)

    def test_quota_option_preserves_matching_artifacts_and_rejects_compiled_probe_changes(self):
        self.run_preflight(quota=True)
        self.run_preflight(True,quota=True)
        self.run_preflight(quota=True,changes='apps/bigbrotr/examples/support/ingest_pending_quota.rs\n')
        self.run_preflight(quota=True,changes='experiments/policy-versions/connected_quota.py\n')

    def test_matching_current_probe_reaches_the_setup_boundary(self):
        self.run_preflight(False)

    def test_changed_product_is_refused_before_engine_setup(self):
        self.run_preflight(product_tampered=True)

    def test_harness_only_revision_can_reuse_the_qualified_product(self):
        self.run_preflight(changes='experiments/policy-versions/run.py\n')

    def test_product_build_and_unknown_changes_cannot_reuse_the_product(self):
        for path in ['apps/bigbrotr/src/main.rs', 'tools/build.py', 'Cargo.lock', 'unknown.py']:
            with self.subTest(path=path):
                self.run_preflight(changes=path + '\n')

    def test_selected_subnet_reaches_the_unchanged_isolation_guard(self):
        self.run_preflight(subnet='10.253.240.48/28')

    def test_historical_option_preserves_tampered_probe_refusal_before_engines(self):
        self.run_preflight(True,historical=True)

    def test_historical_option_preserves_matching_probe_admission(self):
        self.run_preflight(False,historical=True)

    def test_historical_option_checks_the_parent_hash_before_engines(self):
        self.run_preflight(product_tampered=True,historical=True)

    def test_historical_fix_reuses_only_its_exact_noncompiled_paths(self):
        self.run_preflight(historical=True,changes='\n'.join([
            'experiments/ingest-bootstrap/historical.py',
            'experiments/policy-versions/run.py',
            'experiments/policy-versions/README.md',
            'tests/acceptance/test_historical_integrity.py',
            'tests/acceptance/test_policy_versions.py'])+'\n')

    def test_policy_codec_reuse_does_not_gain_the_historical_allowance(self):
        for path in ['experiments/ingest-bootstrap/historical.py',
                     'tests/acceptance/test_historical_integrity.py']:
            with self.subTest(path=path):
                self.run_preflight(changes=path+'\n')

    def test_historical_reuse_refuses_compiled_probe_schema_fixture_and_unknown_changes(self):
        for path in ['apps/bigbrotr/examples/ingest_bootstrap_probe.rs',
                     'apps/bigbrotr/examples/support/ingest_history_integrity.rs',
                     'migrations/postgres/0001_control.sql',
                     'tests/fixtures/configuration/policy-v3.json',
                     'experiments/policy-versions/schema_inventory.py',
                     'Cargo.lock','tools/build.py','unknown.py']:
            with self.subTest(path=path):
                self.run_preflight(historical=True,changes=path+'\n')

    def test_renamed_compiled_input_cannot_hide_in_an_allowed_path(self):
        self.run_preflight(historical=True,changes='apps/bigbrotr/src/main.rs\nexperiments/ingest-bootstrap/historical.py\n')
