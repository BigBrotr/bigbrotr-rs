"""Independent process controls for shared CI execution and honest receipts."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class CiControls(unittest.TestCase):
    def load(self):
        path = ROOT / 'tools/ci.py'
        self.assertTrue(path.is_file(), 'shared CI entrypoint missing')
        spec = importlib.util.spec_from_file_location('shared_ci', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_failure_keeps_later_checks_not_run(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'receipt'
            checks = [('positive', [sys.executable, '-c', 'print(42)']),
                      ('negative', [sys.executable, '-c', 'raise SystemExit(7)']),
                      ('unreached', [sys.executable, '-c', 'raise SystemExit(0)'])]
            report = ci.execute(checks, output, ROOT, os.environ.copy(), {}, 20)
            self.assertEqual(report['status'], 'FAIL')
            self.assertEqual([r['status'] for r in report['checks']], ['PASS', 'FAIL', 'NOT_RUN'])
            self.assertEqual(report['checks'][1]['exit_code'], 7)
            self.assertEqual(report['counts'], {'collected': 3, 'executed': 2, 'failed': 1, 'not_run': 1})
            self.assertEqual(json.loads((output/'result.json').read_text()), report)
            self.assertEqual((output/'positive.log').read_text().strip(), '42')
            with self.assertRaises(FileExistsError):
                ci.execute(checks, output, ROOT, os.environ.copy(), {}, 20)

    def test_timeout_kills_owned_descendant(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'receipt'
            command = [sys.executable, '-u', '-c',
                       "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); print(p.pid); time.sleep(60)"]
            report = ci.execute([('hang', command)], output, ROOT, os.environ.copy(), {}, .5)
            self.assertEqual(report['status'], 'FAIL')
            self.assertEqual(report['checks'][0]['failure'], 'deadline')
            pid = int((output/'hang.log').read_text())
            state = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
            self.assertTrue(not state or state.startswith('Z'), state)

    def test_missing_tool_is_not_executed(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = ci.execute([('missing', [str(root/'absent')])], root/'receipt', ROOT, os.environ.copy(), {}, 20)
            self.assertEqual(report['status'], 'FAIL')
            self.assertEqual(report['checks'][0]['status'], 'NOT_RUN')
            self.assertEqual(report['counts'], {'collected': 1, 'executed': 0, 'failed': 0, 'not_run': 1})

    def test_empty_execution_cannot_pass(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                ci.execute([], Path(directory)/'receipt', ROOT, os.environ.copy(), {}, 20)


class CiSelectionControls(unittest.TestCase):
    def test_dependency_policy_precedes_workspace_compilation(self):
        ci = CiControls().load()
        output = Path('/test-output')
        checks = ci.commands(output, 'Linux', 'x86_64', 2)
        names = [name for name, _ in checks]
        self.assertEqual(names[:7], ['toolchain', 'format', 'install-deny', 'dependency-policy',
                                    'acceptance-controls', 'export-profile-controls', 'exact-source-build'])
        commands = dict(checks)
        self.assertEqual(commands['install-deny'],
                         ['cargo', 'install', 'cargo-deny', '--version', '0.20.2', '--locked',
                          '--root', str(output/'tools')])
        self.assertEqual(commands['dependency-policy'],
                         ['cargo', 'deny', '--locked', 'check', 'licenses', 'sources', 'advisories'])
        for name in ['clippy', 'rust-doc', 'exact-source-build', 'rust-tests']:
            self.assertLess(names.index('dependency-policy'), names.index(name))

    def test_policy_scope_and_packaged_worker_test_order_are_preserved(self):
        ci = CiControls().load()
        common = ['toolchain', 'format', 'acceptance-controls', 'export-profile-controls', 'exact-source-build',
                  'clippy', 'rust-doc', 'rust-tests']
        for system, architecture in [('Linux', 'x86_64'), ('Linux', 'aarch64'),
                                     ('Darwin', 'arm64'), ('Darwin', 'x86_64')]:
            with self.subTest(system=system, architecture=architecture):
                selected = ci.commands(Path('/test-output'), system, architecture, 2)
                names = [name for name, _ in selected]
                expected = common.copy()
                if (system, architecture) == ('Linux', 'x86_64'):
                    expected[2:2] = ['install-deny', 'dependency-policy']
                    expected += ['planning-artifacts', 'prepare-links', 'documentation-links']
                if system == 'Linux':
                    expected += ['p01-event-cli', 'p01-config-static']
                self.assertEqual(names, expected)
                for name in ['clippy', 'rust-doc', 'rust-tests']:
                    self.assertLess(names.index('exact-source-build'), names.index(name))
                    self.assertEqual(dict(selected)[name],
                                     [sys.executable, 'tools/ci.py', '--workspace-check', name,
                                      '--from-build', '/test-output/build'])

    def test_prebuild_failure_leaves_later_checks_unexecuted(self):
        ci = CiControls().load()
        cases = [('Linux', 'x86_64', failed)
                 for failed in ['install-deny', 'dependency-policy', 'exact-source-build']]
        cases += [(system, architecture, failed)
                  for system, architecture in [('Linux', 'x86_64'), ('Linux', 'aarch64'),
                                               ('Darwin', 'arm64'), ('Darwin', 'x86_64')]
                  for failed in ['acceptance-controls', 'export-profile-controls']]
        for system, architecture, failed in cases:
            with self.subTest(system=system, architecture=architecture, failed=failed), tempfile.TemporaryDirectory() as directory:
                output = Path(directory)/'receipt'
                selected = ci.commands(output, system, architecture, 2)
                # Keep the selected schedule; finite child commands supply an
                # independent failure without invoking Cargo or the full suite.
                checks = [(name, [sys.executable, '-c',
                                 'raise SystemExit(' + ('7' if name == failed else '0') + ')'])
                          for name, _ in selected]
                report = ci.execute(checks, output, ROOT, os.environ.copy(), {}, 20)
                position = [name for name, _ in selected].index(failed)
                self.assertEqual(report['status'], 'FAIL')
                self.assertEqual([check['status'] for check in report['checks']],
                                 ['PASS'] * position + ['FAIL'] + ['NOT_RUN'] * (len(selected)-position-1))
                self.assertEqual(report['counts'], {'collected': len(selected), 'executed': position+1,
                                                   'failed': 1, 'not_run': len(selected)-position-1})
                for index, (name, _) in enumerate(selected):
                    self.assertEqual((output/(name+'.log')).exists(), index <= position)

    def test_only_explicit_documentation_inputs_avoid_native_checks(self):
        from tools.ci_changes import documentation_only
        self.assertTrue(documentation_only(['README.md', 'docs/implementation/progress.md',
                                           'docs/research/laboratory-inventory.json']))
        for path in ['docs/reference/configuration.md', 'docs/tutorials/first-event.md',
                     'docs/research/probe.py', 'tools/ci.py', '.github/workflows/ci.yml',
                     'Cargo.lock', 'contracts/config/schema.json', 'new-owner.txt']:
            with self.subTest(path=path):
                self.assertFalse(documentation_only(['README.md', path]))
        self.assertFalse(documentation_only([]))

    def test_missing_or_unknown_comparison_runs_full_checks(self):
        from tools.ci_changes import select
        for base in ['', '0'*40, '--output=unsafe', 'a'*40]:
            with self.subTest(base=base):
                self.assertTrue(select(base, 'b'*40)['foundation'])
                self.assertFalse(select(base, 'b'*40)['controls'])

    def test_control_scope_covers_only_explicit_nonproduct_inputs(self):
        from tools.ci_changes import controls_only
        for path in ['tests/acceptance/test_ci.py', 'tests/acceptance/cases/first_launch_retained.py',
                     'experiments/identity-contract/signed_fixture.py',
                     'experiments/ingest-bootstrap/installed_archive_export_client.py',
                     'experiments/ingest-bootstrap/installed_archive_export_oracle.py',
                     'experiments/ingest-bootstrap/test_installed_archive_profile.py',
                     'docs/development/verification.md']:
            with self.subTest(path=path):
                self.assertTrue(controls_only(['docs/implementation/progress.md', path]))
        for path in ['apps/bigbrotr/src/main.rs', 'Cargo.lock', 'Cargo.toml', 'rust-toolchain.toml',
                     'tools/build.py', 'tools/ci.py', 'tools/ci_changes.py',
                     '.github/workflows/ci.yml', 'contracts/release/archive-engine.json',
                     'tests/acceptance/linux/Dockerfile', 'experiments/unknown/probe.py',
                     'docs/tutorials/first-event.md', 'contracts/config/schema.json']:
            with self.subTest(path=path):
                self.assertFalse(controls_only(['tests/acceptance/test_ci.py', path]))
        self.assertFalse(controls_only([]))

    def test_complete_git_diff_keeps_deleted_and_renamed_code(self):
        from tools.ci_changes import select
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(['git', '-C', str(root), *args],
                                               stderr=subprocess.DEVNULL, text=True).strip()
            def commit(message):
                git('add', '.')
                git('-c', 'user.name=CI fixture', '-c', 'user.email=ci@example.invalid',
                    'commit', '-qm', message)
                return git('rev-parse', 'HEAD')
            git('init', '-q')
            (root/'README.md').write_text('before\n')
            (root/'product.rs').write_text('fn main() {}\n')
            base = commit('fixture')
            (root/'README.md').write_text('after\n')
            docs = commit('docs')
            self.assertFalse(select(base, docs, root)['foundation'])
            (root/'product.rs').rename(root/'notes.md')
            renamed = commit('rename')
            result = select(docs, renamed, root)
            self.assertTrue(result['foundation'])
            self.assertIn('product.rs', result['paths'])
            self.assertIn('notes.md', result['paths'])
            self.assertTrue(select(base, renamed, root)['foundation'])
            self.assertTrue(select(renamed, renamed, root)['foundation'])

    def test_real_diff_selects_controls_and_keeps_deleted_product_native(self):
        from tools.ci_changes import select
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(['git', '-C', str(root), *args],
                                               stderr=subprocess.DEVNULL, text=True).strip()
            def commit(message):
                git('add', '.')
                git('-c', 'user.name=CI fixture', '-c', 'user.email=ci@example.invalid',
                    'commit', '-qm', message)
                return git('rev-parse', 'HEAD')
            git('init', '-q')
            (root/'README.md').write_text('before\n')
            (root/'product.rs').write_text('fn main() {}\n')
            base = commit('base')
            tests = root/'tests/acceptance'; tests.mkdir(parents=True)
            (tests/'test_example.py').write_text('# portable control\n')
            controls = commit('controls')
            result = select(base, controls, root)
            self.assertFalse(result['foundation']); self.assertTrue(result['controls'])
            (tests/'test_example.py').unlink()
            deleted = commit('delete control')
            self.assertTrue(select(controls, deleted, root)['controls'])
            (root/'product.rs').rename(tests/'test_product.py')
            mixed = commit('rename product to control')
            result = select(deleted, mixed, root)
            self.assertTrue(result['foundation']); self.assertFalse(result['controls'])

    def test_control_track_shares_checks_with_the_full_track(self):
        ci = CiControls().load()
        expected = [
            ('acceptance-controls', [sys.executable, '-m', 'unittest', 'discover',
                                     '-s', 'tests/acceptance', '-p', 'test_*.py']),
            ('export-profile-controls', [sys.executable, '-m', 'unittest', 'discover',
                                         '-s', 'experiments/ingest-bootstrap',
                                         '-p', 'test_installed_archive_profile.py']),
        ]
        self.assertEqual(ci.control_commands(), expected)
        for system, architecture in [('Linux', 'x86_64'), ('Linux', 'aarch64'), ('Darwin', 'arm64')]:
            full = dict(ci.commands(Path('/test-output'), system, architecture, 2))
            for name, command in expected:
                self.assertEqual(full[name], command)

    def test_documentation_track_shares_the_full_track_commands(self):
        ci = CiControls().load()
        output = Path('/test-output')
        full = dict(ci.commands(output, 'Linux', 'x86_64', 2))
        docs = ci.documentation_commands(output)
        self.assertEqual(len(docs), 3)
        for name, command in docs:
            self.assertEqual(command, full[name])


if __name__ == '__main__':
    unittest.main()
