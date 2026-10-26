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

    def test_changed_packaged_worker_is_refused_before_workspace_tests(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            worker = build/'archive-worker'/'bigbrotr-archive-worker'
            worker.parent.mkdir()
            worker.write_bytes(b'abd')
            (build/'build-result.json').write_text(json.dumps({
                'format': 'bigbrotr-build-result/1',
                'archive_worker': {'sha256': 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'}
            }))
            with self.assertRaisesRegex(ValueError, 'worker hash'):
                ci.workspace_test_environment(build)


class CiSelectionControls(unittest.TestCase):
    def test_dependency_policy_precedes_workspace_compilation(self):
        ci = CiControls().load()
        output = Path('/test-output')
        checks = ci.commands(output, 'Linux', 'x86_64', 2)
        names = [name for name, _ in checks]
        self.assertEqual(names[:5], ['toolchain', 'format', 'install-deny', 'dependency-policy', 'clippy'])
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
        common = ['toolchain', 'format', 'clippy', 'rust-doc', 'exact-source-build',
                  'rust-tests', 'acceptance-controls']
        for system, architecture in [('Linux', 'x86_64'), ('Linux', 'aarch64'),
                                     ('Darwin', 'arm64'), ('Darwin', 'x86_64')]:
            with self.subTest(system=system, architecture=architecture):
                names = [name for name, _ in ci.commands(Path('/test-output'), system, architecture, 2)]
                expected = common.copy()
                if (system, architecture) == ('Linux', 'x86_64'):
                    expected[2:2] = ['install-deny', 'dependency-policy']
                    expected += ['planning-artifacts', 'prepare-links', 'documentation-links']
                if system == 'Linux':
                    expected += ['p01-event-cli', 'p01-config-static']
                self.assertEqual(names, expected)
                self.assertLess(names.index('exact-source-build'), names.index('rust-tests'))

    def test_policy_failure_leaves_workspace_checks_unexecuted(self):
        ci = CiControls().load()
        for failed in ['install-deny', 'dependency-policy']:
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as directory:
                output = Path(directory)/'receipt'
                selected = ci.commands(output, 'Linux', 'x86_64', 2)
                # Keep the selected schedule; finite child commands supply an
                # independent failing installer/policy without invoking Cargo.
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
                for name in ['clippy', 'rust-doc', 'exact-source-build', 'rust-tests', 'acceptance-controls']:
                    self.assertFalse((output/(name+'.log')).exists())

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
