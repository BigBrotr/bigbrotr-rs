"""Actual finite child controls for CI reuse; no product or native build fixture."""
from contextlib import ExitStack, contextmanager, redirect_stdout
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from tools import archive_bundle, ci

ROOT = Path(__file__).resolve().parents[2]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


@contextmanager
def fixture():
    """Real files/Git/child, synthetic identity bytes; compiler discovery only is stubbed."""
    with tempfile.TemporaryDirectory() as temporary, ExitStack() as patches:
        root = Path(temporary).resolve(strict=True)
        checkout, build = root/'checkout', root/'build'
        checkout.mkdir(); build.mkdir()
        target = ci.native_target()
        assets = {'library': {'name': 'library', 'bytes': 3, 'sha256': sha(b'lib')},
                  'header': {'name': 'header', 'bytes': 3, 'sha256': sha(b'hdr')}, 'extensions': []}
        for name in ['tools/ci.py', 'tools/build.py', 'tools/archive_bundle.py', '.cargo/config.toml']:
            path = checkout/name; path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT/name, path)
        for name in ['Cargo.lock', 'rust-toolchain.toml', 'contracts/release/dependencies.json',
                     'contracts/protocol/versions.json', 'contracts/protocol/nip01-event.json', 'product.rs']:
            path = checkout/name; path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('independent fixture bytes for ' + name)
        registry = 'contracts/release/archive-engine.json'
        (checkout/registry).write_text(json.dumps({'platforms': {target: assets}}))
        def git(*args):
            return subprocess.check_output(['git', '-C', str(checkout), *args], text=True,
                                           stderr=subprocess.DEVNULL).strip()
        git('init', '-q'); git('add', '.')
        git('-c', 'user.name=CI fixture', '-c', 'user.email=ci@example.invalid', 'commit', '-qm', 'fixture')
        git('archive', '--format=tar', '--output', str(build/'source.tar'), 'HEAD')
        source = build/'source'; source.mkdir()
        with tarfile.open(build/'source.tar') as archive:
            archive.extractall(source)
        files = {str(p.relative_to(source)): sha(p.read_bytes()) for p in source.rglob('*') if p.is_file()}
        manifest = {'format': 'bigbrotr-source/1', 'revision': git('rev-parse', 'HEAD'),
                    'tree': git('rev-parse', 'HEAD^{tree}'), 'archive_sha256': sha((build/'source.tar').read_bytes()),
                    'files_sha256': files}
        (build/'source-manifest.json').write_text(json.dumps(manifest))
        manifest_hash = sha((build/'source-manifest.json').read_bytes())
        for name in ['target/release', 'cargo-home', 'archive-worker/lib', 'archive-worker/extensions']:
            (build/name).mkdir(parents=True, exist_ok=True)
        (build/'archive-worker/lib/library').write_bytes(b'lib')
        (build/'archive-worker/lib/header').write_bytes(b'hdr')
        (build/'archive-worker/bigbrotr-archive-worker').write_bytes(b'worker copy')
        (build/'bigbrotr').write_bytes(b'parent copy')
        tool = root/'native-tool'; tool.write_bytes(b'compiler fixture')
        sdk = root/'sdk'; sdk.mkdir(); (sdk/'SDKSettings.json').write_bytes(b'sdk fixture')
        selected = {'CC': str(tool), 'CXX': str(tool), 'AR': str(tool), 'LIBCLANG_PATH': str(tool),
                    'CLANG_PATH': str(tool), 'CARGO_BUILD_JOBS': '2'}
        native = {'os': ci.platform.system(), 'architecture': ci.platform.machine(),
                  'discovery': 'fixture', 'environment': selected,
                  'tools': {'CC': {'path': str(tool), 'realpath': str(tool), 'sha256': sha(tool.read_bytes()), 'version': 'fixture'}},
                  'sdk': {'path': str(sdk), 'version': 'fixture', 'settings_sha256': sha((sdk/'SDKSettings.json').read_bytes())}}
        worker = {'bytes': len(b'worker copy'), 'sha256': sha(b'worker copy')}
        inputs = {n:files[n] for n in ['Cargo.lock', 'rust-toolchain.toml', 'contracts/release/dependencies.json',
                                      'contracts/protocol/versions.json', 'contracts/protocol/nip01-event.json']}
        receipt = {'format': 'bigbrotr-build-result/1', 'binary_sha256': sha(b'parent copy'),
                   'source_manifest_sha256': manifest_hash, 'helper_sha256': files['tools/build.py'],
                   'native_toolchain': native,
                   'archive_worker': dict(worker, registry_sha256=files[registry], assets=assets),
                   'build_info': {'source': dict(state='builder-supplied', revision=manifest['revision'],
                                                tree=manifest['tree'], dirty=False, manifest_sha256=manifest_hash),
                                  'profile_assertion': 'release', 'target': target, 'inputs_sha256': inputs,
                                  'archive_worker': worker, 'rustc': 'fixture Rust identity'}}
        (build/'build-result.json').write_text(json.dumps(receipt))
        binary_dir = root/'bin'; binary_dir.mkdir()
        cargo = binary_dir/'cargo'
        # This child records dispatch and optionally corrupts one actual file.
        # It never compiles, executes a product or manufactures a test PASS.
        cargo.write_text('#!' + sys.executable + '\n' + '''import json, os, sys
from pathlib import Path
root = Path(__file__).resolve().parents[1]
(root/'called.json').write_text(json.dumps({'args': sys.argv[1:], 'cwd': os.getcwd(), 'env': dict(os.environ)}))
action = json.loads((root/'action.json').read_text())
if action.get('change'):
    Path(action['change']).write_bytes(b'changed by finite child')
raise SystemExit(action.get('exit', 0))
''')
        cargo.chmod(0o700); (root/'action.json').write_text('{}')
        patches.enter_context(patch.object(ci, 'ROOT', checkout))
        patches.enter_context(patch.object(archive_bundle, 'REGISTRY', source/registry))
        discovery = patches.enter_context(patch.object(ci, 'native_toolchain', return_value=(selected, native)))
        rustc = patches.enter_context(patch.object(ci, 'command_output', return_value='fixture Rust identity'))
        patches.enter_context(patch.dict(os.environ, {'PATH': str(binary_dir) + os.pathsep + os.environ['PATH']}))
        patches.enter_context(redirect_stdout(io.StringIO()))
        yield root, build, receipt, discovery, rustc


class WorkspaceReuseControls(unittest.TestCase):
    def test_undeclared_ancestor_configuration_refuses_before_cargo(self):
        settings = [b'[profile.test]\ndebug-assertions = false\n',
                    b'[build]\nrustflags = ["--cfg", "secp256k1_fuzz"]\n',
                    b'[build]\nrustc-wrapper = "/untrusted/wrapper"\n']
        for location in ['build', 'home-ancestor']:
            for name in ['config', 'config.toml']:
                for contents in settings:
                    with self.subTest(location=location, name=name, contents=contents), fixture() as (root, build, _, _, _):
                        directory = build if location == 'build' else root
                        (directory/'.cargo').mkdir()
                        (directory/'.cargo'/name).write_bytes(contents)
                        # This file appears after the clean build receipt. An
                        # explicit CARGO_HOME does not bypass an ancestral HOME.
                        with patch.dict(os.environ, {'HOME': str(root)}):
                            with self.assertRaisesRegex(ValueError, 'ancestor Cargo configuration'):
                                ci.workspace_check(build, 'rust-tests')
                        self.assertFalse((root/'called.json').exists())

    def test_inherited_checkout_configuration_must_match_the_committed_export(self):
        with fixture() as (root, build, _, _, _):
            checkout = root/'checkout'
            nested = checkout/'.research'/'build'
            nested.parent.mkdir()
            build.rename(nested)
            with patch.object(archive_bundle, 'REGISTRY', nested/'source/contracts/release/archive-engine.json'):
                self.assertEqual(ci.workspace_check(nested, 'clippy'), 0)
                (root/'called.json').unlink()
                config = checkout/'.cargo/config.toml'
                original = config.read_bytes()
                config.unlink()
                with self.assertRaisesRegex(ValueError, 'configuration is missing'):
                    ci.workspace_check(nested, 'clippy')
                self.assertFalse((root/'called.json').exists())
                config.write_bytes(original + b'\n[profile.dev]\ndebug-assertions = false\n')
                with self.assertRaisesRegex(ValueError, 'configuration differs from selected source'):
                    ci.workspace_check(nested, 'clippy')
                self.assertFalse((root/'called.json').exists())
                config.write_bytes(original)
                # Cargo prefers this legacy spelling over the bound TOML file.
                (checkout/'.cargo/config').write_bytes(b'[build]\nrustc-wrapper = "/untrusted"\n')
                with self.assertRaisesRegex(ValueError, 'ancestor Cargo configuration'):
                    ci.workspace_check(nested, 'clippy')
                self.assertFalse((root/'called.json').exists())

    def test_a_child_cannot_add_an_undeclared_ancestor_config_and_report_success(self):
        for name in ['config', 'config.toml']:
            with self.subTest(name=name), fixture() as (root, build, _, _, _):
                (build/'.cargo').mkdir()
                (root/'action.json').write_text(json.dumps({'change': str(build/'.cargo'/name)}))
                with self.assertRaisesRegex(ValueError, 'ancestor Cargo configuration'):
                    ci.workspace_check(build, 'rust-doc')
                self.assertTrue((root/'called.json').exists())

    def test_dispatch_cli_requires_complete_exclusive_options(self):
        for args in [['--workspace-check', 'clippy'], ['--from-build', '/absent'],
                     ['--workspace-check', 'clippy', '--from-build', '/absent', '--output', '/unused'],
                     ['--workspace-check', 'clippy', '--from-build', '/absent', '--controls-only'],
                     ['--documentation-only', '--controls-only', '--output', '/unused'],
                     ['--workspace-check', 'unknown', '--from-build', '/absent']]:
            with self.subTest(args=args):
                result = subprocess.run([sys.executable, str(ROOT/'tools/ci.py'), *args],
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, '')
                self.assertIn('error:', result.stderr)

    def test_actual_dispatch_preserves_default_profiles_full_scope_and_sanitized_inputs(self):
        expected = {
            'clippy': ['clippy', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
            'rust-doc': ['doc', '--locked', '--workspace', '--no-deps'],
            'rust-tests': ['test', '--locked', '--workspace'],
        }
        with fixture() as (root, build, _, _, _), patch.dict(os.environ, {
                'RUSTFLAGS': 'untrusted', 'CARGO_ENCODED_RUSTFLAGS': 'untrusted',
                'RUSTC_WRAPPER': '/untrusted', 'RUSTC_WORKSPACE_WRAPPER': '/untrusted',
                'CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS': 'false', 'CARGO_BUILD_BUILD_DIR': '/untrusted',
                'CFLAGS': 'untrusted', 'ROCKSDB_LIB_DIR': '/untrusted', 'CARGO_HOME': '/untrusted',
                'BIGBROTR_SOURCE_REVISION': 'untrusted', 'BIGBROTR_ARCHIVE_WORKER_SHA256': 'untrusted'}):
            for name, args in expected.items():
                self.assertEqual(ci.workspace_check(build, name), 0)
                call = json.loads((root/'called.json').read_text())
                self.assertEqual(call['args'], args)
                self.assertEqual(call['cwd'], str(build/'source'))
                env = call['env']
                self.assertEqual(env['CARGO_TARGET_DIR'], str(build/'target'))
                self.assertEqual(env['CARGO_HOME'], str(build/'cargo-home'))
                self.assertEqual(env['DUCKDB_LIB_DIR'], str(build/'archive-worker/lib'))
                self.assertEqual(env['BIGBROTR_BUILD_PROFILE'], 'dev')
                self.assertEqual(env['BIGBROTR_ARCHIVE_WORKER_SHA256'], sha(b'worker copy'))
                self.assertEqual(env['BIGBROTR_ARCHIVE_TEST_INSTALLATION'], str(build/'archive-worker'))
                self.assertEqual(env['RUSTDOCFLAGS'], '-D warnings')
                for key in ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER',
                            'CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS', 'CARGO_BUILD_BUILD_DIR', 'CFLAGS', 'ROCKSDB_LIB_DIR']:
                    self.assertNotIn(key, env)

    def test_changed_inputs_are_refused_before_the_child_can_run(self):
        for name in ['bigbrotr', 'archive-worker/bigbrotr-archive-worker', 'source/product.rs',
                     'source/Cargo.lock', 'source/tools/ci.py', 'source/extra.rs', 'source.tar',
                     'archive-worker/lib/library', 'cargo-home/config.toml', 'cargo-home/credentials.toml']:
            with self.subTest(path=name), fixture() as (root, build, _, _, _):
                (build/name).write_bytes(b'changed')
                with self.assertRaises((ValueError, OSError)):
                    ci.workspace_check(build, 'rust-tests')
                self.assertFalse((root/'called.json').exists())
        for name in ['native-tool', 'sdk/SDKSettings.json', 'checkout/tools/ci.py']:
            with self.subTest(path=name), fixture() as (root, build, _, _, _):
                (root/name).write_bytes(b'changed')
                with self.assertRaises(ValueError): ci.workspace_check(build, 'clippy')
                self.assertFalse((root/'called.json').exists())

    def test_links_and_mismatched_receipts_are_refused(self):
        for name in ['bigbrotr', 'archive-worker/bigbrotr-archive-worker', 'source/product.rs',
                     'source', 'target', 'cargo-home', 'archive-worker/lib']:
            with self.subTest(path=name), fixture() as (root, build, _, _, _):
                path = build/name; saved = root/'substitute'; path.rename(saved); path.symlink_to(saved)
                with self.assertRaises((ValueError, OSError)): ci.workspace_check(build, 'rust-tests')
                self.assertFalse((root/'called.json').exists())
        for kind in ['source', 'target', 'worker', 'native-env', 'manifest', 'missing']:
            with self.subTest(kind=kind), fixture() as (root, build, receipt, _, _):
                if kind == 'source': receipt['build_info']['source']['revision'] = '0'*40
                if kind == 'target': receipt['build_info']['target'] = 'wrong-target'
                if kind == 'worker': receipt['build_info']['archive_worker'] = {'bytes': 3, 'sha256': sha(b'bad')}
                if kind == 'native-env':
                    receipt = copy.deepcopy(receipt)
                    receipt['native_toolchain']['environment']['RUSTFLAGS'] = 'untrusted'
                if kind == 'manifest': receipt['source_manifest_sha256'] = '0'*64
                (build/'build-result.json').write_text(json.dumps(receipt))
                if kind == 'missing': (build/'build-result.json').unlink()
                with self.assertRaises((ValueError, OSError)): ci.workspace_check(build, 'clippy')
                self.assertFalse((root/'called.json').exists())

    def test_successful_child_cannot_hide_mutated_source_artifact_or_receipt(self):
        for name in ['source/product.rs', 'bigbrotr', 'archive-worker/bigbrotr-archive-worker',
                     'archive-worker/lib/header', 'source-manifest.json', 'build-result.json']:
            with self.subTest(path=name), fixture() as (root, build, _, _, _):
                (root/'action.json').write_text(json.dumps({'change': str(build/name)}))
                with self.assertRaises((ValueError, OSError)): ci.workspace_check(build, 'rust-doc')
                self.assertTrue((root/'called.json').exists())
        for name in ['native-tool', 'sdk/SDKSettings.json']:
            with self.subTest(path=name), fixture() as (root, build, _, _, _):
                (root/'action.json').write_text(json.dumps({'change': str(root/name)}))
                with self.assertRaises(ValueError): ci.workspace_check(build, 'clippy')
                self.assertTrue((root/'called.json').exists())
        with fixture() as (root, build, _, _, rustc):
            rustc.side_effect = ['fixture Rust identity', 'changed Rust compiler']
            with self.assertRaisesRegex(ValueError, 'Rust compiler changed'): ci.workspace_check(build, 'clippy')
            self.assertTrue((root/'called.json').exists())

    def test_mutable_target_output_is_separate_and_cargo_failure_remains_failure(self):
        with fixture() as (root, build, _, _, _):
            for code in [0, 7]:
                (root/'action.json').write_text(json.dumps({'change': str(build/'target/release/bigbrotr'), 'exit': code}))
                self.assertEqual(ci.workspace_check(build, 'rust-tests'), code)
                self.assertEqual((build/'bigbrotr').read_bytes(), b'parent copy')
                self.assertEqual((build/'archive-worker/bigbrotr-archive-worker').read_bytes(), b'worker copy')

    def test_changed_compiler_selection_refuses_before_cargo(self):
        with fixture() as (root, build, _, discovery, rustc):
            rustc.return_value = 'different Rust compiler'
            with self.assertRaisesRegex(ValueError, 'Rust compiler'): ci.workspace_check(build, 'clippy')
            self.assertFalse((root/'called.json').exists())
            rustc.return_value = 'fixture Rust identity'
            discovery.return_value = ({}, {})
            with self.assertRaisesRegex(ValueError, 'native toolchain'): ci.workspace_check(build, 'clippy')
            self.assertFalse((root/'called.json').exists())


if __name__ == '__main__':
    unittest.main()
