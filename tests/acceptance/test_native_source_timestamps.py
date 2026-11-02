"""Real Git/archive/file controls for optional native timestamps; no build/cache."""
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools import build, ci, native_source_timestamps as timestamps

ROOT = Path(__file__).resolve().parents[2]
OLD = 1_000_000_111
FRESH = 2_000_000_333


def git(root, *args):
    return subprocess.check_output(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false',
                                    '-C', str(root), *args], text=True,
                                   stderr=subprocess.DEVNULL).strip()


def commit(root):
    git(root, 'add', '.')
    git(root, '-c', 'user.name=Timestamp fixture', '-c', 'user.email=fixture@example.invalid',
        '-c', 'commit.gpgsign=false', 'commit', '-qm', 'source fixture')


def export(root, output, mtime):
    output.mkdir(); source = output/'source'; source.mkdir()
    git(root, 'archive', '--format=tar', '--output', str(output/'source.tar'), 'HEAD')
    with tarfile.open(output/'source.tar') as archive:
        # This is the fixture's own Git archive, never supplied input. Omit the
        # newer filter keyword to support the declared Python3.11 minimum.
        archive.extractall(source)
    for path in [source/'vendor', *(source/'vendor').rglob('*')]:
        os.utime(path, ns=(mtime, mtime))
    manifest = {'format': 'bigbrotr-source/1', 'revision': git(root, 'rev-parse', 'HEAD'),
                'tree': git(root, 'rev-parse', 'HEAD^{tree}'),
                'archive_sha256': build.file_hash(output/'source.tar'),
                'files_sha256': build.source_inventory(source)}
    (output/'source-manifest.json').write_text(json.dumps(manifest))
    return manifest


@contextmanager
def fixture(change=None):
    with tempfile.TemporaryDirectory() as directory:
        base = Path(directory).resolve()
        checkout = base/'checkout'; checkout.mkdir()
        files = {'Cargo.toml': '[workspace]\n', 'Cargo.lock': 'lock fixture',
                 'rust-toolchain.toml': 'pinned fixture', '.cargo/config.toml': '[build]\n',
                 'apps/application/Cargo.toml': 'features fixture',
                 'contracts/release/archive-engine.json': 'pinned assets fixture',
                 'vendor/native/build.rs': 'native flags fixture',
                 'vendor/native/src/value.cpp': 'int value() { return 42; }\n',
                 'vendor/native/include/.hidden.h': '#define ANSWER 42\n',
                 'product.rs': 'unrelated Rust fixture'}
        for name, contents in files.items():
            path=checkout/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text(contents)
        for name in ['tools/build.py', 'tools/archive_bundle.py', 'tools/native_source_timestamps.py']:
            (checkout/name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT/name, checkout/name)
        git(checkout, 'init', '-q'); commit(checkout)
        previous = base/'previous'; old = export(checkout, previous, OLD)
        tool = base/'compiler'; tool.write_bytes(b'independent native compiler fixture')
        sdk = base/'sdk'; sdk.mkdir(); (sdk/'SDKSettings.json').write_bytes(b'independent SDK fixture')
        native = {'environment': {'CC': str(tool), 'CARGO_BUILD_JOBS': '2'},
                  'tools': {'CC': {'path': str(tool), 'realpath': str(tool),
                                   'sha256': build.file_hash(tool), 'version': 'fixture compiler'}},
                  'sdk': {'path': str(sdk), 'version': 'fixture SDK',
                          'settings_sha256': build.file_hash(sdk/'SDKSettings.json')}}
        assets = {'library': 'independent pinned library fixture'}
        manifest_hash = build.file_hash(previous/'source-manifest.json')
        receipt = {'format': 'bigbrotr-build-result/1', 'source_manifest_sha256': manifest_hash,
                   'helper_sha256': old['files_sha256']['tools/build.py'], 'native_toolchain': native,
                   'archive_worker': {'assets': assets, 'registry_sha256': old['files_sha256']['contracts/release/archive-engine.json']},
                   'build_info': {'source': dict(state='builder-supplied', revision=old['revision'],
                                                tree=old['tree'], dirty=False, manifest_sha256=manifest_hash),
                                  'rustc': 'independent Rust compiler fixture',
                                  'target': 'fixture-target', 'profile_assertion': 'release'}}
        (previous/'build-result.json').write_text(json.dumps(receipt))
        # Deliberately no target, Cargo home, installed binary or native worker.
        # Timestamp reference metadata confers ownership of none of those.
        if change:
            change(checkout); commit(checkout)
        else:
            (checkout/'product.rs').write_text('new unrelated Rust fixture'); commit(checkout)
        current = base/'current'; manifest = export(checkout, current, FRESH)
        yield SimpleNamespace(base=base, checkout=checkout, previous=previous, current=current,
                              manifest=manifest, native=native, assets=assets, receipt=receipt, tool=tool, sdk=sdk)


def prepare(f):
    return timestamps.preserve_native_timestamps(f.current/'source', f.current/'source.tar',
                                                 f.manifest, f.native,
                                                 'independent Rust compiler fixture',
                                                 'fixture-target', f.assets, f.previous)


def times(source):
    return {p.relative_to(source).as_posix(): p.lstat().st_mtime_ns
            for p in [source/'vendor', *(source/'vendor').rglob('*')]}


def full_stat(path):
    metadata=path.lstat()
    return {name:getattr(metadata,name) for name in dir(metadata)
            if name.startswith('st_') and isinstance(getattr(metadata,name),(int,float))}


class NativeTimestampControls(unittest.TestCase):
    def test_after_inventory_substitution_never_mutates_external_inode_or_escaping_tree(self):
        for change in ['hardlink','intermediate-symlink','root-symlink']:
            with self.subTest(change=change), fixture() as f:
                source=f.current/'source'
                external=f.base/'external'; external.mkdir()
                if change=='root-symlink':
                    shutil.copytree(source,external/'source')
                    external_paths=[external/'source',*(external/'source').rglob('*')]
                else:
                    (external/'value.cpp').write_bytes(b'external inode must remain untouched')
                    os.utime(external/'value.cpp',ns=(7_000_000_999,7_000_000_999))
                    external_paths=[external,external/'value.cpp']
                original=timestamps.inventory; injected=False; protected=None
                def substitute(path,*args,**kwargs):
                    nonlocal injected,protected
                    verified=original(path,*args,**kwargs)
                    if path==source and not injected:
                        injected=True
                        if change=='hardlink':
                            (source/'vendor/native/src/value.cpp').unlink()
                            os.link(external/'value.cpp',source/'vendor/native/src/value.cpp')
                        elif change=='intermediate-symlink':
                            (source/'vendor/native/src').rename(f.base/'retained-original-src')
                            (source/'vendor/native/src').symlink_to(external,target_is_directory=True)
                        else:
                            source.rename(f.base/'retained-original-source')
                            source.symlink_to(external/'source',target_is_directory=True)
                        # Linking itself changes nlink/ctime. Freeze ALL available
                        # stat fields after injection; the helper may change none.
                        protected={path:full_stat(path) for path in external_paths}
                    return verified
                with patch.object(timestamps,'inventory',side_effect=substitute):
                    with self.assertRaises((ValueError,OSError)): prepare(f)
                self.assertTrue(injected)
                self.assertEqual({path:full_stat(path) for path in external_paths},protected)

    def test_identical_native_files_and_directories_reuse_exact_timestamps_without_target_ownership(self):
        with fixture() as f:
            source=f.current/'source'; before=build.source_inventory(source)
            product_time=(source/'product.rs').stat().st_mtime_ns
            report=prepare(f)
            self.assertEqual(report['status'], 'preserved-identical-native-inputs')
            self.assertEqual(times(source), times(f.previous/'source'))
            self.assertEqual(set(times(source).values()), {OLD})
            self.assertEqual(build.source_inventory(source), before)
            self.assertEqual((source/'product.rs').stat().st_mtime_ns, product_time)
            self.assertEqual(report['files'], 3)
            self.assertFalse((f.previous/'target').exists())
            self.assertFalse((f.current/'target').exists())

    def test_changed_native_bytes_membership_mode_and_config_refuse_before_backdating(self):
        changes = {
            'bytes': lambda root: (root/'vendor/native/src/value.cpp').write_text('int value() { return 43; }\n'),
            'added': lambda root: (root/'vendor/native/include/new.h').write_text('#define NEW 1\n'),
            'removed': lambda root: (root/'vendor/native/include/.hidden.h').unlink(),
            'mode': lambda root: (root/'vendor/native/build.rs').chmod(0o755),
            'native-flags': lambda root: (root/'vendor/native/build.rs').write_text('changed C++ flags fixture'),
            'cargo-profile': lambda root: (root/'Cargo.toml').write_text('[profile.release]\nopt-level=1\n'),
            'features': lambda root: (root/'apps/application/Cargo.toml').write_text('changed native feature fixture'),
            'config': lambda root: (root/'.cargo/config.toml').write_text('[build]\nrustflags=["-Ctarget-cpu=native"]\n'),
            'legacy-config': lambda root: (root/'.cargo/config').write_text('[build]\n'),
            'lock': lambda root: (root/'Cargo.lock').write_text('new native dependency fixture'),
            'builder': lambda root: (root/'tools/build.py').write_text('changed compiler selection fixture'),
        }
        for name, change in changes.items():
            with self.subTest(change=name), fixture(change) as f:
                before=times(f.current/'source')
                with self.assertRaises(ValueError): prepare(f)
                self.assertEqual(times(f.current/'source'), before)

    def test_missing_corrupt_extra_directory_and_linked_inputs_refuse_without_mutation(self):
        for change in ['missing', 'corrupt', 'directory', 'symlink', 'hardlink', 'mode']:
            with self.subTest(change=change), fixture() as f:
                source=f.current/'source'; path=source/'vendor/native/src/value.cpp'
                if change=='missing': path.unlink()
                elif change=='corrupt': path.write_bytes(b'changed without a new source archive')
                elif change=='directory': (source/'vendor/empty').mkdir()
                elif change=='symlink': path.unlink(); path.symlink_to(f.previous/'source/vendor/native/src/value.cpp')
                elif change=='hardlink': path.unlink(); os.link(f.previous/'source/vendor/native/src/value.cpp', path)
                else: path.chmod(0o600)
                before=times(source)
                with self.assertRaises(ValueError): prepare(f)
                self.assertEqual(times(source), before)

    def test_changed_actual_compiler_sdk_or_declared_context_refuses_before_backdating(self):
        for change in ['compiler', 'sdk', 'native-env', 'rustc', 'target', 'profile', 'current-profile', 'assets']:
            with self.subTest(change=change), fixture() as f:
                rustc='independent Rust compiler fixture'; target='fixture-target'; profile='release'
                if change=='compiler': f.tool.write_bytes(b'changed actual compiler')
                elif change=='sdk': (f.sdk/'SDKSettings.json').write_bytes(b'changed actual SDK')
                elif change=='native-env': f.native=copy.deepcopy(f.native); f.native['environment']['CXXFLAGS']='-DCHANGED'
                elif change=='rustc': rustc='changed Rust compiler'
                elif change=='target': target='changed-target'
                elif change=='current-profile': profile='dev'
                elif change=='assets': f.assets={'library':'changed pinned library'}
                else:
                    f.receipt['build_info']['profile_assertion']='dev'
                    (f.previous/'build-result.json').write_text(json.dumps(f.receipt))
                before=times(f.current/'source')
                with self.assertRaises(ValueError):
                    timestamps.preserve_native_timestamps(f.current/'source',f.current/'source.tar',f.manifest,
                        f.native,rustc,target,f.assets,f.previous,profile=profile)
                self.assertEqual(times(f.current/'source'),before)

    def test_bad_reference_identity_archive_or_source_is_not_reuse(self):
        for change in ['missing', 'receipt', 'manifest', 'archive', 'source']:
            with self.subTest(change=change), fixture() as f:
                if change=='missing': (f.previous/'build-result.json').unlink()
                elif change=='receipt':
                    f.receipt['build_info']['source']['revision']='0'*40
                    (f.previous/'build-result.json').write_text(json.dumps(f.receipt))
                elif change=='manifest': (f.previous/'source-manifest.json').write_text('{}')
                elif change=='archive': (f.previous/'source.tar').write_bytes(b'not the pinned archive')
                else: (f.previous/'source/vendor/native/src/value.cpp').write_bytes(b'changed prior source')
                before=times(f.current/'source')
                with self.assertRaises((ValueError,OSError,KeyError)): prepare(f)
                self.assertEqual(times(f.current/'source'),before)

    def test_post_guard_refuses_a_mutation_during_preparation(self):
        for target in ['source','compiler']:
            with self.subTest(target=target), fixture() as f:
                original=os.utime; changed=False
                def mutate(*args,**kwargs):
                    nonlocal changed
                    original(*args,**kwargs)
                    if not changed:
                        changed=True
                        path=f.current/'source/product.rs' if target=='source' else f.tool
                        path.write_bytes(b'changed during preparation')
                with patch.object(timestamps.os,'utime',side_effect=mutate):
                    with self.assertRaises(ValueError): prepare(f)
                self.assertTrue(changed)

    def test_relabelled_reference_files_cannot_hide_different_archive_contents(self):
        def change(root): (root/'vendor/native/src/value.cpp').write_text('int value() { return 43; }\n')
        with fixture(change) as f:
            name='vendor/native/src/value.cpp'
            shutil.copyfile(f.current/'source'/name,f.previous/'source'/name)
            os.utime(f.previous/'source'/name,ns=(OLD,OLD))
            previous=json.loads((f.previous/'source-manifest.json').read_text())
            previous['files_sha256'][name]=build.file_hash(f.previous/'source'/name)
            (f.previous/'source-manifest.json').write_text(json.dumps(previous))
            digest=build.file_hash(f.previous/'source-manifest.json')
            f.receipt['source_manifest_sha256']=digest
            f.receipt['build_info']['source']['manifest_sha256']=digest
            (f.previous/'build-result.json').write_text(json.dumps(f.receipt))
            before=times(f.current/'source')
            with self.assertRaisesRegex(ValueError,'differs from its archive'): prepare(f)
            self.assertEqual(times(f.current/'source'),before)

    def test_builder_keeps_default_fresh_and_applies_only_explicit_reference_before_compilation(self):
        class CompileBoundary(Exception): pass
        for selected in [False,True]:
            with self.subTest(selected=selected), fixture() as f:
                args=SimpleNamespace(revision='HEAD',output=f.base/'fresh-build',jobs=2,
                                     archive_bundle=None,native_source_reference=f.previous if selected else None)
                def boundary(source,env):
                    self.assertEqual(source,args.output/'source')
                    self.assertEqual(env['CARGO_TARGET_DIR'],str(args.output/'target'))
                    self.assertFalse((args.output/'target').exists())
                    if selected: self.assertEqual(times(source),times(f.previous/'source'))
                    raise CompileBoundary()
                with patch.object(build,'__file__',str(f.checkout/'tools/build.py')), \
                     patch.object(build,'native_toolchain',return_value=({},f.native)), \
                     patch.object(build,'native_target',return_value='fixture-target'), \
                     patch.object(build,'command_output',return_value='independent Rust compiler fixture'), \
                     patch.object(build,'prepare_bundle'),patch.object(build,'verify_bundle',return_value=f.assets), \
                     patch.object(build,'build_environment',return_value={}), \
                     patch.object(build,'build_binaries',side_effect=boundary), \
                     patch.object(timestamps,'preserve_native_timestamps',wraps=timestamps.preserve_native_timestamps) as prepared:
                    with self.assertRaises(CompileBoundary): build.build(args)
                    self.assertEqual(prepared.call_count,int(selected))
                self.assertTrue((args.output/'source-manifest.json').is_file())
                self.assertFalse((args.output/'build-result.json').exists())

    def test_ci_only_forwards_explicit_reference_to_actual_build_without_reducing_checks(self):
        output=Path('/fresh/ci'); reference=Path('/trusted/prior-build')
        fresh=ci.commands(output,'Linux','x86_64',2)
        reuse=ci.commands(output,'Linux','x86_64',2,reference)
        self.assertEqual([name for name,_ in fresh],[name for name,_ in reuse])
        for (name,before),(_,after) in zip(fresh,reuse):
            self.assertEqual(after,before+(['--native-source-reference',str(reference)] if name=='exact-source-build' else []))

    def test_absolute_builder_from_foreign_override_probes_the_cargo_source_cwd_twice(self):
        class AfterChecks(Exception): pass
        with fixture() as f:
            foreign=f.base/'foreign-caller'; foreign.mkdir()
            (foreign/'rust-toolchain.toml').write_text('different caller Rustup override')
            executables=f.base/'bin'; executables.mkdir()
            calls=f.base/'rustc-calls.jsonl'
            rustc=executables/'rustc'
            rustc.write_text('#!'+sys.executable+'\n'+
                'import json\nfrom pathlib import Path\n'+
                'cwd=Path.cwd()\n'+
                'with Path('+repr(str(calls))+').open("a") as log: log.write(json.dumps(str(cwd))+"\\n")\n'+
                'selected=(cwd/"rust-toolchain.toml").read_text()\n'+
                'print("independent Rust compiler fixture" if selected=="pinned fixture" else "foreign override compiler")\n')
            rustc.chmod(0o700)
            previous_cwd=Path.cwd()
            argv=[str(f.checkout/'tools/build.py'),'--output',str(f.base/'absolute-build'),
                  '--native-source-reference',str(f.previous)]
            compiled=[]
            def finite_compile_boundary(source,env):
                compiled.append(source)
                # No compiler/product/worker is launched or materialized. Return
                # only to exercise the second real rustc probe, then stop before
                # any product copy/receipt can pretend this was a native build.
                return {'bytes':1,'sha256':'0'*64}
            try:
                os.chdir(foreign)
                with patch.dict(os.environ,{'PATH':str(executables)+os.pathsep+os.environ['PATH']}):
                    self.assertEqual(build.command_output(['rustc','-vV'],build.clean_environment(),cwd=foreign),
                                     'foreign override compiler')
                    with patch.object(sys,'argv',argv), \
                         patch.object(build,'__file__',argv[0]), \
                         patch.object(build,'native_toolchain',return_value=({},f.native)), \
                         patch.object(build,'native_target',return_value='fixture-target'), \
                         patch.object(build,'prepare_bundle'),patch.object(build,'verify_bundle',return_value=f.assets), \
                         patch.object(build,'build_environment',return_value={}), \
                         patch.object(build,'build_binaries',side_effect=finite_compile_boundary), \
                         patch.object(build.shutil,'copy2',side_effect=AfterChecks):
                        with self.assertRaises(AfterChecks): build.main()
            finally:
                os.chdir(previous_cwd)
            source=f.base/'absolute-build/source'
            self.assertEqual(compiled,[source])
            self.assertEqual([json.loads(line) for line in calls.read_text().splitlines()],
                             [str(foreign),str(source),str(source)])
            self.assertFalse((f.base/'absolute-build/build-result.json').exists())
            self.assertFalse((f.base/'absolute-build/target').exists())


if __name__ == '__main__':
    unittest.main()
