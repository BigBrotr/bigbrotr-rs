#!/usr/bin/env python3
"""Exact-source Linux parent build and selected tests; no archive-worker claim."""
import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True
enrollment = sys.argv[1:] == ['--enrollment']
cursor_runtime = sys.argv[1:] == ['--runtime']
cursor_lifecycle = sys.argv[1:] == ['--lifecycle']
assert not sys.argv[1:] or enrollment or cursor_runtime or cursor_lifecycle
offline = enrollment or cursor_runtime or cursor_lifecycle
source = Path('/out/build/source')
sys.path.insert(0, str(source))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = source.parent
manifest = json.loads((build / 'source-manifest.json').read_text())
files = lambda: {str(p.relative_to(source)): file_hash(p) for p in sorted(source.rglob('*')) if p.is_file()}
assert files() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2); env.update(selected)
env.update(CARGO_HOME=str(build/'cargo-home'), CARGO_TARGET_DIR=str(build/'target'),
           BIGBROTR_SOURCE_REVISION=manifest['revision'], BIGBROTR_SOURCE_TREE=manifest['tree'],
           BIGBROTR_BUILD_PROFILE='release', BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'))
if offline: env['CARGO_NET_OFFLINE']='true'
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                lockfile_sha256=file_hash(source/'Cargo.lock'),
                source_manifest_sha256=file_hash(build/'source-manifest.json'),
                scope='loaded cursor bindings in actual Control startup; no activation or distribution' if cursor_runtime else 'additional cursor key enrollment; no activation, distribution or release' if enrollment else 'initial protected cursor registration; no activation, rotation, archive worker or release')
if cursor_lifecycle:
    identity['scope'] = 'maintenance cursor lifecycle and actual Control readiness; no issuer or release'
out = Path('/out/native-tests')
tests = ['--test', 'cursor_registration', '--test', 'cursor_keyring_cli', '--test', 'control_schema',
         '--test', 'cursor_keyring', '--test', 'read_cursors']
if cursor_runtime or cursor_lifecycle: tests += ['--test', 'control_runtime_entrypoint']
assert execute(([] if offline else [('fetch', ['cargo', 'fetch', '--locked', '--target', 'x86_64-unknown-linux-gnu'])]) + [
    ('build', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr']),
    ('tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', *tests]),
    ('crypto', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'read::cursor::tests']),
    *([('runtime-keys', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', 'interfaces::serve::cursor_keys::tests'])] if cursor_runtime or cursor_lifecycle else []),
    ('collision', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'initial_publication_never']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--bin', 'bigbrotr',
                '--test', 'cursor_registration', '--test', 'cursor_keyring_cli', '--test', 'control_schema', '--', '-D', 'warnings']),
], out, source, env, identity, 1800)['status'] == 'PASS'
binary = out / 'bigbrotr'; shutil.copy2(build/'target/release/bigbrotr', binary); binary.chmod(0o500)
info = json.loads(subprocess.check_output([str(binary), 'build-info'], env={}))
assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'], tree=manifest['tree'],
                             dirty=False, manifest_sha256=identity['source_manifest_sha256'])
assert info['target'] == 'x86_64-unknown-linux-gnu' and info['archive_worker'] is None
assert info['inputs_sha256']['Cargo.lock'] == identity['lockfile_sha256']
assert files() == manifest['files_sha256']
(out/'build-result.json').write_text(json.dumps(dict(identity, format='bigbrotr-cursor-registration-build/1',
    build_info=info, binary_sha256=file_hash(binary)), indent=2)+'\n')
