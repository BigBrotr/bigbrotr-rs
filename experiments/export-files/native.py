#!/usr/bin/env python3
"""Exact trusted source build and real filesystem boundary qualification.

No PostgreSQL, archive worker or job execution/release claim. The outer runner
owns process/container limits, source/cache inventories and the separate bind
alias container. Original artifacts and cache inputs remain untouched.
"""
import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build, out = SOURCE.parent, Path('/out/checks')
manifest = json.loads((build / 'source-manifest.json').read_text())


def source_files():
    return {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert source_files() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'],
                source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                lockfile_sha256=file_hash(SOURCE / 'Cargo.lock'), native_toolchain=native,
                scope='actual filesystem unit; no SQL, native archive observation, runtime job or release')
assert execute([
    ('build', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr',
               '--bin', 'bigbrotr', '--example', 'export-files-probe']),
    ('flush-cancellation', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                            '--lib', 'export::files::tests::cancellation_stops_new_writes_inside_a_partial_buffer_flush']),
    ('filesystem-tests', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr', '--test', 'export_files']),
    ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                '--lib', '--test', 'export_files', '--example', 'export-files-probe', '--', '-D', 'warnings']),
], out, SOURCE, env, identity, 1800)['status'] == 'PASS'
assert '1 passed; 0 failed; 0 ignored' in (out / 'flush-cancellation.log').read_text()
assert '7 passed; 0 failed; 0 ignored' in (out / 'filesystem-tests.log').read_text()
artifacts = Path('/out/artifacts'); artifacts.mkdir()
for original, name in [('examples/export-files-probe', 'export-files-probe'), ('bigbrotr', 'bigbrotr')]:
    shutil.copy2(build / 'target/release' / original, artifacts / name)
    (artifacts / name).chmod(0o500)
info = json.loads(subprocess.check_output([str(artifacts / 'bigbrotr'), 'build-info'], env={}))
assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'],
                             tree=manifest['tree'], dirty=False,
                             manifest_sha256=identity['source_manifest_sha256'])
assert info['archive_worker'] is None and info['target'] == 'x86_64-unknown-linux-gnu'
identity.update(artifacts={p.name: file_hash(p) for p in artifacts.iterdir()}, build_info=info)
(out / 'artifacts.json').write_text(json.dumps(identity, indent=2) + '\n')
assert execute([
    ('filesystem-faults', [sys.executable, '-B', str(SOURCE / 'experiments/export-files/cases.py'),
                          str(artifacts / 'export-files-probe'), '/out/cases', '/instrument/strace']),
], out / 'practical', SOURCE, env, identity, 180)['status'] == 'PASS'
assert source_files() == manifest['files_sha256']
(out / 'complete.json').write_text(json.dumps(dict(identity, status='PASS', source_unchanged=True,
                                                alias_qualification='separate container still required'), indent=2) + '\n')
