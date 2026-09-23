#!/usr/bin/env python3
"""Exact-source filesystem destination qualification, in an isolated Linux lab.

This intentionally builds the parent without a native archive-worker binding:
no archive execution, packaged installation or other target claim is made here.
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

build, out = SOURCE.parent, Path('/out/native-tests')
manifest = json.loads((build / 'source-manifest.json').read_text())


def source_files():
    return {str(p.relative_to(SOURCE)): file_hash(p)
            for p in sorted(SOURCE.rglob('*')) if p.is_file()}


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
                scope='finite local export destination; no worker, job, database or release')
assert execute([
    ('build', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr']),
    ('consumers', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr',
                   '--test', 'export_destination', '--test', 'config_entrypoint',
                   '--test', 'policy_seed', '--test', 'cli_contract']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr',
                '--lib', '--bin', 'bigbrotr', '--test', 'export_destination', '--', '-D', 'warnings']),
], out, SOURCE, env, identity, 1800)['status'] == 'PASS'
binary = out / 'bigbrotr'
shutil.copy2(build / 'target/release/bigbrotr', binary)
binary.chmod(0o500)
info = json.loads(subprocess.check_output([str(binary), 'build-info'], env={}))
assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'],
                             tree=manifest['tree'], dirty=False,
                             manifest_sha256=identity['source_manifest_sha256'])
assert info['archive_worker'] is None and info['target'] == 'x86_64-unknown-linux-gnu'
identity.update(binary_sha256=file_hash(binary), build_info=info)
(out / 'artifact.json').write_text(json.dumps(identity, indent=2) + '\n')
assert execute([
    ('filesystem', [sys.executable, '-B', str(SOURCE / 'experiments/export-destination/cases.py'),
                    str(binary), '/out/cases']),
], out / 'practical', SOURCE, env, identity, 180)['status'] == 'PASS'
assert source_files() == manifest['files_sha256']
(out / 'complete.json').write_text(json.dumps(dict(identity, status='PASS', source_unchanged=True), indent=2) + '\n')
