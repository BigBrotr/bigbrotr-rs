#!/usr/bin/env python3
"""Exact-Git native reopening qualification inside an inventoried offline lab."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import build_binaries, clean_environment, file_hash, native_toolchain
from tools.archive_bundle import build_environment, install_bundle
from tools.ci import execute

build, out, bundle = SOURCE.parent, Path('/out/wal-tests'), Path('/out/archive-worker')
if sys.argv[1:] == ['--build-binaries']:
    worker = build_binaries(SOURCE, os.environ)
    (out/'worker-identity.json').write_text(json.dumps(worker, indent=2)+'\n')
    sys.exit(0)
assert not sys.argv[1:]
manifest = json.loads((build/'source-manifest.json').read_text())


def source_files():
    return {str(p.relative_to(SOURCE)): file_hash(p)
            for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert source_files() == manifest['files_sha256']
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build/'cargo-home'), CARGO_TARGET_DIR=str(build/'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'],
                source_manifest_sha256=file_hash(build/'source-manifest.json'),
                native_toolchain=native, scope='strict native WAL reopening; no noncommit witness')
assert execute([('ordered-build', [sys.executable, __file__, '--build-binaries'])],
               out, SOURCE, env, identity, 1800)['status'] == 'PASS'
worker = json.loads((out/'worker-identity.json').read_text())
env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker['bytes']),
           BIGBROTR_ARCHIVE_WORKER_SHA256=worker['sha256'])
for src, dst in [('bigbrotr', out/'bigbrotr'),
                 ('bigbrotr-archive-worker', bundle/'bigbrotr-archive-worker')]:
    shutil.copy2(build/'target/release'/src, dst)
    dst.chmod(0o500)
assert file_hash(bundle/'bigbrotr-archive-worker') == worker['sha256']
env.update(BIGBROTR_ARCHIVE_TEST_INSTALLATION=str(bundle),
           BIGBROTR_ARCHIVE_TEST_WORKER_BYTES=str(worker['bytes']),
           BIGBROTR_ARCHIVE_TEST_WORKER_SHA256=worker['sha256'])
assert execute([
    ('native-rocks', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr',
                      '--lib', 'storage::rocks', '--', '--nocapture']),
    ('store-and-format', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr',
                          '--test', 'rocks_store', '--test', 'rocks_manifest', '--test', 'rocks_keys']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib',
                '--tests', '--', '-D', 'warnings']),
], out/'checks', SOURCE, env, identity, 1800)['status'] == 'PASS'
assert source_files() == manifest['files_sha256']
info = json.loads(subprocess.check_output([str(out/'bigbrotr'), 'build-info'], env={}))
assert info['archive_worker'] == worker and info['source']['revision'] == manifest['revision']
assert info['source']['dirty'] is False
result = dict(identity, status='PASS', binary_sha256=file_hash(out/'bigbrotr'),
              worker=worker, build_info=info, source_unchanged=True)
(out/'qualification.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(dict(status='PASS', source=manifest['revision'])), flush=True)
