#!/usr/bin/env python3
"""Exact-source native authentication qualification builder; owned Linux lab only."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import build_binaries, clean_environment, file_hash, native_toolchain
from tools.archive_bundle import build_environment, install_bundle
from tools.ci import execute

build = SOURCE.parent
out = Path('/out/native-tests')
bundle = Path('/out/archive-worker')
if sys.argv[1:] == ['--build-binaries']:
    identity = build_binaries(SOURCE, os.environ)
    (out / 'worker-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    sys.exit(0)
assert not sys.argv[1:]
manifest = json.loads((build / 'source-manifest.json').read_text())


def source_files(root):
    return {str(p.relative_to(root)): file_hash(p) for p in sorted(root.rglob('*')) if p.is_file()}


assert source_files(SOURCE) == manifest['files_sha256']
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                native_toolchain=native, scope='actual native password authentication and exact completion replies')
assert execute([('ordered-build', [sys.executable, __file__, '--build-binaries'])],
               out, SOURCE, env, identity, 1800)['status'] == 'PASS'
worker = json.loads((out / 'worker-identity.json').read_text())
env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker['bytes']), BIGBROTR_ARCHIVE_WORKER_SHA256=worker['sha256'])
assert execute([
    ('fixtures', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--example', 'catalog_schema_probe', '--example', 'scientific_guard_probe', '--example', 'archive_receipt_probe']),
    ('tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive', '--', '--nocapture']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--example', 'archive_receipt_probe', '--', '-D', 'warnings']),
], out / 'checks', SOURCE, env, identity, 1800)['status'] == 'PASS'
for src, dst in [('bigbrotr', out / 'bigbrotr'), ('bigbrotr-archive-worker', bundle / 'bigbrotr-archive-worker'),
                 ('examples/catalog_schema_probe', out / 'catalog-schema-probe'),
                 ('examples/scientific_guard_probe', out / 'scientific-guard-probe'),
                 ('examples/archive_receipt_probe', out / 'archive-receipt-probe')]:
    shutil.copy2(build / 'target/release' / src, dst)
    dst.chmod(0o500)

# Independent pre-change parent, rebuilt from its complete published Git input.
# No edits/backported probe or runtime switch can disable the candidate policy.
baseline = '0ca4fffa9c5f1bf5b1c447907dc2c3cc2fa2b1eb'
baseline_source = build / 'baseline-source'
baseline_source.mkdir()
archive = build / 'baseline-source.tar'
subprocess.run(['git', '-C', '/source', 'archive', '--format=tar', '--output', str(archive), baseline], check=True)
with tarfile.open(archive) as tar:
    for member in tar.getmembers():
        assert not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
        assert member.isfile() or member.isdir()
    # Match tools.build's prevalidated regular-file/directory-only extraction;
    # the pinned Python3.11 runner does not provide the later filter keyword.
    tar.extractall(baseline_source)
baseline_manifest = dict(revision=baseline, archive_sha256=file_hash(archive), files_sha256=source_files(baseline_source))
(build / 'baseline-source-manifest.json').write_text(json.dumps(baseline_manifest, indent=2) + '\n')
assert file_hash(baseline_source / 'Cargo.lock') == file_hash(SOURCE / 'Cargo.lock')
baseline_env = dict(env, BIGBROTR_SOURCE_REVISION=baseline,
                    BIGBROTR_SOURCE_TREE=subprocess.check_output(['git', '-C', '/source', 'rev-parse', baseline+'^{tree}'], text=True).strip(),
                    BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'baseline-source-manifest.json'))
assert execute([('baseline', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--example', 'archive_receipt_probe'])],
               out / 'baseline', baseline_source, baseline_env, dict(source=baseline), 1800)['status'] == 'PASS'
shutil.copy2(build / 'target/release/examples/archive_receipt_probe', out / 'baseline-receipt-probe')
(out / 'baseline-receipt-probe').chmod(0o500)
assert source_files(baseline_source) == baseline_manifest['files_sha256']
assert source_files(SOURCE) == manifest['files_sha256']
info = json.loads(subprocess.check_output([str(out / 'bigbrotr'), 'build-info'], env={}))
assert info['archive_worker'] == worker and info['source']['revision'] == manifest['revision']
assert info['source']['dirty'] is False
result = dict(identity, status='READY', control=file_hash(out / 'bigbrotr'), worker=worker['sha256'],
              probe=file_hash(out / 'catalog-schema-probe'), guard=file_hash(out / 'scientific-guard-probe'),
              receipt_reader=file_hash(out / 'archive-receipt-probe'),
              baseline_source=baseline, baseline_reader=file_hash(out / 'baseline-receipt-probe'),
              build_info=info, source_unchanged=True)
(out / 'ready.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'status': 'READY', 'source': manifest['revision']}), flush=True)
