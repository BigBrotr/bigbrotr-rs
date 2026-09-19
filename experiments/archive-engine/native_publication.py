#!/usr/bin/env python3
"""Exact-source native build for the scientific publication risk qualification."""
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, native_toolchain, file_hash
from tools.archive_bundle import install_bundle, build_environment
from tools.ci import execute

build = SOURCE.parent
out = Path('/out/native-tests')
bundle = Path('/out/archive-worker')
manifest = json.loads((build / 'source-manifest.json').read_text())
assert {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()} == manifest['files_sha256']
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
native_env, native = native_toolchain(env, 2)
env.update(native_env)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build/'cargo-home'), CARGO_TARGET_DIR=str(build/'target'),
           LD_LIBRARY_PATH=str(bundle/'lib'), BIGBROTR_ARCHIVE_TEST_BUNDLE=str(bundle),
           BIGBROTR_SOURCE_REVISION=manifest['revision'], BIGBROTR_SOURCE_TREE=manifest['tree'],
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'),
           BIGBROTR_BUILD_PROFILE='release')
checks = [
    ('build', ['cargo','build','--locked','--offline','--release','-p','bigbrotr-archive-worker','--bin','bigbrotr-archive-worker','--example','catalog-fence-probe']),
    ('signed-fixtures', ['cargo','build','--locked','--offline','--release','-p','bigbrotr','-p','bigbrotr-archive-worker','--example','archive_publication_probe','--example','archive_read_probe','--example','archive_receipt_probe']),
    ('embedded', ['cargo','test','--locked','--offline','--release','-p','bigbrotr-archive-worker','--lib','publication']),
    ('input', ['cargo','test','--locked','--offline','--release','-p','bigbrotr-archive-worker','--test','publication']),
    ('receipts', ['cargo','test','--locked','--offline','--release','-p','bigbrotr-archive-worker','--lib','receipt::']),
    ('receipt-input', ['cargo','test','--locked','--offline','--release','-p','bigbrotr-archive-worker','--test','receipt']),
    ('receipt-parent', ['cargo','test','--locked','--offline','--release','-p','bigbrotr','--lib','storage::archive::receipts']),
    ('receipt-protocol', ['cargo','test','--locked','--offline','--release','-p','bigbrotr-protocol','--test','archive_receipt']),
    ('custody', ['cargo','test','--locked','--offline','--release','-p','bigbrotr','--lib','storage::archive::lifetime']),
]
assert execute(checks,out,SOURCE,env,{'source':manifest['revision'],'native_toolchain':native},1800)['status']=='PASS'
identity = dict(source=manifest['revision'], native_toolchain=native)
for source, destination, label in [
        (build/'target/release/bigbrotr-archive-worker',bundle/'bigbrotr-archive-worker','worker'),
        (build/'target/release/examples/catalog-fence-probe',out/'catalog-fence-probe','probe'),
        (build/'target/release/examples/archive_publication_probe',out/'archive-publication-probe','publisher'),
        (build/'target/release/examples/archive_read_probe',out/'archive-read-probe','reader'),
        (build/'target/release/examples/archive_receipt_probe',out/'archive-receipt-probe','receipt_reader')]:
    shutil.copy2(source,destination)
    destination.chmod(0o500)
    identity[label] = file_hash(destination)
assert execute([
    ('native-custody', ['cargo','test','--locked','--offline','--release','-p','bigbrotr','--lib',
                        'native_worker_alone_retains_custody_after_coordinator_descriptors_close',
                        '--','--ignored','--nocapture'])
],out/'custody-native',SOURCE,env,{'source':manifest['revision'],'native_toolchain':native},120)['status']=='PASS'
(out/'ready.json').write_text(json.dumps(identity,indent=2)+'\n')
print(json.dumps({'status':'READY',**identity}),flush=True)
