#!/usr/bin/env python3
"""Build exact-source scientific guard inspection against isolated native catalogs."""
import json
from pathlib import Path
import re
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
assert len(sys.argv) == 1
manifest = json.loads((build / 'source-manifest.json').read_text())
assert {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()} == manifest['files_sha256']
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
native_env, native = native_toolchain(env, 2)
env.update(native_env)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'), LD_LIBRARY_PATH=str(bundle / 'lib'),
    BIGBROTR_SOURCE_REVISION=manifest['revision'], BIGBROTR_SOURCE_TREE=manifest['tree'],
    BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'), BIGBROTR_BUILD_PROFILE='release')
checks = [
    ('build', ['cargo','build','--locked','--offline','--release','-p','bigbrotr','--example','catalog_schema_probe']),
    ('guard', ['cargo','build','--locked','--offline','--release','-p','bigbrotr','--example','scientific_guard_probe']),
    ('worker', ['cargo','build','--locked','--offline','--release','-p','bigbrotr-archive-worker','--bin','bigbrotr-archive-worker']),
    ('clippy', ['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','-p','bigbrotr-archive-worker','--lib','--example','catalog_schema_probe','--example','scientific_guard_probe','--bin','bigbrotr-archive-worker','--','-D','warnings']),
]
assert execute(checks,out,SOURCE,env,{'source':manifest['revision'],'native_toolchain':native},1800)['status']=='PASS'
worker = bundle/'bigbrotr-archive-worker'
shutil.copy2(build/'target/release/bigbrotr-archive-worker',worker)
worker.chmod(0o500)
probe = out/'catalog-schema-probe'
shutil.copy2(build/'target/release/examples/catalog_schema_probe',probe)
probe.chmod(0o500)
identity = {'source':manifest['revision'],'worker':file_hash(worker),'probe':file_hash(probe),'native_toolchain':native}
guard = out/'scientific-guard-probe'
shutil.copy2(build/'target/release/examples/scientific_guard_probe',guard)
guard.chmod(0o500)
identity['guard']=file_hash(guard)
(out/'ready.json').write_text(json.dumps(identity,indent=2)+'\n')
print(json.dumps({'status':'READY',**identity}),flush=True)
