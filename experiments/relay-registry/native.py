#!/usr/bin/env python3
"""Exact-source native registry CLI/probe build; no archive worker qualification."""
import json
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = SOURCE.parent
manifest = json.loads((build/'source-manifest.json').read_text())
def sources():
    return {str(p.relative_to(SOURCE)):file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert sources() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(CARGO_HOME=str(build/'cargo-home'), CARGO_TARGET_DIR=str(build/'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                lockfile_sha256=file_hash(SOURCE/'Cargo.lock'),
                source_manifest_sha256=file_hash(build/'source-manifest.json'),
                scope='Operational endpoint registration build; no acquisition or release qualification')
identity['rustc'] = subprocess.check_output(['rustc','--version','--verbose'],env=env,text=True).strip()
assert 'host: x86_64-unknown-linux-gnu' in identity['rustc'] and identity['rustc'].startswith('rustc 1.98.1 ')
checks = Path('/out/build-checks')
result = execute([
    ('compile', ['cargo','build','--locked','--offline','--release','-p','bigbrotr',
                 '--bin','bigbrotr','--example','relay_registry_probe']),
    ('catalog', ['cargo','test','--locked','--offline','--release','-p','bigbrotr',
                 '--test','relay_registry','--test','control_schema','--test','migrate_entrypoint']),
    ('clippy', ['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','--lib',
                '--test','relay_registry','--example','relay_registry_probe','--','-D','warnings']),
],checks,SOURCE,env,identity,1800)
assert result['status']=='PASS'
artifacts=Path('/out/artifacts'); artifacts.mkdir()
for name,relative in [('bigbrotr','bigbrotr'),('relay_registry_probe','examples/relay_registry_probe')]:
    shutil.copy2(build/'target/release'/relative,artifacts/name)
info=json.loads(subprocess.check_output([str(artifacts/'bigbrotr'),'build-info'],env=env))
assert info['source']==dict(state='builder-supplied',revision=manifest['revision'],tree=manifest['tree'],
                           dirty=False,manifest_sha256=identity['source_manifest_sha256'])
receipt=dict(identity,format='bigbrotr-registry-build/1',binary_sha256=file_hash(artifacts/'bigbrotr'),
             probe_sha256=file_hash(artifacts/'relay_registry_probe'),build_info=info)
(artifacts/'build-result.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert sources()==manifest['files_sha256']
Path('/out/qualification.json').write_text(json.dumps(dict(identity,status='PASS_REGISTRY_BUILD_ONLY'),indent=2)+'\n')
