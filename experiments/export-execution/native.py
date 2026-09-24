#!/usr/bin/env python3
"""Compile the actual501 Rust consumer; retained4e only supplies native worker/seed."""
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
build = SOURCE.parent
manifest = json.loads((build/'source-manifest.json').read_text())
def sources():
    return {str(p.relative_to(SOURCE)):file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert sources() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env,2)
env.update(selected)
env.update(CARGO_HOME=str(build/'cargo-home'),CARGO_TARGET_DIR=str(build/'target'),CARGO_NET_OFFLINE='true',
    BIGBROTR_SOURCE_REVISION=manifest['revision'],BIGBROTR_SOURCE_TREE=manifest['tree'],BIGBROTR_BUILD_PROFILE='release',
    BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'))
identity = dict(source=manifest['revision'],tree=manifest['tree'],lockfile_sha256=file_hash(SOURCE/'Cargo.lock'),
    source_manifest_sha256=file_hash(build/'source-manifest.json'),native_toolchain=native,
    scope='Current typed SQL/native/files consumer research driver; no installed automatic scheduler or signal/recovery qualification')
checks = Path('/out/adapter-checks')
assert execute([
    ('build',['cargo','build','--locked','--offline','--release','-p','bigbrotr','--bin','bigbrotr','--example','export-execution-probe']),
    ('selection-tests',['cargo','test','--locked','--offline','--release','-p','bigbrotr','--lib','export::execution::tests']),
    ('clippy',['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','--lib','--example','export-execution-probe','--','-D','warnings']),
],checks,SOURCE,env,identity,1800)['status']=='PASS'
assert '2 passed; 0 failed; 0 ignored' in (checks/'selection-tests.log').read_text()
artifacts=Path('/out/adapter-artifacts');artifacts.mkdir()
for original,name in [('bigbrotr','bigbrotr'),('examples/export-execution-probe','export-execution-probe')]:
    shutil.copy2(build/'target/release'/original,artifacts/name);(artifacts/name).chmod(0o500)
info=json.loads(subprocess.check_output([str(artifacts/'bigbrotr'),'build-info'],env={}))
assert info['source']==dict(state='builder-supplied',revision=manifest['revision'],tree=manifest['tree'],dirty=False,manifest_sha256=identity['source_manifest_sha256'])
assert info['archive_worker'] is None and info['target']=='x86_64-unknown-linux-gnu'
identity.update(build_info=info,artifacts={p.name:file_hash(p) for p in artifacts.iterdir()})
(checks/'artifacts.json').write_text(json.dumps(identity,indent=2)+'\n')
assert sources()==manifest['files_sha256']
