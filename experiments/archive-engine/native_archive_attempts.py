#!/usr/bin/env python3
"""Exact-source native Control archive intent checks; no catalog publication."""
import json
from pathlib import Path
import re
import shutil
import sys
sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, native_toolchain, file_hash
from tools.ci import execute
build = SOURCE.parent
manifest = json.loads((build / 'source-manifest.json').read_text())
def inventory():
    return {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert inventory() == manifest['files_sha256']
env = clean_environment()
native_env, native = native_toolchain(env, 2)
env.update(native_env)
env.update(CARGO_HOME=str(build/'cargo-home'), CARGO_TARGET_DIR=str(build/'target'),
    BIGBROTR_SOURCE_REVISION=manifest['revision'], BIGBROTR_SOURCE_TREE=manifest['tree'],
    BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'), BIGBROTR_BUILD_PROFILE='release')
out = Path('/out/native-tests')
checks = [
    ('tests', ['cargo','test','--locked','--offline','--release','-p','bigbrotr','--test','archive_attempt','--test','control_schema']),
    ('build', ['cargo','build','--locked','--offline','--release','-p','bigbrotr','--example','archive_attempt_probe']),
    ('clippy', ['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','--lib','--tests','--examples','--','-D','warnings']),
]
result = execute(checks,out,SOURCE,env,{'source':manifest['revision'],'native_toolchain':native},1800)
assert result['status']=='PASS'
assert re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', (out/'tests.log').read_text()) == [('1','0','0'),('12','0','0')]
probe=out/'archive-attempt-probe'
shutil.copy2(build/'target/release/examples/archive_attempt_probe',probe)
probe.chmod(0o500)
identity={'source':manifest['revision'],'sha256':file_hash(probe),'native_toolchain':native}
(out/'probe-identity.json').write_text(json.dumps(identity,indent=2)+'\n')
result=execute([('adapter',[str(probe)])],out/'adapter',SOURCE,env,identity,180)
assert result['status']=='PASS'
assert inventory()==manifest['files_sha256']
print(json.dumps({'status':'PASS','source':manifest['revision'],'probe':identity['sha256']}))
