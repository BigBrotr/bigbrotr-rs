#!/usr/bin/env python3
"""Build only the finite outcome client; refuse changed product/dependency inputs.

Runs inside the owned native runner against a separate exact Git export. The
qualified product binaries and their source export are never overwritten.
"""
import json
import os
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.archive_bundle import build_environment
from tools.ci import execute

work = SOURCE.parent
assert work == Path('/out/outcome-probe')
manifest = json.loads((work/'source-manifest.json').read_text())
baseline = json.loads(Path('/out/build/source-manifest.json').read_text())
assert baseline['revision'] == '9537667dd140baac68ca79ecb56ea7c6ecce081f'
def files():
    return {str(p.relative_to(SOURCE)):file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert files() == manifest['files_sha256']
def fixture(name):
    return name.startswith('experiments/producer-requests/') or name in {
        'apps/bigbrotr/examples/ingest_bootstrap_probe.rs',
        'apps/bigbrotr/examples/support/ingest_event.rs',
        'docs/implementation/progress.md', 'docs/research/laboratory-inventory.json',
    }
assert {k:v for k,v in manifest['files_sha256'].items() if not fixture(k)} == {
    k:v for k,v in baseline['files_sha256'].items() if not fixture(k)}
env = clean_environment()
selected, native = native_toolchain(env,2); env.update(selected)
env.update(build_environment(Path('/out/archive-worker')))
ready = json.loads(Path('/out/native-tests/ready.json').read_text())
assert ready['source'] == baseline['revision'] and ready['status'] == 'READY'
worker = Path('/out/archive-worker/bigbrotr-archive-worker')
assert file_hash(worker) == ready['worker']
env.update(CARGO_HOME='/out/build/cargo-home', CARGO_TARGET_DIR='/out/build/target',
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(work/'source-manifest.json'),
           BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker.stat().st_size),
           BIGBROTR_ARCHIVE_WORKER_SHA256=ready['worker'])
identity = dict(source=manifest['revision'], baseline_product=baseline['revision'],
                source_manifest_sha256=file_hash(work/'source-manifest.json'), native_toolchain=native)
assert execute([
    ('outcome-client', ['cargo','build','--locked','--release','-p','bigbrotr','--example','ingest_bootstrap_probe']),
    ('outcome-client-clippy', ['cargo','clippy','--locked','--release','-p','bigbrotr','--example','ingest_bootstrap_probe','--','-D','warnings']),
],work/'build',SOURCE,env,identity,1200)['status'] == 'PASS'
assert files() == manifest['files_sha256']
artifact = work/'ingest-bootstrap-probe'; assert not artifact.exists()
shutil.copyfile('/out/build/target/release/examples/ingest_bootstrap_probe',artifact)
artifact.chmod(0o500)
identity.update(status='READY', sha256=file_hash(artifact), source_unchanged=True)
(work/'ready.json').write_text(json.dumps(identity,indent=2)+'\n')
print(json.dumps(dict(status='READY',source=manifest['revision'],sha256=identity['sha256'])))
