#!/usr/bin/env python3
"""Build a fixture-only old writer library against actual new-client closed stores.

The sole old-source change is a cfg(test) hook and two new fixture files. Verify
that assertion against the complete baseline Git-export manifest before building.
No source edits, engine execution, credential access or existing artifact writes.
"""
import hashlib
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
WORK = Path('/out/old-closing-probe')
SOURCE = WORK/'source'
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.archive_bundle import build_environment
from tools.ci import execute

BASELINE = '442f3d878b3215e421a71160b3613b7ab0c93c7b'
STORE = 'apps/bigbrotr/src/storage/rocks/store.rs'
HOOK = b'#[cfg(test)]\n#[path = "old_writer_closing_tests.rs"]\nmod old_writer_closing_tests;\n\n'
EXTRAS = {'apps/bigbrotr/src/storage/rocks/old_writer_closing_tests.rs',
          'apps/bigbrotr/src/storage/rocks/old_writer_closing_originals.json'}
manifest = json.loads((WORK/'source-manifest.json').read_text())
baseline = json.loads((WORK/'baseline-source-manifest.json').read_text())
assert baseline['revision'] == BASELINE
assert set(manifest['files_sha256']) == set(baseline['files_sha256']) | EXTRAS
assert not EXTRAS & set(baseline['files_sha256'])
for name, digest in baseline['files_sha256'].items():
    raw = (SOURCE/name).read_bytes()
    if name == STORE:
        assert raw.count(HOOK) == 1
        raw = raw.replace(HOOK, b'', 1)
    assert hashlib.sha256(raw).hexdigest() == digest, name

def files():
    return {str(p.relative_to(SOURCE)):file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert files() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2); env.update(selected)
env.update(build_environment(Path('/out/archive-worker')))
ready = json.loads(Path('/out/native-tests/ready.json').read_text())
worker = Path('/out/archive-worker/bigbrotr-archive-worker')
assert ready['status'] == 'READY' and file_hash(worker) == ready['worker']
env.update(CARGO_HOME='/out/build/cargo-home', CARGO_TARGET_DIR='/out/build/target',
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(WORK/'source-manifest.json'),
           BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker.stat().st_size),
           BIGBROTR_ARCHIVE_WORKER_SHA256=ready['worker'])
identity = dict(source=manifest['revision'], baseline_product=BASELINE,
                source_manifest_sha256=file_hash(WORK/'source-manifest.json'),
                baseline_source_manifest_sha256=file_hash(WORK/'baseline-source-manifest.json'),
                native_toolchain=native, baseline_files_identical_except_exact_test_hook=True,
                builder_sha256=file_hash(Path(__file__)))
assert execute([
    ('old-closing-build', ['cargo','test','--locked','--release','-p','bigbrotr','--lib',
                         '--no-run','--message-format=json']),
], WORK/'build', SOURCE, env, identity, 1200)['status'] == 'PASS'
# Cargo supplies the exact executable path. Never select an arbitrary stale
# artifact from the shared target directory by timestamp or filename glob.
messages = []
for line in (WORK/'build/old-closing-build.log').read_text().splitlines():
    if line.startswith('{'):
        row = json.loads(line)
        if row.get('reason') == 'compiler-artifact' and row.get('executable') and row.get('target',{}).get('name') == 'bigbrotr' and row.get('profile',{}).get('test'):
            messages.append(row)
assert len(messages) == 1
artifact = WORK/'old-closing-tests'; assert not artifact.exists()
shutil.copyfile(messages[0]['executable'], artifact); artifact.chmod(0o500)
assert files() == manifest['files_sha256']
identity.update(status='READY',sha256=file_hash(artifact),bytes=artifact.stat().st_size,source_unchanged=True)
(WORK/'ready.json').write_text(json.dumps(identity,indent=2)+'\n')
print(json.dumps(identity))
