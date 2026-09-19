#!/usr/bin/env python3
"""Build the retained pre-ledger client from an immutable exact Git export.

Run only in the inventoried native builder with /old-source mounted read-only.
No changes to the old source or its dependencies are permitted.
"""
import hashlib
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/old-source')
OUT = Path('/out/compatibility-build')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.archive_bundle import build_environment
from tools.ci import execute


def main():
    manifest_path = OUT/'source-manifest.json'
    manifest = json.loads(manifest_path.read_text())
    assert manifest['revision'] == 'a1da96a09ee73722d26003dedf793317b7bdf7e1'
    def files():
        return {str(p.relative_to(SOURCE)): file_hash(p)
                for p in sorted(SOURCE.rglob('*')) if p.is_file()}
    assert files() == manifest['files_sha256']
    # These are the old source's own build helpers and closed native asset tuple.
    env = clean_environment()
    selected, native = native_toolchain(env, 2)
    env.update(selected)
    env.update(build_environment(Path('/assets')))
    worker = Path('/assets/bigbrotr-archive-worker')
    env.update(CARGO_HOME=str(OUT/'cargo-home'), CARGO_TARGET_DIR=str(OUT/'target'),
               CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
               BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
               BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(manifest_path),
               BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker.stat().st_size),
               BIGBROTR_ARCHIVE_WORKER_SHA256=file_hash(worker))
    identity = dict(source=manifest['revision'], tree=manifest['tree'],
                    source_manifest_sha256=file_hash(manifest_path), native_toolchain=native,
                    harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    scope='pre-ledger compatibility client; no old product requalification')
    result = execute([('old-fixture', ['cargo', 'build', '--locked', '--release',
                      '-p', 'bigbrotr', '--example', 'ingest_bootstrap_probe'])],
                     OUT/'checks', SOURCE, env, identity, 1200)
    assert result['status'] == 'PASS'
    assert files() == manifest['files_sha256']
    target = OUT/'old-ingest-probe'
    assert not target.exists()
    shutil.copy2(OUT/'target/release/examples/ingest_bootstrap_probe', target)
    target.chmod(0o500)
    identity.update(status='READY', sha256=file_hash(target), bytes=target.stat().st_size,
                    lockfile_sha256=file_hash(SOURCE/'Cargo.lock'), source_unchanged=True)
    (OUT/'ready.json').write_text(json.dumps(identity, indent=2)+'\n')
    print(json.dumps(identity), flush=True)


if __name__ == '__main__':
    main()
