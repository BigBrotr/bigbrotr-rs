#!/usr/bin/env python3
"""Exact-Git native session storage tests in the separately inventoried Tux lab."""
import json
import sys
from pathlib import Path
sys.dont_write_bytecode = True
source = Path('/out/build/source')
sys.path.insert(0, str(source))
from tools.build import clean_environment, native_toolchain, file_hash
from tools.archive_bundle import build_environment, install_bundle
from tools.ci import execute
build, out, bundle = source.parent, Path('/out/native-tests'), Path('/out/archive-worker')
manifest = json.loads((build / 'source-manifest.json').read_text())
def files():
    return {str(p.relative_to(source)): file_hash(p) for p in sorted(source.rglob('*')) if p.is_file()}
assert files() == manifest['files_sha256']
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
checks = [
    ('build', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--example', 'browser_sessions_probe']),
    ('schema-and-tokens', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--test', 'control_schema', '--test', 'management_sessions']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--example', 'browser_sessions_probe', '--test', 'management_sessions', '--', '-D', 'warnings']),
    ('native-sessions', [str(build / 'target/release/examples/browser_sessions_probe'), 'sessions']),
]
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                scope='actual fenced Control session storage; no HTTP or live action grant claim')
result = execute(checks, out, source, env, identity, 1800)
result.update(source_unchanged=files() == manifest['files_sha256'],
              artifact_sha256=file_hash(build / 'target/release/examples/browser_sessions_probe'))
(out / 'session-result.json').write_text(json.dumps(result, indent=2) + '\n')
raise SystemExit(0 if result['status'] == 'PASS' and result['source_unchanged'] else 1)
