#!/usr/bin/env python3
"""Exact-source native executable HTTP session tests in an inventoried Tux lab."""
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
    ('build', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', '--example', 'browser_http_probe']),
    ('authority-clock', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'runtime::policy_authority::tests']),
    ('http-sockets', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', 'interfaces::']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--bin', 'bigbrotr', '--example', 'browser_http_probe', '--', '-D', 'warnings']),
    ('native-http', [str(build / 'target/release/examples/browser_http_probe'), str(build / 'target/release/bigbrotr'), '/out/http-fixture']),
]
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                scope='actual executable, TCP clients and PostgreSQL browser sessions; no browser UI or action grant qualification')
result = execute(checks, out, source, env, identity, 1800)
result.update(source_unchanged=files() == manifest['files_sha256'],
              artifacts={name:file_hash(build / 'target/release' / name) for name in ['bigbrotr', 'examples/browser_http_probe'] if (build / 'target/release' / name).is_file()})
(out / 'http-result.json').write_text(json.dumps(result, indent=2) + '\n')
raise SystemExit(0 if result['status'] == 'PASS' and result['source_unchanged'] else 1)
