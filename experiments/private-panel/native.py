#!/usr/bin/env python3
"""Bounded native stages; the host owns a fresh container for each invocation."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True
source = Path('/out/build/source')
sys.path.insert(0, str(source))
from tools.build import clean_environment, native_toolchain, file_hash
from tools.archive_bundle import build_environment, install_bundle
from tools.ci import execute

manifest = json.loads(Path('/out/build/source-manifest.json').read_text())
mode = sys.argv[1]
assert mode in ['rust', 'frontend', 'browser']


def verify_originals(root):
    return all((root / name).is_file() and file_hash(root / name) == digest
               for name, digest in manifest['files_sha256'].items())


assert verify_originals(source)
identity = dict(source=manifest['revision'], tree=manifest['tree'], stage=mode,
                cargo_lock=file_hash(source / 'Cargo.lock'),
                pnpm_lock=file_hash(source / 'pnpm-lock.yaml'))
env = clean_environment()
env.update(PYTHONDONTWRITEBYTECODE='1')
if mode == 'rust':
    build = source.parent
    bundle = Path('/out/archive-worker')
    bundle.mkdir()
    install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
    selected, identity['native_toolchain'] = native_toolchain(env, 2)
    env.update(selected)
    env.update(build_environment(bundle))
    env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
               CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
               BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
               BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
    result = execute([
        ('build', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', '--example', 'browser_http_probe']),
        ('management', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', 'interfaces::management::']),
        ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', '--example', 'browser_http_probe', '--', '-D', 'warnings']),
    ], Path('/out/rust-checks'), source, env, identity, 1800)
    result['artifacts'] = {name: file_hash(build / 'target/release' / name)
                           for name in ['bigbrotr', 'examples/browser_http_probe']
                           if (build / 'target/release' / name).is_file()}
elif mode == 'frontend':
    frontend = Path('/out/frontend-source')
    assert verify_originals(frontend)
    env.update(PATH='/tools/node/bin:' + env['PATH'], HOME='/tmp',
               PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD='1', CI='true',
               BIGBROTR_BUILD_REVISION=manifest['revision'])
    node = '/tools/node/bin/node'
    pnpm = [node, '/tools/pnpm/bin/pnpm.cjs']
    assert subprocess.check_output([node, '--version'], text=True).strip() == 'v26.4.0'
    assert subprocess.check_output(pnpm + ['--version'], env=env, text=True).strip() == '9.15.9'
    identity.update(node_sha256=file_hash(Path(node)), pnpm_sha256=file_hash(Path(pnpm[1])))
    result = execute([
        ('install', pnpm + ['install', '--frozen-lockfile', '--ignore-scripts', '--strict-peer-dependencies', '--store-dir', '/out/pnpm-store']),
        ('contracts', pnpm + ['contracts:check']),
        ('client-types', pnpm + ['check:client']),
        ('client', pnpm + ['test:client']),
        ('admin-types', pnpm + ['--filter', '@bigbrotr/admin', 'check']),
        ('admin-unit', pnpm + ['--filter', '@bigbrotr/admin', 'test:unit']),
    ], Path('/out/frontend-checks'), frontend, env, identity, 600)
    result['bundles'] = {}
    if result['status'] == 'PASS':
        for mount, base in [('root', ''), ('control', '/control')]:
            env['BIGBROTR_ADMIN_BASE'] = base
            built = execute([('build', pnpm + ['--filter', '@bigbrotr/admin', 'build'])],
                            Path('/out') / ('frontend-' + mount), frontend, env, identity, 180)
            if built['status'] != 'PASS':
                result['status'] = 'FAIL'
                break
            target = Path('/out/bundles') / mount
            target.parent.mkdir(exist_ok=True)
            shutil.copytree(frontend / 'apps/admin/build', target)
            assets = json.loads((target / 'manifest.json').read_text())
            assert assets['base_path'] == base + '/'
            assert len(assets['files']) > 0
            for entry in assets['files']:
                path = target / entry['path']
                assert path.stat().st_size == entry['bytes'] and file_hash(path) == entry['sha256']
            result['bundles'][mount] = dict(manifest_sha256=file_hash(target / 'manifest.json'),
                                           files=len(assets['files']), bytes=sum(f['bytes'] for f in assets['files']))
    result['frontend_originals_unchanged'] = verify_originals(frontend)
    if not result['frontend_originals_unchanged']:
        result['status'] = 'FAIL'
else:
    engine, mount = sys.argv[2:]
    assert engine in ['chromium', 'firefox', 'webkit'] and mount in ['root', 'control']
    frontend = Path('/out/frontend-source')
    assert verify_originals(frontend)
    rust = json.loads(Path('/out/rust-result.json').read_text())
    built = json.loads(Path('/out/frontend-result.json').read_text())
    for result in [rust, built]:
        assert result['status'] == 'PASS' and result['source_unchanged']
        assert all(result[key] == identity[key] for key in ['source', 'tree', 'cargo_lock', 'pnpm_lock'])
    assert set(rust['artifacts']) == {'bigbrotr', 'examples/browser_http_probe'}
    for name, digest in rust['artifacts'].items():
        assert file_hash(Path('/out/build/target/release') / name) == digest
    bundle = Path('/out/bundles') / mount
    assert file_hash(bundle / 'manifest.json') == built['bundles'][mount]['manifest_sha256']
    for entry in json.loads((bundle / 'manifest.json').read_text())['files']:
        assert file_hash(bundle / entry['path']) == entry['sha256']
        assert (bundle / entry['path']).stat().st_size == entry['bytes']
    assert file_hash(Path('/tools/node/bin/node')) == built['node_sha256']
    registry = frontend / 'node_modules/.pnpm/playwright-core@1.63.0/node_modules/playwright-core/browsers.json'
    assert file_hash(registry) == '545d52f8382c391e605562c330e9c1c534a16045898203037a49bb8bd769a946'
    env.update(PATH='/tools/node/bin:' + env['PATH'], HOME='/tmp',
               PLAYWRIGHT_BROWSERS_PATH='/ms-playwright',
               LD_LIBRARY_PATH='/out/archive-worker/lib')
    identity.update(engine=engine, mount=mount, artifacts=rust['artifacts'],
                    bundle=built['bundles'][mount], node_sha256=built['node_sha256'],
                    browser_registry_sha256=file_hash(registry))
    result = execute([('browser', [
        '/out/build/target/release/examples/browser_http_probe', '/out/build/target/release/bigbrotr',
        '/case/fixture', '--panel', engine, mount, '/out/bundles/' + mount,
        '/out/frontend-source/experiments/private-panel/browser.mjs',
    ])], Path('/case/checks'), source, env, identity, 240)
    # Docker's init owns orphan reaping. Do not infer browser collection from
    # Node exit: detached processes can survive its SIGKILL, including SIGSTOP.
    deadline = time.monotonic() + 3
    while True:
        remaining = []
        for directory in Path('/proc').iterdir():
            if directory.name.isdecimal() and int(directory.name) not in [1, os.getpid()]:
                try:
                    stat = (directory / 'stat').read_text()
                    remaining.append(dict(pid=int(directory.name), stat=stat))
                except FileNotFoundError:
                    pass
        if not remaining or time.monotonic() >= deadline:
            break
        time.sleep(.05)
    result['remaining_processes_before_container_exit'] = remaining
    if remaining:
        result['status'] = 'FAIL'
result['source_unchanged'] = verify_originals(source)
if not result['source_unchanged']:
    result['status'] = 'FAIL'
output = Path('/case/native-result.json') if mode == 'browser' else Path('/out') / (mode + '-result.json')
output.write_text(json.dumps(result, indent=2) + '\n')
raise SystemExit(0 if result['status'] == 'PASS' else 1)
