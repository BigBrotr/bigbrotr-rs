#!/usr/bin/env python3
"""Trusted native keyfile/CLI checks; no archive worker, SQL or release claim."""
import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True
source = Path('/out/build/source')
sys.path.insert(0, str(source))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = source.parent
out = Path('/out/native-tests')
manifest = json.loads((build/'source-manifest.json').read_text())


def source_files():
    return {str(p.relative_to(source)): file_hash(p) for p in sorted(source.rglob('*')) if p.is_file()}


assert source_files() == manifest['files_sha256']
reuse = sys.argv[1:] == ['--reuse']
rotation = sys.argv[1:] == ['--rotation']
assert not sys.argv[1:] or reuse or rotation
if reuse:
    previous = Path('/reuse')
    old_manifest = json.loads((previous/'out/build/source-manifest.json').read_text())
    checks = json.loads((previous/'out/native-tests/result.json').read_text())
    built = json.loads((previous/'out/native-tests/artifact.json').read_text())
    assert checks['status'] == 'PASS' and checks['source'] == old_manifest['revision'] == built['source']
    assert built['source_manifest_sha256'] == file_hash(previous/'out/build/source-manifest.json')
    def product(files):
        # Generated reference is an actual CLI-test input. Other experiment
        # fixtures can also be included by Rust, so exclude only this recipe.
        return {name:digest for name,digest in files.items()
                if (not name.startswith('docs/') or name == 'docs/reference/configuration.md')
                and not name.startswith('experiments/cursor-keyring/')}
    assert product(old_manifest['files_sha256']) == product(manifest['files_sha256'])
    assert all(file_hash(previous/'out/build/source'/name) == digest for name,digest in old_manifest['files_sha256'].items())
    original_binary = previous/'out/native-tests/bigbrotr'
    assert file_hash(original_binary) == built['binary_sha256']
    out.mkdir()
    binary = out/'bigbrotr'
    shutil.copy2(original_binary, binary)
    assert file_hash(binary) == built['binary_sha256']
    info = json.loads(subprocess.check_output([str(binary),'build-info'],env={}))
    assert info == built['build_info']
    (out/'artifact.json').write_text(json.dumps(built,indent=2)+'\n')
    (out/'artifact-source-manifest.json').write_text(json.dumps(old_manifest,indent=2)+'\n')
    identity = dict(source=manifest['revision'],tree=manifest['tree'],
        source_manifest_sha256=file_hash(build/'source-manifest.json'),
        artifact_source=built['source'],binary_sha256=built['binary_sha256'],
        unchanged_product_files=len(product(manifest['files_sha256'])),
        scope='current practical fixture with unchanged previously built product; no new compiler/test/Clippy claim')
    (out/'reuse.json').write_text(json.dumps(identity,indent=2)+'\n')
    env = clean_environment()
else:
    env = clean_environment()
    selected, native = native_toolchain(env, 2)
    env.update(selected)
    env.update(CARGO_HOME=str(build/'cargo-home'), CARGO_TARGET_DIR=str(build/'target'),
               CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
               BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
               BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'))
    identity = dict(source=manifest['revision'], tree=manifest['tree'],
                    source_manifest_sha256=file_hash(build/'source-manifest.json'),
                    lockfile_sha256=file_hash(source/'Cargo.lock'), native_toolchain=native,
                    scope='protected local cursor key addition; no lifecycle/activation/distribution/release' if rotation else 'protected local cursor key preparation; no Control, envelope, worker or release')
    consumers = ['cursor_keyring','cursor_keyring_cli','cursor_registration','read_cursors'] if rotation else [
        'cursor_keyring','cursor_keyring_cli','secret_files','config_entrypoint','policy_seed','cli_contract']
    assert execute([
        ('build', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr']),
        ('consumers', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr',
                       *[arg for name in consumers for arg in ['--test',name]]]),
        ('collision', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'initial_publication_never']),
        ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--bin', 'bigbrotr',
                    '--test', 'cursor_keyring', '--test', 'cursor_keyring_cli', '--', '-D', 'warnings']),
    ], out, source, env, identity, 1800)['status'] == 'PASS'
    binary = out/'bigbrotr'
    shutil.copy2(build/'target/release/bigbrotr', binary)
    binary.chmod(0o500)
    info = json.loads(subprocess.check_output([str(binary), 'build-info'], env={}))
    assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'], tree=manifest['tree'],
                                 dirty=False, manifest_sha256=identity['source_manifest_sha256'])
    assert info['target'] == 'x86_64-unknown-linux-gnu' and info['archive_worker'] is None
    identity.update(binary_sha256=file_hash(binary), build_info=info)
    (out/'artifact.json').write_text(json.dumps(identity, indent=2)+'\n')
assert execute([
    ('filesystem', [sys.executable, '-B', str(source/'experiments/cursor-keyring'/('prepare_key.py' if rotation else 'cases.py')),
                    str(binary), '/out/cases', '/instrument/strace-exit-fixed']),
], out/'practical', source, env, identity, 180)['status'] == 'PASS'
assert source_files() == manifest['files_sha256']
(out/'complete.json').write_text(json.dumps(dict(identity, status='PASS', source_unchanged=True), indent=2)+'\n')
