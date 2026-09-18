"""Scoped exact-source archive identity and connected signed preparation checks.

Run inside a newly owned native lab. /source is the clean exact Git checkout,
/out/build/source its independently verified readonly archive, and /qualified a
retained qualified build mounted readonly. Writable target/Cargo caches are copies.
This validates no publication receipt or catalog transaction.
"""
import argparse
import json
from pathlib import Path
import platform
import re
import shutil
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, '/source')
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute, workspace_test_environment

parser = argparse.ArgumentParser()
parser.add_argument('--revision', required=True)
args = parser.parse_args()
assert re.fullmatch('[0-9a-f]{40}', args.revision)
build = Path('/out/build')
source = build / 'source'
qualified = Path('/qualified')
manifest = json.loads((build / 'source-manifest.json').read_text())
assert manifest['revision'] == args.revision


def inventory():
    return {str(p.relative_to(source)): file_hash(p)
            for p in sorted(source.rglob('*')) if p.is_file()}


assert inventory() == manifest['files_sha256']
old_receipt = json.loads((qualified / 'build-result.json').read_text())
worker_env = workspace_test_environment(qualified)
assert worker_env['BIGBROTR_ARCHIVE_TEST_WORKER_SHA256'] == '3133542df86c6d2992911434aff6292b232c6ea1f06235ed8e0d437765e3c136'
env = clean_environment()
selected, native = native_toolchain(env, 2)
assert native == old_receipt['native_toolchain']
env.update(selected)
env.update(worker_env)
env.update(CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_HOME=str(build / 'cargo-home'), CARGO_NET_OFFLINE='true',
           BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'],
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'),
           BIGBROTR_BUILD_PROFILE='release', PYTHONDONTWRITEBYTECODE='1')
out = Path('/out/native-tests')
checks = [
    ('membership', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr-model', '--test', 'archive_batch']),
    ('bytes', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr-model', '--lib',
               'fingerprint::tests::archive_bytes_match_literal_multi_range_vector', '--', '--exact']),
    ('signed', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr', '--lib',
                'storage::rocks::store::admission_tests::signed_pending_reopens_and_reaches_the_supervised_native_worker_without_acknowledgement',
                '--', '--exact', '--test-threads=1']),
    ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr-model', '-p', 'bigbrotr',
                '--lib', '--tests', '--', '-D', 'warnings']),
]
result = execute(checks, out, source, env, {
    'source': manifest['revision'], 'tree': manifest['tree'], 'native_toolchain': native,
    'host': {'os': platform.system(), 'architecture': platform.machine()},
    'packaged_worker': worker_env, 'worker_build_source': old_receipt['build_info']['source'],
    'scope': 'Archive membership identity and actual signed Rocks reopen/private worker preparation; no catalog/ACK/reclaim',
}, 1800)
assert inventory() == manifest['files_sha256']
assert workspace_test_environment(qualified) == worker_env
assert result['status'] == 'PASS'
for name, expected in [('membership', 5), ('bytes', 1), ('signed', 1)]:
    summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',
                           (out / (name + '.log')).read_text())
    assert summaries == [(str(expected), '0', '0')], (name, summaries)
# Retain exact executed test artifacts independently of subsequent cache use.
artifacts = []
for name in ['membership', 'bytes', 'signed']:
    log = (out / (name + '.log')).read_text()
    executable = Path(re.search(r'Running .* \(([^\n]+)\)', log).group(1))
    if not executable.is_absolute():
        executable = source / executable
    retained = out / (name + '-test-executable')
    shutil.copy2(executable, retained)
    retained.chmod(0o500)
    artifacts.append({'case': name, 'path': str(retained), 'sha256': file_hash(retained)})
(out / 'artifact-identities.json').write_text(json.dumps(artifacts, indent=2) + '\n')
