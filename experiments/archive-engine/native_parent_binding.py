"""Exact-revision native parent binding, including stopped-worker SIGKILL recovery."""
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

build = Path('/out/build')
source = build / 'source'
out = Path('/out/native-tests')
manifest = json.loads((build / 'source-manifest.json').read_text())
receipt = json.loads((build / 'build-result.json').read_text())
assert manifest['revision'] == 'e4e2a30160a5119195e7ba5b31b82b1482a5c432'
assert file_hash(build / 'source-manifest.json') == receipt['source_manifest_sha256']


def inventory():
    return {str(p.relative_to(source)): file_hash(p)
            for p in sorted(source.rglob('*')) if p.is_file()}


assert inventory() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
assert native == receipt['native_toolchain']
env.update(selected)
worker_env = workspace_test_environment(build)
env.update(worker_env)
env.update(CARGO_TARGET_DIR=str(build / 'target'), CARGO_HOME=str(build / 'cargo-home'),
           BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'],
           BIGBROTR_SOURCE_MANIFEST_SHA256=receipt['source_manifest_sha256'],
           BIGBROTR_BUILD_PROFILE='release', PYTHONDONTWRITEBYTECODE='1')
checks = [
    ('worker', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker',
                '--test', 'handoff', '--test', 'parent_binding', '--', '--test-threads=1']),
    ('signed', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib',
                'storage::rocks::store::admission_tests::signed_pending_reopens_and_reaches_the_supervised_native_worker_without_acknowledgement',
                '--', '--exact', '--test-threads=1']),
    ('parent-death', ['python3', 'experiments/archive-engine/parent_death.py',
                      '--bundle', str(build / 'archive-worker'),
                      '--output', str(out / 'parent-death'),
                      '--worker-sha256', receipt['archive_worker']['sha256']]),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr',
                '-p', 'bigbrotr-archive-worker', '--all-targets', '--', '-D', 'warnings']),
]
result = execute(checks, out, source, env,
                 {'source': manifest['revision'], 'native_toolchain': native,
                  'host': {'os': platform.system(), 'architecture': platform.machine()},
                  'packaged_worker': worker_env,
                  'scope': 'Private parent binding; no persisted recovery or PG fence'}, 1800)
assert inventory() == manifest['files_sha256']
assert workspace_test_environment(build) == worker_env
if result['status'] != 'PASS':
    raise SystemExit(1)
for name, expected in {'worker': [('1', '0', '0')] * 2, 'signed': [('1', '0', '0')]}.items():
    summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',
                           (out / (name + '.log')).read_text())
    assert summaries == expected, (name, summaries)
death = json.loads((out / 'parent-death/result.json').read_text())
assert death['status'] == 'PASS' and not death['negative_control']
assert death['stopped_before_parent_kill'] and death['exited_after_parent_kill']
assert death['worker_sha256'] == receipt['archive_worker']['sha256']
assert json.loads((out / 'parent-death/cleanup.json').read_text()) == {
    'parent_reaped': True, 'worker_reaped': True}
artifacts = {}
(out / 'artifacts').mkdir()
for name in ['worker', 'signed']:
    for relative in re.findall(r'Running [^\n]+ \(([^\n]+)\)', (out / (name + '.log')).read_text()):
        executable = Path(relative)
        if not executable.is_absolute():
            executable = source / executable
        with executable.open('rb') as stream:
            header = stream.read(20)
        assert header[:5] == b'\x7fELF\x02' and int.from_bytes(header[18:20], 'little') == 62
        artifacts[executable.name] = {'sha256': file_hash(executable),
                                      'bytes': executable.stat().st_size}
        shutil.copy2(executable, out / 'artifacts' / executable.name)
assert len(artifacts) == 3
(out / 'artifacts.json').write_text(json.dumps(artifacts, indent=2) + '\n')
worker = build / 'target/release/bigbrotr-archive-worker'
executed = {'sha256': file_hash(worker), 'packaged_sha256': receipt['archive_worker']['sha256']}
shutil.copy2(worker, out / 'artifacts/executed-bigbrotr-archive-worker')
(out / 'executed-worker.json').write_text(json.dumps(executed, indent=2) + '\n')
print(json.dumps({'status': 'PASS', 'source': manifest['revision'],
                  'rust_cases': 3, 'stopped_parent_death': death, 'artifacts': artifacts}))
