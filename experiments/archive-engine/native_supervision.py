"""Exact-revision Linux qualification of private archive process supervision.

Run only inside the inventoried trusted native lab with /source read-only and
the preceding exact build under /out/build. This is not a complete CI/release.
"""
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
assert manifest['revision'] == 'c365053afae92eb2cf8b10760e0d8bec2f42a897'
assert file_hash(build / 'source-manifest.json') == receipt['source_manifest_sha256']


def inventory():
    return {str(p.relative_to(source)): file_hash(p)
            for p in sorted(source.rglob('*')) if p.is_file()}


assert inventory() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
assert native == receipt['native_toolchain']
env.update(selected)
# Exercise the same positive receipt/bundle verification used by workspace CI.
worker_env = workspace_test_environment(build)
env.update(worker_env)
env.update(CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_HOME=str(build / 'cargo-home'),
           BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'],
           BIGBROTR_SOURCE_MANIFEST_SHA256=receipt['source_manifest_sha256'],
           BIGBROTR_BUILD_PROFILE='release', PYTHONDONTWRITEBYTECODE='1')
checks = [
    ('archive', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr',
                 '--lib', 'storage::archive', '--', '--test-threads=1', '--nocapture']),
    ('signed', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib',
                'storage::rocks::store::admission_tests::signed_pending_reopens_and_reaches_the_supervised_native_worker_without_acknowledgement',
                '--', '--exact', '--test-threads=1']),
    ('cli', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr',
             '--test', 'cli_contract']),
    ('ci-controls', ['python3', '-m', 'unittest', 'discover', '-s',
                     'tests/acceptance', '-p', 'test_ci.py', '-v']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr',
                '--all-targets', '--', '-D', 'warnings']),
]
result = execute(checks, out, source, env,
                 {'source': manifest['revision'], 'native_toolchain': native,
                  'host': {'os': platform.system(), 'architecture': platform.machine()},
                  'packaged_worker': worker_env,
                  'scope': 'Private preparation supervision only'}, 1800)
assert inventory() == manifest['files_sha256']
assert workspace_test_environment(build) == worker_env
if result['status'] != 'PASS':
    raise SystemExit(1)
for name, expected in {'archive': 10, 'signed': 1, 'cli': 3}.items():
    summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',
                           (out / (name + '.log')).read_text())
    assert summaries == [(str(expected), '0', '0')], (name, summaries)
controls = (out / 'ci-controls.log').read_text()
assert re.search(r'Ran 5 tests in [\d.]+s\s+OK\s*$', controls)
pipes = re.findall(r'full-pipe stderr=(false|true) capacity_bytes=(\d+) termination_us=(\d+)',
                   (out / 'archive.log').read_text())
assert [p[0] for p in pipes] == ['false', 'true']
assert all(int(capacity) > 0 and 0 < int(micros) < 2_000_000 for _, capacity, micros in pipes)
(out / 'full-pipes.json').write_text(json.dumps([
    {'stderr': stderr == 'true', 'capacity_bytes': int(capacity),
     'termination_us': int(micros)} for stderr, capacity, micros in pipes], indent=2) + '\n')

artifacts = {}
(out / 'artifacts').mkdir()
for name in ['archive', 'signed', 'cli']:
    for relative in re.findall(r'Running [^\n]+ \(([^\n]+)\)', (out / (name + '.log')).read_text()):
        executable = Path(relative)
        if not executable.is_absolute():
            executable = source / executable
        with executable.open('rb') as stream:
            header = stream.read(20)
        assert header[:5] == b'\x7fELF\x02' and int.from_bytes(header[18:20], 'little') == 62
        identity = {'sha256': file_hash(executable), 'bytes': executable.stat().st_size}
        if executable.name in artifacts:
            assert artifacts[executable.name] == identity
        artifacts[executable.name] = identity
        shutil.copy2(executable, out / 'artifacts' / executable.name)
assert len(artifacts) == 2
(out / 'artifacts.json').write_text(json.dumps(artifacts, indent=2) + '\n')
print(json.dumps({'status': 'PASS', 'source': manifest['revision'],
                  'rust_cases': 14, 'python_cases': 5, 'artifacts': artifacts,
                  'packaged_worker_sha256': receipt['archive_worker']['sha256']}))
