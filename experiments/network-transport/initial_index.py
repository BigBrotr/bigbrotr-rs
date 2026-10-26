"""Actual application Index build/tests; no engine, installed or release claim."""
import json
from pathlib import Path
import re
import shutil
import time

from tools.build import file_hash
from tools.ci import execute

STATUS = 'PASS_INITIAL_INDEX_APPLICATION_COMPONENTS'
GROUPS = {
    'clickhouse': 'storage::clickhouse::',
    'indexer': 'storage::indexer::',
    'index-publication': 'storage::postgres::control_owner::index_publication::',
    'postgres-route': 'storage::postgres::transport::route_tests::',
    'archive-driver-unpolled': 'storage::archive::lifetime::tests::catalog_transport_retains_custody_even_before_its_future_is_polled',
    'archive-driver-drop': 'storage::archive::lifetime::tests::cancelled_transport_is_destroyed_before_its_custody_descriptor',
}
# A broad filter must not pass when a new owning module was never wired in.
REQUIRED_MODULES = (
    'storage::clickhouse::adapter::operation::tests::',
    'storage::clickhouse::adapter::readback::tests::',
    'storage::clickhouse::adapter::recovery::tests::',
    'storage::clickhouse::schema::tests::',
    'storage::clickhouse::lane::tests::',
    'storage::clickhouse::rows::tests::',
    'storage::clickhouse::transport::tests::',
    'storage::clickhouse::transport::metadata_tests::',
    'storage::indexer::plan::tests::',
    'storage::indexer::custody::tests::',
    'storage::indexer::state::tests::',
)


def listed_tests(text):
    names = re.findall(r'^([\w:]+): test$', text, re.M)
    summaries = re.findall(r'^(\d+) tests?, (\d+) benchmarks?$', text, re.M)
    if (len(summaries) != 1 or summaries[0] != (str(len(names)), '0')
            or not names or len(set(names)) != len(names)):
        raise ValueError('invalid compiled test inventory')
    return sorted(names)


def selected_tests(names):
    selected = {name: [test for test in names if test.startswith(prefix)]
                for name, prefix in GROUPS.items()}
    if (not all(selected.values())
            or any(not any(test.startswith(prefix) for test in names)
                   for prefix in REQUIRED_MODULES)):
        raise ValueError('required Index test module is absent from actual application')
    return selected


def completed_tests(text, expected):
    passed = re.findall(r'^test ([\w:]+) \.\.\. ok$', text, re.M)
    summaries = re.findall(
        r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; '
        r'(\d+) measured; (\d+) filtered out; finished in .+$', text, re.M)
    if (len(summaries) != 1 or summaries[0][:4] != (str(len(expected)), '0', '0', '0')
            or sorted(passed) != sorted(expected) or not expected):
        raise ValueError('actual executed tests differ from the compiled inventory')


def retain_executable(log, kind, test, destination, target):
    candidates = []
    for line in log.read_text().splitlines():
        if not line.startswith('{'):
            continue
        item = json.loads(line)
        if (item.get('reason') == 'compiler-artifact' and item.get('executable')
                and item['target']['name'] == 'bigbrotr'
                and item['target']['kind'] == [kind] and item['profile']['test'] is test):
            candidates.append(Path(item['executable']))
    if len(candidates) != 1:
        raise ValueError('actual application executable is absent or ambiguous')
    executable = candidates[0]
    if (not executable.is_relative_to(target) or executable.is_symlink()
            or not executable.resolve(strict=True).is_relative_to(target.resolve(strict=True))
            or not executable.is_file() or destination.exists()):
        raise ValueError('invalid retained executable boundary')
    digest = file_hash(executable)
    shutil.copy2(executable, destination)
    if file_hash(destination) != digest:
        raise ValueError('retained executable differs from compiler output')
    return digest


def run(source, build, env, identity, sources):
    original = sources()
    identity = dict(identity, scope='Actual bigbrotr library and CLI build, focused Index/PG route unit tests and strict Clippy; no stubs, engine transactions, installed service, release or capacity qualification',
                    profile='initial-index', archive_worker='not built or bound')
    output = build.parent
    qualification = output / 'qualification.json'
    report = dict(identity, status='RUNNING', not_run=[
        'ClickHouse engine/schema/settings/insert/readback and PostgreSQL transactions',
        'Archive worker build or binding', 'Installed service/recovery/outage and release',
        'Full workspace/integration tests and capacity'])
    deadline = time.monotonic() + 1800

    def remaining(maximum):
        return max(0, min(maximum, deadline - time.monotonic()))

    def record():
        qualification.write_text(json.dumps(report, indent=2) + '\n')

    record()
    try:
        packages = ['-p', 'bigbrotr']
        common = ['--locked', '--offline', '--release', *packages]
        checks = output / 'build-checks'
        commands = [
            ('compile', ['cargo', 'build', *common, '--lib', '--bin', 'bigbrotr', '--message-format=json']),
            ('compile-tests', ['cargo', 'test', *common, '--lib', '--no-run', '--message-format=json']),
            ('clippy', ['cargo', 'clippy', *common, '--lib', '--bin', 'bigbrotr', '--', '-D', 'warnings']),
        ]
        if execute(commands, checks, source, env, identity, remaining(1800))['status'] != 'PASS':
            raise ValueError('actual application build or Clippy failed')
        artifacts = output / 'artifacts'
        artifacts.mkdir()
        executables = {
            name: retain_executable(checks / log, kind, test, artifacts / name, build / 'target')
            for name, log, kind, test in [
                ('bigbrotr', 'compile.log', 'bin', False),
                ('bigbrotr-lib-tests', 'compile-tests.log', 'lib', True),
            ]
        }
        identity['executables_sha256'] = executables
        report.update(identity)
        record()
        inventory = output / 'test-inventory'
        if execute([
            ('list', [str(artifacts / 'bigbrotr-lib-tests'), '--list', '--format', 'pretty']),
            ('build-info', [str(artifacts / 'bigbrotr'), 'build-info']),
        ], inventory, source, env, identity, remaining(60))['status'] != 'PASS':
            raise ValueError('application identity or test listing failed')
        info = json.loads((inventory / 'build-info.log').read_text())
        if (info['source'] != dict(state='builder-supplied', revision=identity['source'],
                                  tree=identity['tree'], dirty=False,
                                  manifest_sha256=identity['source_manifest_sha256'])
                or info['target'] != 'x86_64-unknown-linux-gnu'
                or info['profile_assertion'] != 'release' or info['archive_worker'] is not None
                or info['inputs_sha256'] != {name: original[name] for name in (
                    'Cargo.lock', 'rust-toolchain.toml', 'contracts/release/dependencies.json',
                    'contracts/protocol/versions.json', 'contracts/protocol/nip01-event.json')}):
            raise ValueError('application build identity mismatch')
        tests = selected_tests(listed_tests((inventory / 'list.log').read_text()))
        (inventory / 'selected.json').write_text(json.dumps(tests, indent=2) + '\n')
        identity['selected_tests'] = tests
        report.update(identity)
        record()
        checks_run = output / 'tests'
        if execute([(name, [str(artifacts / 'bigbrotr-lib-tests'), prefix, '--test-threads=1'])
                    for name, prefix in GROUPS.items()], checks_run, source, env, identity, remaining(120))['status'] != 'PASS':
            raise ValueError('focused actual application tests failed')
        for name, expected in tests.items():
            completed_tests((checks_run / (name + '.log')).read_text(), expected)
        if sources() != original or any(file_hash(artifacts / name) != digest
                                        for name, digest in executables.items()):
            raise ValueError('source or retained executable changed during checks')
        if any(file_hash(Path(tool['path'])) != tool['sha256']
               for tool in identity['native_toolchain']['tools'].values()):
            raise ValueError('native tool changed during checks')
        report.update(identity, status=STATUS, build_info=info,
                      tests={name: len(names) for name, names in tests.items()},
                      evidence_sha256={str(p.relative_to(output)): file_hash(p)
                                       for directory in (checks, inventory, checks_run)
                                       for p in sorted(directory.iterdir()) if p.is_file()})
    except BaseException as error:
        report.update(status='FAIL', failure=type(error).__name__)
        raise
    finally:
        record()
