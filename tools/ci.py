#!/usr/bin/env python3
"""Shared foundation CI commands for disposable Actions and trusted native labs."""
import argparse
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.build import (cargo_environment, clean_environment, command_output, file_hash, git,
                         native_toolchain, positive_jobs, regular_file_hash, source_inventory,
                         verify_cargo_configuration, verify_native_toolchain)
from tools.archive_bundle import build_environment, native_target, prepare_bundle, verify_bundle


WORKSPACE_COMMANDS = {
    'clippy': ['cargo', 'clippy', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
    'rust-doc': ['cargo', 'doc', '--locked', '--workspace', '--no-deps'],
    'rust-tests': ['cargo', 'test', '--locked', '--workspace'],
}

CONTROL_SUITES = {
    'acceptance': ('tests/acceptance', 'test_*.py'),
    'export-profile': ('experiments/ingest-bootstrap', 'test_installed_archive_profile.py'),
}


class ControlResult(unittest.TextTestResult):
    # Successful execution can finish with success or expected failure. Setup
    # skips do not start a test and must not count as an executed case.
    completed_non_skipped = 0

    def addSuccess(self, test):
        self.completed_non_skipped += 1
        super().addSuccess(test)

    def addExpectedFailure(self, test, error):
        self.completed_non_skipped += 1
        super().addExpectedFailure(test, error)


def control_suite(name):
    start, pattern = CONTROL_SUITES[name]
    suite = unittest.defaultTestLoader.discover(start, pattern)
    discovered = suite.countTestCases()
    result = unittest.TextTestRunner(resultclass=ControlResult).run(suite)
    if not result.wasSuccessful():
        return 1
    if not discovered or not result.completed_non_skipped:
        print('CI control suite has no ' + ('discovered cases' if not discovered else
              'executed non-skipped cases') + ': ' + name, file=sys.stderr)
        return 1
    return 0


def write_report(output, report):
    temporary = output / 'result.tmp'
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(output / 'result.json')


def execute(checks, output, root, env, identity, seconds):
    if not checks:
        raise ValueError('empty CI execution')
    output.mkdir(parents=True, exist_ok=False)
    report = dict(identity, format='bigbrotr-foundation-ci/1', status='RUNNING',
                  checks=[{'name': name, 'command': command, 'status': 'NOT_RUN'}
                          for name, command in checks])
    deadline = time.monotonic() + seconds
    write_report(output, report)
    for check in report['checks']:
        process = None
        started = time.monotonic()
        check['status'] = 'RUNNING'
        write_report(output, report)
        print('CI ' + check['name'] + ': RUNNING', flush=True)
        try:
            if started >= deadline:
                raise subprocess.TimeoutExpired(check['command'], seconds)
            with (output / (check['name'] + '.log')).open('xb') as log:
                process = subprocess.Popen(check['command'], cwd=root, env=env,
                                           stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True)
                check['executed'] = True
                check['exit_code'] = process.wait(timeout=max(.001, deadline-time.monotonic()))
            check['status'] = 'PASS' if check['exit_code'] == 0 else 'FAIL'
        except (OSError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
            check['status'] = 'FAIL' if process is not None else 'NOT_RUN'
            check['failure'] = ('deadline' if isinstance(error, subprocess.TimeoutExpired)
                                else 'interrupted' if isinstance(error, KeyboardInterrupt)
                                else type(error).__name__)
        finally:
            # Own and reap the whole command group, including a timed-out child.
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
            check['seconds'] = time.monotonic() - started
            write_report(output, report)
        print('CI ' + check['name'] + ': ' + check['status'], flush=True)
        if check['status'] != 'PASS':
            break
    report['counts'] = {'collected': len(checks),
                        'executed': sum(c.get('executed', False) for c in report['checks']),
                        'failed': sum(c['status'] == 'FAIL' for c in report['checks']),
                        'not_run': sum(c['status'] == 'NOT_RUN' for c in report['checks'])}
    report['status'] = 'PASS' if all(c['status'] == 'PASS' for c in report['checks']) else 'FAIL'
    write_report(output, report)
    return report


def documentation_commands(output):
    return [('planning-artifacts', [sys.executable, 'experiments/verify_artifacts.py']),
            ('prepare-links', [sys.executable, 'experiments/verify_handoff.py', '--prepare']),
            ('documentation-links', [sys.executable, 'experiments/verify_handoff.py', '--output', str(output/'handoff.json')])]


def control_commands():
    return [('acceptance-controls', [sys.executable, 'tools/ci.py', '--control-suite', 'acceptance']),
            ('export-profile-controls', [sys.executable, 'tools/ci.py', '--control-suite', 'export-profile'])]


def commands(output, system, architecture, jobs):
    python = sys.executable
    build = output / 'build'
    checks = [('toolchain', ['rustup', 'show']),
              ('format', ['cargo', 'fmt', '--all', '--check'])]
    if system == 'Linux' and architecture == 'x86_64':
        # Dependency policy reads source/metadata and advisories, not compiled
        # product artifacts. Keep the lock immutable before the first --locked
        # workspace command, and refuse before native compilation.
        checks += [('install-deny', ['cargo', 'install', 'cargo-deny', '--version', '0.20.2', '--locked', '--root', str(output/'tools')]),
                   ('dependency-policy', ['cargo', 'deny', '--locked', 'check', 'licenses', 'sources', 'advisories'])]
    # These controls use temporary fixtures and the declared native tools, not
    # the product build. Refuse a broken verifier before workspace compilation.
    checks += control_commands()
    checks += [('exact-source-build', [python, 'tools/build.py', '--output', str(build), '--jobs', str(jobs),
                                      '--archive-bundle', str(output.with_name(output.name+'-native-inputs')/'archive-bundle')])]
    checks += [(name, [python, 'tools/ci.py', '--workspace-check', name, '--from-build', str(build)])
               for name in WORKSPACE_COMMANDS]
    if system == 'Linux' and architecture == 'x86_64':
        checks += documentation_commands(output)
    if system == 'Linux':
        for scenario in ['p01-event-cli', 'p01-config-static']:
            checks += [(scenario, [python, 'tests/acceptance/run.py', scenario,
                                  '--binary', str(build/'bigbrotr'), '--source-root', str(build/'source'),
                                  '--source-manifest', str(build/'source-manifest.json'),
                                  '--build-result', str(build/'build-result.json'),
                                  '--evidence', str(output/scenario)])]
    return checks


def verify_workspace_inputs(build, manifest, receipt, anchors):
    """Recheck frozen inputs and copied artifacts, never Cargo's mutable outputs."""
    for name in ['source', 'target', 'cargo-home', 'archive-worker',
                 'archive-worker/lib', 'archive-worker/extensions']:
        path = build / name
        if path.is_symlink() or not path.is_dir():
            raise ValueError('invalid run-local directory: ' + name)
    for name in ['config', 'config.toml', 'credentials', 'credentials.toml']:
        path = build / 'cargo-home' / name
        if path.exists() or path.is_symlink():
            raise ValueError('unexpected Cargo home configuration or credentials')
    for name, expected in anchors.items():
        if regular_file_hash(build / name) != expected:
            raise ValueError('build input or copied artifact changed: ' + name)
    if source_inventory(build / 'source') != manifest['files_sha256']:
        raise ValueError('exported source changed')
    verify_cargo_configuration(build / 'source', ROOT, manifest['files_sha256'], build / 'cargo-home')
    assets = verify_bundle(build / 'archive-worker', native_target())
    if assets != receipt['archive_worker']['assets']:
        raise ValueError('native archive assets differ from the build receipt')
    verify_native_toolchain(receipt['native_toolchain'])


def workspace_context(build):
    """Reuse only a preceding clean build of this exact checkout and helper."""
    build = build.resolve(strict=True)
    anchors = {name: regular_file_hash(build / name)
               for name in ['build-result.json', 'source-manifest.json']}
    receipt = json.loads((build/'build-result.json').read_text())
    manifest = json.loads((build/'source-manifest.json').read_text())
    if receipt['format'] != 'bigbrotr-build-result/1' or manifest['format'] != 'bigbrotr-source/1':
        raise ValueError('invalid build/source receipt')
    if (receipt['source_manifest_sha256'] != anchors['source-manifest.json']
            or manifest['revision'] != git(ROOT, 'rev-parse', 'HEAD')
            or manifest['tree'] != git(ROOT, 'rev-parse', 'HEAD^{tree}')):
        raise ValueError('build source does not match this checkout')
    for name in ['tools/ci.py', 'tools/build.py', 'tools/archive_bundle.py',
                 'contracts/release/archive-engine.json']:
        if regular_file_hash(ROOT / name) != manifest['files_sha256'][name]:
            raise ValueError('executing helper or native registry differs from selected source')
    info = receipt['build_info']
    expected_source = dict(state='builder-supplied', revision=manifest['revision'],
                           tree=manifest['tree'], dirty=False,
                           manifest_sha256=anchors['source-manifest.json'])
    if (info['source'] != expected_source or info['profile_assertion'] != 'release'
            or receipt['helper_sha256'] != manifest['files_sha256']['tools/build.py']
            or info['target'] != native_target()):
        raise ValueError('build identity does not match the native source/profile')
    for name, expected in info['inputs_sha256'].items():
        if manifest['files_sha256'][name] != expected:
            raise ValueError('embedded build input differs from selected source')
    worker = build/'archive-worker'/'bigbrotr-archive-worker'
    identity = {name: receipt['archive_worker'][name] for name in ['bytes', 'sha256']}
    if (info['archive_worker'] != identity or worker.lstat().st_size != identity['bytes']
            or receipt['archive_worker']['registry_sha256'] != manifest['files_sha256']['contracts/release/archive-engine.json']):
        raise ValueError('packaged worker binding differs from build receipt')
    anchors.update({'source.tar': manifest['archive_sha256'], 'bigbrotr': receipt['binary_sha256'],
                    'archive-worker/bigbrotr-archive-worker': identity['sha256']})
    verify_workspace_inputs(build, manifest, receipt, anchors)
    env = clean_environment()
    selected, native = native_toolchain(env, int(receipt['native_toolchain']['environment']['CARGO_BUILD_JOBS']))
    if native != receipt['native_toolchain']:
        raise ValueError('selected native toolchain differs from the build')
    env.update(selected)
    declared = dict(selected, **cargo_environment(build, manifest, anchors['source-manifest.json'], 'dev'),
                    **build_environment(worker.parent), RUSTDOCFLAGS='-D warnings', PYTHONDONTWRITEBYTECODE='1',
                    BIGBROTR_ARCHIVE_WORKER_BYTES=str(identity['bytes']), BIGBROTR_ARCHIVE_WORKER_SHA256=identity['sha256'],
                    BIGBROTR_ARCHIVE_TEST_INSTALLATION=str(worker.parent),
                    BIGBROTR_ARCHIVE_TEST_WORKER_BYTES=str(identity['bytes']), BIGBROTR_ARCHIVE_TEST_WORKER_SHA256=identity['sha256'])
    env.update(declared)
    if command_output(['rustc', '-vV'], env) != info['rustc']:
        raise ValueError('selected Rust compiler differs from the build')
    return build, manifest, receipt, anchors, env, declared


def workspace_check(build, name):
    command = WORKSPACE_COMMANDS[name]
    build, manifest, receipt, anchors, env, declared = workspace_context(build)
    print(json.dumps({'command': command, 'cwd': str(build/'source'), 'environment': declared,
                      'build_inputs_sha256': anchors, 'profile': 'default dev/test; packaged artifact remains release'}), flush=True)
    try:
        return subprocess.run(command, cwd=build/'source', env=env).returncode
    finally:
        verify_workspace_inputs(build, manifest, receipt, anchors)
        if command_output(['rustc', '-vV'], env) != receipt['build_info']['rustc']:
            raise ValueError('selected Rust compiler changed during the check')
        print('Post-build source, native inputs and copied artifacts unchanged', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, help='new ignored evidence directory')
    parser.add_argument('--jobs', type=positive_jobs, default=2)
    parser.add_argument('--documentation-only', action='store_true',
                        help='run the documentation checks only; does not qualify product behavior')
    parser.add_argument('--controls-only', action='store_true',
                        help='run portable controls and documentation; no product build or qualification')
    parser.add_argument('--download-cache', type=Path,
                        help='optional native asset download cache; every asset is size/hash checked')
    parser.add_argument('--workspace-check', choices=WORKSPACE_COMMANDS,
                        help='run a default-profile workspace check from this revision\'s clean build')
    parser.add_argument('--from-build', type=Path, help='preceding trusted exact-source build directory')
    parser.add_argument('--control-suite', choices=CONTROL_SUITES,
                        help='run one required nonempty portable control suite')
    args = parser.parse_args()
    if args.control_suite:
        if (args.output or args.documentation_only or args.controls_only or
                args.download_cache or args.workspace_check or args.from_build):
            parser.error('--control-suite is a separate operation')
        return control_suite(args.control_suite)
    if args.workspace_check or args.from_build:
        if args.output or args.documentation_only or args.controls_only or args.download_cache:
            parser.error('--workspace-check/--from-build and --output are separate operations')
        if not args.workspace_check or not args.from_build:
            parser.error('--workspace-check and --from-build are required together')
        return workspace_check(args.from_build, args.workspace_check)
    if not args.output:
        parser.error('--output is required for a complete CI run')
    if args.documentation_only and args.controls_only:
        parser.error('--documentation-only and --controls-only are separate scopes')
    if git(ROOT, 'status', '--porcelain', '--untracked-files=all'):
        raise ValueError('CI requires a clean committed source checkout')
    output = args.output.resolve()
    if output.exists():
        raise ValueError('CI requires a new output directory')
    if args.documentation_only or args.controls_only:
        if args.download_cache:
            parser.error('--download-cache is only used by native foundation checks')
        identity = {'source': {'revision': git(ROOT, 'rev-parse', 'HEAD'),
                               'tree': git(ROOT, 'rev-parse', 'HEAD^{tree}'), 'dirty': False},
                    'helper_sha256': file_hash(Path(__file__)),
                    'scope': ('Portable controls and documentation; no product build or qualification'
                              if args.controls_only else
                              'Documentation only; no native build or product qualification')}
        checks = (control_commands() if args.controls_only else []) + documentation_commands(output)
        report = execute(checks, output, ROOT, clean_environment(), identity, 600)
        if git(ROOT, 'status', '--porcelain', '--untracked-files=all'):
            report.update(status='FAIL', failure='source-changed-during-CI')
            write_report(output, report)
        print(json.dumps({'status': report['status'], 'counts': report['counts'], 'output': str(output)}))
        return 0 if report['status'] == 'PASS' else 1
    system, architecture = platform.system(), platform.machine()
    if (system, architecture) not in [('Linux', 'x86_64'), ('Linux', 'aarch64'), ('Darwin', 'arm64'), ('Darwin', 'x86_64')]:
        raise ValueError('unsupported native CI host')
    env = clean_environment()
    selected, native = native_toolchain(env, args.jobs)
    env.update(selected)
    # Prepare once for the fresh clean build. Its installed bundle then remains
    # the native input for every post-build workspace check.
    native_inputs = output.with_name(output.name+'-native-inputs')
    native_inputs.mkdir(parents=True, exist_ok=False)
    cache = args.download_cache.resolve() if args.download_cache else native_inputs/'downloads'
    archive = prepare_bundle(native_inputs/'archive-bundle', native_target(), cache, download=True)
    env.update(build_environment(native_inputs/'archive-bundle'))
    env.update(CARGO_HOME=str(output/'cargo-home'), CARGO_TARGET_DIR=str(output/'target'),
               RUSTDOCFLAGS='-D warnings', PYTHONDONTWRITEBYTECODE='1',
               PATH=str(output/'tools/bin') + os.pathsep + env['PATH'])
    identity = {'source': {'revision': git(ROOT, 'rev-parse', 'HEAD'), 'tree': git(ROOT, 'rev-parse', 'HEAD^{tree}'), 'dirty': False},
                'host': {'os': system, 'architecture': architecture}, 'native_toolchain': native,
                'helper_sha256': file_hash(Path(__file__)), 'lockfile_sha256': file_hash(ROOT/'Cargo.lock'),
                'archive_bundle': archive,
                'deadline_seconds': 7200,
                'scope': 'Foundation CI commands only; not a release, engine matrix or other native target qualification'}
    report = execute(commands(output, system, architecture, args.jobs), output, ROOT, env, identity, 7200)
    if git(ROOT, 'status', '--porcelain', '--untracked-files=all'):
        report.update(status='FAIL', failure='source-changed-during-CI')
        write_report(output, report)
    print(json.dumps({'status': report['status'], 'counts': report['counts'], 'output': str(output)}))
    return 0 if report['status'] == 'PASS' else 1


def interrupted(_signal, _frame):
    raise KeyboardInterrupt


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print('CI prerequisite failed: ' + str(error), file=sys.stderr)
        sys.exit(1)
