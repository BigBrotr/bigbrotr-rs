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

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.build import clean_environment, file_hash, git, native_toolchain, positive_jobs
from tools.archive_bundle import build_environment, native_target, prepare_bundle


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


def commands(output, system, architecture, jobs):
    python = sys.executable
    checks = [('toolchain', ['rustup', 'show']),
              ('format', ['cargo', 'fmt', '--all', '--check']),
              ('clippy', ['cargo', 'clippy', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings']),
              ('rust-tests', ['cargo', 'test', '--locked', '--workspace']),
              ('rust-doc', ['cargo', 'doc', '--locked', '--workspace', '--no-deps']),
              ('acceptance-controls', [python, '-m', 'unittest', 'discover', '-s', 'tests/acceptance', '-p', 'test_*.py'])]
    if system == 'Linux' and architecture == 'x86_64':
        checks += [('planning-artifacts', [python, 'experiments/verify_artifacts.py']),
                   ('prepare-links', [python, 'experiments/verify_handoff.py', '--prepare']),
                   ('documentation-links', [python, 'experiments/verify_handoff.py', '--output', str(output/'handoff.json')]),
                   ('install-deny', ['cargo', 'install', 'cargo-deny', '--version', '0.20.2', '--locked', '--root', str(output/'tools')]),
                   ('dependency-policy', ['cargo', 'deny', 'check', 'licenses', 'sources', 'advisories'])]
    build = output / 'build'
    checks += [('exact-source-build', [python, 'tools/build.py', '--output', str(build), '--jobs', str(jobs),
                                      '--archive-bundle', str(output.with_name(output.name+'-native-inputs')/'archive-bundle')])]
    if system == 'Linux':
        for scenario in ['p01-event-cli', 'p01-config-static']:
            checks += [(scenario, [python, 'tests/acceptance/run.py', scenario,
                                  '--binary', str(build/'bigbrotr'), '--source-root', str(build/'source'),
                                  '--source-manifest', str(build/'source-manifest.json'),
                                  '--build-result', str(build/'build-result.json'),
                                  '--evidence', str(output/scenario)])]
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='new ignored evidence directory')
    parser.add_argument('--jobs', type=positive_jobs, default=2)
    args = parser.parse_args()
    if git(ROOT, 'status', '--porcelain', '--untracked-files=all'):
        raise ValueError('CI requires a clean committed source checkout')
    output = args.output.resolve()
    if output.exists():
        raise ValueError('CI requires a new output directory')
    system, architecture = platform.system(), platform.machine()
    if (system, architecture) not in [('Linux', 'x86_64'), ('Linux', 'aarch64'), ('Darwin', 'arm64'), ('Darwin', 'x86_64')]:
        raise ValueError('unsupported native CI host')
    env = clean_environment()
    selected, native = native_toolchain(env, args.jobs)
    env.update(selected)
    # Prepare once for this CI run and share the exact hashed input with the
    # second clean build. No library from an ambient workstation is substituted.
    native_inputs = output.with_name(output.name+'-native-inputs')
    native_inputs.mkdir(parents=True, exist_ok=False)
    archive = prepare_bundle(native_inputs/'archive-bundle', native_target(), native_inputs/'downloads', download=True)
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
