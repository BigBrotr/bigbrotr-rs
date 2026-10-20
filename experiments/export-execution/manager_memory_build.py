#!/usr/bin/env python3
"""Link the standalone manager-memory diagnostic from recorded Cargo artifacts."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import tomllib
import traceback

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
BUILD = SOURCE.parent
ARTIFACTS = Path('/out/artifacts')
CHECKS = Path('/out/manager-memory-checks')
EXTERNS = ('bigbrotr', 'bigbrotr_model', 'bigbrotr_protocol', 'getrandom', 'serde',
           'serde_json', 'sha2', 'tokio', 'tokio_postgres')


def require(value, message):
    if not value:
        raise ValueError(message)


def pin(path):
    with path.open('rb') as stream:
        return dict(path=str(path), bytes=path.stat().st_size,
                    sha256=hashlib.file_digest(stream, 'sha256').hexdigest())


def main():
    require(len(sys.argv) == 1 and ARTIFACTS.is_dir(), 'existing profile artifacts')
    CHECKS.mkdir()
    manifest_path = BUILD / 'source-manifest.json'
    manifest = json.loads(manifest_path.read_text())
    shutil.copy2(manifest_path, CHECKS / 'source-manifest.json')
    report = dict(source=manifest['revision'], tree=manifest['tree'], status='RUNNING',
        source_manifest=pin(CHECKS / 'source-manifest.json'), checks=[], runtime='NOT_RUN')
    env = dict(os.environ)
    deadline = time.monotonic() + 1800

    def save():
        temporary = CHECKS / 'result.tmp'
        temporary.write_text(json.dumps(report, indent=2) + '\n')
        temporary.replace(CHECKS / 'result.json')

    def run(name, command):
        stdout, stderr = CHECKS / (name + '.stdout'), CHECKS / (name + '.stderr')
        check = dict(name=name, command=[str(arg) for arg in command], status='RUNNING')
        report['checks'].append(check);save()
        try:
            require(time.monotonic() < deadline, 'diagnostic build deadline')
            # Inherit the outer execute() process group so its deadline cleanup
            # also owns Cargo/rustc descendants. Preserve the two raw streams.
            with stdout.open('xb') as out, stderr.open('xb') as err:
                result = subprocess.run(check['command'], cwd=SOURCE, env=env,
                    stdout=out, stderr=err, timeout=max(.001, deadline-time.monotonic()))
            check.update(returncode=result.returncode, status='PASS' if result.returncode == 0 else 'FAIL')
        except BaseException as error:
            check.update(status='FAIL', error=repr(error));raise
        finally:
            check.update(stdout=pin(stdout) if stdout.exists() else None,
                         stderr=pin(stderr) if stderr.exists() else None)
            save()
        require(check['status'] == 'PASS', 'failed diagnostic build command: ' + name)
        return stdout.read_text()

    save()
    try:
        rustc = Path(run('rustc-path', ['rustup', 'which', 'rustc']).strip())
        require(rustc.is_absolute() and rustc.resolve() == rustc and rustc.is_file(), 'canonical actual rustc')
        report['compiler'] = pin(rustc)
        version = run('rustc-version', [rustc, '--version', '--verbose'])
        channel = tomllib.loads((SOURCE / 'rust-toolchain.toml').read_text())['toolchain']['channel']
        require(version.startswith('rustc ' + channel + ' ') and 'host: x86_64-unknown-linux-gnu' in version,
                'pinned source compiler and host')
        report['compiler']['version'] = version
        env['RUSTC'] = str(rustc)
        target = Path(env['CARGO_TARGET_DIR'])
        cargo_home = Path(env['CARGO_HOME'])
        require(target == BUILD / 'target' and target.resolve() == target and cargo_home.resolve() == cargo_home,
                'owned canonical Cargo paths')
        metadata = json.loads(run('cargo-metadata', ['cargo', 'metadata', '--locked', '--offline',
            '--format-version=1', '--filter-platform', 'x86_64-unknown-linux-gnu']))
        require(Path(metadata['target_directory']) == target, 'matching Cargo target graph')
        packages = {item['id']: item for item in metadata['packages']}
        nodes = {item['id']: item for item in metadata['resolve']['nodes']}
        require(len(packages) == len(metadata['packages']) and len(nodes) == len(metadata['resolve']['nodes']),
                'unambiguous Cargo package IDs')
        for package in packages.values():
            path = Path(package['manifest_path'])
            require(path.is_absolute() and path.resolve() == path and path.is_file()
                    and (path.is_relative_to(SOURCE) if package['source'] is None else
                         package['source'].startswith('registry+') and path.is_relative_to(cargo_home / 'registry/src')),
                    'package manifest from current source or owned registry')
        roots = [item['id'] for item in packages.values()
                 if item['name'] == 'bigbrotr' and Path(item['manifest_path']) == SOURCE / 'apps/bigbrotr/Cargo.toml']
        require(len(roots) == 1, 'one current bigbrotr package')
        selected = {'bigbrotr': roots[0]}
        for name in EXTERNS[1:]:
            candidates = {item['pkg'] for item in nodes[roots[0]]['deps'] if item['name'] == name
                          and any(kind['kind'] is None for kind in item['dep_kinds'])}
            require(len(candidates) == 1, 'one normal direct dependency: ' + name)
            selected[name] = candidates.pop()
            require(packages[selected[name]]['name'].replace('-', '_') == name, 'matching direct crate name')
        graph = set()
        pending = [roots[0]]
        while pending:
            package = pending.pop()
            if package in graph: continue
            graph.add(package)
            pending.extend(item['pkg'] for item in nodes[package]['deps']
                           if any(kind['kind'] is None for kind in item['dep_kinds']))
        raw = run('cargo-lib', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                               '--lib', '--message-format=json'])
        messages = [json.loads(line) for line in raw.splitlines() if line.strip()]
        finished = [item for item in messages if item.get('reason') == 'build-finished']
        require(len(finished) == 1 and finished[0]['success'] is True, 'one completed Cargo library build')
        libraries, direct, native_paths = {}, {name: set() for name in EXTERNS}, set()
        for item in messages:
            if item.get('reason') not in ('compiler-artifact', 'build-script-executed'): continue
            package = item['package_id']
            require(package in packages, 'Cargo record belongs to resolved graph')
            if package not in graph: continue
            if item['reason'] == 'build-script-executed':
                for linked in item['linked_paths']:
                    kind, separator, value = linked.partition('=')
                    require(not separator or kind in ('native', 'all'), 'native Cargo search-path kind')
                    path = Path(value if separator else linked)
                    require(path.is_absolute() and path.is_relative_to(target) and path.resolve() == path and path.is_dir(),
                            'native search path from current compiled graph')
                    native_paths.add(str(path))
                continue
            require(Path(item['manifest_path']) == Path(packages[package]['manifest_path']), 'artifact package manifest')
            if item['profile']['test'] or not set(item['target']['kind']) & {'lib', 'proc-macro'}: continue
            for filename in item['filenames']:
                path = Path(filename)
                if path.suffix not in ('.rlib', '.so'): continue
                require(path.is_absolute() and path.is_relative_to(target) and path.resolve() == path and path.is_file(),
                        'regular compiled-graph library within target root')
                require(path.name not in libraries or libraries[path.name] == path, 'unique compiled library filename')
                libraries[path.name] = path
                for name, package_id in selected.items():
                    if package == package_id and item['target']['name'] == name and path.suffix == '.rlib' \
                            and item['profile']['opt_level'] == '3':
                        direct[name].add(path)
        require(all(len(paths) == 1 for paths in direct.values()), 'exactly nine unambiguous direct release rlibs')
        staged = ARTIFACTS / 'manager-memory-deps'
        staged.mkdir()
        graph_pins = {}
        for name, path in sorted(libraries.items()):
            original = pin(path)
            destination = staged / name
            shutil.copy2(path, destination);destination.chmod(0o400)
            require(pin(destination)['sha256'] == original['sha256'], 'unchanged retained compiled library')
            graph_pins[name] = dict(original=original, retained=pin(destination))
        externs = {name: staged / next(iter(paths)).name for name, paths in direct.items()}
        report.update(direct_externs={name: dict(package_id=selected[name], **pin(path)) for name, path in externs.items()},
            compiled_graph=graph_pins, native_search_paths=sorted(native_paths),
            environment={key: value for key, value in env.items() if key.startswith(('CARGO_', 'BIGBROTR_', 'DUCKDB_')) or key == 'RUSTC'})
        wrapper = SOURCE / 'experiments/export-execution/memory_measure.rs'
        require(wrapper.is_file() and wrapper.resolve() == wrapper, 'current standalone diagnostic source')
        report['wrapper'] = pin(wrapper)
        require(report['wrapper']['sha256'] == manifest['files_sha256'][str(wrapper.relative_to(SOURCE))],
                'diagnostic source matches exact source manifest')
        save()
        common = [rustc, '--edition=2024', '--crate-name', 'export_manager_memory_probe', '-D', 'warnings',
                  '-C', 'opt-level=3', '-C', 'lto=thin', '-C', 'strip=debuginfo',
                  '-L', 'dependency=' + str(staged), '-L', 'native=' + str(ARTIFACTS / 'archive-worker/lib')]
        for path in sorted(native_paths): common += ['-L', 'native=' + path]
        for name in EXTERNS: common += ['--extern', name + '=' + str(externs[name])]
        executable = ARTIFACTS / 'export-manager-memory-probe'
        tests = ARTIFACTS / 'export-manager-memory-tests'
        run('rustc-diagnostic', [*common, wrapper, '-o', executable])
        run('rustc-counter-tests', [*common, '--test', wrapper, '-o', tests])
        results = run('allocator-counter-tests', [tests, '--test-threads=1', '--nocapture'])
        summary = re.search(r'test result: ok\. ([1-9][0-9]*) passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;', results)
        require(summary is not None, 'nonempty deterministic counter tests without skips')
        for path in (executable, tests):
            path.chmod(0o500)
            with path.open('rb') as stream:
                require(stream.read(4) == b'\x7fELF', 'diagnostic/test executable ELF')
        require(pin(rustc)['sha256'] == report['compiler']['sha256'], 'unchanged selected compiler')
        require(all(pin(staged / name)['sha256'] == value['retained']['sha256'] for name, value in graph_pins.items()),
                'unchanged retained compiled graph through linking/tests')
        report.update(status='PASS', counter_tests=int(summary[1]), executable=pin(executable), test_binary=pin(tests))
    except BaseException:
        report.update(status='FAIL', error=traceback.format_exc());raise
    finally:
        save()


if __name__ == '__main__':
    main()
