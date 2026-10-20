#!/usr/bin/env python3
"""Compile export consumers; the memory probe needs no retained runtime seed."""
import json
import shutil
import subprocess
import sys
from pathlib import Path
sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain, build_binaries
from tools.archive_bundle import build_environment, install_bundle, verify_file
from tools.ci import execute
job_pages_cli = sys.argv[1:] == ['--runtime','--job-pages-cli']
job_pages = job_pages_cli or sys.argv[1:] == ['--runtime','--job-pages']
runtime = job_pages or sys.argv[1:] == ['--runtime']
memory_probe = sys.argv[1:] == ['--memory-probe']
admission = sys.argv[1:] == ['--admission']
assert sys.argv[1:] in ([], ['--runtime'], ['--runtime','--job-pages-cli'], ['--runtime','--job-pages'], ['--build-runtime'], ['--memory-probe'], ['--admission'])
if sys.argv[1:] == ['--build-runtime']:
    import os
    worker=build_binaries(SOURCE,dict(os.environ))
    Path('/out/runtime-worker.json').write_text(json.dumps(worker)+'\n')
    # Cargo integration tests may rebuild the package binary with different
    # build-script inputs. Preserve the installed pair before running checks.
    artifacts=Path('/out/runtime-artifacts');artifacts.mkdir()
    for name in ['bigbrotr','bigbrotr-archive-worker']:
        shutil.copy2(Path(os.environ['CARGO_TARGET_DIR'])/'release'/name,artifacts/name)
    info=json.loads(subprocess.check_output([str(artifacts/'bigbrotr'),'build-info'],env={}))
    assert info['archive_worker']==worker
    sys.exit(0)
build = SOURCE.parent
manifest = json.loads((build/'source-manifest.json').read_text())
def sources():
    return {str(p.relative_to(SOURCE)):file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
assert sources() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env,2)
env.update(selected)
env.update(CARGO_HOME=str(build/'cargo-home'),CARGO_TARGET_DIR=str(build/'target'),CARGO_NET_OFFLINE='true',
    BIGBROTR_SOURCE_REVISION=manifest['revision'],BIGBROTR_SOURCE_TREE=manifest['tree'],BIGBROTR_BUILD_PROFILE='release',
    BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build/'source-manifest.json'))
identity = dict(source=manifest['revision'],tree=manifest['tree'],lockfile_sha256=file_hash(SOURCE/'Cargo.lock'),
    source_manifest_sha256=file_hash(build/'source-manifest.json'),native_toolchain=native,
    scope='Current typed SQL/native/files consumer research driver; no installed automatic scheduler or signal/recovery qualification')
checks = Path('/out/adapter-checks')
if admission:
    artifacts = Path('/out/artifacts')
    artifacts.mkdir()
    bundle = artifacts / 'archive-worker'
    bundle.mkdir()
    install_bundle(Path('/archive-inputs'), bundle, 'x86_64-unknown-linux-gnu')
    worker_identity = dict(bytes=14421792,
        sha256='0b9107ab422f2ae72af164d863f3e90e24e213231dcaeade9e0b5fe10320b4c1')
    retained_worker = Path('/archive-inputs/bigbrotr-archive-worker')
    verify_file(retained_worker, worker_identity)
    worker = bundle / 'bigbrotr-archive-worker'
    shutil.copy2(retained_worker, worker)
    verify_file(worker, worker_identity)
    worker.chmod(0o500)
    env.update(build_environment(bundle))
    env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker_identity['bytes']),
               BIGBROTR_ARCHIVE_WORKER_SHA256=worker_identity['sha256'])
    identity.update(scope='Installed export admission unit boundaries and build only; runtime/memory qualification NOT_RUN',
                    worker_source='6c42a41534a0d7ec3e425fa34b1b3c88bdceab79')
    assert execute([
        ('admission', ['cargo', 'test', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                       '--bin', 'bigbrotr', 'interfaces::serve::export_jobs::tests::']),
        ('build', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr']),
        ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr',
                    '--bin', 'bigbrotr', '--', '-D', 'warnings']),
    ], checks, SOURCE, env, identity, 1800)['status'] == 'PASS'
    assert '3 passed; 0 failed; 0 ignored' in (checks / 'admission.log').read_text()
    executable = artifacts / 'bigbrotr'
    shutil.copy2(build / 'target/release/bigbrotr', executable)
    executable.chmod(0o500)
    info = json.loads(subprocess.check_output([str(executable), 'build-info'], env={}))
    assert info['archive_worker'] == worker_identity
    assert info['source'] == dict(state='builder-supplied', revision=manifest['revision'],
        tree=manifest['tree'], dirty=False, manifest_sha256=identity['source_manifest_sha256'])
    assert info['target'] == 'x86_64-unknown-linux-gnu'
    identity.update(build_info=info, artifacts={
        str(p.relative_to(artifacts)): dict(sha256=file_hash(p), bytes=p.stat().st_size)
        for p in sorted(artifacts.rglob('*')) if p.is_file()})
    (checks / 'artifacts.json').write_text(json.dumps(identity, indent=2) + '\n')
elif memory_probe:
    identity['scope'] = 'Export memory probe compilation and Clippy only; no runtime or memory qualification'
    assert execute([
        ('build', ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'bigbrotr', '--example', 'export-execution-probe']),
        ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', '-p', 'bigbrotr', '--example', 'export-execution-probe', '--', '-D', 'warnings']),
    ], checks, SOURCE, env, identity, 1800)['status'] == 'PASS'
    artifacts = Path('/out/artifacts')
    artifacts.mkdir()
    executable = artifacts / 'export-execution-probe'
    original = build / 'target/release/examples/export-execution-probe'
    assert original.is_file() and not original.is_symlink()
    shutil.copy2(original, executable)
    executable.chmod(0o500)
    with executable.open('rb') as stream:
        assert stream.read(4) == b'\x7fELF'
    assert file_hash(executable) == file_hash(original)
    identity['artifacts'] = {executable.name: dict(sha256=file_hash(executable), bytes=executable.stat().st_size)}
    (checks / 'artifacts.json').write_text(json.dumps(identity, indent=2) + '\n')
elif runtime:
    env.update(build_environment(Path('/out/archive-worker')))
    identity['scope']='Current installed executable and worker; practical profile recorded by the outer laboratory'
    assert execute([
        ('installed-build',[sys.executable,__file__,'--build-runtime']),
        ('management-tests',['cargo','test','--locked','--offline','--release','-p','bigbrotr','--bin','bigbrotr']),
        *([('page-domain',['cargo','test','--locked','--offline','--release','-p','bigbrotr','--lib','management::job_list']),
           ('page-schema',['cargo','test','--locked','--offline','--release','-p','bigbrotr','--test','control_schema'])] if job_pages else []),
        *([('cli-sessions',['cargo','test','--locked','--offline','--release','-p','bigbrotr','--test','cli_sessions','--test','cli_contract'])] if job_pages_cli else []),
        ('clippy',['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','--bin','bigbrotr','--','-D','warnings']),
    ],checks,SOURCE,env,identity,1800)['status']=='PASS'
    worker=json.loads(Path('/out/runtime-worker.json').read_text())
    artifacts=Path('/out/runtime-artifacts')
    info=json.loads(subprocess.check_output([str(artifacts/'bigbrotr'),'build-info'],env={}))
    assert info['archive_worker']==worker
    assert info['source']==dict(state='builder-supplied',revision=manifest['revision'],tree=manifest['tree'],dirty=False,manifest_sha256=identity['source_manifest_sha256'])
    assert info['target']=='x86_64-unknown-linux-gnu'
    identity.update(build_info=info,artifacts={p.name:file_hash(p) for p in artifacts.iterdir()})
    (checks/'artifacts.json').write_text(json.dumps(identity,indent=2)+'\n')
    # Preserve original seed identities; replace only the installed subject.
    ready_path=Path('/out/native-tests/ready.json')
    ready=json.loads(ready_path.read_text())
    Path('/out/seed-ready.json').write_bytes(ready_path.read_bytes())
    for key,name,target in [('control','bigbrotr','native-tests'),('worker','bigbrotr-archive-worker','archive-worker')]:
        installed=Path('/out')/target/name
        assert not installed.is_symlink() and file_hash(installed)==ready[key]
        installed.unlink() # Replace only this lab's verified read-only seed copy.
        shutil.copy2(artifacts/name,installed)
        ready[key]=file_hash(artifacts/name)
    ready.update(runtime_source=manifest['revision'],runtime_source_manifest_sha256=identity['source_manifest_sha256'])
    ready_path.write_text(json.dumps(ready,indent=2)+'\n')
else:
    assert execute([
        ('build',['cargo','build','--locked','--offline','--release','-p','bigbrotr','--bin','bigbrotr','--example','export-execution-probe']),
        ('selection-tests',['cargo','test','--locked','--offline','--release','-p','bigbrotr','--lib','export::execution::tests']),
        ('clippy',['cargo','clippy','--locked','--offline','--release','-p','bigbrotr','--lib','--example','export-execution-probe','--','-D','warnings']),
    ],checks,SOURCE,env,identity,1800)['status']=='PASS'
    assert '2 passed; 0 failed; 0 ignored' in (checks/'selection-tests.log').read_text()
    artifacts=Path('/out/adapter-artifacts');artifacts.mkdir()
    for original,name in [('bigbrotr','bigbrotr'),('examples/export-execution-probe','export-execution-probe')]:
        shutil.copy2(build/'target/release'/original,artifacts/name);(artifacts/name).chmod(0o500)
    info=json.loads(subprocess.check_output([str(artifacts/'bigbrotr'),'build-info'],env={}))
    assert info['source']==dict(state='builder-supplied',revision=manifest['revision'],tree=manifest['tree'],dirty=False,manifest_sha256=identity['source_manifest_sha256'])
    assert info['archive_worker'] is None and info['target']=='x86_64-unknown-linux-gnu'
    identity.update(build_info=info,artifacts={p.name:file_hash(p) for p in artifacts.iterdir()})
    (checks/'artifacts.json').write_text(json.dumps(identity,indent=2)+'\n')
assert sources()==manifest['files_sha256']
if memory_probe or admission:
    Path('/out/qualification.json').write_text(json.dumps(
        dict(identity, status='PASS_EXPORT_ADMISSION_BUILD_ONLY' if admission else 'PASS_EXPORT_MEMORY_PROBE_BUILD_ONLY'), indent=2) + '\n')
