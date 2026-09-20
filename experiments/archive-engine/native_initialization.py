#!/usr/bin/env python3
"""Exact-source build for reserved initialization; isolated native Linux lab only."""
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import build_binaries, clean_environment, file_hash, native_toolchain
from tools.archive_bundle import build_environment, install_bundle
from tools.ci import execute

build, out, bundle = SOURCE.parent, Path('/out/native-tests'), Path('/out/archive-worker')
if sys.argv[1:] == ['--build-binaries']:
    identity = build_binaries(SOURCE, os.environ)
    (out / 'worker-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    sys.exit(0)
assert sys.argv[1:] in ([], ['--tls'], ['--publication'], ['--producer-inputs'], ['--producer-ledger'], ['--producer-ledger-fixture'], ['--import-source'], ['--input-bindings'], ['--certification'], ['--producer-closing'], ['--producer-recovery'], ['--archive-event'], ['--publication-sync'], ['--archive-membership'], ['--archive-cut'], ['--archive-frontier'], ['--reader-custody'])
reader_custody = sys.argv[1:] == ['--reader-custody']
archive_frontier = sys.argv[1:] == ['--archive-frontier']
archive_cut = archive_frontier or sys.argv[1:] == ['--archive-cut']
archive_membership = sys.argv[1:] == ['--archive-membership']
publication_sync = sys.argv[1:] == ['--publication-sync']
archive_event = reader_custody or publication_sync or sys.argv[1:] == ['--archive-event']
producer_recovery = sys.argv[1:] == ['--producer-recovery']
producer_closing = producer_recovery or sys.argv[1:] == ['--producer-closing']
certification = producer_closing or sys.argv[1:] == ['--certification']
input_bindings = sys.argv[1:] == ['--input-bindings']
import_source = sys.argv[1:] == ['--import-source']
tls = sys.argv[1:] == ['--tls']
fixture_only = sys.argv[1:] == ['--producer-ledger-fixture']
producer_inputs = certification or input_bindings or import_source or sys.argv[1:] in (['--producer-inputs'], ['--producer-ledger'], ['--producer-ledger-fixture'])
producer_ledger = certification or input_bindings or import_source or sys.argv[1:] in (['--producer-ledger'], ['--producer-ledger-fixture'])
publication = archive_cut or archive_membership or archive_event or producer_inputs or sys.argv[1:] == ['--publication']
manifest = json.loads((build / 'source-manifest.json').read_text())


def source_files():
    return {str(p.relative_to(SOURCE)): file_hash(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert source_files() == manifest['files_sha256']
if fixture_only:
    # Qualification-only corrections need the changed actual client, not a
    # mechanical rerun of unrelated suites. Refuse this mode if any product,
    # dependency or build-tool input differs from the retained tested baseline.
    baseline = json.loads((build / 'baseline-source-manifest.json').read_text())
    def fixture_input(name):
        return (name.startswith('experiments/producer-requests/') or name in {
            'apps/bigbrotr/examples/ingest_bootstrap_probe.rs',
            'experiments/archive-engine/native_initialization.py',
            'docs/implementation/progress.md',
            'docs/research/laboratory-inventory.json',
        })
    assert {k: v for k, v in manifest['files_sha256'].items() if not fixture_input(k)} == {
        k: v for k, v in baseline['files_sha256'].items() if not fixture_input(k)}
bundle.mkdir()
install_bundle(Path('/assets'), bundle, 'x86_64-unknown-linux-gnu')
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(build_environment(bundle))
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                native_toolchain=native, scope='permanent local reservation and actual native initializer')
assert execute([('ordered-build', [sys.executable, __file__, '--build-binaries'])],
               out, SOURCE, env, identity, 1800)['status'] == 'PASS'
worker = json.loads((out / 'worker-identity.json').read_text())
env.update(BIGBROTR_ARCHIVE_WORKER_BYTES=str(worker['bytes']), BIGBROTR_ARCHIVE_WORKER_SHA256=worker['sha256'])
tls_fixture = ['--example', 'archive_receipt_probe'] if tls or publication else []
if archive_event: tls_fixture += ['--example', 'archive_event_probe']
if producer_ledger: tls_fixture += ['--example', 'ingest_bootstrap_probe']
publication_fixture = ['-p', 'bigbrotr-archive-worker', '--example', 'archive_publication_probe', '--example', 'archive_read_probe', '--example', 'archive_coordinator_probe', '--example', 'archive_attempt_probe'] if publication else []
checks = [
    ('fixture', ['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr', '--example', 'catalog_schema_probe', '--example', 'catalog_initialization_probe', *tls_fixture, *publication_fixture]),
    ('schema-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--test', 'control_schema']),
    ('parent-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive', '--', '--nocapture']),
    ('cli-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--bin', 'bigbrotr', '--test', 'catalog_entrypoint', '--test', 'control_runtime_entrypoint']),
    ('clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '-p', 'bigbrotr-archive-worker', '--lib', '--example', 'catalog_schema_probe', '--example', 'catalog_initialization_probe', *tls_fixture, *publication_fixture[2:], '--bin', 'bigbrotr', '--bin', 'bigbrotr-archive-worker', '--', '-D', 'warnings']),
]
if fixture_only:
    identity.update(scope='producer reservation fixture corrections; unchanged product inputs',
                    baseline_source=baseline['revision'])
    checks = [checks[0],
        ('fixture-policy', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--example', 'ingest_bootstrap_probe']),
        ('fixture-clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--example', 'ingest_bootstrap_probe', '--', '-D', 'warnings'])]
if archive_event:
    identity.update(scope='private exact historical event hydration and supervised bounded body delivery')
    env.update(BIGBROTR_ARCHIVE_TEST_BUNDLE=str(bundle))
    checks = [checks[0], checks[2],
        ('native-events', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'event::tests', '--', '--nocapture']),
        checks[4]]
    if publication_sync:
        identity.update(scope='held local file/directory synchronization before native registration')
        checks = [checks[0],
            ('native-publication', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'publication::tests', '--', '--nocapture']),
            checks[-1]]
    if reader_custody:
        identity.update(scope='actual coordinator custody inherited by native readers; no pin or read authority')
        checks = [checks[0],
            ('coordinator-custody', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive::initialization::tests', '--', '--test-threads=1']),
            ('reader-parent', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive::events']),
            ('cut-parent', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive::cut']),
            checks[-1]]
if import_source:
    identity.update(scope='captured JSONL source and existing connected signed-admission consumer')
    checks = [checks[0],
        ('source-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--test', 'acquisition_import_jsonl']),
        ('source-faults', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'acquisition::import']),
        ('source-lifetime', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--doc', 'acquisition::import::jsonl::JsonlRecord']),
        ('source-clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--test', 'acquisition_import_jsonl', '--example', 'ingest_bootstrap_probe', '--', '-D', 'warnings'])]
if archive_membership:
    identity.update(scope='retained batch source/digest/original-file census and installed administrative CLI')
    env.update(BIGBROTR_ARCHIVE_TEST_BUNDLE=str(bundle))
    checks = [checks[0],
        ('canonical-membership', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'membership::tests']),
        ('native-membership', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'native_membership_requires_complete_source_and_original_files_in_one_snapshot', '--', '--nocapture']),
        ('receipt-regression', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'receipt::tests', '--', '--nocapture']),
        ('parent-membership', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive::membership']),
        ('parent-receipts', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive::receipts']),
        ('cli', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--test', 'catalog_entrypoint']),
        ('protocol', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-protocol', '--lib', 'archive_membership']),
        checks[-1]]
if archive_cut:
    identity.update(scope='complete native cut, catalog barrier and fresh persistence participation; frontier/pins remain unimplemented')
    env.update(BIGBROTR_ARCHIVE_TEST_BUNDLE=str(bundle))
    checks = [checks[0], checks[1],
        ('native-cut', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'native_complete_cut', '--', '--nocapture']),
        ('native-closure', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'native_cut_closure', '--', '--nocapture']),
        ('collision', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'existing_attempt_files_and_symlinked_table_parent_are_never_reused']),
        ('receipt-regression', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'receipt::tests']),
        ('membership-regression', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-archive-worker', '--lib', 'native_membership_requires_complete_source_and_original_files_in_one_snapshot']),
        ('parent-cut', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::archive::cut']),
        ('protocol-cut', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-protocol', '--lib', 'archive_cut']),
        checks[3], checks[4]]
if input_bindings:
    identity.update(scope='atomic import position/original input binding prerequisite')
    checks = [checks[0],
        ('binding-tests', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--test', 'import_input_bindings', '--test', 'control_schema']),
        ('binding-clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--test', 'import_input_bindings', '--example', 'ingest_bootstrap_probe', '--', '-D', 'warnings'])]
if archive_frontier:
    identity.update(scope='actual native cut to immutable fenced Archive frontier; pin/served-read qualification remains open')
    checks = [checks[0], checks[1], checks[-1]]
if certification:
    identity.update(scope='native producer closing and historical target under current Control' if producer_closing else 'initial existing-effect native read and connected Control consumer')
    if producer_recovery:
        identity.update(scope='positive original-request resolution and current-Control copy after native producer closing')
    checks = [checks[0],
        ('certification-clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr', '--lib', '--example', 'ingest_bootstrap_probe', '--', '-D', 'warnings'])]
assert execute(checks, out / 'checks', SOURCE, env, identity, 1800)['status'] == 'PASS'
for src, dst in [*([('examples/ingest_bootstrap_probe', out / 'ingest-bootstrap-probe')] if producer_ledger else []), ('bigbrotr', out / 'bigbrotr'), ('bigbrotr-archive-worker', bundle / 'bigbrotr-archive-worker'),
                 ('examples/catalog_schema_probe', out / 'catalog-schema-probe'),
                 ('examples/catalog_initialization_probe', out / 'catalog-initialization-probe'),
                 *([('examples/archive_receipt_probe', out / 'archive-receipt-probe')] if tls or publication else []),
                 *([('examples/archive_event_probe', out / 'archive-event-probe')] if archive_event else []),
                 *([('examples/archive_publication_probe', out / 'archive-publication-probe'),
                    ('examples/archive_read_probe', out / 'archive-read-probe'),
                    ('examples/archive_coordinator_probe', out / 'archive-coordinator-probe'),
                    ('examples/archive_attempt_probe', out / 'archive-attempt-probe')] if publication else [])]:
    shutil.copy2(build / 'target/release' / src, dst)
    dst.chmod(0o500)
if certification:
    assert file_hash(bundle / 'bigbrotr-archive-worker') == worker['sha256']
    env.update(BIGBROTR_ARCHIVE_TEST_INSTALLATION=str(bundle),
               BIGBROTR_ARCHIVE_TEST_WORKER_BYTES=str(worker['bytes']),
               BIGBROTR_ARCHIVE_TEST_WORKER_SHA256=worker['sha256'])
    assert execute([
        ('native-admission-certification', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::rocks::store::' if producer_closing else 'storage::rocks::store::admission_tests', '--', '--nocapture']),
    ], out / 'certification', SOURCE, env, identity, 1200)['status'] == 'PASS'
if producer_inputs and not fixture_only and not import_source and not input_bindings and not certification:
    # The actual same-source packaged worker is required by the native pending
    # consumer test; library-only success cannot substitute for that process.
    assert file_hash(bundle / 'bigbrotr-archive-worker') == worker['sha256']
    env.update(BIGBROTR_ARCHIVE_TEST_INSTALLATION=str(bundle),
               BIGBROTR_ARCHIVE_TEST_WORKER_BYTES=str(worker['bytes']),
               BIGBROTR_ARCHIVE_TEST_WORKER_SHA256=worker['sha256'])
    assert execute([
        ('model', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr-model']),
        ('native-owner', ['cargo', 'test', '--locked', '--release', '-p', 'bigbrotr', '--lib', 'storage::rocks', '--', '--nocapture']),
        ('input-clippy', ['cargo', 'clippy', '--locked', '--release', '-p', 'bigbrotr-model', '-p', 'bigbrotr', '--all-targets', '--', '-D', 'warnings']),
    ], out / 'producer-inputs', SOURCE, env, identity, 1200)['status'] == 'PASS'
assert source_files() == manifest['files_sha256']
info = json.loads(subprocess.check_output([str(out / 'bigbrotr'), 'build-info'], env={}))
assert info['archive_worker'] == worker and info['source']['revision'] == manifest['revision']
assert info['source']['dirty'] is False
result = dict(identity, status='READY', control=file_hash(out / 'bigbrotr'), worker=worker['sha256'],
              probe=file_hash(out / 'catalog-schema-probe'), initializer=file_hash(out / 'catalog-initialization-probe'),
              build_info=info, source_unchanged=True)
if producer_ledger:
    result['ingest_probe'] = file_hash(out / 'ingest-bootstrap-probe')
if publication:
    result.update(publisher=file_hash(out / 'archive-publication-probe'), reader=file_hash(out / 'archive-read-probe'))
    result.update(coordinator_probe=file_hash(out/'archive-coordinator-probe'), attempt_probe=file_hash(out/'archive-attempt-probe'))
if tls or publication:
    result['receipt_reader'] = file_hash(out / 'archive-receipt-probe')
if archive_event:
    result['event_reader'] = file_hash(out / 'archive-event-probe')
if publication_sync:
    log = (out / 'checks/native-publication.log').read_text()
    paths = re.findall(r'Running unittests src/lib.rs \((/out/build/target/release/deps/bigbrotr_archive_worker-[0-9a-f]+)\)', log)
    assert len(paths) == 1
    result['publication_tests'] = dict(path=paths[0], sha256=file_hash(Path(paths[0])))
(out / 'ready.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'status': 'READY', 'source': manifest['revision']}), flush=True)
