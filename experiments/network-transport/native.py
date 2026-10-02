#!/usr/bin/env python3
"""Native guarded transport qualification; not installed acquisition acceptance."""
import json
from pathlib import Path
import shutil
import sys
import subprocess

sys.dont_write_bytecode = True
SOURCE = Path('/out/build/source')
sys.path.insert(0, str(SOURCE))
from tools.build import clean_environment, file_hash, native_toolchain
from tools.ci import execute

build = SOURCE.parent
manifest = json.loads((build / 'source-manifest.json').read_text())


def sources():
    return {str(p.relative_to(SOURCE)): file_hash(p)
            for p in sorted(SOURCE.rglob('*')) if p.is_file()}


assert sources() == manifest['files_sha256']
env = clean_environment()
selected, native = native_toolchain(env, 2)
env.update(selected)
env.update(CARGO_HOME=str(build / 'cargo-home'), CARGO_TARGET_DIR=str(build / 'target'),
           CARGO_NET_OFFLINE='true', BIGBROTR_SOURCE_REVISION=manifest['revision'],
           BIGBROTR_SOURCE_TREE=manifest['tree'], BIGBROTR_BUILD_PROFILE='release',
           BIGBROTR_SOURCE_MANIFEST_SHA256=file_hash(build / 'source-manifest.json'))
identity = dict(source=manifest['revision'], tree=manifest['tree'], native_toolchain=native,
                lockfile_sha256=file_hash(SOURCE / 'Cargo.lock'),
                source_manifest_sha256=file_hash(build / 'source-manifest.json'),
                scope='Native actual direct TCP/TLS/DNS and destination policy; no installed acquisition, WebSocket, Control authorization or capacity claim')
identity['rustc'] = subprocess.check_output(['rustc', '--version', '--verbose'], env=env, text=True).strip()
assert 'host: x86_64-unknown-linux-gnu' in identity['rustc']
assert identity['rustc'].startswith('rustc 1.98.1 ')
mode = sys.argv[1] if len(sys.argv) == 2 else 'direct'
assert len(sys.argv) in (1, 2) and mode in ('direct', 'websocket', 'relay-wire', 'selector', 'request-session', 'acquisition-transport')
tests = {
    'acquisition-transport': {'acquisition_transport': 6, 'network_direct': 14, 'postgres_connection_settings': 7},
    'direct': {'network_direct': 12},
    'websocket': {'network_websocket': 30},
    'relay-wire': {'network_websocket': 1, 'relay_wire': 8},
    'selector': {'network_websocket': 2, 'collection_selector': 8, 'selector': 5},
    'request-session': {'acquisition_session': 7, 'relay_request': 2},
}[mode]
packages = ['-p', 'bigbrotr']
if mode in ('relay-wire', 'selector', 'request-session'):
    packages += ['-p', 'bigbrotr-protocol']
if mode == 'selector':
    packages += ['-p', 'bigbrotr-model']
selectors = [arg for name in tests for arg in ['--test', name]]
if mode == 'acquisition-transport':
    identity['scope'] = 'Acquisition deployment roots and configured TLS; no installed Sync or capacity claim'
elif mode == 'websocket':
    identity['scope'] = 'Native bounded WebSocket over actual guarded TCP/TLS; no installed Nostr acquisition, shared service quotas or production capacity claim'
elif mode == 'relay-wire':
    identity['scope'] = 'Native preserving Nostr relay decoder and signed actual socket corpus; no installed acquisition, request-generation authority, durable admission or coverage claim'
elif mode == 'selector':
    identity['scope'] = 'Native bounded collection identity/membership and signed actual socket corpus; no persistent scope registration, installed Sync, durable admission, coverage or capacity claim'
elif mode == 'request-session':
    identity['scope'] = 'Native actual WebSocket request generations, borrowed predicates, bounded encoding and cancellation; no job/scope authority, observation custody, durable admission, progress or installed Sync claim'
compile_selectors = selectors + (['--lib'] if mode == 'websocket' else [])
checks = Path('/out/build-checks')
build_commands = [
    ('compile', ['cargo', 'test', '--locked', '--offline', '--release', *packages,
                 *compile_selectors, '--no-run', '--message-format=json']),
    ('clippy', ['cargo', 'clippy', '--locked', '--offline', '--release', *packages,
                '--lib', *selectors, '--', '-D', 'warnings']),
]
if mode == 'relay-wire':
    build_commands.append(('bounded-api', ['cargo', 'test', '--locked', '--offline', '--release',
                                          '-p', 'bigbrotr-protocol', '--doc']))
result = execute(build_commands, checks, SOURCE, env, identity, 1800)
assert result['status'] == 'PASS'
artifacts = Path('/out/artifacts')
artifacts.mkdir()
executables = {}
for line in (checks / 'compile.log').read_text().splitlines():
    if not line.startswith('{'):
        continue
    message = json.loads(line)
    if message.get('reason') != 'compiler-artifact' or not message.get('executable'):
        continue
    name = message['target']['name']
    selected = name in tests and message['target']['kind'] == ['test']
    selected |= mode == 'acquisition-transport' and name == 'bigbrotr' and message['target']['kind'] == ['bin'] and not message['profile']['test']
    selected |= mode == 'websocket' and name == 'bigbrotr' and message['target']['kind'] == ['lib'] and message['profile']['test']
    if selected:
        if name == 'bigbrotr' and mode == 'websocket':
            name = 'network_websocket_input'
        assert name not in executables
        executable = Path(message['executable'])
        assert executable.is_relative_to(build / 'target') and not executable.is_symlink()
        shutil.copy2(executable, artifacts / name)
        executables[name] = file_hash(artifacts / name)
expected = set(tests) | ({'network_websocket_input'} if mode == 'websocket' else {'bigbrotr'} if mode == 'acquisition-transport' else set())
assert executables.keys() == expected
identity['executables_sha256'] = executables
socket_filter = {'relay-wire': ['nostr::signed_socket_corpus_checks_every_payload_and_preserves_eose_hints', '--exact'],
                 'selector': ['nostr::']}.get(mode, [])
commands = [(name, [str(artifacts / name),
                   *(socket_filter if name == 'network_websocket' else []),
                   '--test-threads=1', '--nocapture']) for name in tests]
if mode == 'websocket':
    tests['network_websocket_input'] = 2
    commands.append(('network_websocket_input', [str(artifacts / 'network_websocket_input'), 'network::websocket', '--test-threads=1', '--nocapture']))
result = execute(commands,
                 Path('/out/tests'), SOURCE, env, identity, 60)
assert result['status'] == 'PASS'
for name, count in tests.items():
    assert f'test result: ok. {count} passed; 0 failed; 0 ignored;' in (Path('/out/tests') / (name + '.log')).read_text()
assert sources() == manifest['files_sha256']
assert all(file_hash(artifacts / name) == digest for name, digest in executables.items())
status = {'acquisition-transport': 'PASS_ACQUISITION_TRANSPORT_ROOTS', 'direct': 'PASS_SCOPED_NETWORK_COMPONENTS',
          'websocket': 'PASS_SCOPED_WEBSOCKET_COMPONENTS',
          'relay-wire': 'PASS_SCOPED_RELAY_WIRE_COMPONENTS',
          'selector': 'PASS_SCOPED_SELECTOR_COMPONENTS',
          'request-session': 'PASS_SCOPED_REQUEST_SESSION_COMPONENTS'}[mode]
Path('/out/qualification.json').write_text(json.dumps(
    dict(identity, status=status, tests=tests), indent=2) + '\n')
