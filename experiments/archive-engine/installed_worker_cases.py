#!/usr/bin/env python3
"""Actual installed CLI checks, with independent hashes and reversible bad copies."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(65536), b''):
            digest.update(block)
    return digest.hexdigest()


def run(package, output):
    output.mkdir(parents=True, exist_ok=False)
    relocated = output / 'relocated'
    shutil.copytree(package, relocated)
    binary = relocated / 'bigbrotr'
    bundle = relocated / 'archive-worker'
    worker = bundle / 'bigbrotr-archive-worker'
    expected = {'bytes': worker.stat().st_size, 'sha256': sha(worker)}
    original = {str(p.relative_to(relocated)): sha(p) for p in relocated.rglob('*') if p.is_file()}
    cwd = output / 'empty-cwd'
    cwd.mkdir()
    result = {'format': 'bigbrotr-installed-worker-cases/1', 'status': 'RUNNING',
              'binary_sha256': sha(binary), 'worker': expected, 'cases': []}

    def save():
        (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')

    def check(name, code, explicit=False, env=None):
        command = [str(binary), 'archive', 'verify-worker']
        if explicit:
            command += ['--root', str(bundle)]
        completed = subprocess.run(command, cwd=cwd, env=env or {}, capture_output=True, timeout=45)
        row = {'name': name, 'command': command, 'expected_exit': code,
               'exit_code': completed.returncode, 'stdout': completed.stdout.decode(),
               'stderr': completed.stderr.decode(), 'status': 'FAIL'}
        result['cases'].append(row)
        save()
        assert completed.returncode == code, row
        if code == 0:
            document = json.loads(completed.stdout)
            assert document['format'] == 'bigbrotr-worker-verification/1'
            assert document['result'] == 'verified' and document['executable'] == expected
            assert document['scope'] == 'bundle bytes only; no engine, catalog or readiness check'
            assert not completed.stderr
        else:
            assert not completed.stdout
            assert b'archive worker verification refused' in completed.stderr
        assert not list(cwd.iterdir())
        row['status'] = 'PASS'
        save()

    def changed_byte(path, name, forged=False):
        mode = path.stat().st_mode & 0o777
        path.chmod(0o600)
        with path.open('r+b') as stream:
            old = stream.read(1)
            stream.seek(0)
            stream.write(bytes([old[0] ^ 1]))
        path.chmod(mode)
        sidecar = relocated / 'build-result.json'
        try:
            env = None
            if forged:
                substitute = {'bytes': path.stat().st_size, 'sha256': sha(path)}
                sidecar.write_text(json.dumps({'archive_worker': substitute}))
                env = {'BIGBROTR_ARCHIVE_WORKER_BYTES': str(substitute['bytes']),
                       'BIGBROTR_ARCHIVE_WORKER_SHA256': substitute['sha256']}
            check(name, 4, env=env)
        finally:
            if forged:
                sidecar.unlink()
            path.chmod(0o600)
            with path.open('r+b') as stream:
                stream.write(old)
            path.chmod(mode)

    save()
    try:
        check('relocated-default-root', 0)
        check('explicit-root', 0, explicit=True)
        changed_byte(worker, 'altered-worker-forged-receipt-and-environment', forged=True)
        libraries = sorted((bundle / 'lib').glob('libduckdb.*'))
        assert len(libraries) == 1
        for path in libraries:
            changed_byte(path, 'altered-library')
        extensions = sorted((bundle / 'extensions').glob('*.duckdb_extension'))
        assert len(extensions) == 3
        for path in extensions:
            changed_byte(path, 'altered-' + path.stem)
        path = extensions[0]
        backup = path.with_suffix('.retained')
        path.rename(backup)
        try:
            check('missing-extension', 4)
        finally:
            backup.rename(path)
        backup = worker.with_suffix('.retained')
        worker.rename(backup)
        try:
            worker.symlink_to(backup)
            check('symlink-worker', 4)
        finally:
            worker.unlink()
            backup.rename(worker)
        mode = worker.stat().st_mode & 0o777
        try:
            worker.chmod(0o700)
            with worker.open('ab') as stream:
                stream.write(b'x')
            check('wrong-worker-length', 4)
        finally:
            with worker.open('r+b') as stream:
                stream.truncate(expected['bytes'])
            worker.chmod(mode)
        try:
            worker.chmod(0o722)
            check('writable-worker', 4)
        finally:
            worker.chmod(mode)
        check('restored-bundle', 0)
        assert original == {str(p.relative_to(relocated)): sha(p) for p in relocated.rglob('*') if p.is_file()}
        result.update(status='PASS', unchanged_after_restoration=True)
    finally:
        save()
    return result
