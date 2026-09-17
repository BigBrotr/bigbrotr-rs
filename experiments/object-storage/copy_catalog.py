"""Quiescent lab catalog backup into a fresh database on the same server."""
import hashlib
import json
from pathlib import Path
import subprocess

root = Path('/home/tux/bigbrotr-object-storage-20260917')
assert json.loads((root / 'reports/object-storage.json').read_text())['status'].startswith('PASS_')
backup = root / 'catalog-s3.dump'
assert not backup.exists()
with backup.open('xb') as output:
    subprocess.run(['docker', 'exec', 'bigbrotr-object-storage-pg-20260917',
                    'pg_dump', '-U', 'postgres', '-Fc', 'catalog_s3'], stdout=output, check=True)
subprocess.run(['docker', 'exec', 'bigbrotr-object-storage-pg-20260917',
                'createdb', '-U', 'postgres', 'catalog_restore'], check=True)
with backup.open('rb') as source:
    subprocess.run(['docker', 'exec', '-i', 'bigbrotr-object-storage-pg-20260917',
                    'pg_restore', '-U', 'postgres', '-d', 'catalog_restore', '--exit-on-error'],
                   stdin=source, check=True)
(root / 'reports/catalog-copy.json').write_text(json.dumps({
    'status': 'PASS_quiescent_catalog_copy', 'bytes': backup.stat().st_size,
    'sha256': hashlib.file_digest(backup.open('rb'), 'sha256').hexdigest(),
    'scope': 'same-server fresh PG database, no writers/maintenance; not off-host recovery'}, indent=2) + '\n')
