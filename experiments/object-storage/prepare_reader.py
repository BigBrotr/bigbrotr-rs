"""Lab-only credential provisioning; never prints generated secrets."""
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path('/home/tux/bigbrotr-object-storage-20260917')


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout


if '--existing' in sys.argv:
    raw = run('docker', 'exec', 'bigbrotr-object-storage-garage-20260917', '/garage', 'key',
              'info', '--show-secret', 'archive-reader')
else:
    raw = run('docker', 'exec', 'bigbrotr-object-storage-garage-20260917', '/garage', 'key',
              'create', 'archive-reader')
credentials = {'key': re.search(r'Key ID:\s+(\S+)', raw).group(1),
               'secret': re.search(r'Secret key:\s+(\S+)', raw).group(1)}
path = ROOT / 'reader.json'
path.write_text(json.dumps(credentials))
path.chmod(0o600)
run('docker', 'exec', 'bigbrotr-object-storage-garage-20260917', '/garage', 'bucket', 'allow',
    '--read', 'qualification', '--key', 'archive-reader')
sql = '''CREATE ROLE archive_reader LOGIN;
ALTER ROLE archive_reader SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE catalog_s3 TO archive_reader;
GRANT USAGE ON SCHEMA public TO archive_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO archive_reader;
'''
run('docker', 'exec', '-i', 'bigbrotr-object-storage-pg-20260917', 'psql', '-U', 'postgres',
    '-d', 'catalog_s3', '-v', 'ON_ERROR_STOP=1', input=sql)
print('Provisioned lab reader with read-only bucket access and PG read-only defaults')
