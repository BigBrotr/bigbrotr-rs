#!/usr/bin/env python3
"""Execute one private finite worker with fixture-only credentials, never argv/env."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

database, mode, root = sys.argv[1:]
assert database.startswith('bbcat_') and all(c in 'abcdefghijklmnopqrstuvwxyz_' for c in database)
assert mode in ['initialize-catalog','bootstrap-catalog']
assert root.startswith('/out/schema_') and root.endswith('/')
password=Path('/secrets/BootstrapMixed').read_text()
assert len(password)==64 and all(c in '0123456789abcdef' for c in password)
request=json.dumps({'dsn':f'host=127.0.0.1 port=5432 dbname={database} user=BootstrapMixed password={password} sslmode=disable connect_timeout=5','data_root':root})
with tempfile.TemporaryDirectory(prefix='schema-child-',dir='/out') as temp:
    result=subprocess.run(['/out/archive-worker/bigbrotr-archive-worker',mode,'/out/archive-worker',temp,'134217728','67108864','1'],input=request,text=True,capture_output=True,timeout=30,
        env={'BIGBROTR_ARCHIVE_PARENT_PID':str(os.getpid()),'LD_LIBRARY_PATH':'/out/archive-worker/lib'})
    assert len(result.stdout)+len(result.stderr)<4096
    assert password not in result.stdout+result.stderr
    print(json.dumps({'exit':result.returncode,'stdout':result.stdout,'stderr':result.stderr}))
