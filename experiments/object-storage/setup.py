"""Create an isolated, loopback-only S3 research lab; never a product installer."""

import json
import os
from pathlib import Path
import secrets
import subprocess

ROOT = Path('/home/tux/bigbrotr-object-storage-20260917')
PREFIX = 'bigbrotr-object-storage-'
GARAGE = 'dxflrs/garage@sha256:866bd13ed2038ba7e7190e840482bc27234c4afaf77be8cfa439ae088c1e4690'
PYTHON = 'python@sha256:1042b61448fef4ba92d16a8c7eb4996d027568ce64792a7877fd88511e0af7c6'
POSTGRES = 'postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94'


def run(*args):
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout


def main():
    if ROOT.exists():
        raise SystemExit('Refusing to overwrite an existing lab')
    if os.statvfs('/home/tux').f_bavail * os.statvfs('/home/tux').f_frsize < 200 * 2**30:
        raise SystemExit('Insufficient disk headroom')
    ROOT.mkdir(mode=0o700)
    for name in ('garage-meta', 'garage-data', 'pg', 'reports', 'src', 'local'):
        (ROOT / name).mkdir()
    credentials = {'key': 'GK' + secrets.token_hex(16), 'secret': secrets.token_hex(32)}
    (ROOT / 'credentials.json').write_text(json.dumps(credentials))
    (ROOT / 'credentials.json').chmod(0o600)
    (ROOT / 'garage.env').write_text(
        f"GARAGE_DEFAULT_ACCESS_KEY={credentials['key']}\n"
        f"GARAGE_DEFAULT_SECRET_KEY={credentials['secret']}\n"
        'GARAGE_DEFAULT_BUCKET=qualification\n')
    (ROOT / 'garage.env').chmod(0o600)
    (ROOT / 'garage.toml').write_text(
        'metadata_dir = "/lab/garage-meta"\ndata_dir = "/lab/garage-data"\n'
        'db_engine = "sqlite"\nreplication_factor = 1\n'
        'rpc_bind_addr = "127.0.0.1:48701"\nrpc_public_addr = "127.0.0.1:48701"\n'
        f'rpc_secret = "{secrets.token_hex(32)}"\n'
        '[s3_api]\ns3_region = "garage"\napi_bind_addr = "127.0.0.1:48700"\n'
        'root_domain = ".s3.garage.localhost"\n'
        '[admin]\napi_bind_addr = "127.0.0.1:48703"\n'
        f'admin_token = "{secrets.token_hex(32)}"\n')
    (ROOT / 'garage.toml').chmod(0o600)
    run('docker', 'run', '-d', '--name', PREFIX + 'garage-20260917', '--network', 'host',
        '--cpus', '2', '--memory', '4g', '--user', '1000:1000',
        '--env-file', str(ROOT / 'garage.env'), '-v', f'{ROOT}:/lab',
        '-v', f'{ROOT}/garage.toml:/etc/garage.toml:ro', GARAGE,
        '/garage', 'server', '--single-node', '--default-bucket')
    run('docker', 'run', '-d', '--name', PREFIX + 'pg-20260917', '--cpus', '1',
        '--memory', '1g', '-p', '127.0.0.1:48732:5432',
        '-e', 'POSTGRES_DB=qualification', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust',
        '-v', f'{ROOT}/pg:/var/lib/postgresql/data', POSTGRES)
    run('docker', 'run', '-d', '--name', PREFIX + 'worker-20260917', '--network', 'host',
        '--cpus', '4', '--memory', '10g', '--user', '1000:1000', '-e', 'HOME=/lab',
        '-v', f'{ROOT}:/lab',
        '-v', '/home/tux/bigbrotr-qualification-20260917:/baseline:ro',
        PYTHON, 'sleep', 'infinity')
    print(json.dumps({'root': str(ROOT), 'images': [GARAGE, POSTGRES, PYTHON],
                      'memory_limit_gib_total': 15, 'cpu_quota_total': 7,
                      'single_node_redundancy': False}))


if __name__ == '__main__':
    main()
