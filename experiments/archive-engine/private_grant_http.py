#!/usr/bin/env python3
"""Isolated installed private-grant client; literal signed scope, no grant mocks.

The host harness owns PostgreSQL setup and checks durable rows independently.
Passwords/tokens stay under the private fixture root and are never case output.
"""
import http.client
from http.cookies import SimpleCookie
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path('/out/private-grant-http')
BINARY = Path('/out/bigbrotr')
CONFIG = ROOT / 'instance.toml'
PASSWORD = 'Independent HTTP native grant password 2026'
ADDRESS = '127.0.0.1:18080'


def cli(*args):
    result = subprocess.run([str(BINARY), *args, '--file', str(CONFIG)], env={}, capture_output=True, timeout=30)
    assert result.returncode == 0, (args[:2], result.returncode, result.stderr.decode())
    return json.loads(result.stdout)


def prepare():
    ROOT.mkdir(mode=0o700)
    (ROOT / 'scratch').mkdir(mode=0o700)
    shutil.copy2('/out/native-tests/bigbrotr', BINARY)
    for role in ['postgres', 'CatalogManager', 'bigbrotr_control', 'bigbrotr_receipt_reader']:
        path = ROOT / role
        path.write_bytes(Path('/secrets', role).read_bytes())
        path.chmod(0o600)
    CONFIG.write_text(f'''[config]
schema_version=1
recipe="archive-only"
authority_mode="managed"
[topology]
local_roles=["control","discoverer","validator","monitor","sync","ingest","archive"]
instance_id="{'02'*16}"
dataset_id="{'01'*16}"
[archive]
catalog_id="{'5a'*16}"
worker_memory_bytes="512MiB"
[archive.reader]
enabled=true
[administration]
enabled=true
[administration.listen]
host="127.0.0.1"
port=18080
[storage]
archive_root="/out/signed_normal/kind=7/"
archive_state_root="/out/signed_normal/archive-state"
scratch_root="{ROOT}/scratch"
[postgres.control]
endpoint="127.0.0.1:5432"
database="bbpub_control_normal"
user="bigbrotr_control"
password_ref="file:{ROOT}/bigbrotr_control"
[postgres.catalog]
endpoint="127.0.0.1:5432"
database="bbpub_normal"
user="CatalogManager"
password_ref="file:{ROOT}/CatalogManager"
read_user="bigbrotr_receipt_reader"
read_password_ref="file:{ROOT}/bigbrotr_receipt_reader"
''')
    cli('config', 'validate')
    cli('migrate', 'apply', '--target', '811', '--user', 'postgres', '--password-ref', f'file:{ROOT}/postgres')


def enroll():
    token = cli('access', 'owner-token', 'issue', '--username', 'alice', '--request-id', '31'*16)
    for name, value in [('owner-token', token['token']), ('owner-password', PASSWORD)]:
        path = ROOT / name
        path.write_text(value)
        path.chmod(0o600)
    account = cli('access', 'enroll-owner', '--challenge-id', '31'*16,
                  '--token-ref', f'file:{ROOT}/owner-token', '--password-ref', f'file:{ROOT}/owner-password')
    (ROOT / 'principal').write_text(account['principal_id'])


def request(method, path, value=None, auth=None):
    connection = http.client.HTTPConnection('127.0.0.1', 18080, timeout=8)
    headers = {'Origin': f'http://{ADDRESS}', 'Content-Type': 'application/json'}
    headers.update(auth or {})
    connection.request(method, path, json.dumps(value) if value is not None else None, headers)
    response = connection.getresponse()
    data = response.read(16385)
    assert len(data) <= 16384
    result = response.status, json.loads(data), response.getheaders()
    connection.close()
    return result


class Server:
    def __init__(self, name):
        self.output = ROOT / f'{name}.stdout'
        self.error = ROOT / f'{name}.stderr'
        with self.output.open('xb') as out, self.error.open('xb') as err:
            self.process = subprocess.Popen([str(BINARY), 'serve', '--roles', 'control', '--file', str(CONFIG)], env={}, stdout=out, stderr=err)
    def ready(self):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert self.process.poll() is None, self.error.read_text()
            lines = self.output.read_text().splitlines()
            if lines:
                ready = json.loads(lines[0])
                assert ready['state'] == 'idle' and ready['policy_revision'] == '2'
                return ready
            time.sleep(.02)
        raise TimeoutError('private listener readiness')
    def stop(self):
        self.process.send_signal(signal.SIGTERM)
        assert self.process.wait(timeout=10) == 0, self.error.read_text()
        assert json.loads(self.output.read_text().splitlines()[-1])['release'] == 'committed'
        assert not self.error.read_text()
    def cleanup(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)


def exercise():
    principal = (ROOT / 'principal').read_text()
    cases = []
    def case(name, **detail):
        cases.append(dict(case=name, status='PASS', **detail))
        (ROOT / 'http-cases.json').write_text(json.dumps(cases, indent=2)+'\n')
    server = Server('initial')
    bundle, unavailable = Path('/out/archive-worker'), Path('/out/archive-worker-unavailable')
    try:
        server.ready()
        status, _, headers = request('POST', '/management/v1/sessions', dict(username='alice', password=PASSWORD))
        assert status == 201
        cookies = SimpleCookie()
        for name, value in headers:
            if name.lower() == 'set-cookie': cookies.load(value)
        auth = {'Cookie': 'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
                'X-CSRF-Token': cookies['BigBrotrLocalCsrf'].value, 'Idempotency-Key': 'c1'*16}
        command = dict(principal_id=principal, action='history.read', enabled=True,
                       scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
                       expected_config_revision='1',expected_policy_revision='2',expected_grant_revision='1',
                       allocation=dict(catalog_id='5a'*16,frontier_revision='1'))
        def proof(command, auth):
            status, value, _ = request('POST', '/management/v1/sessions/current/reauthenticate', dict(password=PASSWORD,command=command), auth)
            assert status == 201, (status,value)
            return value['proof_id']
        def change(command, auth, evidence):
            return request('PUT', f'/management/v1/principals/{principal}/grants', dict(command=command,proof_id=evidence), auth)[:2]
        evidence = proof(command, auth)
        bundle.rename(unavailable)
        status, value = change(command, auth, evidence)
        assert status == 503 and value['error']['code'] == 'authority_unavailable', (status,value)
        assert request('GET','/management/v1/me',auth=auth)[0] == 200
        assert not list((ROOT/'scratch').iterdir())
        unavailable.rename(bundle)
        case('missing-bundle-refuses-with-healthy-Control-and-no-scratch')
        started = time.monotonic()
        status, enabled = change(command, auth, evidence)
        assert status == 200 and enabled['enabled'] and enabled['created'], (status, enabled)
        assert enabled['scope_generation'] == '1' and enabled['account_grant_revision'] == '2'
        assert not list((ROOT/'scratch').iterdir())
        case('native-http-enable-reuses-unconsumed-proof', elapsed_ms=round((time.monotonic()-started)*1000,3))
        status, retried = change(command, auth, evidence)
        assert status == 200 and not retried['created']
        assert {**enabled,'created':False} == retried
        case('identical-http-retry-preserves-original-acceptance')
        server.stop()
        server = Server('restart')
        server.ready()
        assert change(command, auth, evidence) == (200,retried)
        revoke = {**command,'enabled':False,'allocation':None,'expected_grant_revision':'2'}
        revoke_auth = {**auth,'Idempotency-Key':'c2'*16}
        revoke_proof = proof(revoke,revoke_auth)
        status, disabled = change(revoke,revoke_auth,revoke_proof)
        assert status == 200 and not disabled['enabled'] and disabled['created'], (status,disabled)
        assert disabled['scope_generation'] == '2' and disabled['account_grant_revision'] == '3'
        assert change(command,auth,evidence) == (200,retried)
        case('restart-retry-and-revoke-do-not-reapply-old-enable')
        server.stop()
        assert not list((ROOT/'scratch').iterdir())
    finally:
        server.cleanup()
        if unavailable.exists(): unavailable.rename(bundle)
    print(json.dumps(dict(status='PASS',cases=len(cases))))


if __name__ == '__main__':
    {'prepare':prepare,'enroll':enroll,'exercise':exercise}[sys.argv[1]]()
