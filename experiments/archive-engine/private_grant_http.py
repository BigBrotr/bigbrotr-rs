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
from urllib.parse import quote

ROOT = Path('/out/private-grant-http')
BINARY = Path('/out/bigbrotr')
CONFIG = ROOT / 'instance.toml'
PASSWORD = 'Independent HTTP native grant password 2026'
ADDRESS = '127.0.0.1:18080'


def cli(*args):
    result = subprocess.run([str(BINARY), *args, '--file', str(CONFIG)], env={}, capture_output=True, timeout=30)
    assert result.returncode == 0, (args[:2], result.returncode, result.stderr.decode())
    return json.loads(result.stdout)


def prepare(mode='normal'):
    assert mode in ('normal', 'large')
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
archive_root="/out/signed_{mode}/kind=7/"
archive_state_root="/out/signed_{mode}/archive-state"
scratch_root="{ROOT}/scratch"
[postgres.control]
endpoint="127.0.0.1:5432"
database="bbpub_control_{mode}"
user="bigbrotr_control"
password_ref="file:{ROOT}/bigbrotr_control"
[postgres.catalog]
endpoint="127.0.0.1:5432"
database="bbpub_{mode}"
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
    def ready(self, revision=2):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert self.process.poll() is None, self.error.read_text()
            lines = self.output.read_text().splitlines()
            if lines:
                ready = json.loads(lines[0])
                assert ready['state'] == 'idle' and ready['policy_revision'] == str(revision)
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


def exercise(history=False):
    principal = (ROOT / 'principal').read_text()
    policy_revision = 2
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
        from signed_publication_oracle import EVENT
        history_query = dict(version=1,resource='event_history',mode='snapshot',scope=command['scope'],filters=[dict(ids=[EVENT['id']])])
        def query(value=history_query, method='POST', credentials=None):
            credentials = auth if credentials is None else credentials
            path='/v1/resources/event_history'
            if method=='GET':
                parts={key:val for key,val in value.items() if key!='resource'}
                encoded='&'.join(key+'='+quote(val if key=='mode' else json.dumps(val,separators=(',',':')),safe='') for key,val in parts.items())
                return request(method,path+'?'+encoded,auth=credentials)
            return request(method,path,value,credentials)
        def history_page(method='POST', missing=False):
            value={**history_query,'filters':[dict(ids=['00'*32])]} if missing else history_query
            status,page,headers=query(value,method)
            assert status==200, (status,page)
            assert set(page)=={'version','resource','items','effective_page_size','next_cursor','mode','candidate_cut','view_stamp','eligibility_lag_us'}
            assert page['version']==1 and page['resource']=='event_history' and page['mode']=='snapshot'
            assert page['items']==([] if missing else [EVENT])
            assert page['effective_page_size']==100 and page['next_cursor'] is None and page['candidate_cut'] is None and page['eligibility_lag_us'] is None
            stamp=page['view_stamp']
            assert set(stamp)=={'source_cut','input_generations','target_generations','visibility_as_of_us','temporal_generation','expiry_deadline_us','policy_epoch'}
            assert stamp['source_cut']==dict(dataset_id='01'*16,topology_generation='0',bounds=[dict(stream=dict(shard_id=0,owner_epoch='1'),last_complete_sequence='10')])
            assert stamp['input_generations']==[dict(kind='archive',catalog_id='5a'*16,frontier_revision='1',snapshot_id='3')]
            assert stamp['target_generations']==[] and stamp['temporal_generation'] is None and stamp['expiry_deadline_us'] is None and stamp['policy_epoch']==str(policy_revision)
            assert isinstance(stamp['visibility_as_of_us'],str) and stamp['visibility_as_of_us'].isdigit() and int(stamp['visibility_as_of_us'])>0
            assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
            assert not any(k.lower()=='set-cookie' for k,_ in headers)
        if history:
            assert query()[0]==403
            assert query(credentials={})[0]==401
            case('history-needs-real-session-and-separate-scope-grant')
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
        if history:
            history_page('GET'); history_page('POST'); history_page(missing=True)
            case('history-get-post-exact-signed-event-and-completed-empty-page')
            other={**history_query,'scope':{**command['scope'],'ordinal':'2'}}
            assert query(other)[0]==403
            assert query({**history_query,'principal':principal})[0]==400
            assert query({**history_query,'cursor':'unissued'})[0]==400
            assert query(credentials={**auth,'X-CSRF-Token':'00'*32})[0]==401
            case('history-other-scope-csrf-and-unsupported-query-refuse')
            bundle.rename(unavailable)
            assert query()[0]==503
            unavailable.rename(bundle)
            assert request('GET','/management/v1/me',auth=auth)[0]==200
            case('history-missing-native-bundle-is-unavailable-not-empty')
            # Host accepts complete immutable fixture policies only while this
            # actual Control process is stopped. This does not test live rollout.
            for policy_revision, name in [(3,'memory'),(4,'scan'),(5,'response'),(6,'restored')]:
                server.stop()
                (ROOT/f'policy-{policy_revision}.waiting').write_text('stopped\n')
                end=time.monotonic()+20
                while not (ROOT/f'policy-{policy_revision}.ready').exists():
                    if time.monotonic()>=end: raise TimeoutError('fixture policy acceptance')
                    time.sleep(.02)
                server=Server(f'limit-{name}')
                server.ready(policy_revision)
                if name=='restored': history_page()
                else:
                    status,value,_=query()
                    assert status==429, (name,status,value)
                    assert request('GET','/management/v1/me',auth=auth)[0]==200
                assert not list((ROOT/'scratch').iterdir())
                case(f'history-configured-{name}-limit')
        status, retried = change(command, auth, evidence)
        assert status == 200 and not retried['created']
        assert {**enabled,'created':False} == retried
        case('identical-http-retry-preserves-original-acceptance')
        server.stop()
        server = Server('restart')
        server.ready(policy_revision)
        assert change(command, auth, evidence) == (200,retried)
        if history:
            history_page('GET')
            case('history-real-server-restart-preserves-signed-page')
        revoke = {**command,'enabled':False,'allocation':None,'expected_grant_revision':'2','expected_policy_revision':str(policy_revision)}
        revoke_auth = {**auth,'Idempotency-Key':'c2'*16}
        revoke_proof = proof(revoke,revoke_auth)
        status, disabled = change(revoke,revoke_auth,revoke_proof)
        assert status == 200 and not disabled['enabled'] and disabled['created'], (status,disabled)
        assert disabled['scope_generation'] == '2' and disabled['account_grant_revision'] == '3'
        assert change(command,auth,evidence) == (200,retried)
        case('restart-retry-and-revoke-do-not-reapply-old-enable')
        if history:
            assert query()[0]==403
            case('history-revoked-scope-remains-denied-after-old-enable-retry')
        # Later fault clients continue this real session. Additional logins would
        # exhaust the shared five-attempt account quota before native work starts.
        # The collector excludes this file and scans its values as secret canaries.
        session = {key: auth[key] for key in ['Cookie', 'X-CSRF-Token']}
        descriptor = os.open(ROOT/'session.private', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(session, stream)
        server.stop()
        assert not list((ROOT/'scratch').iterdir())
    finally:
        server.cleanup()
        if unavailable.exists(): unavailable.rename(bundle)
    print(json.dumps(dict(status='PASS',cases=len(cases))))


if __name__ == '__main__':
    {'prepare':prepare,'prepare-large':lambda:prepare('large'),'enroll':enroll,'exercise':exercise,'exercise-history':lambda:exercise(True)}[sys.argv[1]]()
