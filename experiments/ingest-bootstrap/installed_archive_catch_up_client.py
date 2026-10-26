"""Bounded child/process and real HTTP client for the same-root Archive journey.

This client never reads Control rows, native records, or expected receipts. Its
inputs are the explicit signed events and the independently selected allocation.
"""
import hashlib
import http.client as http_client
import json
import os
from pathlib import Path
import subprocess
import sys
import time

FLAGS = {'abandon1': '--abandon-acquisition', 'retire1': '--retire-initial',
    'abandon2': '--abandon-activated-acquisition', 'retire2': '--retire-activated-initial',
    'source3': '--close-successor-source', 'abandon3': '--abandon-successor-acquisition',
    'retire3': '--retire-successor'}


def redacted(value, private_values):
    serialized = json.dumps(value)
    assert all(json.dumps(secret)[1:-1] not in serialized for secret in private_values), 'private observations contain sensitive bytes'


def argv(operation, config):
    assert config.is_absolute() and config.parent == Path('/lab') and config.suffix == '.toml'
    base = ['/build/bigbrotr']
    if operation in FLAGS:
        return base + ['ingest', 'recover', '--file', str(config), FLAGS[operation]]
    if operation == 'catchup':
        return base + ['archive', 'catch-up', '--file', str(config)]
    assert operation in ('live2', 'live3', 'ordinary', 'ordinary-restart')
    args = base + ['serve', '--file', str(config)]
    if operation.startswith('live'):
        args += ['--activate-initial-successor' if operation == 'live2' else '--activate-successor']
    return args + ['--roles', 'control,ingest,sync' if operation == 'live3' else
        'control,ingest' if operation == 'live2' else 'control']


def publish(path, value):
    tmp = Path(str(path) + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream)
    tmp.rename(path)


def process(operation, config, prefix):
    command = argv(operation, config)
    assert prefix.is_absolute() and prefix.parent == Path('/lab')
    started, forced, error = time.monotonic(), False, None
    with Path(str(prefix) + '.stdout').open('xb') as out, Path(str(prefix) + '.stderr').open('xb') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env={})
        identity = dict(pid=child.pid, config=str(config), prefix=str(prefix), command=command)
        try:
            identity['start_ticks'] = int(Path(f'/proc/{child.pid}/stat').read_text().split()[21])
            publish(Path(str(prefix) + '.pid.json'), identity)
            child.wait(timeout=100 if operation == 'catchup' else 60)
        except BaseException as caught:
            forced, error = True, repr(caught)
            child.kill(); child.wait(timeout=5)
            raise
        finally:
            publish(Path(str(prefix) + '.terminal.json'), dict(identity, reaped=child.returncode is not None,
                exit_code=child.returncode, forced=forced, error=error,
                elapsed_seconds=time.monotonic() - started))


def http(home, restart, *, history_only=False):
    assert type(history_only) is bool and not (history_only and restart)
    profile = None
    if history_only:
        from installed_archive_profile import load, password_file
        profile = load(home)
        assert profile is not None, 'history requires an explicit fixture profile'
        password = password_file(profile['password_ref'])
    expected = json.loads((home / 'read-input.json').read_text())
    events = json.loads((home / 'events.json').read_text())
    assert len(events) == 2
    origin, observations = 'http://127.0.0.1:18081', []
    trace = home / ('restart-observations.json' if restart else 'read-observations.json')
    def record(label, **value):
        observations.append(dict(case=label, **value)); trace.write_text(json.dumps(observations, indent=2)+'\n')
    def request(method, path, body=None, headers=None):
        connection = http_client.HTTPConnection('127.0.0.1', 18081, timeout=6)
        try:
            connection.request(method, path, None if body is None else json.dumps(body),
                {'Origin': origin, 'Content-Type': 'application/json', **(headers or {})})
            answer = connection.getresponse(); raw = answer.read(65537)
            assert len(raw) <= 65536
            return answer.status, json.loads(raw)
        finally:
            connection.close()
    def access(*args):
        answer = subprocess.run([profile['product_binary'] if profile else '/build/bigbrotr',
            'access', *args], env={}, capture_output=True, timeout=6)
        assert answer.returncode == 0 and not answer.stderr, 'private access command refused'
        return json.loads(answer.stdout)
    secret = home / 'read-password.private'
    if not restart and not history_only:
        with os.fdopen(os.open(secret, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600), 'w') as stream:
            stream.write('Independent owner password 2026')
    session = home / ('restart-session.private' if restart else 'read-session.private')
    login = access('login', '--origin', origin, '--username', 'alice', '--password-ref',
        profile['password_ref'] if profile else 'file:'+str(secret), '--session-output', str(session))
    saved = json.loads(session.read_text())
    auth = {'Cookie':'BigBrotrLocalSession='+saved['session'], 'X-CSRF-Token':saved['csrf']}
    status, me = request('GET', '/management/v1/me', headers=auth)
    assert status == 200 and me['principal_id'] == (profile['expected_principal_id'] if profile else '10'*16)
    if profile:
        assert me['stored_actions'] == profile['expected_actions']
        assert me['config_revision'] == expected['config_revision'] == '1'
        assert me['policy_revision'] == expected['policy_revision'] == '1'
    record('login', response=login, me=me, digests={k:hashlib.sha256(domain+bytes.fromhex(saved[k])).hexdigest()
        for k,domain in [('session',b'bigbrotr/browser-session/1\0'),('csrf',b'bigbrotr/browser-csrf/1\0')]})
    allocation = dict(catalog_id=expected['catalog_id'], frontier_revision=str(expected['frontier_revision']))
    def query(event):
        return dict(version=1, resource='event_history', mode='snapshot', scope=expected['scope'],
            filters=[dict(ids=[event['id']])])
    body = None if history_only else dict(kind='export', format='signed-events-jsonl', query=query(events[1]), allocation=allocation,
        destination=dict(id=expected['destination_id'], binding_sha256=expected['binding_sha256']),
        expected_config_revision=expected['config_revision'], expected_policy_revision=expected['policy_revision'])
    keyed = {**auth,'Idempotency-Key':'d1'*16}
    if not restart and not history_only:
        export_denied=request('POST','/management/v1/jobs',body,keyed)
        history_denied=request('POST','/v1/resources/event_history',query(events[0]),auth)
        record('no-implicit-scope-authority',export=list(export_denied),history=list(history_denied))
        assert export_denied[0]==history_denied[0]==403
        for n,action in enumerate(('history.read','export','private-export'),1):
            command = dict(principal_id='10'*16, action=action, enabled=True, scope=expected['scope'],
                expected_config_revision=expected['config_revision'],expected_policy_revision=expected['policy_revision'],
                expected_grant_revision=str(int(me['grant_revision'])+n-1),allocation=allocation)
            headers = {**auth,'Idempotency-Key':f'{0xc0+n:02x}'*16}
            status,proof=request('POST','/management/v1/sessions/current/reauthenticate',
                dict(password='Independent owner password 2026',command=command),headers)
            record('reauthenticate-'+action,status=status,response=proof); assert status==201
            status,receipt=request('PUT','/management/v1/principals/'+'10'*16+'/grants',
                dict(command=command,proof_id=proof['proof_id']),headers)
            record('enable-'+action,command=command,key=headers['Idempotency-Key'],status=status,receipt=receipt)
            assert status==200 and receipt['created'] is True
    histories=[]
    for event in events:
        status,value=request('POST','/v1/resources/event_history',query(event),auth)
        record('history-'+event['id'],query=query(event),status=status,response=value)
        assert status==200 and value['items']==[event] and value['next_cursor'] is None
        assert value['view_stamp']['input_generations']==[dict(kind='archive',**allocation,
            snapshot_id=str(expected['snapshot_id']) if history_only else '8')]
        histories.append(value)
    if history_only:
        logout=access('logout','--session-ref','file:'+str(session))
        record('logout',response=logout)
        assert request('GET','/management/v1/me',headers=auth)[0]==401
        result=dict(status='PASS',histories=histories)
        redacted(observations, (saved['session'], saved['csrf'], password))
        publish(home/'history-client-result.json',result)
        return
    status,receipt=request('POST','/management/v1/jobs',body,keyed)
    record('accept-export',command=body,key='d1'*16,status=status,receipt=receipt)
    assert status==(200 if restart else 202) and receipt['created'] is not restart
    if restart:
        first=json.loads((home/'read-client-result.json').read_text())
        assert receipt==dict(first['receipt'],created=False)
    retry=request('POST','/management/v1/jobs',body,keyed)
    record('retry-export',status=retry[0],receipt=retry[1])
    assert retry==(200,dict(receipt,created=False))
    deadline=time.monotonic()+15
    while True:
        status,completed=request('GET','/management/v1/jobs/'+receipt['job_id'],headers=auth)
        record('export-status',status=status,response=completed)
        assert status==200 and completed['kind']=='export'
        if completed['state']=='succeeded': break
        assert not restart and completed['state'] in ('accepted','queued','running') and time.monotonic()<deadline
        time.sleep(.2)
    assert completed['checkpoint_revision']=='1'
    logout=access('logout','--session-ref','file:'+str(session))
    record('logout',response=logout)
    assert request('GET','/management/v1/me',headers=auth)[0]==401
    result=dict(status='PASS',receipt=receipt,histories=histories,completed=completed)
    assert all(value not in json.dumps(observations) for value in (saved['session'],saved['csrf'],'Independent owner password 2026'))
    publish(home/('restart-client-result.json' if restart else 'read-client-result.json'),result)


if __name__=='__main__':
    if sys.argv[1] in ('read','read-restart','history'):
        assert len(sys.argv)==3; http(Path(sys.argv[2]),sys.argv[1]=='read-restart',history_only=sys.argv[1]=='history')
    else:
        assert len(sys.argv)==4; process(sys.argv[1],Path(sys.argv[2]),Path(sys.argv[3]))
