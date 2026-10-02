"""Independent installed finite acceptance/status client; no product digest or reducer."""
import json
import time
from scope_http_client import prepare


def run(directory, request, login, logout):
    logged_in, session, saved, auth, _ = login(('policy.change', 'roles.control', 'status.read'))
    observations = []

    def call(method, path, value=None, key=None, expected=200, credentials=None):
        headers = dict(auth if credentials is None else credentials)
        if key is not None:
            headers['Idempotency-Key'] = key
        status, body = request(method, '/management/v1/' + path, value, headers)
        answer = json.loads(body)
        observations.append(dict(method=method, path=path, key=key, status=status, body=answer))
        assert status == expected, (status, answer)
        return answer

    def barrier(stage):
        (directory / (stage + '.json')).write_text(json.dumps(observations, indent=2))
        deadline = time.monotonic() + 12
        while not (directory / (stage + '.continue')).exists():
            assert time.monotonic() < deadline, stage
            time.sleep(.01)

    scope_command = prepare(directory, 'http://127.0.0.1:18081', saved)
    scope = call('POST', 'acquisition-scopes', scope_command, 'a1'*16, 202)
    command = dict(kind='acquisition', scope_id=scope['scope_id'], deadline_us='120000000',
        expected_config_revision='1', expected_policy_revision='1')
    (directory/'job-command.json').write_text(json.dumps(command)+'\n')
    other_auth = None
    if directory.name == 'jobs-other-actor':
        _, _, _, other_auth, _ = login(('status.read',), username='bob', principal='20'*16, stem='bob-')
    barrier('before-refusals')
    call('POST', 'jobs', command, 'b1'*16, 401, {})
    call('POST', 'jobs', command, 'b1'*16, 403, {'Cookie': auth['Cookie']})
    for patch, expected in [({'deadline_us':'0'},400), ({'deadline_us':'86400000001'},400),
        ({'scope_id':'77'*16},409), ({'expected_config_revision':'99'},409),
        ({'expected_policy_revision':'99'},409), ({'actor_id':'88'*16},400)]:
        call('POST', 'jobs', dict(command, **patch), 'b1'*16, expected)
    barrier('before-acceptance')
    receipt = call('POST', 'jobs', command, 'b1'*16, 202)
    assert set(receipt) == {'format','job_id','command_key','accepted_at_us','created','execution','authority'}
    assert receipt['format']=='bigbrotr-job-acceptance/1' and receipt['created'] is True
    assert receipt['command_key']=='b1'*16 and receipt['authority']=='acceptance-only'
    assert receipt['execution']=='unobserved'
    barrier('accepted')
    assert call('POST', 'jobs', command, 'b1'*16)==dict(receipt,created=False)
    call('POST', 'jobs', dict(command, deadline_us='120000001'), 'b1'*16, 409)
    path='jobs/'+receipt['job_id']
    if other_auth:
        # Bob is a live, independently authenticated status reader. The target
        # really exists; broadening ownership must change these exact responses.
        call('GET',path,expected=404,credentials=other_auth)
        call('POST','jobs',command,'b1'*16,403,other_auth)
    status=call('GET',path)
    expected_status=dict(format='bigbrotr-job-status/1',job_id=receipt['job_id'],kind='acquisition',
        state='accepted',job_revision='1',checkpoint_revision='0',created_at_us=receipt['accepted_at_us'],
        completed_at_us=None,observed_at_us=status['observed_at_us'],reason_code=None,authority='observation-only')
    assert status==expected_status
    assert int(status['observed_at_us'])>=int(receipt['accepted_at_us'])
    call('GET','jobs/'+'78'*16,expected=404)
    call('GET',path,expected=401,credentials={})
    call('GET',path,expected=403,credentials={'Cookie':auth['Cookie']})
    barrier('replayed')  # Observer withdraws roles.control, preserving status.read.
    assert call('POST','jobs',command,'b1'*16)==dict(receipt,created=False)
    call('POST','jobs',command,'b2'*16,403)
    current=call('GET',path)
    assert current==dict(expected_status,observed_at_us=current['observed_at_us'])
    barrier('permission-withdrawn')  # Observer now withdraws status.read.
    call('GET',path,expected=403)
    assert call('POST','jobs',command,'b1'*16)==dict(receipt,created=False)
    barrier('status-withdrawn')
    logged_out=logout(session,saved,auth)
    call('GET',path,expected=401)
    return dict(login=logged_in,logout=logged_out,observations=observations)
