"""Actual ordinary Serve status requests against the retained pre0022 schema."""
import json
import time


def run(directory, request, login, logout):
    logged_in, session, saved, auth, _ = login(('policy.change', 'status.read'))
    observations = []

    def barrier(stage):
        (directory/(stage+'.json')).write_text(json.dumps(observations,indent=2))
        deadline=time.monotonic()+12
        while not (directory/(stage+'.continue')).exists():
            assert time.monotonic()<deadline,stage
            time.sleep(.01)

    def call(method,path,body,expected):
        status,raw=request(method,'/management/v1/'+path,body,
            dict(auth,**{'Idempotency-Key':'b1'*16}) if method=='POST' else auth)
        value=json.loads(raw)
        observations.append(dict(method=method,path=path,status=status,body=value))
        assert status==expected,(status,value)
        return value

    barrier('before-reads')
    for ident in ('77'*16,'78'*16):
        response=call('GET','jobs/'+ident,None,404)
        assert response['error']['code']=='job_unavailable'
    # A valid acquisition envelope cannot select an unsupported schema on the
    # operational profile; it is refused before entering the Control queue.
    call('POST','jobs',dict(kind='acquisition',scope_id='79'*16,deadline_us='120000000',
        expected_config_revision='1',expected_policy_revision='1'),400)
    barrier('after-reads')
    return dict(login=logged_in,logout=logout(session,saved,auth),observations=observations)
