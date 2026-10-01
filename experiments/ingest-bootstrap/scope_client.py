"""Independent installed URL→enrollment→scope HTTP journey; no product codec."""
import http.client
import json
import re
import time


def run(directory, request, login, logout):
    inputs = json.loads((directory / 'command-input.json').read_text())
    permitted = inputs['permitted']
    actions = ('policy.change', 'roles.control') if permitted else ('policy.change',)
    logged_in, session, saved, auth, _ = login(actions)
    observations = []

    def observe(path, key, status, body, expected):
        answer = json.loads(body)
        observations.append(dict(path=path, key=key, status=status, body=answer))
        assert status == expected, (status, answer)
        return answer

    def change(path, value, key, expected, credentials=None):
        status, body = request('POST', '/management/v1/' + path, value,
            {**(auth if credentials is None else credentials), 'Idempotency-Key': key})
        return observe(path, key, status, body, expected)

    endpoint = change('relay-endpoints', dict(url='WS://127.0.0.1:7447/a/../installed-fixture',
        expected_config_revision='1', expected_policy_revision='1'), '91'*16, 202)
    enrolled = dict(inputs['enrollment'], endpoint_id=endpoint['endpoint_id'])
    enrollment = change('relay-enrollments', enrolled, '92'*16, 202)
    selector = dict(version=1, filters=[{'kinds':[2,1,1], '#t':['β','a','β']}],
        history_since=0, ephemeral='retain')
    command = dict(endpoint_id=endpoint['endpoint_id'], access_scope_id=enrollment['access_scope_id'],
        visibility=enrolled['visibility'], selector=selector,
        expected_config_revision='1', expected_policy_revision='1')
    equivalent = dict(command, selector=dict(selector, filters=[{'#t':['a','β'], 'kinds':[1,2]}]))

    def scope(value, key='a1'*16, expected=202, credentials=None):
        return change('acquisition-scopes', value, key, expected, credentials)

    scope(command, expected=401, credentials={})
    scope(command, expected=401, credentials={**auth, 'X-CSRF-Token':'00'*32})
    scope(command, expected=403, credentials={'Cookie':auth['Cookie']})
    if not permitted:
        scope(command, expected=403)
    else:
        for field, value, status in [
            ('endpoint_id','71'*16,409), ('access_scope_id','72'*16,409),
            ('visibility',dict(command['visibility'],allocation_domain='73'*16),409),
            ('visibility',dict(command['visibility'],ordinal='777'),409),
            ('visibility',dict(command['visibility'],dataset_id='74'*16),400),
            ('visibility',[command['visibility'][k] for k in ('dataset_id','dictionary_kind','allocation_domain','ordinal')],400),
            ('expected_config_revision','777',409), ('expected_policy_revision','777',409),
        ]:
            scope(dict(command, **{field:value}), expected=status)
        scope(dict(command, actor_id='10'*16), expected=400)
        first = scope(command)
        assert set(first) == {'format','command_key','scope_id','accepted_at_us','created','authority'}
        assert first['format'] == 'bigbrotr-acquisition-scope-registration/1'
        assert first['command_key'] == 'a1'*16 and first['created'] is True
        assert re.fullmatch('[0-9a-f]{32}',first['scope_id']) and first['scope_id'] != '00'*16
        assert str(int(first['accepted_at_us'])) == first['accepted_at_us']
        assert 0 < int(first['accepted_at_us']) <= 9223372036854775807
        assert first['authority'] == 'identity-only'
        assert scope(equivalent, expected=200) == dict(first, created=False)
        scope(dict(command, selector=dict(selector, history_since=1)), expected=409)
        second = scope(equivalent, key='a2'*16)
        assert second == dict(first, command_key='a2'*16, accepted_at_us=second['accepted_at_us'])
        assert int(second['accepted_at_us']) >= int(first['accepted_at_us'])
        withdrawn = change('relay-enrollments', dict(enrolled, enabled=False, expected_generation='1'), '93'*16, 202)
        assert withdrawn['enabled'] is False and withdrawn['generation'] == '2'
        # Intent registration is independent of current execution enablement.
        third = scope(equivalent, key='a3'*16)
        assert third == dict(first, command_key='a3'*16, accepted_at_us=third['accepted_at_us'])
        assert int(third['accepted_at_us']) >= int(second['accepted_at_us'])

        def barrier(name):
            (directory/(name+'.json')).write_text(json.dumps(observations,indent=2))
            deadline = time.monotonic()+5
            while not (directory/(name+'.continue')).exists():
                assert time.monotonic() < deadline, 'scope independent SQL observer'
                time.sleep(.01)

        def raw(body, expected, chunked=False):
            connection = http.client.HTTPConnection('127.0.0.1',18081,timeout=4)
            try:
                headers = {**auth,'Idempotency-Key':'a1'*16,'Origin':'http://127.0.0.1:18081',
                    'Content-Type':'application/json'}
                payload = (bytes([byte]) for byte in body) if chunked else body
                connection.request('POST','/management/v1/acquisition-scopes',payload,headers,
                    encode_chunked=chunked)
                response=connection.getresponse()
                return observe('acquisition-scopes','a1'*16,response.status,response.read(),expected)
            finally:
                connection.close()

        barrier('before-replay')
        for receipt in [first,second,third]:
            assert scope(equivalent,key=receipt['command_key'],expected=200)==dict(receipt,created=False)
        wire=json.dumps(equivalent,separators=(',',':'),ensure_ascii=False).encode()
        maximum=wire+b' '*(2098176-len(wire))
        assert raw(maximum,200)==dict(first,created=False)
        raw(maximum+b' ',413)
        assert raw(wire+b' '*(65536-len(wire)),200,chunked=True)==dict(first,created=False)
        barrier('after-replay')
    # A live observer after all refusals proves business errors left the owner usable.
    assert request('GET','/management/v1/me',headers=auth)[0] == 200
    logged_out=logout(session,saved,auth)
    scope(command,expected=401)
    return dict(permitted=permitted,observations=observations,login=logged_in,logout=logged_out)
