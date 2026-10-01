"""Installed URL registration followed by enrollment, using an independent HTTP client."""
import json
import time


def run(directory, request, login, logout):
    inputs = json.loads((directory / 'command-input.json').read_text())
    permitted = inputs['permitted']
    logged_in, session, saved, auth, _ = login(
        ('policy.change',) if permitted else ('principals.manage',))
    command = dict(url='WS://127.0.0.1:7447/a/../installed-fixture',
                   expected_config_revision='1', expected_policy_revision='1')
    canonical = dict(command, url='ws://127.0.0.1:7447/installed-fixture')
    observations = []

    def change(path, value, key, expected, credentials=auth):
        status, body = request('POST', '/management/v1/' + path, value,
                               {**credentials, 'Idempotency-Key': key})
        answer = json.loads(body)
        observations.append(dict(path=path, key=key, status=status, body=answer))
        assert status == expected, (status, answer)
        return answer

    def register(value, key, expected, credentials=auth):
        return change('relay-endpoints', value, key, expected, credentials)

    register(command, '91' * 16, 401, {})
    register(command, '91' * 16, 401, {**auth, 'X-CSRF-Token': '00' * 32})
    if not permitted:
        register(command, '91' * 16, 403)
    else:
        register(dict(command, expected_config_revision='777'), '91' * 16, 409)
        register(dict(command, url='https://example.test/'), '91' * 16, 400)
        first = register(command, '91' * 16, 202)
        assert set(first) == {'format', 'command_key', 'endpoint_id', 'accepted_at_us', 'created', 'authority'}
        assert first['format'] == 'bigbrotr-relay-registration/1'
        assert first['command_key'] == '91' * 16 and first['created']
        assert len(first['endpoint_id']) == 32 and first['endpoint_id'] != '00' * 16
        assert str(int(first['accepted_at_us'])) == first['accepted_at_us']
        assert first['authority'] == 'identity-only'
        assert register(canonical, '91' * 16, 200) == dict(first, created=False)
        register(dict(command, url=canonical['url'] + '?'), '91' * 16, 409)
        second = register(canonical, '92' * 16, 202)
        assert second['endpoint_id'] == first['endpoint_id'] and second['created']
        assert int(second['accepted_at_us']) >= int(first['accepted_at_us'])
        prefix = 'wss://example.test/?'
        maximum = dict(command, url=prefix + '"' * (8192 - len(prefix)))
        assert len(json.dumps(maximum, separators=(',', ':')).encode()) == 16436
        large = register(maximum, '93' * 16, 202)
        assert large['endpoint_id'] != first['endpoint_id'] and large['created']
        register(dict(maximum, url=maximum['url'] + 'x'), '94' * 16, 400)
        enrolled = dict(inputs['enrollment'], endpoint_id=first['endpoint_id'])
        enabled = change('relay-enrollments', enrolled, '95' * 16, 202)
        assert enabled['endpoint_id'] == first['endpoint_id'] and enabled['generation'] == '1'
        withdrawn = dict(enrolled, enabled=False, expected_generation='1')
        disabled = change('relay-enrollments', withdrawn, '96' * 16, 202)
        assert disabled['generation'] == '2' and not disabled['enabled']

        def barrier(name):
            (directory / (name + '.json')).write_text(json.dumps(observations, indent=2))
            deadline = time.monotonic() + 5
            while not (directory / (name + '.continue')).exists():
                assert time.monotonic() < deadline, 'independent registration observer'
                time.sleep(.01)
        barrier('before-replay')
        assert register(canonical, '91' * 16, 200) == dict(first, created=False)
        assert register(canonical, '92' * 16, 200) == dict(second, created=False)
        assert register(maximum, '93' * 16, 200) == dict(large, created=False)
        assert change('relay-enrollments', enrolled, '95' * 16, 200) == dict(enabled, created=False)
        assert change('relay-enrollments', withdrawn, '96' * 16, 200) == dict(disabled, created=False)
        barrier('after-replay')
    logged_out = logout(session, saved, auth)
    register(command, '91' * 16, 401)
    return dict(permitted=permitted, observations=observations, login=logged_in, logout=logged_out)
