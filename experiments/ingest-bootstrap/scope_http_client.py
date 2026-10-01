"""Actual prerequisite HTTP commands for the finite scope COMMIT experiment."""
import http.client
import json


def prepare(directory, origin, saved):
    observations = []

    def change(path, key, command):
        connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=3)
        try:
            connection.request('POST', '/management/v1/' + path, json.dumps(command), {
                'Origin': origin, 'Content-Type': 'application/json',
                'Cookie': 'BigBrotrLocalSession=' + saved['session'],
                'X-CSRF-Token': saved['csrf'], 'Idempotency-Key': key,
            })
            response = connection.getresponse()
            body = response.read(4097)
            assert len(body) <= 4096 and response.read(1) == b''
            assert response.length in (None, 0) and response.status == 202
            value = json.loads(body)
            assert value['command_key'] == key and value['created'] is True
            observations.append(dict(path=path, key=key, status=response.status, body=value))
            return value
        finally:
            connection.close()

    endpoint = change('relay-endpoints', '91' * 16, dict(
        url='WS://127.0.0.1:7447/a/../installed-fixture',
        expected_config_revision='1', expected_policy_revision='1'))
    assert endpoint['format'] == 'bigbrotr-relay-registration/1'
    visibility = json.loads((directory / 'visibility.json').read_text())
    enrollment = change('relay-enrollments', '92' * 16, dict(
        endpoint_id=endpoint['endpoint_id'], visibility=visibility,
        route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True,
        expected_generation=None, expected_config_revision='1', expected_policy_revision='1'))
    assert enrollment['enabled'] is True and enrollment['generation'] == '1'
    command = dict(endpoint_id=endpoint['endpoint_id'], access_scope_id=enrollment['access_scope_id'],
        visibility=visibility, selector=dict(version=1, filters=[{'kinds': [1, 2], '#t': ['a', 'β']}],
            history_since=0, ephemeral='retain'), expected_config_revision='1', expected_policy_revision='1')
    (directory / 'setup-http.json').write_text(json.dumps(observations, indent=2) + '\n')
    # The fresh SQL owner must retry exactly this original normalized intent.
    (directory / 'command.json').write_text(json.dumps(command) + '\n')
    return command
