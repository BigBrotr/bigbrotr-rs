"""Real scope setup before the separately faulted finite job command."""
import http.client
import json
from scope_http_client import prepare as prepare_scope


def prepare(directory, origin, saved):
    scope_command = prepare_scope(directory, origin, saved)
    connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=3)
    try:
        connection.request('POST', '/management/v1/acquisition-scopes', json.dumps(scope_command), {
            'Origin': origin, 'Content-Type': 'application/json',
            'Cookie': 'BigBrotrLocalSession=' + saved['session'],
            'X-CSRF-Token': saved['csrf'], 'Idempotency-Key': 'a1'*16,
        })
        response = connection.getresponse()
        raw = response.read(4097)
        assert response.status == 202 and len(raw) <= 4096 and response.read(1) == b''
        assert response.length in (None, 0)
        accepted = json.loads(raw)
        assert accepted['format'] == 'bigbrotr-acquisition-scope-registration/1'
        assert accepted['created'] and accepted['command_key'] == 'a1'*16
        assert accepted['authority'] == 'identity-only'
        (directory/'scope-http.json').write_text(json.dumps(accepted)+'\n')
    finally:
        connection.close()
    command = dict(kind='acquisition', scope_id=accepted['scope_id'], deadline_us='120000000',
        expected_config_revision='1', expected_policy_revision='1')
    (directory/'job-command.json').write_text(json.dumps(command)+'\n')
    return command
