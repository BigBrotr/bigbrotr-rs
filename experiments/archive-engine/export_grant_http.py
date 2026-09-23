#!/usr/bin/env python3
"""Real private command/action separation before the durable exporter consumer.

The host checks PostgreSQL independently. This exercises grants, not an export.
Only public command inputs and case results are retained; no browser credentials.
"""
from http.cookies import SimpleCookie
import json
import private_grant_http as http
from signed_publication_oracle import EVENT


def exercise():
    principal = (http.ROOT / 'principal').read_text()
    scope = dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1')
    query = dict(version=1, resource='event_history', mode='snapshot', scope=scope,
                 filters=[dict(ids=[EVENT['id']])])
    cases, accepted = [], []
    server = http.Server('export-grants')

    def case(name):
        cases.append(dict(case=name, status='PASS'))
        (http.ROOT / 'http-cases.json').write_text(json.dumps(cases, indent=2)+'\n')

    try:
        server.ready()
        status, _, headers = http.request('POST', '/management/v1/sessions',
                                         dict(username='alice', password=http.PASSWORD))
        assert status == 201
        cookies = SimpleCookie()
        for name, value in headers:
            if name.lower() == 'set-cookie': cookies.load(value)
        auth = {'Cookie': 'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
                'X-CSRF-Token': cookies['BigBrotrLocalCsrf'].value}

        def read(expected):
            status, value, _ = http.request('POST', '/v1/resources/event_history', query, auth)
            assert status == expected, (status, value)
            if expected == 200:
                assert value['items'] == [EVENT] and value['next_cursor'] is None
                assert value['view_stamp']['input_generations'] == [dict(
                    kind='archive', catalog_id='5a'*16, frontier_revision='1', snapshot_id='3')]
            return value

        def command(action, enabled, revision):
            return dict(principal_id=principal, action=action, enabled=enabled, scope=scope,
                        expected_config_revision='1', expected_policy_revision='2',
                        expected_grant_revision=str(revision),
                        allocation=dict(catalog_id='5a'*16, frontier_revision='1') if enabled else None)

        def proof(value, key):
            status, evidence, _ = http.request('POST', '/management/v1/sessions/current/reauthenticate',
                dict(password=http.PASSWORD, command=value), {**auth, 'Idempotency-Key': key})
            assert status == 201, (status, evidence)
            return evidence['proof_id']

        def change(value, key, evidence):
            return http.request('PUT', f'/management/v1/principals/{principal}/grants',
                dict(command=value, proof_id=evidence), {**auth, 'Idempotency-Key': key})[:2]

        def accept(value, key, evidence, generation):
            status, receipt = change(value, key, evidence)
            assert status == 200 and receipt['created'], (status, receipt)
            assert receipt['scope_generation'] == str(generation)
            assert receipt['account_grant_revision'] == str(int(value['expected_grant_revision'])+1)
            assert receipt['enabled'] == value['enabled'] and receipt['scope'] == scope
            assert receipt['command_key'] == key and receipt['principal_id'] == principal
            assert receipt['application'] == 'unobserved' and receipt['authority'] == 'acceptance-only'
            assert change(value, key, evidence) == (200, {**receipt, 'created': False})
            assert not list((http.ROOT / 'scratch').iterdir())
            accepted.append((value, key, evidence, receipt))

        read(403)
        status, me, _ = http.request('GET', '/management/v1/me', auth=auth)
        assert status == 200 and set(me['stored_actions']) == {
            'data.read', 'status.read', 'roles.control', 'budgets.change', 'policy.change',
            'principals.manage', 'audit.read'}
        case('initial-administrator-has-no-implicit-data-action')
        export = command('export', True, 1)
        export_key = 'c1'*16
        evidence = proof(export, export_key)
        for action in ['history.read', 'private-export']:
            assert change({**export, 'action': action}, export_key, evidence)[0] == 401
        case('reauthentication-proof-cannot-change-export-action')
        accept(export, export_key, evidence, 1)
        assert change({**export, 'action': 'private-export'}, export_key, evidence)[0] == 409
        case('export-enable-and-exact-retry-bind-the-command-action')
        read(403)
        private = command('private-export', True, 2)
        private_key = 'c2'*16
        accept(private, private_key, proof(private, private_key), 1)
        case('private-export-has-its-own-scope-generation')
        read(403)
        case('either-export-grant-leaves-history-read-forbidden')
        revoke = command('export', False, 3)
        revoke_key = 'c3'*16
        accept(revoke, revoke_key, proof(revoke, revoke_key), 2)
        case('revoke-updates-only-the-selected-export-action')
        history = command('history.read', True, 4)
        history_key = 'c4'*16
        accept(history, history_key, proof(history, history_key), 1)
        read(200)
        case('history-enablement-uses-independent-generation-and-real-signed-read')
        server.stop()
        server = http.Server('export-grants-restart')
        server.ready()
        for value, key, evidence, receipt in accepted:
            assert change(value, key, evidence) == (200, {**receipt, 'created': False})
        read(200)
        assert not list((http.ROOT / 'scratch').iterdir())
        server.stop()
        case('restart-retry-does-not-reapply-the-revoked-export-grant')
        # Exact public inputs let the separate host recompute the stored digests.
        (http.ROOT / 'accepted-commands.json').write_text(json.dumps([
            dict(command=value, key=key) for value, key, _, _ in accepted], indent=2)+'\n')
    finally:
        server.cleanup()
    print(json.dumps(dict(status='PASS', cases=len(cases))))


if __name__ == '__main__':
    exercise()
