"""Independent fresh-scope effects for the installed finite COMMIT schedules."""
import hashlib
import json
from scope_command_faults import ACTOR, CANONICAL, receipt as command_receipt

CASES = {f'scope-http-{mode}': mode for mode in ('pass', 'before', 'after', 'disconnect', 'signal')}
TABLES = {'management_commands', 'management_audit', 'acquisition_scope_commands',
          'acquisition_selectors', 'acquisition_scopes'}


def prepare(state, directory, initialized):
    assert len(state) == 69 and not state['acquisition_scope_commands']
    assert not state['acquisition_selectors'] and not state['acquisition_scopes']
    endpoint, = state['relay_endpoints']
    access, = state['relay_access_contexts']
    enrollment, = state['relay_enrollments']
    route, = state['network_routes']
    change, = state['relay_enrollment_changes']
    detail, = state['relay_registration_commands']
    dataset = state['datasets'][0]['dataset_id']
    assert bytes.fromhex(endpoint['url_bytes'][2:]) == b'ws://127.0.0.1:7447/installed-fixture'
    assert detail['endpoint_id'] == enrollment['endpoint_id'] == change['endpoint_id'] == route['endpoint_id'] == endpoint['endpoint_id']
    assert access['access_scope_id'] == enrollment['access_scope_id'] == change['access_scope_id']
    assert route['route_id'] == enrollment['route_id'] == change['route_id']
    assert route['route_version'] == 1 and route['route_bytes'] == '\\x0104207f000001'
    assert enrollment['enabled'] is True and enrollment['generation'] == 1
    visibility = dict(initialized['public_scope'], ordinal='2')
    assert access['dataset_id'] == dataset and access['dictionary_kind'] == 4
    assert access['allocation_domain'] == '\\x' + visibility['allocation_domain'] and access['ordinal'] == 2
    command = json.loads((directory / 'command.json').read_text())
    assert command == dict(endpoint_id=endpoint['endpoint_id'][2:], access_scope_id=access['access_scope_id'][2:],
        visibility=visibility, selector=json.loads(CANONICAL),
        expected_config_revision='1', expected_policy_revision='1')
    observations = json.loads((directory / 'setup-http.json').read_text())
    assert [(o['path'], o['key'], o['status']) for o in observations] == [
        ('relay-endpoints', '91' * 16, 202), ('relay-enrollments', '92' * 16, 202)]
    for observed in observations:
        accepted, = [c for c in state['management_commands'] if c['request_key'] == '\\x' + observed['key']]
        assert accepted['actor_id'] == ACTOR and accepted['expected_config_revision'] == accepted['policy_revision'] == 1
        expected = dict(command_key=observed['key'], endpoint_id=endpoint['endpoint_id'][2:],
            accepted_at_us=str(accepted['accepted_at_us']), created=True)
        if observed['path'] == 'relay-endpoints':
            expected.update(format='bigbrotr-relay-registration/1', authority='identity-only')
            assert detail['request_key'] == accepted['request_key']
        else:
            expected.update(format='bigbrotr-relay-enrollment/1', authority='acceptance-only',
                application='unobserved', access_scope_id=access['access_scope_id'][2:],
                route_id=route['route_id'][2:], enabled=True, generation='1')
            assert change['request_key'] == accepted['request_key']
        assert observed['body'] == expected


def effects(before, after, created):
    assert before.keys() == after.keys() and len(before) == 69
    assert {t for t in before if before[t] != after[t]} == (TABLES if created else set())
    if created:
        for table in TABLES:
            assert len(after[table]) == len(before[table]) + 1
            assert all(row in after[table] for row in before[table])


def receipt(state, ready):
    accepted = command_receipt(state, ready)
    if accepted is None:
        assert not state['acquisition_selectors'] and not state['acquisition_scopes']
        return None
    scope, = state['acquisition_scopes']
    selector, = state['acquisition_selectors']
    detail, = state['acquisition_scope_commands']
    access, = state['relay_access_contexts']
    assert scope['selector_id'] == selector['selector_id']
    assert selector['profile_version'] == 1 and selector['candidate_digest'] == '\\x' + hashlib.sha256(CANONICAL).hexdigest()
    assert scope['access_scope_id'] == access['access_scope_id']
    assert scope['visibility_domain'] == access['allocation_domain'] and scope['visibility_ordinal'] == access['ordinal'] == 2
    for row in (selector, scope):
        assert row['dataset_id'] == detail['dataset_id']
        assert row['config_revision'] == row['policy_revision'] == 1
        for field in ('owner_resource_id', 'owner_epoch', 'owner_incarnation', 'owner_role'):
            assert row[field] == detail[field]
    assert selector['registered_at_us'] <= scope['registered_at_us'] <= accepted['accepted_at_us']
    return accepted
