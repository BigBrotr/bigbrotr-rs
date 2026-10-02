"""Independent finite-job meaning over the shared installed COMMIT schedule."""
import hashlib
import json
from initial_jobs import EFFECTS, validate_job
from scopes import CANONICAL

CASES = {f'job-http-{mode}': mode for mode in ('pass', 'before', 'after', 'disconnect', 'signal')}


def prepare(state, directory, initialized):
    assert len(state) == 69 and not any(state[t] for t in ('jobs','acquisition_jobs','job_admissions'))
    scope, = state['acquisition_scopes']
    selector, = state['acquisition_selectors']
    detail, = state['acquisition_scope_commands']
    endpoint, = state['relay_endpoints']
    access, = state['relay_access_contexts']
    enrollment, = state['relay_enrollments']
    assert endpoint['url_bytes'] == '\\x'+b'ws://127.0.0.1:7447/installed-fixture'.hex()
    assert scope['endpoint_id'] == enrollment['endpoint_id'] == endpoint['endpoint_id']
    assert scope['access_scope_id'] == enrollment['access_scope_id'] == access['access_scope_id']
    assert scope['visibility_domain'] == access['allocation_domain'] == '\\x'+initialized['public_scope']['allocation_domain']
    assert scope['visibility_ordinal'] == access['ordinal'] == 2 and scope['visibility_kind'] == 4
    assert scope['selector_id'] == selector['selector_id'] and scope['policy_revision'] == 1
    assert selector['document'] == '\\x'+CANONICAL.hex()
    assert selector['candidate_digest'] == '\\x'+hashlib.sha256(CANONICAL).hexdigest()
    assert enrollment['enabled'] and enrollment['generation'] == 1
    accepted = json.loads((directory/'scope-http.json').read_text())
    command, = [r for r in state['management_commands'] if r['request_key']=='\\x'+'a1'*16]
    assert detail['request_key'] == command['request_key'] and detail['scope_id'] == scope['scope_id']
    assert accepted == dict(format='bigbrotr-acquisition-scope-registration/1',command_key='a1'*16,
        scope_id=scope['scope_id'][2:],accepted_at_us=str(command['accepted_at_us']),created=True,authority='identity-only')
    assert json.loads((directory/'job-command.json').read_text()) == dict(kind='acquisition',
        scope_id=scope['scope_id'][2:],deadline_us='120000000',expected_config_revision='1',expected_policy_revision='1')


def effects(before, after, created):
    assert before.keys() == after.keys() and len(before) == 69
    assert {t for t in before if before[t] != after[t]} == (EFFECTS if created else set())
    if created:
        for table in EFFECTS:
            assert len(after[table]) == len(before[table]) + 1
            assert all(row in after[table] for row in before[table])


def receipt(state, ready):
    commands = [r for r in state['management_commands'] if r['request_key']=='\\x'+'31'*16]
    if not commands:
        assert not any(state[t] for t in ('jobs','acquisition_jobs','job_admissions'))
        return None
    command, = commands
    job, = state['jobs']
    expected = dict(format='bigbrotr-job-acceptance/1',job_id=job['job_id'][2:],command_key='31'*16,
        accepted_at_us=str(command['accepted_at_us']),created=True,execution='unobserved',authority='acceptance-only')
    validate_job(state, expected, key='31'*16)
    detail, = state['acquisition_jobs']
    session, = [r for r in state['browser_sessions'] if r['session_id']==detail['session_id']]
    assert session['expires_at_us'] == ready['session_expires']
    assert len({state[t][0]['xmin'] for t in ('jobs','acquisition_jobs','job_admissions')} |
        {command['xmin'], next(r['xmin'] for r in state['management_audit'] if r['audit_id']==command['audit_id'])}) == 1
    # The helper's typed receipt omits only HTTP presentation fields.
    return dict(job_id=expected['job_id'], command_key=expected['command_key'],
        accepted_at_us=command['accepted_at_us'])
