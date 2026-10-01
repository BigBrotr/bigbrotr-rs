"""Independent scope-command receipt oracle over the shared finite COMMIT schedule."""
import hashlib
import json
import lab
from registration_faults import ACTOR, KEY, CASES as REGISTRATION_CASES, write_policies
from scopes import CANONICAL

CASES = {name.replace('register-', 'scope-command-', 1): case
         for name, case in REGISTRATION_CASES.items()}
DOMAIN_TABLES = {'management_commands', 'management_audit', 'acquisition_scope_commands'}


def prepare(directory, original):
    assert len(original)==69
    assert [r['epoch'] for r in original['owner_epochs'] if r['role']=='control']==[1]
    scope,=original['acquisition_scopes']
    selector,=original['acquisition_selectors']
    assert scope['owner_epoch']==1 and selector['document']=='\\x'+CANONICAL.hex()
    assert receipt(original,{}) is None
    write_policies(directory,original)
    lab.write_json(directory/'command.json',dict(
        endpoint_id=scope['endpoint_id'][2:],access_scope_id=scope['access_scope_id'][2:],
        visibility=dict(dataset_id=scope['dataset_id'][2:],dictionary_kind=4,
            allocation_domain=scope['visibility_domain'][2:],ordinal='2'),
        selector=json.loads(CANONICAL),expected_config_revision='1',expected_policy_revision='1'))
    return scope


def receipt(state, ready, acceptance_account=None):
    commands=[r for r in state['management_commands'] if r['actor_id']==ACTOR and r['request_key']==KEY]
    if not commands:
        assert not any(r['request_key']==KEY for r in state['acquisition_scope_commands'])
        return None
    command,=commands
    detail,=[r for r in state['acquisition_scope_commands'] if r['actor_id']==ACTOR and r['request_key']==KEY]
    scope,=[r for r in state['acquisition_scopes'] if r['scope_id']==detail['scope_id']]
    selector,=[r for r in state['acquisition_selectors'] if r['selector_id']==scope['selector_id']]
    audit,=[r for r in state['management_audit'] if r['audit_id']==command['audit_id']]
    owner,=[r for r in state['owner_epochs'] if r['resource_id']==detail['owner_resource_id'] and r['epoch']==detail['owner_epoch']]
    account=acceptance_account
    if account is None:account,=[r for r in state['local_accounts'] if r['principal_id']==ACTOR]
    session,=[r for r in state['browser_sessions'] if r['session_id']==detail['session_id']]
    dataset=state['datasets'][0]['dataset_id']
    assert selector['document']=='\\x'+CANONICAL.hex() and scope['visibility_kind']==4 and scope['visibility_ordinal']==2
    original=(b'bigbrotr/acquisition-scope-registration/1\0'+bytes.fromhex(dataset[2:]+'10'*16+'31'*16)
        +bytes.fromhex(scope['endpoint_id'][2:]+scope['access_scope_id'][2:]+dataset[2:])+b'\0\4'
        +bytes.fromhex(scope['visibility_domain'][2:])+b''.join(n.to_bytes(8,'big') for n in [2,1,1])
        +len(CANONICAL).to_bytes(4,'big')+CANONICAL)
    assert command['request_digest']=='\\x'+hashlib.sha256(original).hexdigest()
    assert command['action']=='acquisition.scope.register'
    assert command['expected_config_revision']==command['policy_revision']==scope['policy_revision']==1
    assert command['dataset_id']==detail['dataset_id']==scope['dataset_id']==dataset
    assert command['xmin']==detail['xmin']==audit['xmin']
    assert audit['actor_id']==ACTOR and audit['subject_id']==KEY and audit['dataset_id']==dataset
    assert audit['action']==command['action'] and audit['outcome']=='accepted'
    assert audit['created_at_us']==command['accepted_at_us'] and audit['config_revision']==audit['policy_revision']==1
    assert owner['epoch']==ready['epoch'] and owner['control_backend_pid']==ready['pid']
    assert owner['process_incarnation']==detail['owner_incarnation'] and detail['owner_role']=='control'
    assert session['principal_id']==ACTOR and session['expires_at_us']==ready['session_expires']
    for revision in ('credential_revision','authentication_revision','grant_revision'):
        assert detail['actor_'+revision]==account[revision]
    return dict(command_key=KEY[2:],scope_id=scope['scope_id'][2:],accepted_at_us=command['accepted_at_us'])


def effects(before,after,committed):
    assert before.keys()==after.keys() and len(before)==69
    changed={t for t in before if before[t]!=after[t]}
    assert changed==(DOMAIN_TABLES if committed else set()),changed
    for table in changed:
        assert all(row in after[table] for row in before[table])
        assert len(after[table])==len(before[table])+1
