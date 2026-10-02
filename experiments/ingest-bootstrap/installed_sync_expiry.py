"""Natural expiry of real HTTP acceptance, with an exact-owned process barrier.

No seeded identity, disabled guard, adjusted timestamp or adopted native state.
The short job must still be accepted/unowned and live when the host observes it.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import signal
import time

DURATION_US = 500_000
KEY = 'b0'*16


@contextmanager
def suspended_cli(directory, publish):
    identity = json.loads(Path(str(directory)+'.pid.json').read_text())
    process = Path('/proc')/str(identity['pid'])

    def verified():
        fields = process.joinpath('stat').read_text().split()
        assert fields[21] == identity['start_ticks'], 'owned CLI incarnation changed'
        assert process.joinpath('cmdline').read_bytes().split(b'\0')[:4] == [
            b'/build/bigbrotr', b'serve', b'--file', identity['config'].encode()]
        return fields[2]

    verified()
    started = time.monotonic()
    os.kill(identity['pid'], signal.SIGSTOP)
    try:
        while verified() != 'T':
            assert time.monotonic()-started < .1, 'FAIL_SETUP: CLI did not stop promptly'
            time.sleep(.001)
        publish(directory, 'expiry-stopped.json', dict(pid=identity['pid'],
            start_ticks=identity['start_ticks'], stopped_at_us=time.time_ns()//1000))
        yield started+1.5
        assert time.monotonic()-started < 1.5, 'FAIL_SETUP: expiry suspension exceeded bound'
    finally:
        verified()
        os.kill(identity['pid'], signal.SIGCONT)
        publish(directory, 'expiry-resumed.json', dict(resumed_at_us=time.time_ns()//1000,
            suspension_seconds=time.monotonic()-started))


def validate_expired_acceptance(state, receipt):
    """Independent complete accepted job/command rows for this500ms HTTP input."""
    job, = state['jobs']; detail, = state['acquisition_jobs']; admission, = state['job_admissions']
    command, = [r for r in state['management_commands'] if r['request_key'] == '\\x'+KEY]
    audit, = [r for r in state['management_audit'] if r['audit_id'] == command['audit_id']]
    actor, = state['local_accounts']; session, = state['browser_sessions']; scope, = state['acquisition_scopes']
    dataset = state['datasets'][0]['dataset_id']
    ident, key, accepted = '\\x'+receipt['job_id'], '\\x'+KEY, int(receipt['accepted_at_us'])
    preimage = (b'bigbrotr/acquisition-job/1\0'+bytes.fromhex(dataset[2:])
        +bytes.fromhex(actor['principal_id'][2:])+bytes.fromhex(KEY)+bytes.fromhex(scope['scope_id'][2:])
        +DURATION_US.to_bytes(8, 'big')+(1).to_bytes(8, 'big')*2)
    digest = '\\x'+hashlib.sha256(preimage).hexdigest()
    clean = lambda row: {k:v for k,v in row.items() if k != 'xmin'}
    assert job['state'] == 'accepted' and job['job_revision'] == 1 and job['owner_resource_id'] is None, \
        'FAIL_SETUP: short job claimed before suspension'
    assert clean(job) == dict(job_id=ident, dataset_id=dataset, principal_id=actor['principal_id'],
        request_key=key, request_digest=digest, kind='acquisition', config_revision=1, policy_revision=1,
        created_at_us=accepted, state='accepted', job_revision=1, checkpoint_revision=0, reason_code=None,
        result_manifest_digest=None, owner_resource_id=None, owner_epoch=None, owner_role='control',
        owner_incarnation=None, completed_at_us=None, input_topology_generation=None, input_complete=True,
        export_job_protocol=None, acquisition_job_protocol=1)
    assert clean(detail) == dict(job_id=ident, dataset_id=dataset, actor_id=actor['principal_id'],
        request_key=key, scope_id=scope['scope_id'], duration_us=DURATION_US,
        deadline_at_us=accepted+DURATION_US, session_id=session['session_id'],
        actor_credential_revision=actor['credential_revision'],
        actor_authentication_revision=actor['authentication_revision'], actor_grant_revision=actor['grant_revision'])
    assert clean(admission) == dict(job_id=ident, admission_ordinal=1)
    assert clean(command) == dict(dataset_id=dataset, actor_id=actor['principal_id'], request_key=key,
        request_digest=digest, action='job.acquisition.accept', accepted_at_us=accepted,
        expected_config_revision=1, policy_revision=1, audit_id=audit['audit_id'],
        authority_mode=None, document_digest=None)
    assert clean(audit) == dict(dataset_id=dataset, actor_id=actor['principal_id'], audit_id=command['audit_id'],
        action='job.acquisition.accept', outcome='accepted', subject_id=key, created_at_us=accepted,
        config_revision=1, policy_revision=1, reason=None, requester_database_role=None, requester_os_uid=None)
    assert receipt == dict(format='bigbrotr-job-acceptance/1', job_id=ident[2:], command_key=KEY,
        accepted_at_us=str(accepted), created=True, execution='unobserved', authority='acceptance-only')
    assert session['dataset_id'] == dataset and session['principal_id'] == actor['principal_id']
    assert session['revoked_at_us'] is None and session['issued_at_us'] <= accepted < min(
        session['expires_at_us'], session['idle_expires_at_us'])
    return detail['deadline_at_us']


def preserve_expired(original, current):
    """All older acceptance rows including xmin survive later dispatch unchanged."""
    for table in ('jobs', 'acquisition_jobs', 'job_admissions', 'management_commands', 'management_audit'):
        assert all(row in current[table] for row in original[table]), 'expired history changed: '+table
    old, = original['jobs']
    assert all(row['acquisition_job_id'] != old['job_id'] for row in current['producer_requests'])
