#!/usr/bin/env python3
"""PostgreSQL resume boundary after the independently checked native seed."""
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import cases
from signed_publication_cases import command, sha


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    source = root / 'source'
    result = dict(status='INCOMPLETE', source=state['source'], tree=state['tree'],
        scope='SQL resume receipt, transition, rollback and same-unit custody only; no Rust API or artifact output execution qualification')
    try:
        cases.main(root)
        migration = source / 'migrations/postgres/0814_export_resume.sql'
        result['migration_sha256'] = sha(migration)
        result['migration_stdout'] = command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XAtq',
            '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bbpub_control_normal',
            input="BEGIN; SELECT pg_advisory_xact_lock(1111651442,1296648018); " + migration.read_text()
                + "\nINSERT INTO control.schema_versions VALUES('postgres-control',814,'0814_export_resume.sql',decode('"
                + result['migration_sha256'] + "','hex'),1,1,1,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,current_user); COMMIT;")
        trial = source / 'experiments/export-execution/resume_transition.sql'
        result['trial_sha256'] = sha(trial)
        output = command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XAtq',
            '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bbpub_control_normal', input=trial.read_text())
        result['original_stdout'] = output
        observed = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
        assert len(observed) == 1
        result['transition'] = observed[0]
        assert observed[0]['unchanged']
        assert observed[0]['code'] == '23514' and observed[0]['phase'] in ('update', 'constraints')
        assert observed[0]['message'] == 'paused export resume requires its atomic command receipt'
        seed = json.loads(command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XAtq',
            '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bbpub_control_normal',
            input="SELECT json_build_object('dataset',encode(j.dataset_id,'hex'),'actor',encode(j.principal_id,'hex'),'job',encode(j.job_id,'hex'),'job_revision',j.job_revision::text,'config_revision',d.config_revision::text,'policy_revision',d.policy_revision::text) FROM control.jobs j JOIN control.datasets d USING(dataset_id) WHERE j.export_job_protocol=1"))
        raw = b'bigbrotr/export-resume/1\0' + bytes.fromhex(seed['dataset'] + seed['actor'] + 'e4'*16 + seed['job'])
        raw += b''.join(int(seed[k]).to_bytes(8, 'big') for k in ['job_revision', 'config_revision', 'policy_revision'])
        digest = hashlib.sha256(raw).hexdigest()
        result['independent_command'] = dict(seed=seed, bytes_hex=raw.hex(), sha256=digest)
        acceptance = source / 'experiments/export-execution/resume_acceptance.sql'
        result['acceptance_sha256'] = sha(acceptance)
        accepted = command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XAtq',
            '-v', 'ON_ERROR_STOP=1', '-v', 'digest='+digest, '-U', 'postgres', '-d', 'bbpub_control_normal',
            input=acceptance.read_text())
        result['acceptance_stdout'] = accepted
        results = [json.loads(line) for line in accepted.splitlines() if line.startswith(('[', '{'))]
        assert len(results) == 3 and len(results[0]) == 11
        result['cases'], result['committed'], result['session_cleanup'] = results
        assert results[1] == dict(state='paused', revision='7', accepted_revision='5', resume_commands=1, units=1, artifacts=0, pin_active=True)
        assert results[2] == dict(cleanup_session_removed=True, resume_receipt_unchanged=True)
        result['status'] = 'PASS'
    except BaseException as error:
        result['status'] = 'FAIL'
        result['failure'] = type(error).__name__
        raise
    finally:
        (root / 'resume-schema-cases.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main(Path(sys.argv[1]).resolve())
