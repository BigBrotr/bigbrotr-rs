#!/usr/bin/env python3
"""Actual Rust resume consumer after the signed native seed and SQL boundary."""
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
import resume_cases
from signed_publication_cases import command, execute, sha


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    result = dict(status='INCOMPLETE', source=state['source'], tree=state['tree'], cases={},
        scope='Actual Rust resume and application role on retained SQL fixture; no HTTP or resumed artifact output')
    path = root / 'resume-adapter-cases.json'
    assert not path.exists()
    def save(): path.write_text(json.dumps(result, indent=2) + '\n')
    def sql(text):
        return command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
            '-U', 'postgres', '-d', 'bbpub_control_normal', input=text)
    def run(name, mode):
        observed = result['cases'][name] = {}
        execute(['docker', 'exec', state['container_id'], '/out/adapter-artifacts/export-execution-probe', mode], 30, observed)
        save()
        assert observed['exit'] == 0 and not observed['stderr'] and not observed['overflow']
        return [json.loads(line) for line in observed['stdout'].splitlines()]
    def observe():
        tables = ['jobs', 'export_jobs', 'export_execution_units', 'artifacts', 'artifact_files', 'snapshot_leases', 'archive_snapshot_pins', 'retention_claims', 'export_resume_commands', 'management_commands', 'management_audit']
        results = {}
        for name in tables:
            where = " WHERE action='job.export.resume'" if name in ['management_commands', 'management_audit'] else ''
            results[name] = json.loads(sql(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',t.xmin::text) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM control.{name} t{where}"))
        return results
    try:
        resume_cases.main(root)
        result['before'] = observe(); save()
        sql('GRANT SELECT,INSERT ON control.export_execution_units,control.artifacts,control.artifact_files TO bigbrotr_control; GRANT UPDATE(artifact_id) ON control.artifacts TO bigbrotr_control; GRANT UPDATE(principal_id) ON control.principal_grants TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        assert run('missing-resume-ledger-read', 'resume-denied') == [dict(case='resume-ledger-select-required-before-existing-job-writes', status='PASS')]
        sql('GRANT SELECT,INSERT ON control.export_resume_commands TO bigbrotr_control;')
        assert run('resume', 'resume') == [dict(case=name, status='PASS') for name in [
            'fresh-session-resolves-old-receipt-after-second-pause-and-session-cleanup',
            'retry-needs-no-current-input-destination-or-role-control-and-cannot-requeue',
            'new-resume-refuses-missing-destination-stale-revision-and-invalid-csrf',
            'resume-commits-once-without-roles-control-status-or-manager-and-preserves-unit',
            'resume-control-driver-joined-before-destination-release']]
        result['after'] = observe(); save()
        before, after = result['before'], result['after']
        assert len(before['jobs']) == len(after['jobs']) == 1
        job = after['jobs'][0]['row']
        assert job['state'] == 'queued' and job['job_revision'] == 8 and job['reason_code'] is None
        changed = ['state', 'job_revision', 'reason_code', 'owner_resource_id', 'owner_epoch', 'owner_incarnation']
        assert {k:v for k,v in before['jobs'][0]['row'].items() if k not in changed} == {k:v for k,v in job.items() if k not in changed}
        for table in before.keys() - {'jobs', 'export_resume_commands', 'management_commands', 'management_audit'}: assert before[table] == after[table], table
        for table in ['export_resume_commands', 'management_commands', 'management_audit']:
            assert len(before[table]) == 1 and len(after[table]) == 2
            assert before[table][0] in after[table]
        result['terminal'] = json.loads(sql("SELECT json_build_object('active_control',(SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'),'backends',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()),'forbidden_extra_grants',(SELECT count(*) FROM control.principal_grants WHERE action IN ('roles.control','status.read','principals.manage')),'command_count',(SELECT count(*) FROM control.management_commands WHERE action='job.export.resume'),'audit_count',(SELECT count(*) FROM control.management_audit WHERE action='job.export.resume'))"))
        assert result['terminal'] == dict(active_control=0, backends=0, forbidden_extra_grants=0, command_count=2, audit_count=2)
        result['status'] = 'PASS'
    except BaseException as error:
        result['failure'] = type(error).__name__
        raise
    finally: save()


if __name__ == '__main__': main(Path(sys.argv[1]).resolve())
