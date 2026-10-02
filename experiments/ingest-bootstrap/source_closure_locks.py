"""Current-schema raw job cuts versus the actual signed source-close consumer.

This checks relational concurrency, not authenticated job acceptance. All product
triggers remain active, including0815's earlier dataset admission lock.
"""
import json
from pathlib import Path
import shutil

import lab
from concurrency_cases import Session
from installed import terminal, wait_for
from source_closure_faults import LOCK, assert_effects, install_barrier

JOB = 'fa' * 16
REQUEST = 'fb' * 16
DIGEST = 'fc' * 32

INSERT = f"""
    INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,
        kind,config_revision,policy_revision,created_at_us,input_topology_generation)
    SELECT decode('{JOB}','hex'),dataset_id,decode('{JOB}','hex'),decode('{REQUEST}','hex'),
        decode('{DIGEST}','hex'),'export',config_revision,policy_revision,1,topology_generation
      FROM control.datasets;
"""
SEAL = f"""
    INSERT INTO control.job_input_bounds
    SELECT decode('{JOB}','hex'),dataset_id,topology_generation,shard_id,owner_epoch,13
      FROM control.topology_streams
     WHERE topology_generation=(SELECT topology_generation FROM control.datasets);
    UPDATE control.jobs SET input_complete=true WHERE job_id=decode('{JOB}','hex');
"""


def blocked(pg, database, holder, waiter=None):
    identity = f'pid={waiter}' if waiter is not None else "usename='bigbrotr_control'"
    return json.loads(lab.sql(pg, database, f"""
        SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,
            'query',query,'blockers',pg_blocking_pids(pid),'wait_event',wait_event)), '[]')
          FROM pg_stat_activity WHERE datname=current_database() AND {identity}
           AND wait_event_type='Lock' AND {holder}=ANY(pg_blocking_pids(pid))
    """)) or None


def check_job(before, after):
    for table in before.keys() - {'jobs', 'job_input_bounds', 'job_admissions'}:
        assert before[table] == after[table], table
    for table in ('jobs', 'job_input_bounds', 'job_admissions'):
        assert len(after[table]) == len(before[table]) + 1
        assert all(row in after[table] for row in before[table])
    job, = [r for r in after['jobs'] if r['job_id'] == '\\x' + JOB]
    assert job['state'] == 'accepted' and job['input_complete']
    assert job['request_key'] == '\\x' + REQUEST and job['request_digest'] == '\\x' + DIGEST
    assert job['export_job_protocol'] is None and job['acquisition_job_protocol'] is None
    bound, = [r for r in after['job_input_bounds'] if r['job_id'] == job['job_id']]
    assert bound['last_complete_sequence'] == 13
    assert bound['topology_generation'] == before['datasets'][0]['topology_generation']
    admission, = [r for r in after['job_admissions'] if r['job_id'] == job['job_id']]
    assert admission['admission_ordinal'] == max(r['admission_ordinal'] for r in before['job_admissions']) + 1


def run(pg, client, output, rows, prepare, check_native, cases, digest):
    shutil.copy2(Path(__file__).with_name('installed_client.py'), output / 'installed_client.py')
    pg_identity = json.loads(lab.docker('inspect', pg['postgres']).stdout)[0]['Id']

    def owned():
        current = json.loads(lab.docker('inspect', pg_identity).stdout)[0]
        assert current['State']['Running'] and current['Config']['Labels'][lab.LABEL] == pg['owner']

    for first in ('job', 'source'):
        name = 'source_' + first + '_first'
        database, config, directory = prepare(name)
        before = rows(database)
        lab.write_json(output / (name + '-before.json'), before)
        assert len(before['source_streams']) == len(before['topology_streams']) == 1
        assert before['topology_streams'][0]['handoff_last_complete_sequence'] is None
        assert all(r['job_id'] != '\\x' + JOB for r in before['jobs'])
        sessions = []
        observations = {}
        try:
            job = Session(pg, database, name + '_cut', sessions)
            job.execute('BEGIN;')
            if first == 'job':
                job.execute(INSERT)
                job.execute(SEAL)
                job.execute('SET CONSTRAINTS ALL IMMEDIATE;')
                assert rows(database) == before, 'uncommitted job became visible'
            else:
                ddl = install_barrier(pg, database, deferred=True)
                (output / (name + '-barrier.sql')).write_text(ddl)
                barrier = Session(pg, database, name + '_barrier', sessions)
                barrier.execute(f'SELECT pg_advisory_lock({LOCK[0]},{LOCK[1]});')
            prefix = output / name
            lab.docker('exec', '-d', client, 'python3', '/lab/installed_client.py', config,
                '/lab/' + name, 'source-closure')
            wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'source-CLI-pid', 5)
            if first == 'job':
                observed = wait_for(lambda: blocked(pg, database, job.backend['pid']), 'job-blocks-recovery', 2)
                assert len(observed) == 1 and 'control.datasets' in observed[0]['query']
                observations['job_blocks_cli'] = observed
                job.execute('COMMIT;')
                ended = terminal(prefix)
                assert ended['exit_code'] == 4 and not Path(str(prefix) + '.stdout').read_bytes()
                assert 'Backend' in Path(str(prefix) + '.stderr').read_text()
            else:
                observed = wait_for(lambda: blocked(pg, database, barrier.backend['pid']), 'source-COMMIT-barrier', 2)
                assert len(observed) == 1 and observed[0]['query'].strip() == 'COMMIT'
                owner_pid = observed[0]['pid']
                observations['source_commit'] = observed
                job.start(INSERT)
                observed = wait_for(lambda: blocked(pg, database, owner_pid, job.backend['pid']), 'source-blocks-job', 2)
                assert len(observed) == 1 and 'INSERT INTO control.jobs' in observed[0]['query']
                observations['cli_blocks_job'] = observed
                barrier.execute(f'SELECT pg_advisory_unlock({LOCK[0]},{LOCK[1]});')
                job.expect(job.finish())
                job.execute(SEAL)
                observations['job_commit'] = job.execute('COMMIT;', expected='23514')
                ended = terminal(prefix)
                assert ended['exit_code'] == 0 and not Path(str(prefix) + '.stderr').read_bytes()
                response, = [json.loads(line) for line in Path(str(prefix) + '.stdout').read_text().splitlines()]
                assert response['state'] == 'source-closed' and response['resolved_requests'] == '4'
                assert response['source_closure'] == dict(last_complete_sequence='12', sha256=digest)
                assert response['native'] == 'closed' and response['owner_release'] == 'committed'
            assert ended['reaped'] and not ended['forced'] and ended['elapsed_seconds'] < 6
            wait_for(lambda: lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'source-driver-ended', 5)
            after = rows(database)
            lab.write_json(output / (name + '-after.json'), after)
            if first == 'job':
                # Separate the job's exact durable rows from CLI effects without
                # hiding any unrelated changes in the full69-table comparison.
                with_job = dict(before, **{t: after[t] for t in ('jobs', 'job_input_bounds', 'job_admissions')})
                check_job(before, with_job)
                assert_effects(with_job, after, False, digest)
            else:
                assert_effects(before, after, True, digest, released=True)
            check_native(directory, name)
            result = dict(name=name, status='PASS', terminal=ended, locks=observations,
                source_closed=first == 'source', job13_committed=first == 'job',
                control_tables=69, native_rows=49, original_requests=4,
                qualification='Installed source consumer versus privileged relational job writer; all product triggers active')
            lab.write_json(output / (name + '-result.json'), result)
            cases.append(result)
        finally:
            for session in reversed(sessions):
                session.close(owned)
    lab.write_json(output / 'signed-recovery-cases.json', cases)
