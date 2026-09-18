#!/usr/bin/env python3
"""MON-009 actual PostgreSQL cases; synthetic cuts, not admission receipts."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import stat

import lab
from concurrency_cases import Session, identity, require, sha, wait_blocked


def fixture(s):
    s.execute("BEGIN;")
    for statement in (
        f"INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES({identity(1)},{identity(2)},0);",
        f"INSERT INTO control.config_revisions VALUES(1,{identity(1)},NULL,1,'managed','{{}}',{identity(1,32)},{identity(2,32)},0,{identity(3)});",
        f"INSERT INTO control.policy_revisions VALUES(1,{identity(1)},NULL,1,1,'{{}}',{identity(3,32)},0,{identity(3)});",
        f"INSERT INTO control.role_instances VALUES({identity(11)},{identity(1)},'ingest',0,NULL);",
        f"INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES({identity(30)},1,{identity(1)},'ingest',{identity(11)},{identity(31)},0,-1,'active');",
        f"INSERT INTO control.topology_generations VALUES({identity(1)},0,NULL,0,false);",
    ):
        s.execute(statement)
    for shard in (1, 2):
        s.execute(f"INSERT INTO control.topology_streams VALUES({identity(1)},0,{shard},1,NULL);")
        s.execute(f"INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,owner_incarnation,opened_at_us) VALUES({identity(1)},{shard},1,{identity(30)},{identity(31)},0);")
    s.execute("UPDATE control.topology_generations SET sealed=true WHERE generation=0;")
    s.execute("UPDATE control.datasets SET config_revision=1,policy_revision=1,topology_generation=0;")
    s.execute("COMMIT;")
    s.execute("UPDATE control.source_streams SET state='closing';")


def job(s, sequences=(5, 0), number=40):
    s.execute(f"INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_topology_generation) VALUES({identity(number)},{identity(1)},{identity(3)},{identity(number)},{identity(40,32)},'export',1,1,0,0);")
    for shard, sequence in enumerate(sequences, 1):
        s.execute(f"INSERT INTO control.job_input_bounds VALUES({identity(number)},{identity(1)},0,{shard},1,{sequence});")
    s.execute(f"UPDATE control.jobs SET input_complete=true WHERE job_id={identity(number)};")


def close_sql(shard=1, cap=5):
    return f"UPDATE control.source_streams SET state='closed',last_complete_sequence={cap},closed_at_us=1,closure_digest={identity(55,32)} WHERE shard_id={shard};"


def observe(state, database):
    return json.loads(lab.sql(state, database, """SELECT json_build_object(
      'streams',(SELECT json_agg(json_build_array(shard_id,state,last_complete_sequence) ORDER BY shard_id) FROM control.source_streams),
      'topology',(SELECT json_agg(json_build_array(shard_id,handoff_last_complete_sequence) ORDER BY shard_id) FROM control.topology_streams),
      'jobs',(SELECT coalesce(json_agg(json_build_array(encode(job_id,'hex'),state,input_complete) ORDER BY job_id),'[]') FROM control.jobs),
      'bounds',(SELECT coalesce(json_agg(json_build_array(encode(job_id,'hex'),shard_id,last_complete_sequence) ORDER BY job_id,shard_id),'[]') FROM control.job_input_bounds));"""))


def expected(cap=None, sequences=None):
    key = format(40, '032x')
    return dict(streams=[[1, 'closing' if cap is None else 'closed', cap], [2, 'closing', None]],
                topology=[[1, None], [2, None]],
                jobs=[] if sequences is None else [[key, 'accepted', True]],
                bounds=[] if sequences is None else [[key, i, v] for i, v in enumerate(sequences, 1)])


def run(root, name):
    state = lab.load(root)
    source = Path(__file__).resolve().parents[2]
    require(not lab.command(['git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    out = root / (name + '-job-bounds')
    out.mkdir(mode=0o700)
    report = dict(status='RUNNING', scope='MON-009 negative controls on migration 2', cases=[], cleanup='NOT_RUN',
                  revision=lab.command(['git', '-C', str(source), 'rev-parse', 'HEAD']).stdout.strip(),
                  binary_sha256=state['binary_sha256'], application_source=state['build']['build_info']['source'],
                  harness_sha256=sha(Path(__file__)), server=state['server'], sessions=[])
    sessions = []
    owned_ids = {key: json.loads(lab.docker('inspect', state[key]).stdout)[0]['Id'] for key in ('postgres', 'client')}

    def owned():
        lab.load(root)
        for key, value in owned_ids.items():
            info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
            require(info['Id'] == value and info['State']['Running'], 'lab-incarnation-changed')

    def save():
        lab.write_json(out / 'result.json', report)

    try:
        owned()
        for case in ('closure_then_job', 'job_then_closure'):
            database = 'bb_' + name + '_' + case
            lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
            config = lab.config(root, state, database)
            result = lab.migrate(state, 'apply', config, target=2)
            require(result.returncode == 0 and json.loads(result.stdout)['current_version'] == '2', 'migration-2-failed')
            receipts = json.loads(lab.sql(state, database, "SELECT json_agg(json_build_array(migration,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions;"))
            require(receipts == [[n, sha(source / 'migrations/postgres' / filename)] for n, filename in ((1, '0001_control.sql'), (2, '0002_ingest.sql'))], 'receipt-mismatch')
            s = Session(state, database, name + '_' + case, sessions)
            report['sessions'].append(s.backend)
            fixture(s)
            require(observe(state, database) == expected(), 'bootstrap-rows')
            if case == 'closure_then_job':
                s.execute(close_sql())
            s.execute('BEGIN;')
            job(s, (6, 0))
            s.execute('COMMIT;')
            if case == 'job_then_closure':
                s.execute(close_sql())
            actual = observe(state, database)
            require(actual == expected(5, (6, 0)), 'counterexample-not-reproduced')
            report['cases'].append(dict(name=case, status='COUNTEREXAMPLE_REPRODUCED', rows=actual, receipts=receipts))
            save()
        report['status'] = 'COUNTEREXAMPLES_REPRODUCED'
    finally:
        try:
            for s in sessions:
                s.close(owned)
            report['cleanup'] = 'PASS'
        finally:
            save()
    print(json.dumps(dict(status=report['status'], output=str(out))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-name', required=True)
    args = parser.parse_args()
    require(args.root.is_absolute() and args.root.resolve() == args.root, 'root-must-be-canonical')
    info = args.root.stat()
    require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700, 'root-ownership')
    require(re.fullmatch('[a-z_]{1,16}', args.run_name), 'run-name')
    with (args.root / 'job-bounds.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(args.root, args.run_name)
