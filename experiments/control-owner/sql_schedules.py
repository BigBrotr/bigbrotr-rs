#!/usr/bin/env python3
"""SQL boundary schedules after the actual Control-owner lifecycle probe."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
sys.dont_write_bytecode = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve(strict=True)
    sys.path.insert(0, str(source / 'experiments/control-migrations'))
    import lab
    root = args.root.resolve(strict=True)
    state = lab.load(root)
    if Path(state['source']) != source: raise RuntimeError('source path mismatch')
    if lab.command(['git','-C',str(source),'status','--porcelain','--untracked-files=all']).stdout: raise RuntimeError('dirty product source')
    if lab.command(['git','-C',str(source),'rev-parse','HEAD']).stdout.strip() != state['build']['build_info']['source']['revision']: raise RuntimeError('artifact source mismatch')
    report = {'format':'bigbrotr-control-sql-schedules/1','status':'RUNNING','cases':[], 'helper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    output = root / 'control-sql-schedules.json'
    if output.exists(): raise RuntimeError('existing evidence')
    database = 'bb_owner_lifecycle'
    before = lab.schema_digest(state, database)
    try:
        fixture = Path(__file__).with_name('schema_cases.sql')
        report['fixture_sha256'] = hashlib.sha256(fixture.read_bytes()).hexdigest()
        lab.sql(state, database, fixture.read_text())
        if lab.schema_digest(state, database) != before: raise RuntimeError('SQL negative mutation')
        report['cases'].append({'name':'six-SQL-negatives','result':'PASS','sqlstate':'23514','schema_unchanged':True})
        for guarded in [True,False]:
            name = 'bb-control-retain' if guarded else 'bb-control-negative'
            command = ['docker','exec','-i',state['postgres'],'psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-U','bigbrotr_control','-d',database]
            sql = f"SET application_name='{name}'; BEGIN; SELECT pg_advisory_lock(1111651442,1129599564); " + ('SELECT control.retain_control_lock(); ' if guarded else '') + 'SELECT pg_advisory_unlock(1111651442,1129599564); SELECT pg_sleep(4); COMMIT;'
            with (root / (name+'.stdout')).open('w') as stdout, (root / (name+'.stderr')).open('w') as stderr:
                process = subprocess.Popen(command,stdin=subprocess.PIPE,stdout=stdout,stderr=stderr,text=True)
                process.stdin.write(sql);process.stdin.close()
                try:
                    deadline = time.monotonic()+3
                    observation = None
                    while time.monotonic()<deadline:
                        found = json.loads(lab.sql(state,database,f"SELECT coalesce(json_agg(t),'[]') FROM (SELECT pid,backend_start,state,wait_event FROM pg_stat_activity WHERE application_name='{name}' AND wait_event='PgSleep') t;"))
                        if found: observation=found;break
                        time.sleep(.05)
                    if observation is None: raise RuntimeError('schedule not observed')
                    obtained = lab.sql(state,database,'SELECT pg_try_advisory_lock(1111651442,1129599564);')
                    if obtained != ('f' if guarded else 't'): raise RuntimeError('wrong transaction exclusion')
                    if process.wait(timeout=8) != 0: raise RuntimeError('scheduled backend failed')
                finally:
                    if process.poll() is None: process.kill();process.wait()
            if lab.sql(state,database,'SELECT pg_try_advisory_lock(1111651442,1129599564);') != 't': raise RuntimeError('lock retained beyond completion')
            report['cases'].append({'name':name,'result':'PASS','blocked_successor':guarded,'active_backend':observation})
        if lab.schema_digest(state,database) != before: raise RuntimeError('schedules mutated fixture')
        report['status']='PASS';report['schema_sha256']=before
    except BaseException as error:
        report['status']='FAIL';report['failure']=str(error);raise
    finally:lab.write_json(output,report)
    print(json.dumps(report))

if __name__ == '__main__': main()
