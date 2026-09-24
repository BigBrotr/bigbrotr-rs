#!/usr/bin/env python3
"""Restore the revoked grant, explicitly resume, and resolve after restart."""
from http.cookies import SimpleCookie
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available


def main(job):
    if job == 'migrate':
        result = http.cli('migrate', 'apply', '--target', '814', '--user', 'postgres',
            '--password-ref', f'file:{http.ROOT}/postgres')
        emit('migration', observation=result)
        return
    assert len(job)==32 and all(c in '0123456789abcdef' for c in job)
    server = http.Server('executor-resume-output')
    def terminal():
        server.stop()
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT/'scratch').iterdir())
        return dict(exit=0, parent_absent=True, lock_available=True, scratch_empty=True)
    try:
        owner = server.ready(4)
        status, _, headers = http.request('POST', '/management/v1/sessions',
            dict(username='alice', password='Independent owner password 2026'))
        assert status==201, status
        cookies = SimpleCookie()
        for name,value in headers:
            if name.lower()=='set-cookie':cookies.load(value)
        auth = {'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
                'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
        body = dict(expected_job_revision='4', expected_config_revision='1', expected_policy_revision='4')
        key = 'f2'*16
        def resume():
            status,value,headers = http.request('POST', f'/management/v1/jobs/{job}/resume', body,
                {**auth, 'Idempotency-Key':key})
            assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
            assert not any(k.lower()=='set-cookie' for k,_ in headers)
            return status,value
        status,value = resume()
        assert status==403, (status,value)
        emit('ready', owner=owner, parent_pid=server.process.pid, denied=status)
        assert sys.stdin.readline()=='restore\n'
        status,me,_ = http.request('GET','/management/v1/me',auth=auth)
        assert status==200 and me['grant_revision']=='5', (status,me)
        command = dict(principal_id=me['principal_id'],action='history.read',enabled=True,
            scope=dict(dataset_id='01'*16,dictionary_kind=4,allocation_domain='04'*16,ordinal='1'),
            expected_config_revision='1',expected_policy_revision='4',expected_grant_revision='5',
            allocation=dict(catalog_id='5a'*16,frontier_revision='1'))
        grant_auth = {**auth,'Idempotency-Key':'e6'*16}
        status,proof,_ = http.request('POST','/management/v1/sessions/current/reauthenticate',
            dict(password='Independent owner password 2026',command=command),grant_auth)
        assert status==201, status
        status,receipt,_ = http.request('PUT',f"/management/v1/principals/{me['principal_id']}/grants",
            dict(command=command,proof_id=proof['proof_id']),grant_auth)
        assert status==200 and receipt['created'] and receipt['enabled'], (status,receipt)
        assert receipt['scope_generation']=='3' and receipt['account_grant_revision']=='6'
        emit('restored', status=status, receipt=receipt)
        assert sys.stdin.readline()=='resume\n'
        status,receipt = resume()
        assert status==202 and receipt['created'] and receipt['job_revision']=='5', (status,receipt)
        emit('resumed', status=status, receipt=receipt, command=body, key=key)
        assert sys.stdin.readline()=='retry\n'
        status,value = resume()
        assert status==200 and value=={**receipt,'created':False}, (status,value)
        emit('retried', status=status, receipt=value)
        assert sys.stdin.readline()=='restart\n'
        stopped = terminal()
        server = http.Server('executor-resume-output-restart')
        owner = server.ready(4)
        status,value = resume()
        assert status==200 and value=={**receipt,'created':False}, (status,value)
        emit('restarted', status=status, receipt=value, owner=owner, stopped=stopped)
        assert sys.stdin.readline()=='stop\n'
        emit('terminal', **terminal())
    finally:
        server.cleanup()


if __name__=='__main__':main(sys.argv[1])
