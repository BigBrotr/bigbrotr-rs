#!/usr/bin/env python3
"""One in-memory browser identity across both resume COMMIT-loss boundaries."""
from http.cookies import SimpleCookie
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available
from resume_http_client import DESTINATION


def main(port, job):
    assert 0<port<65536 and len(job)==32 and all(c in '0123456789abcdef' for c in job)
    original=http.CONFIG;config=original.read_text();server=None
    needle='[postgres.control]\nendpoint="127.0.0.1:5432"'
    assert config.count(needle)==1 and config.endswith(DESTINATION)
    key='f3'*16
    body=dict(expected_job_revision='7',expected_config_revision='1',expected_policy_revision='2')
    def start(name, port):
        nonlocal server
        http.CONFIG=http.ROOT/('job-resume-commit-'+name+'.toml')
        http.CONFIG.write_text(config.replace(needle,f'[postgres.control]\nendpoint="127.0.0.1:{port}"') if port else config.removesuffix(DESTINATION))
        server=http.Server('job-resume-commit-'+name)
        return server.ready(2)
    try:
        ready=start('before',port)
        status,_,headers=http.request('POST','/management/v1/sessions',dict(username='alice',password='Independent owner password 2026'))
        assert status==201,status
        cookies=SimpleCookie()
        for k,v in headers:
            if k.lower()=='set-cookie':cookies.load(v)
        auth={'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value,'Idempotency-Key':key}
        def submit():return http.request('POST',f'/management/v1/jobs/{job}/resume',body,auth)
        for mode in ['before','after']:
            emit('ready-'+mode,owner=ready,parent_pid=server.process.pid,key=key,command=body)
            assert sys.stdin.readline()=='submit\n'
            try:response=dict(status=submit()[0])
            except (OSError,http.http.client.HTTPException) as error:response=dict(closed=type(error).__name__)
            assert response.get('status') not in (200,202),response
            code=server.process.wait(timeout=12)
            error=server.error.read_text();lines=server.output.read_text().splitlines()
            assert code==4 and 'CommitUnknown' in error,(code,error)
            assert all(json.loads(line).get('release')!='committed' for line in lines)
            assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
            assert not list((http.ROOT/'scratch').iterdir())
            emit('failed-'+mode,response=response,exit=code,error=error,parent_absent=True,lock_available=True,scratch_empty=True)
            if mode=='before':
                instruction=json.loads(sys.stdin.readline());assert set(instruction)=={'port'} and 0<instruction['port']<65536
                ready=start('after',instruction['port'])
        assert sys.stdin.readline()=='retry\n'
        ready=start('retry',0)
        status,receipt,headers=submit()
        assert status==200 and not receipt['created'],(status,receipt)
        assert receipt['command_key']==key and receipt['job_id']==job and receipt['job_revision']=='8'
        assert receipt['execution']=='unobserved' and receipt['authority']=='acceptance-only'
        assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers] and not any(k.lower()=='set-cookie' for k,_ in headers)
        assert submit()[:2]==(200,receipt)
        emit('retried',status=status,receipt=receipt,owner=ready,parent_pid=server.process.pid)
        assert sys.stdin.readline()=='stop\n'
        server.stop()
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT/'scratch').iterdir())
        emit('final-terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True)
    finally:
        if server:server.cleanup()
        http.CONFIG=original


if __name__=='__main__':main(int(sys.argv[1]),sys.argv[2])
