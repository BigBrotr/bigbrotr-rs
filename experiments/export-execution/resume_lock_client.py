#!/usr/bin/env python3
"""Real HTTP resume with a short seeded session; secrets stay in client memory."""
from http.cookies import SimpleCookie
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import select
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available


def main(name, port, job):
    assert name in ('account-expiry','source-expiry','source-live','commit-idle-expiry','commit-maximum-expiry','commit-live') and 0<port<65536
    assert re.fullmatch('[0-9a-f]{32}',job)
    original=http.CONFIG
    needle='[postgres.control]\nendpoint="127.0.0.1:5432"'
    text=original.read_text();assert text.count(needle)==1
    http.CONFIG=http.ROOT/('job-resume-lock-'+name+'.toml')
    http.CONFIG.write_text(text.replace(needle,f'[postgres.control]\nendpoint="127.0.0.1:{port}"')+'\n[runtime]\nheartbeat_interval="500ms"\ngraceful_shutdown_timeout="2s"\n')
    server=None
    try:
        server=http.Server('job-resume-lock-'+name)
        ready=server.ready(2)
        status,value,headers=http.request('POST','/management/v1/sessions',dict(username='alice',password='Independent owner password 2026'))
        assert status==201 and value['config_revision']=='1' and value['policy_revision']=='2'
        cookies=SimpleCookie()
        for k,v in headers:
            if k.lower()=='set-cookie':cookies.load(v)
        long={'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
        token,csrf=secrets.token_bytes(32),secrets.token_bytes(32)
        short={'Cookie':'BigBrotrLocalSession='+token.hex(),'X-CSRF-Token':csrf.hex()}
        identity=secrets.token_hex(16)
        private=http.ROOT/('job-resume-lock-'+name+'.private')
        # Only hashes leave this process, via a private file omitted from capture.
        with os.fdopen(os.open(private,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as stream:
            json.dump(dict(id=identity,token=hashlib.sha256(b'bigbrotr/browser-session/1\0'+token).hexdigest(),csrf=hashlib.sha256(b'bigbrotr/browser-csrf/1\0'+csrf).hexdigest()),stream)
        emit('ready',owner=ready,parent_pid=server.process.pid,session=identity)
        for _ in range(8):
            assert select.select([sys.stdin],[],[],15)[0], 'host instruction deadline'
            command=json.loads(sys.stdin.readline());op=command['operation']
            if op=='stop':
                server.stop()
                assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
                assert not list((http.ROOT/'scratch').iterdir())
                emit('terminal',exit=0,parent_absent=True,lock_available=True,scratch_empty=True);return
            assert op in ('resume','historical-short','historical-long','health')
            auth=long if op in ('health','historical-long') else short
            method,path,body='POST',f'/management/v1/jobs/{job}/resume',dict(expected_job_revision='7',expected_config_revision='1',expected_policy_revision='2')
            if op=='health':method,path,body='GET',f'/management/v1/jobs/{job}',None
            started=time.monotonic()
            emit(op+'-submitted',monotonic=started)
            status,value,headers=http.request(method,path,body,{**auth,'Idempotency-Key':command['key']} if op!='health' else auth)
            elapsed=time.monotonic()-started
            assert not any(k.lower()=='set-cookie' for k,_ in headers)
            assert ('cache-control','no-store') in [(k.lower(),v) for k,v in headers]
            emit(op+'-response',status=status,body=value,elapsed_seconds=elapsed)
        raise AssertionError('instruction budget')
    finally:
        if server:server.cleanup()
        http.CONFIG=original


if __name__=='__main__':main(sys.argv[1],int(sys.argv[2]),sys.argv[3])
