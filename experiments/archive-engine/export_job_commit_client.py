#!/usr/bin/env python3
"""Real export acceptance loses its SQL COMMIT reply, then retries after restart."""
import json
from pathlib import Path
import sys
import private_grant_http as http
import export_job_http as jobs
from export_destination_blocked import lock_available


def emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def main(mode, port):
    assert mode in ('before-forward', 'after-complete') and 0 < port < 65536
    original=http.CONFIG;original_text=original.read_text()
    needle='[postgres.control]\nendpoint="127.0.0.1:5432"'
    assert original_text.count(needle)==1
    config=http.ROOT/('commit-'+mode+'.toml')
    config.write_text(original_text.replace(needle,f'[postgres.control]\nendpoint="127.0.0.1:{port}"'))
    http.CONFIG=config
    auth=json.loads((http.ROOT/'session.private').read_text())
    body=jobs.command();key=('e3' if mode=='before-forward' else 'e4')*16
    def request(value=body, request_key=key):
        return http.request('POST','/management/v1/jobs',value,{**auth,'Idempotency-Key':request_key})
    server=http.Server('commit-'+mode)
    try:
        ready=server.ready()
        assert http.request('GET','/management/v1/me',auth=auth)[0]==200
        assert not lock_available()
        emit('ready', parent_pid=server.process.pid, owner=ready, key=key, command=body)
        assert sys.stdin.readline()=='submit\n'
        try:response=dict(status=request()[0])
        except (OSError,http.http.client.HTTPException) as error:response=dict(closed=type(error).__name__)
        assert response.get('status') not in (200,202),response
        code=server.process.wait(timeout=12)
        stderr=server.error.read_text();stdout=server.output.read_text()
        assert code==4 and 'CommitUnknown' in stderr,(code,stderr)
        assert '"release":"committed"' not in stdout
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT/'scratch').iterdir())
        emit('failed', response=response, exit=code, stderr=stderr, parent_absent=True, lock_available=True)
        assert sys.stdin.readline()=='retry\n'
        http.CONFIG=original
        if mode=='after-complete':
            assert '[archive.reader]\nenabled=true' in original_text
            original.write_text(original_text.split('\n[export.destination]\n')[0].replace('[archive.reader]\nenabled=true','[archive.reader]\nenabled=false'))
        server=http.Server('commit-retry-'+mode);ready=server.ready()
        status,receipt,_=request()
        assert status==(202 if mode=='before-forward' else 200),(status,receipt)
        assert receipt['created']==(mode=='before-forward')
        assert receipt['command_key']==key and receipt['execution']=='unobserved' and receipt['authority']=='acceptance-only'
        historical={**receipt,'created':False}
        assert request()[:2]==(200,historical)
        assert request({**body,'query':{**body['query'],'page_size':99}})[0]==409
        if mode=='after-complete':assert request(request_key='e5'*16)[0]==503
        emit('retried', status=status, receipt=receipt, owner=ready, parent_pid=server.process.pid)
        assert sys.stdin.readline()=='stop\n'
        server.stop()
        assert not list((http.ROOT/'scratch').iterdir())
        emit('terminal', exit=server.process.returncode, lock_available=lock_available())
    finally:
        try:server.cleanup()
        finally:
            http.CONFIG=original;original.write_text(original_text)


if __name__=='__main__':main(sys.argv[1],int(sys.argv[2]))
