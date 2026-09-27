#!/usr/bin/env python3
"""Discriminating session expiry at a real status-grant SQL lock wait."""
import json
import re
import secrets
import subprocess
import time
from signed_publication_cases import Process, event, send


def session_waits(root,state,sql,client,exchange,actor):
    db='bbpub_control_normal'
    records=[]
    path=root/'job-pages-waits.json';assert not path.exists()
    def save():path.write_text(json.dumps(records,indent=2)+'\n')
    def clock():return int(sql(db,'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
    for expired in [False,True]:
        name='grant-wait-expired' if expired else 'grant-wait-live'
        record=dict(name=name,passed=False);records.append(record);save()
        holder=None
        try:
            prepared=exchange(name,'prepare-short')
            private=json.loads((root/'out/private-grant-http'/(name+'.private')).read_text())
            assert prepared['session']==private['id'] and re.fullmatch('[0-9a-f]{32}',private['id'])
            assert all(re.fullmatch('[0-9a-f]{64}',private[k]) for k in ['token','csrf'])
            # Open the holder before starting the short lifetime; all guards and
            # the immutable session issuance audit remain enabled.
            holder=Process(['docker','exec','-i',state['postgres_id'],'psql','-XAtq',
                '-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input_pipe=True)
            send(holder,"SET application_name='job-page-grant-holder'; SET statement_timeout='4s'; SET idle_in_transaction_session_timeout='8s'; SELECT json_build_object('event','holder-open','pid',pg_backend_pid());\n")
            record['holder']=event(holder,'holder-open')
            provision=f"""BEGIN;
                WITH t AS (SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint n)
                INSERT INTO control.browser_sessions(session_id,dataset_id,principal_id,
                    credential_revision,authentication_revision,token_digest,csrf_digest,
                    issued_at_us,expires_at_us,idle_window_us,last_seen_at_us,idle_expires_at_us)
                SELECT decode('{private['id']}','hex'),dataset_id,principal_id,
                    credential_revision,authentication_revision,decode('{private['token']}','hex'),
                    decode('{private['csrf']}','hex'),n,n+60000000,1250000,n,n+1250000
                FROM control.local_accounts,t WHERE principal_id=decode('{actor}','hex');
                INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,
                    created_at_us,config_revision,subject_id,policy_revision)
                SELECT decode('{secrets.token_hex(16)}','hex'),dataset_id,principal_id,
                    'browser.session.issue','accepted',issued_at_us,1,session_id,3
                FROM control.browser_sessions WHERE session_id=decode('{private['id']}','hex');
                SELECT json_build_object('id',encode(session_id,'hex'),'issued',issued_at_us,
                    'expires',idle_expires_at_us,'last_seen',last_seen_at_us,'xmin',xmin::text)
                FROM control.browser_sessions WHERE session_id=decode('{private['id']}','hex'); COMMIT;"""
            # Error DETAIL may contain digests. Never print the provisioning SQL
            # or PostgreSQL stderr through a generic command exception.
            seeded=subprocess.run(['docker','exec','-i',state['postgres_id'],'psql','-XAtq',
                '-v','ON_ERROR_STOP=1','-U','postgres','-d',db],input=provision,text=True,
                stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=5)
            assert seeded.returncode==0,'short session provisioning refused (details withheld)'
            record['session']=json.loads(seeded.stdout)
            del private,provision,seeded
            expires=record['session']['expires']
            send(holder,"BEGIN; LOCK TABLE control.principal_grants IN ACCESS EXCLUSIVE MODE; SELECT json_build_object('event','locked');\n")
            event(holder,'locked',timeout=2)
            request=name+'-response'
            started=time.monotonic()
            send(client,json.dumps(dict(request=request,operation='page',username=name,invalid='abc'))+'\n')
            deadline=time.monotonic()+1
            while True:
                rows=json.loads(sql(db,f"""SELECT coalesce(json_agg(json_build_object(
                    'pid',w.pid,'start',w.backend_start,'wait',w.wait_event_type,
                    'time_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
                    'status_query',w.query LIKE '%FROM control.principal_grants%'
                        AND w.query LIKE '%status.read%')),'[]')
                    FROM pg_stat_activity w JOIN control.owner_epochs o
                        ON w.pid=o.control_backend_pid AND w.backend_start=o.control_backend_start
                    WHERE o.role='control' AND o.state='active' AND w.wait_event_type='Lock'
                        AND {record['holder']['pid']}=ANY(pg_blocking_pids(w.pid))"""))
                if rows:break
                assert time.monotonic()<deadline,'actual status-grant wait missing'
                time.sleep(.01)
            assert len(rows)==1 and rows[0]['status_query'] and rows[0]['time_us']<expires
            record['blocked']=rows[0];save()
            if expired:
                while clock()<expires+30000:time.sleep(.01)
            record['release_time_us']=clock()
            assert (record['release_time_us']>=expires)==expired
            send(holder,"ROLLBACK; SELECT json_build_object('event','unlocked');\n\\q\n")
            event(holder,'unlocked');assert holder.wait(5)==0 and not holder.stderr
            holder.stop();holder=None
            response=record['response']=event(client,request,timeout=5)
            record['response_time_us']=clock()
            assert (record['response_time_us']>=expires)==expired
            record['elapsed_seconds']=time.monotonic()-started
            assert record['elapsed_seconds']<4
            expected=(401,'authentication_denied') if expired else (400,'invalid_cursor')
            record['expected']=dict(status=expected[0],code=expected[1])
            record['passed']=(response['status'],response['observation']['error']['code'])==expected
            after=json.loads(sql(db,f"""SELECT json_build_object('id',encode(session_id,'hex'),
                'issued',issued_at_us,'expires',idle_expires_at_us,'last_seen',last_seen_at_us,
                'xmin',xmin::text) FROM control.browser_sessions
                WHERE session_id=decode('{record['session']['id']}','hex')"""))
            record['session_after']=after;assert after==record['session']
        finally:
            if holder:
                try:send(holder,'ROLLBACK;\n\\q\n');holder.wait(5)
                finally:holder.stop()
                record['holder_terminal']=dict(exit=holder.process.poll(),overflow=holder.overflow)
            save()
    return records
