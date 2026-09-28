#!/usr/bin/env python3
"""Installed CLI pages, restart, current grants and logout against real PostgreSQL."""
import hashlib
import json
from job_pages_cases import EnvelopeOracle
from signed_publication_cases import Process, event, send


def run(root, state, sql):
    db = 'bbpub_control_normal'
    private = root/'out/private-grant-http'
    result = dict(status='INCOMPLETE', source=state['source'], cases={}, boundaries={})
    path = root/'job-pages-cli-cases.json'; assert not path.exists()
    client = None
    def save(): path.write_text(json.dumps(result, indent=2)+'\n')
    def query(text): return sql(db, text)
    def rows(table):
        return json.loads(query(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{table} t"))
    def ledger():
        return {t: rows(t) for t in ('jobs','job_admissions','job_input_bounds','export_jobs',
            'export_execution_units','artifacts','artifact_files','snapshot_leases',
            'archive_snapshot_pins','retention_claims')}
    def sessions():
        # Deliberately exclude digests/credentials from ordinary evidence captures.
        return json.loads(query("SELECT jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'actor',encode(principal_id,'hex'),'issued',issued_at_us,'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id) FROM control.browser_sessions"))
    def now(): return int(query('SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint'))
    def exchange(name, operation='page', **fields):
        assert name not in result['cases']
        before = now()
        send(client, json.dumps(dict(request=name, operation=operation, **fields))+'\n')
        response = event(client, name, timeout=15)
        assert response['operation'] == operation
        result['cases'][name] = dict(before_us=before, after_us=now(), response=response)
        save(); return response
    def page(name, expected, more, username='alice', **fields):
        response = exchange(name, username=username, **fields)
        assert response['exit'] == 0 and not response['stderr'], response
        assert response['credential_file_present'] and response['credential_file_unchanged']
        body = response['observation']
        assert set(body) == {'format','items','effective_page_size','observed_at_us','authority','has_next'} | ({'cursor_sha256'} if more else set())
        assert body['format'] == 'bigbrotr-job-list/1' and body['authority'] == 'observation-only'
        assert body['effective_page_size'] == 2 and body['has_next'] == more
        assert [{k:v for k,v in row.items() if k != 'observed_at_us'} for row in body['items']] == expected
        assert all(row['observed_at_us'] == body['observed_at_us'] for row in body['items'])
        assert result['cases'][name]['before_us'] <= int(body['observed_at_us']) <= result['cases'][name]['after_us']
        if more:
            token = (private/'cli-sessions'/(name+'.cursor.private')).read_text()
            assert hashlib.sha256(token.encode()).hexdigest() == body['cursor_sha256']
            decoded = oracle.open(token)
            result['cases'][name]['decoded'] = decoded; save()
            assert decoded['actor'] == actor and decoded['policy'] == 3 and decoded['cut'] == 6
            assert decoded['page_size'] == 2 and decoded['expires']-decoded['issued'] == 900_000_000
        return body
    def refusal(name, diagnostic, **fields):
        response = exchange(name, username=fields.pop('username','alice'), **fields)
        assert response['exit'] == 3 and response['observation'] is None
        assert response['stderr'] == 'error: '+diagnostic+'\n'
        assert response['credential_file_unchanged']
    def boundary(name):
        value = dict(ledger=ledger(), sessions=sessions(), audit=rows('management_audit'))
        result['boundaries'][name] = value; save()
        assert value['ledger'] == baseline and value['sessions'] == session_baseline
        assert value['audit'] == audit_baseline
    def grant(enabled):
        before = rows('principal_grants')
        old_revision = int(query(f"SELECT grant_revision FROM control.local_accounts WHERE principal_id=decode('{actor}','hex')"))
        change = (f"INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'status.read','dataset',dataset_id FROM control.local_accounts WHERE principal_id=decode('{actor}','hex');" if enabled else
            f"DELETE FROM control.principal_grants WHERE principal_id=decode('{actor}','hex') AND action='status.read';")
        query(f"BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; SELECT 1 FROM control.local_accounts WHERE principal_id=decode('{actor}','hex') FOR UPDATE; {change} UPDATE control.local_accounts SET grant_revision=grant_revision+1 WHERE principal_id=decode('{actor}','hex'); COMMIT;")
        after = rows('principal_grants')
        small, large = (before, after) if enabled else (after, before)
        assert len(large) == len(small)+1 and all(row in large for row in small)
        assert int(query(f"SELECT grant_revision FROM control.local_accounts WHERE principal_id=decode('{actor}','hex')")) == old_revision+1
        result.setdefault('grants',[]).append(dict(enabled=enabled,before=before,after=after,prior_revision=old_revision)); save()
    try:
        baseline = result['before'] = ledger()
        jobs = sorted([v['row'] for v in baseline['jobs']], key=lambda v:(v['created_at_us'],v['job_id']), reverse=True)
        assert len(jobs) == 6 and [v['state'] for v in jobs] == ['accepted']*5+['succeeded']
        actor = jobs[0]['principal_id'][2:]
        expected = [dict(format='bigbrotr-job-status/1',kind='export',job_id=v['job_id'][2:],
            state=v['state'],job_revision=str(v['job_revision']),checkpoint_revision=str(v['checkpoint_revision']),
            created_at_us=str(v['created_at_us']),completed_at_us=str(v['completed_at_us']) if v['completed_at_us'] is not None else None,
            reason_code=None,authority='observation-only') for v in jobs]
        oracle = EnvelopeOracle((private/'job-page-keys/ring').read_bytes()); result['oracle'] = oracle.identity
        result['sessions_before_login'] = sessions(); result['audit_before_login'] = rows('management_audit')
        client = Process(['docker','exec','-i',state['container_id'],'python3','-B',
            '/out/build/source/experiments/export-execution/job_pages_cli_client.py'],input_pipe=True)
        exchange('initial','start')
        for username in ('alice','bob'):
            response = exchange(username+'-login','login',username=username)
            assert response['exit'] == 0 and not response['stderr'] and response['credential_file_present']
            assert response['observation'] == dict(format='bigbrotr-cli-login/1',status='session-persisted',authority='observation-only')
        session_baseline = result['sessions_after_login'] = sessions()
        audit_baseline = result['audit_after_login'] = rows('management_audit')
        issued = [s for s in session_baseline if s not in result['sessions_before_login']]
        added = [r for r in audit_baseline if r not in result['audit_before_login']]
        assert len(issued) == len(added) == 2
        assert all(s in session_baseline for s in result['sessions_before_login'])
        assert all(r in audit_baseline for r in result['audit_before_login'])
        saved_ids = {}
        for username in ('alice','bob'):
            principal = query(f"SELECT encode(principal_id,'hex') FROM control.local_accounts WHERE username='{username}'")
            s = next(v for v in issued if v['actor'] == principal)
            window = result['cases'][username+'-login']
            assert window['before_us'] <= s['issued'] <= window['after_us']
            assert s['last_seen'] == s['issued'] and s['revoked'] is None
            stored = json.loads((private/'cli-sessions'/(username+'.private')).read_text())
            digests = json.loads(query(f"SELECT json_build_object('session',encode(token_digest,'hex'),'csrf',encode(csrf_digest,'hex')) FROM control.browser_sessions WHERE session_id=decode('{s['id']}','hex')"))
            assert digests == {key:hashlib.sha256(domain+bytes.fromhex(stored[key])).hexdigest()
                for key,domain in [('session',b'bigbrotr/browser-session/1\0'),('csrf',b'bigbrotr/browser-csrf/1\0')]}
            saved_ids[username] = s['id']
            result.setdefault('credential_bindings',{})[username] = dict(session_id=s['id'],actor=principal,token_and_csrf_match=True)
            receipt = next(r for r in added if r['row']['subject_id'] == '\\x'+s['id'])
            assert receipt['row']['action'] == 'browser.session.issue' and receipt['row']['actor_id'] == '\\x'+s['actor']
            assert receipt['row']['outcome'] == 'accepted' and receipt['xmin'] == s['xmin']
            assert receipt['row']['config_revision'] == 1 and receipt['row']['policy_revision'] == 3
            assert window['before_us'] <= receipt['row']['created_at_us'] <= window['after_us']
        page('first',expected[:2],True)
        page('second',expected[2:4],True,**{'continue':'first'})
        page('last',expected[4:],False,**{'continue':'second'})
        page('repeat-second',expected[2:4],True,**{'continue':'first'})
        original = result['cases']['first']['decoded']
        for name in ('second','repeat-second'):
            decoded = result['cases'][name]['decoded']
            assert (decoded['issued'],decoded['expires']) == (original['issued'],original['expires'])
        page('empty-advancing',[],True,state='succeeded')
        page('empty-next',[],True,state='succeeded',**{'continue':'empty-advancing'})
        page('matching-last',expected[5:],False,state='succeeded',**{'continue':'empty-next'})
        page('bob-empty',[],False,username='bob')
        refusal('foreign-token','cursor binding changed; explicitly start a new listing',username='bob',**{'continue':'first'})
        page('time-lower',expected[:2],True,**{'created-from-us':expected[3]['created_at_us']})
        lower = max(int(v['created_at_us']) for v in expected)+1
        assert all(int(v['created_at_us']) < lower for v in expected)
        page('time-lower-empty',[],False,**{'created-from-us':str(lower)})
        page('time-upper',expected[2:4],True,**{'created-before-us':expected[1]['created_at_us']})
        boundary('reads')
        exchange('before-restart','pause')
        assert query("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()") == '0'
        restart = exchange('after-restart','start')
        assert restart['owner']['owner_epoch'] != result['cases']['initial']['response']['owner']['owner_epoch']
        page('restart-second',expected[2:4],True,**{'continue':'first'})
        boundary('restart')
        grant(False)
        refusal('grant-denied','job visibility denied',**{'continue':'first'})
        boundary('denied')
        grant(True)
        refusal('grant-binding-changed','cursor binding changed; explicitly start a new listing',**{'continue':'first'})
        page('fresh-restored',expected[:2],True)
        boundary('restored')
        response = exchange('logout','logout',username='alice')
        expected_logout = dict(format='bigbrotr-cli-logout/1',status='revocation-acknowledged',local_file='retained',drain='unobserved')
        assert response['exit'] == 0 and not response['stderr'] and response['observation'] == expected_logout
        assert response['credential_file_unchanged']
        revoked = result['sessions_after_logout'] = sessions()
        changed = [s for s in revoked if s not in session_baseline]
        assert len(changed) == 1
        current = changed[0]; previous = next(s for s in session_baseline if s['id'] == current['id'])
        assert current['id'] == saved_ids['alice']
        assert previous in issued and previous['actor'] == actor and previous['revoked'] is None
        assert current == {**previous,'revoked':current['revoked'],'xmin':current['xmin']}
        assert current['xmin'] != previous['xmin']
        assert result['cases']['logout']['before_us'] <= current['revoked'] <= result['cases']['logout']['after_us']
        assert [s for s in revoked if s != current] == [s for s in session_baseline if s != previous]
        after_audit = result['audit_after_logout'] = rows('management_audit')
        added = [r for r in after_audit if r not in audit_baseline]
        assert len(added) == 1 and all(r in after_audit for r in audit_baseline)
        assert added[0]['row']['action'] == 'browser.session.revoke' and added[0]['row']['subject_id'] == '\\x'+current['id']
        assert added[0]['row']['actor_id'] == '\\x'+actor and added[0]['xmin'] == current['xmin']
        session_baseline, audit_baseline = revoked, after_audit
        response = exchange('logout-retry','logout',username='alice')
        assert response['exit'] == 0 and not response['stderr'] and response['observation'] == expected_logout
        assert response['credential_file_unchanged']
        refusal('revoked-list','session denied or expired; sign in explicitly')
        page('bob-still-empty',[],False,username='bob')
        boundary('logout-retry')
        exchange('terminal','stop')
        assert client.wait(10) == 0 and not client.stderr and not client.overflow
        assert query("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'") == '0'
        result['backends'] = {d:int(sql(d,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for d in (db,'bbpub_normal')}
        assert not any(result['backends'].values())
        boundary('terminal')
        result['status'] = 'PASS'
    except BaseException as error:
        result['failure'] = type(error).__name__; raise
    finally:
        if client:
            try: client.stop()
            except BaseException as error:
                result['cleanup_error'] = type(error).__name__; result['status'] = 'INCOMPLETE'
            result['client_output'] = client.stdout.decode(); result['client_error'] = client.stderr.decode()
        save()
    return result
