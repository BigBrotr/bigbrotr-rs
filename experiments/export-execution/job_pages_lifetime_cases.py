#!/usr/bin/env python3
"""Actual page expiry and current authority, with independent clock/envelope oracles."""
import base64
import ctypes
import hashlib
import json
import secrets
import struct
import time
from job_pages_cases import EnvelopeOracle
from signed_publication_cases import Process, event, send


def seal_fixture(oracle, fields):
    """Encode the documented payload independently; use libsodium 1.0.18's ABI."""
    payload = struct.pack('>BB16sQQBBqBqHQq16sqq', 1, 1,
        bytes.fromhex(fields['actor']), fields['policy'], fields['grants'], fields['state'],
        fields['from_present'], fields['created_from'], fields['before_present'],
        fields['created_before'], fields['page_size'], fields['cut'], fields['last_time'],
        bytes.fromhex(fields['last_job']), fields['issued'], fields['expires'])
    assert len(payload) == 103
    key_id = bytes.fromhex(fields['key'])
    keys = {oracle.wire[i:i+16]:oracle.wire[i+16:i+48] for i in range(42,len(oracle.wire)-32,48)}
    header = b'\x01'+key_id
    nonce = secrets.token_bytes(24)
    resource = b'management.jobs/1'
    aad = b'BigBrotr cursor\0'+oracle.wire[8:40]+header+struct.pack('>H',len(resource))+resource
    # Official pinned declaration is recorded by EnvelopeOracle.identity.
    encrypt = oracle.lib.crypto_aead_xchacha20poly1305_ietf_encrypt
    encrypt.argtypes = [ctypes.c_void_p,ctypes.POINTER(ctypes.c_ulonglong),
        ctypes.c_void_p,ctypes.c_ulonglong,ctypes.c_void_p,ctypes.c_ulonglong,
        ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p]
    output = ctypes.create_string_buffer(len(payload)+16)
    size = ctypes.c_ulonglong()
    assert encrypt(output,ctypes.byref(size),payload,len(payload),aad,len(aad),
        None,nonce,keys[key_id]) == 0
    assert size.value == len(output)
    token = base64.urlsafe_b64encode(header+nonce+output.raw).decode().rstrip('=')
    assert oracle.open(token) == fields
    return token


def run(root, state, sql):
    db = 'bbpub_control_normal'
    private = root/'out/private-grant-http'
    result = dict(status='INCOMPLETE',source=state['source'],cases={},boundaries={})
    path = root/'job-pages-lifetime-cases.json'; assert not path.exists()
    client = None
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def query(text): return sql(db,text)
    def rows(table):
        return json.loads(query(f"SELECT coalesce(jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY to_jsonb(t)::text),'[]') FROM control.{table} t"))
    def ledger():
        return {table:rows(table) for table in ['jobs','job_admissions','job_input_bounds',
            'export_jobs','export_execution_units','artifacts','artifact_files',
            'snapshot_leases','archive_snapshot_pins','retention_claims']}
    def sessions():
        return json.loads(query("SELECT jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'actor',encode(principal_id,'hex'),'issued',issued_at_us,'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id) FROM control.browser_sessions"))
    def now(): return int(query('SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint'))
    def wait_until(target):
        deadline = time.monotonic()+20
        while now() < target:
            assert time.monotonic() < deadline, 'SQL clock did not reach bounded target'
            time.sleep(.02)
    def exchange(name, operation='page', **fields):
        assert name not in result['cases']
        before = now()
        send(client,json.dumps(dict(request=name,operation=operation,**fields))+'\n')
        response = event(client,name,timeout=10)
        assert response['operation'] == operation
        result['cases'][name] = dict(before_us=before,after_us=now(),response=response)
        save(); return response
    def opened(name):
        token = json.loads((private/'job-pages-lifetime-tokens.private').read_text())[name]
        fields = oracle.open(token)
        result['cases'][name]['decoded'] = fields; save(); return fields
    def page(name, selected, **fields):
        authority = json.loads(query(f"SELECT json_build_object('policy',policy_revision,'grants',(SELECT grant_revision FROM control.local_accounts WHERE principal_id=decode('{actor}','hex'))) FROM control.datasets"))
        response = exchange(name,username='alice',**fields)
        assert response['status'] == 200, response
        body = response['observation']
        assert body['format'] == 'bigbrotr-job-list/1' and body['effective_page_size'] == 2
        assert body['authority'] == 'observation-only' and body['has_next']
        assert [{k:v for k,v in row.items() if k != 'observed_at_us'} for row in body['items']] == selected
        assert all(row['observed_at_us'] == body['observed_at_us'] for row in body['items'])
        assert result['cases'][name]['before_us'] <= int(body['observed_at_us']) <= result['cases'][name]['after_us']
        decoded = opened(name)
        assert {k:v for k,v in decoded.items() if k not in ('issued','expires')} == dict(
            actor=actor,policy=authority['policy'],grants=authority['grants'],state=0,
            from_present=0,created_from=0,before_present=0,created_before=0,page_size=2,
            cut=6,last_time=int(selected[-1]['created_at_us']),last_job=selected[-1]['job_id'],
            key=oracle.wire[42:58].hex())
        return decoded
    def refusal(name, status, code, **fields):
        response = exchange(name,username='alice',**fields)
        assert response['status'] == status and response['observation']['error']['code'] == code, response
    def boundary(name):
        value = dict(ledger=ledger(),sessions=sessions(),retention=rows('cursor_key_lifecycle'))
        result['boundaries'][name] = value; save()
        assert value['ledger'] == baseline and value['sessions'] == session_baseline
        return value
    def grants(name, action, enabled):
        before = dict(accounts=rows('local_accounts'),grants=rows('principal_grants'))
        # Privileged, isolated dataset-grant fixture, not a production editor.
        assert action in ('data.read','status.read')
        if enabled:
            changes = f"INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'{action}','dataset',dataset_id FROM control.local_accounts WHERE principal_id=decode('{actor}','hex');"
        else:
            changes = f"DELETE FROM control.principal_grants WHERE principal_id=decode('{actor}','hex') AND action='{action}';"
        query(f"""BEGIN;
            SELECT 1 FROM control.datasets FOR UPDATE;
            SELECT 1 FROM control.local_accounts WHERE principal_id=decode('{actor}','hex') FOR UPDATE;
            {changes}
            UPDATE control.local_accounts SET grant_revision=grant_revision+1
                WHERE principal_id=decode('{actor}','hex'); COMMIT;""")
        after = dict(accounts=rows('local_accounts'),grants=rows('principal_grants'))
        result.setdefault('grant_changes',{})[name] = dict(before=before,after=after)
        old = next(v for v in before['accounts'] if v['row']['username'] == 'alice')
        new = next(v for v in after['accounts'] if v['row']['username'] == 'alice')
        assert new['row'] == {**old['row'],'grant_revision':old['row']['grant_revision']+1}
        assert [v for v in after['accounts'] if v != new] == [v for v in before['accounts'] if v != old]
        smaller,larger = (before['grants'],after['grants']) if enabled else (after['grants'],before['grants'])
        assert len(larger) == len(smaller)+1 and all(v in larger for v in smaller)
        changed = next(v for v in larger if v not in smaller)['row']
        assert changed == dict(dataset_id=old['row']['dataset_id'],principal_id=old['row']['principal_id'],
            action=action,resource_kind='dataset',resource_id=old['row']['dataset_id'])
        save()
    try:
        baseline = result['before'] = ledger()
        jobs = sorted([v['row'] for v in baseline['jobs']],key=lambda v:(v['created_at_us'],v['job_id']),reverse=True)
        assert len(jobs) == 6 and [v['state'] for v in jobs] == ['accepted']*5+['succeeded']
        actor = jobs[0]['principal_id'][2:]
        expected = [dict(format='bigbrotr-job-status/1',kind='export',job_id=v['job_id'][2:],
            state=v['state'],job_revision=str(v['job_revision']),checkpoint_revision=str(v['checkpoint_revision']),
            created_at_us=str(v['created_at_us']),completed_at_us=str(v['completed_at_us']) if v['completed_at_us'] is not None else None,
            reason_code=None,authority='observation-only') for v in jobs]
        oracle = EnvelopeOracle((private/'job-page-keys/ring').read_bytes())
        result['oracle'] = oracle.identity
        client = Process(['docker','exec','-i',state['container_id'],'python3','-B',
            '/out/build/source/experiments/export-execution/job_pages_client.py','--lifetime'],input_pipe=True)
        exchange('initial','start'); exchange('login','login',username='alice')
        session_baseline = result['sessions_before'] = sessions()
        original = page('first',expected[:2])
        assert original['policy'] == 3 and original['actor'] == actor
        assert original['expires']-original['issued'] == 12_000_000
        assert result['cases']['first']['before_us'] <= original['issued'] <= result['cases']['first']['after_us']
        retained = boundary('first')['retention']
        assert len(retained) == 1 and retained[0]['row']['decrypt_until_us'] == original['expires']+30_000_000

        future = {**original,'issued':now()+1_500_000}
        result['within_skew_fixture'] = future; save()
        within = page('future-within-skew',expected[2:4],fixture_cursor=seal_fixture(oracle,future))
        assert within['issued'] == future['issued'] and within['expires'] == original['expires']
        assert int(result['cases']['future-within-skew']['response']['observation']['observed_at_us']) < future['issued']
        future = {**original,'issued':now()+5_000_000}
        assert future['issued'] < original['expires']
        result['outside_skew_fixture'] = future; save()
        refusal('future-outside-skew',400,'invalid_cursor',fixture_cursor=seal_fixture(oracle,future))
        assert result['cases']['future-outside-skew']['after_us']+2_000_000 < future['issued']
        continued = page('continued',expected[2:4],**{'continue':'first'})
        assert (continued['issued'],continued['expires']) == (original['issued'],original['expires'])
        wait_until(original['expires']+200_000)
        grace = page('expiry-skew-grace',expected[2:4],**{'continue':'first'})
        assert (grace['issued'],grace['expires']) == (original['issued'],original['expires'])
        assert original['expires'] < result['cases']['expiry-skew-grace']['before_us']
        assert result['cases']['expiry-skew-grace']['after_us'] < original['expires']+2_000_000
        wait_until(original['expires']+2_100_000)
        refusal('expired-first',410,'cursor_expired',**{'continue':'first'})
        refusal('expired-continuation',410,'cursor_expired',**{'continue':'expiry-skew-grace'})
        assert boundary('natural-expiry')['retention'] == retained
        fresh = page('fresh-after-expiry',expected[:2])
        assert fresh['issued'] > original['expires'] and fresh['expires']-fresh['issued'] == 12_000_000
        long_retention = boundary('fresh-before-policy')['retention']
        assert long_retention[0]['row'] == {**retained[0]['row'],'decrypt_until_us':fresh['expires']+30_000_000}
        assert long_retention[0]['xmin'] != retained[0]['xmin']

        exchange('before-policy','pause')
        prior = json.loads(query('SELECT document FROM control.policy_revisions WHERE revision=3'))
        policy = json.loads(json.dumps(prior))
        policy['settings'].update({'read.cursor.ttl':5_000_000,'read.cursor.clock_skew':0})
        document = json.dumps(policy,sort_keys=True,separators=(',',':'))
        digest = hashlib.sha256(b'bigbrotr/product-policy/1\0'+document.encode()).hexdigest()
        query(f"""BEGIN;
            INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,
                schema_version,document,document_digest,accepted_at_us,actor_id)
            SELECT 4,dataset_id,3,config_revision,schema_version,$policy${document}$policy$::jsonb,
                decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=3;
            UPDATE control.datasets SET policy_revision=4; COMMIT;""")
        result['policy_change'] = dict(before=prior,after=json.loads(query('SELECT document FROM control.policy_revisions WHERE revision=4')),digest=digest)
        assert result['policy_change']['after'] == policy
        exchange('after-policy','start',policy_revision=4)
        refusal('old-policy',409,'cursor_incompatible',**{'continue':'fresh-after-expiry'})
        assert result['cases']['old-policy']['after_us'] < fresh['expires']
        short = page('short-policy',expected[:2])
        assert short['policy'] == 4 and short['expires']-short['issued'] == 5_000_000
        assert short['expires'] < fresh['expires']
        wait_until(short['expires']+100_000)
        refusal('zero-skew-expired',410,'cursor_expired',**{'continue':'short-policy'})
        assert result['cases']['zero-skew-expired']['after_us'] < short['expires']+2_000_000
        assert boundary('policy-expiry')['retention'] == long_retention

        before_grants = page('before-grant-change',expected[:2])
        grants('remove-data-read','data.read',False)
        refusal('changed-grant-binding',409,'cursor_incompatible',**{'continue':'before-grant-change'})
        assert result['cases']['changed-grant-binding']['after_us'] < before_grants['expires']
        without_data = page('status-without-data-read',expected[:2])
        grants('remove-status-read','status.read',False)
        refusal('revoked-valid-token',403,'permission_denied',**{'continue':'status-without-data-read'})
        assert result['cases']['revoked-valid-token']['after_us'] < without_data['expires']
        refusal('revoked-malformed-token',403,'permission_denied',invalid='abc')
        boundary('grant-revoked')
        grants('restore-status-read','status.read',True)
        refusal('restored-stale-binding',409,'cursor_incompatible',**{'continue':'status-without-data-read'})
        assert result['cases']['restored-stale-binding']['after_us'] < without_data['expires']
        restored = page('restored-fresh-list',expected[:2])
        boundary('grant-restored')
        result['account_before_deactivate'] = rows('local_accounts')
        before_deactivate = now()
        query(f"BEGIN; SELECT 1 FROM control.datasets FOR UPDATE; UPDATE control.local_accounts SET active=false WHERE principal_id=decode('{actor}','hex'); COMMIT;")
        after_deactivate = now()
        result['account_after_deactivate'] = rows('local_accounts')
        result['deactivation_clock_us'] = [before_deactivate,after_deactivate]
        old = next(v for v in result['account_before_deactivate'] if v['row']['username'] == 'alice')
        new = next(v for v in result['account_after_deactivate'] if v['row']['username'] == 'alice')
        assert new['row'] == {**old['row'],'active':False,'authentication_revision':old['row']['authentication_revision']+1}
        assert [v for v in result['account_before_deactivate'] if v != old] == [v for v in result['account_after_deactivate'] if v != new]
        revoked_sessions = sessions()
        assert len(revoked_sessions) == len(session_baseline)
        changed_sessions = 0
        for previous, current in zip(session_baseline,revoked_sessions):
            if previous['actor'] == actor and previous['revoked'] is None:
                assert max(previous['issued'],before_deactivate) <= current['revoked'] <= after_deactivate
                assert current == {**previous,'revoked':current['revoked'],'xmin':new['xmin']}
                assert current['xmin'] != previous['xmin']
                changed_sessions += 1
            else: assert current == previous
        assert changed_sessions > 0
        result['sessions_after_deactivate'] = revoked_sessions; save()
        session_baseline = revoked_sessions
        refusal('inactive-valid-token',401,'authentication_denied',**{'continue':'restored-fresh-list'})
        assert result['cases']['inactive-valid-token']['after_us'] < restored['expires']
        refusal('inactive-malformed-token',401,'authentication_denied',invalid='abc')
        boundary('account-inactive')
        exchange('terminal','stop')
        assert client.wait(10) == 0 and not client.stderr and not client.overflow
        result['after'] = ledger(); assert result['after'] == baseline
        result['backends'] = {database:int(sql(database,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")) for database in [db,'bbpub_normal']}
        assert not any(result['backends'].values())
        assert query("SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'") == '0'
        result['status'] = 'PASS'; return result
    except BaseException as error:
        result['failure'] = type(error).__name__; raise
    finally:
        if client:
            try: client.stop()
            except BaseException as error:
                result['cleanup_error'] = type(error).__name__; result['status'] = 'INCOMPLETE'
            result['client_output'] = client.stdout.decode()
            result['client_error'] = client.stderr.decode()
        save()
