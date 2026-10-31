#!/usr/bin/env python3
"""Independent actor/page/retention oracle over real signed export acceptance."""
import base64
import copy
import ctypes
import hashlib
import json
from pathlib import Path
import struct
import time
from signed_publication_cases import Process, command, event, send, sha
from signed_publication_oracle import EVENT


class EnvelopeOracle:
    """Independent libsodium decryption; no Rust codec/query/serializer reuse."""
    def __init__(self, wire):
        self.lib = ctypes.CDLL('libsodium.so.23')
        self.lib.sodium_version_string.restype = ctypes.c_char_p
        self.lib.crypto_aead_xchacha20poly1305_ietf_decrypt.argtypes = [
            ctypes.c_void_p,ctypes.POINTER(ctypes.c_ulonglong),ctypes.c_void_p,
            ctypes.c_void_p,ctypes.c_ulonglong,ctypes.c_void_p,ctypes.c_ulonglong,
            ctypes.c_void_p,ctypes.c_void_p]
        assert self.lib.sodium_init() >= 0
        self.wire = wire
        libraries = {line.split()[-1] for line in Path('/proc/self/maps').read_text().splitlines() if '/libsodium.so' in line}
        assert len(libraries) == 1
        library = Path(libraries.pop()).resolve(strict=True)
        self.identity = dict(version=self.lib.sodium_version_string().decode(),path=str(library),sha256=sha(library))
        assert self.identity['version']=='1.0.18'
        self.identity['api_source']='https://github.com/jedisct1/libsodium/blob/1.0.18/src/libsodium/include/sodium/crypto_aead_xchacha20poly1305.h'

    def open(self, token):
        raw = base64.urlsafe_b64decode(token+'='*((-len(token))%4))
        assert len(raw) == 160 and raw[0] == 1
        resource = b'management.jobs/1'
        aad = b'BigBrotr cursor\0'+self.wire[8:40]+raw[:17]+struct.pack('>H',len(resource))+resource
        keys = {self.wire[i:i+16]:self.wire[i+16:i+48] for i in range(42,len(self.wire)-32,48)}
        out = ctypes.create_string_buffer(103); size = ctypes.c_ulonglong()
        assert self.lib.crypto_aead_xchacha20poly1305_ietf_decrypt(
            out,ctypes.byref(size),None,raw[41:],len(raw)-41,aad,len(aad),raw[17:41],keys[raw[1:17]]) == 0
        assert size.value == 103
        data = out.raw
        fields = struct.unpack('>BB16sQQBBqBqH Qq16sqq',data)
        assert fields[0:2] == (1,1)
        return dict(actor=fields[2].hex(),policy=fields[3],grants=fields[4],state=fields[5],
            from_present=fields[6],created_from=fields[7],before_present=fields[8],created_before=fields[9],
            page_size=fields[10],cut=fields[11],last_time=fields[12],last_job=fields[13].hex(),
            issued=fields[14],expires=fields[15],key=raw[1:17].hex())


def run(root,state,sql,policy, *, prepare_consumer=False, cursor_lifetime=None):
    db='bbpub_control_normal'; runner=state['container_id']; source=root/'source'
    private=root/'out/private-grant-http'; config=private/'instance.toml'
    result=dict(status='INCOMPLETE',source=state['source'],cases={})
    path=root/'job-pages-cases.json'; assert not path.exists()
    client=None
    def save(): path.write_text(json.dumps(result,indent=2)+'\n')
    def cli(*args):
        text=command('docker','exec',runner,'/out/bigbrotr',*args,'--file','/out/private-grant-http/instance.toml')
        return json.loads(text)
    def exchange(name,op,**fields):
        assert name not in result['cases']
        send(client,json.dumps(dict(request=name,operation=op,**fields))+'\n')
        value=event(client,name,timeout=10)
        assert value['operation']==op
        result['cases'][name]=value;save();return value
    def contract_refusal(name,status,code,**fields):
        response=exchange(name,'page',**fields)
        result.setdefault('regressions',{})[name]=dict(expected_status=status,expected_code=code,
            passed=response['status']==status and response['observation'].get('error',{}).get('code')==code)
        save()
    def sessions():
        return json.loads(sql(db,"SELECT coalesce(jsonb_agg(jsonb_build_object('id',encode(session_id,'hex'),'principal',encode(principal_id,'hex'),'last_seen',last_seen_at_us,'idle_expires',idle_expires_at_us,'revoked',revoked_at_us,'xmin',xmin::text) ORDER BY session_id),'[]') FROM control.browser_sessions"))
    def retained():
        return json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_object('key',encode(key_id,'hex'),'first',first_activation_revision::text,'revoked',revoked_revision::text,'until',decrypt_until_us::text,'xmin',xmin::text) ORDER BY key_id) FROM control.cursor_key_lifecycle"))
    def job_rows():
        return json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_object('job',encode(j.job_id,'hex'),'actor',encode(j.principal_id,'hex'),'state',j.state,'time',j.created_at_us::text,'revision',j.job_revision::text,'checkpoint',j.checkpoint_revision::text,'ordinal',a.admission_ordinal::text) ORDER BY a.admission_ordinal) FROM control.jobs j JOIN control.job_admissions a USING(job_id)"))
    def opened(name):
        token=json.loads((private/'job-page-tokens.json').read_text())[name]
        value=oracle.open(token)
        result.setdefault('decoded',{})[name]=value;save();return value
    def page(name,expected,more,**fields):
        response=exchange(name,'page',**fields)
        assert response['status']==200,response
        value=response['observation']
        assert value['format']=='bigbrotr-job-list/1' and value['authority']=='observation-only'
        assert value['effective_page_size']==2 and value['has_next']==more
        assert [r['job_id'] for r in value['items']]==expected
        for item in value['items']:
            expected_row=known[item['job_id']]
            assert item==dict(format='bigbrotr-job-status/1',kind='export',job_id=expected_row['job'],
                state=expected_row['state'],job_revision=expected_row['revision'],checkpoint_revision=expected_row['checkpoint'],
                created_at_us=expected_row['time'],completed_at_us=completed.get(expected_row['job']),
                observed_at_us=value['observed_at_us'],reason_code=None,authority='observation-only')
        return value
    def refusal(name,status,code,**fields):
        response=exchange(name,'page',**fields)
        assert response['status']==status and response['observation']['error']['code']==code,response
    try:
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state='active'")=='0'
        # Upgrade the actually executed old job with the product migrator. Preserve
        # original rows/xmin, not merely migration exit status.
        before={table:json.loads(sql(db,f"SELECT jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY to_jsonb(t)::text) FROM control.{table} t")) for table in ['jobs','job_admissions','export_jobs','artifacts','artifact_files']}
        result['upgrade']=cli('migrate','apply','--target','816','--user','postgres','--password-ref','file:/out/private-grant-http/postgres')
        after={table:json.loads(sql(db,f"SELECT jsonb_agg(jsonb_build_object('row',to_jsonb(t),'xmin',xmin::text) ORDER BY to_jsonb(t)::text) FROM control.{table} t")) for table in before}
        result['upgrade_original_rows']=dict(before=before,after=after);save();assert before==after
        entries=json.loads((source/'migrations/manifest.json').read_text())['application_order']
        actual=json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_array(migration,encode(checksum,'hex')) ORDER BY migration) FROM control.schema_versions"))
        assert actual==sorted([[e['version'],sha(source/'migrations/postgres'/e['name'])] for e in entries])
        result['migration_receipts']=actual
        sql(db,'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control; GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
        selected=copy.deepcopy(policy);selected['settings'].update({'administration.enabled':True,'export.memory.max_bytes':268435456})
        if cursor_lifetime is not None:
            assert prepare_consumer
            ttl,skew = cursor_lifetime
            assert ttl > 0 and 0 <= skew <= 30_000_000
            selected['settings'].update({'read.cursor.ttl':ttl,'read.cursor.clock_skew':skew})
            result['cursor_lifetime'] = dict(ttl_us=ttl,skew_us=skew)
        document=json.dumps(selected,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(b'bigbrotr/product-policy/3\0'+document.encode()).hexdigest()
        sql(db,f"BEGIN; INSERT INTO control.policy_revisions(revision,dataset_id,previous_revision,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 3,dataset_id,2,config_revision,schema_version,$policy${document}$policy$::jsonb,decode('{digest}','hex'),accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=2; UPDATE control.datasets SET policy_revision=3; COMMIT;")
        # A separate status-only fixture account shares independently hashed
        # credentials. This seeds identity, not an account-management claim.
        bob='b7'*16
        sql(db,f"BEGIN; INSERT INTO control.principals SELECT decode('{bob}','hex'),dataset_id,'local',created_at_us,database_role,local_os_uid FROM control.principals WHERE kind='installation'; INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision) SELECT decode('{bob}','hex'),dataset_id,'bob',false,true,credential_revision,1 FROM control.local_accounts WHERE username='alice'; INSERT INTO control.local_credentials SELECT decode('{bob}','hex'),dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash FROM control.local_credentials WHERE principal_id=(SELECT principal_id FROM control.local_accounts WHERE username='alice'); INSERT INTO control.principal_grants SELECT dataset_id,decode('{bob}','hex'),'status.read','dataset',dataset_id FROM control.datasets; COMMIT;")
        fixture=json.loads((source/'tests/fixtures/read/keyring-vectors.json').read_text())
        wire=bytearray.fromhex(fixture['cases'][0]['hex']);wire[8:24]=bytes.fromhex('01'*16)
        wire[-32:]=hashlib.sha256(wire[:-32]).digest();wire=bytes(wire)
        keydir=private/'job-page-keys';keydir.mkdir(mode=0o700)
        keyfile=keydir/'ring';keyfile.write_bytes(wire);keyfile.chmod(0o600)
        # Renew at500ms so the half-remaining SQL admission budget exceeds
        # the1.25s session wait in either policy-refresh phase. Do not extend
        # authorization TTLs or modify the production deadline calculation.
        config.write_text(config.read_text()+'\n[runtime]\nheartbeat_interval="500ms"\n'
            +'\n[read.cursor]\nkeyring_ref="file:/out/private-grant-http/job-page-keys/ring"\n')
        result['registration']=cli('access','cursor-keyring','register')
        a,b=wire[42:58].hex(),wire[90:106].hex()
        oracle=EnvelopeOracle(wire);result['oracle']=oracle.identity;save()
        # Preserve the seed's actual four attempt charges; do not erase them.
        started=time.monotonic();time.sleep(60);result['rate_wait_seconds']=time.monotonic()-started
        client=Process(['docker','exec','-i',runner,'python3','-B','/out/build/source/experiments/export-execution/job_pages_client.py'],input_pipe=True)
        exchange('initial','start')
        exchange('alice-login','login',username='alice');exchange('bob-login','login',username='bob')
        contract_refusal('never-active-empty',503,'authority_unavailable',username='bob')
        contract_refusal('never-active-terminal',503,'authority_unavailable',username='alice')
        exchange('before-initial-activation','pause')
        result['activate_a']=cli('access','cursor-keyring','activate-key','--key-id',a,'--request-id','a1'*16,'--expected-revision','0')
        exchange('after-initial-activation','start')
        seed=job_rows()[0]; accepted=[]
        for index in range(5):
            value=exchange('accept-'+str(index),'accept',username='alice',key=format(100+index,'032x'))
            assert value['submitted']['query']['filters']==[dict(ids=[EVENT['id']])]
            accepted.append(value['receipt']['job_id'])
        rows=job_rows();result['accepted_jobs']=rows;save()
        assert len(rows)==6 and [r['job'] for r in rows]==[seed['job'],*accepted]
        assert [r['ordinal'] for r in rows]==list(map(str,range(1,7)))
        assert all(r['state']=='accepted' for r in rows[1:]) and rows[0]['state']=='succeeded'
        known={r['job']:r for r in rows}
        completed={r[0]:r[1] for r in json.loads(sql(db,"SELECT jsonb_agg(jsonb_build_array(encode(job_id,'hex'),completed_at_us::text)) FROM control.jobs WHERE completed_at_us IS NOT NULL"))}
        if prepare_consumer:
            # The new consumer starts from real HTTP acceptances. Keep this
            # preparation distinct from the separately qualified page regressions.
            result['consumer_expected']=[dict(format='bigbrotr-job-status/1',kind='export',
                job_id=row['job'],state=row['state'],job_revision=row['revision'],
                checkpoint_revision=row['checkpoint'],created_at_us=row['time'],
                completed_at_us=completed.get(row['job']),reason_code=None,
                authority='observation-only') for row in reversed(rows)]
            exchange('prepared-terminal','stop')
            assert client.wait(10)==0 and not client.stderr and not client.overflow
            result['terminal_jobs']=job_rows();assert result['terminal_jobs']==rows
            assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state<>'released'")=='0'
            assert sql(db,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")=='0'
            result['scope']='Consumer preparation only; no page regression qualification in this profile'
            result['status']='PASS';save();return result
        result['sessions_before_reads']=sessions();save()
        first=page('first',accepted[-1:-3:-1],True,username='alice')
        token=opened('first');assert token==dict(actor=seed['actor'],policy=3,grants=4,state=0,
            from_present=0,created_from=0,before_present=0,created_before=0,page_size=2,cut=6,
            last_time=int(known[accepted[-2]]['time']),last_job=accepted[-2],issued=token['issued'],expires=token['issued']+900_000_000,key=a)
        obligations=retained();result['first_retention']=obligations;save()
        assert obligations==[dict(key=a,first='1',revoked=None,until=str(token['expires']+30_000_000),xmin=obligations[0]['xmin'])]
        page('second',[accepted[2],accepted[1]],True,username='alice',**{'continue':'first'})
        continued=opened('second');assert (continued['issued'],continued['expires'],continued['cut'])==(token['issued'],token['expires'],6)
        page('last',[accepted[0],seed['job']],False,username='alice',**{'continue':'second'})
        page('repeat-second',[accepted[2],accepted[1]],True,username='alice',**{'continue':'first'})
        assert retained()==obligations
        page('empty-advancing',[],True,username='alice',query='page_size=2&state=succeeded')
        page('empty-next',[],True,username='alice',query='page_size=2&state=succeeded',**{'continue':'empty-advancing'})
        page('matching-last',[seed['job']],False,username='alice',query='page_size=2&state=succeeded',**{'continue':'empty-next'})
        page('bob-empty',[],False,username='bob')
        refusal('bob-foreign-token',409,'cursor_incompatible',username='bob',**{'continue':'first'})
        refusal('different-filter',409,'cursor_incompatible',username='alice',query='page_size=2&state=accepted',**{'continue':'first'})
        refusal('malformed-authenticated',400,'invalid_cursor',username='alice',invalid='abc')
        refusal('malformed-no-session',401,'authentication_denied',invalid='abc')
        result['sessions_after_reads']=sessions();save();assert result['sessions_after_reads']==result['sessions_before_reads']
        # A new accepted job lies outside the original cut. Reusing a cursor is
        # stable, while a fresh first page sees the actual newly admitted job.
        later=exchange('accept-later','accept',username='alice',key=format(200,'032x'))['receipt']['job_id']
        known.update({r['job']:r for r in job_rows()})
        page('old-cut-second',[accepted[2],accepted[1]],True,username='alice',**{'continue':'first'})
        page('new-first',[later,accepted[-1]],True,username='alice')
        assert opened('new-first')['cut']==7
        # Retention guards reject another SQL backend even while Control owns
        # the lease. Record its specific SQLSTATE and the durable no-write result.
        before=retained()
        result['foreign_sql']=sql(db,f"""DO $$ BEGIN
            UPDATE control.cursor_key_lifecycle SET decrypt_until_us=decrypt_until_us+1 WHERE key_id=decode('{a}','hex');
            RAISE EXCEPTION 'foreign retention was accepted';
            EXCEPTION WHEN SQLSTATE '23514' THEN
                IF SQLERRM<>'Control backend does not own commit exclusion' THEN RAISE; END IF;
                RAISE NOTICE 'foreign retention refused 23514'; END $$;
            SELECT 'foreign-retention-refused';""")
        assert retained()==before
        from job_pages_wait_cases import session_waits
        result['session_waits']=session_waits(root,state,sql,client,exchange,bob)
        for case in result['session_waits']:
            result.setdefault('regressions',{})[case['name']]=dict(passed=case['passed'])
        save()
        exchange('before-rotation','pause')
        result['activate_b']=cli('access','cursor-keyring','activate-key','--key-id',b,'--request-id','a2'*16,'--expected-revision','1')
        exchange('after-rotation','start')
        page('rotated-second',[accepted[2],accepted[1]],True,username='alice',**{'continue':'first'})
        rotated=opened('rotated-second');assert rotated['key']==b and (rotated['issued'],rotated['expires'])==(token['issued'],token['expires'])
        result['rotation_retention']=retained();save()
        assert next(v for v in result['rotation_retention'] if v['key']==a)==next(v for v in before if v['key']==a)
        assert next(v for v in result['rotation_retention'] if v['key']==b)['until']==str(token['expires']+30_000_000)
        exchange('before-revocation','pause')
        result['revoke_a']=cli('access','cursor-keyring','revoke-key','--key-id',a,'--request-id','a3'*16,'--expected-revision','2')
        exchange('after-revocation','start')
        refusal('revoked-cached-key',400,'invalid_cursor',username='alice',**{'continue':'first'})
        page('unrevoked-key',[accepted[0],seed['job']],False,username='alice',**{'continue':'rotated-second'})
        exchange('before-last-revocation','pause')
        result['revoke_b']=cli('access','cursor-keyring','revoke-key','--key-id',b,'--request-id','a4'*16,'--expected-revision','3')
        exchange('after-last-revocation','start')
        contract_refusal('last-revoked-empty',503,'authority_unavailable',username='bob')
        contract_refusal('last-revoked-terminal',503,'authority_unavailable',username='alice',query='page_size=200')
        exchange('terminal','stop');assert client.wait(10)==0 and not client.stderr and not client.overflow
        result['terminal_retention']=retained();result['terminal_jobs']=job_rows();save()
        assert result['terminal_jobs']==sorted(known.values(),key=lambda row:int(row['ordinal']))
        assert sql(db,"SELECT count(*) FROM control.owner_epochs WHERE role='control' AND state<>'released'")=='0'
        assert sql(db,"SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database()")=='0'
        assert all(case['passed'] for case in result['regressions'].values()),'page authority regressions'
        result['status']='PASS'
        result['not_run']=['held/lost retention COMMIT','expired token and session during socket delivery',
            'backdated/tied aged jobs and mixed load','raw retention mutation combinations and concurrent locks',
            'generated client/private panel/browser pages','complete M1/Compose release journey']
    except BaseException as error:
        result['failure']=type(error).__name__;raise
    finally:
        if client:
            client.stop()
            (root/'job-pages-client.stdout').write_bytes(client.stdout)
            (root/'job-pages-client.stderr').write_bytes(client.stderr)
            result['client_terminal']=dict(exit=client.process.poll(),overflow=client.overflow)
        save()
    return result
