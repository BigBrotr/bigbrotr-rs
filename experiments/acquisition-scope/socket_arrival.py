"""Independent literal expectations for a real three-frame signed acquisition.

Receive/record timestamps are unpredictable fixture parameters, constrained by
an independent host clock bracket. All fact shapes, keys and logical effects
come from the encoding contract and frozen signed input, never native output.
"""
import hashlib
import json
import math
import time
from observations import expected as bootstrap

ENDPOINT = 'ws://127.0.0.1:7447/private'


def expected(dataset, event, received, clock_resolution, clock_flags, recorded, cbor):
    rows, first_input, first_receipt = bootstrap(dataset, True, True, cbor, ENDPOINT)
    rows = {(cf, k): v for cf, k, v in rows}
    be = lambda n, width=8: n.to_bytes(width, 'big')
    sha = lambda value: hashlib.sha256(value).digest()
    domains = {1:70, 2:65, 3:71, 4:64, 5:72, 8:68}
    ref = lambda kind: [dataset, kind, bytes([domains[kind]])*16, 1]
    ref_key = lambda kind: dataset + be(kind,2) + bytes([domains[kind]])*16 + be(1)
    src = lambda n: [dataset, 0, 1, n]
    src_key = lambda n: dataset + be(0,4) + be(1) + be(n)
    scope, slot = ref(4), bytes([66])*16
    # These are refs from our independently constructed bootstrap, not a dump.
    refs = {int.from_bytes(k[-8:],'big'):[src(int.from_bytes(k[-8:],'big')),v[2:34]]
            for (cf,k),v in rows.items() if cf == 'facts' and k[:2] == b'\x01\x20'}

    def fact(n, kind, value):
        encoded = b'bigbrotr/fact/1\0' + cbor([src(n),kind,1,scope,recorded,value])
        digest = sha(encoded)
        refs[n] = [src(n),digest]
        rows['facts',b'\x01\x20'+src_key(n)] = b'\x01\x20'+digest+encoded

    def index(domain, key, n):
        prefix = bytes([1,domain])
        rows['default',prefix+key] = prefix+src_key(n)+refs[n][1]

    body = [bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],
            event['kind'],event['tags'],event['content']]
    signature = bytes.fromhex(event['sig'])
    body_digest = sha(b'bigbrotr/body/1\0'+cbor(body))
    assert body_digest.hex() == '1e05f420946c84c3ee8b178daf05dd564c9904c7ed18f7ab99874aec878337fb'
    descriptor = [1,'event',1,{'event_id':body[0]}]
    fact(7,'dictionary',[ref(8),*descriptor])
    index(4,ref_key(8),7)
    canonical = cbor(descriptor)
    rows['default',b'\x01\x05'+dataset+be(8,2)+sha(canonical)+ref_key(8)] = b'\x01\x05'+canonical
    rows['default',b'\x01\x03'+dataset+be(8,2)+bytes([68])*16] = b'\x01\x03'+be(1)
    fact(8,'event-payload',[*body,ref(8)])
    index(6,dataset+body[0],8)
    fact(9,'event-eligibility',[body[0],body_digest,signature,recorded,'network',None])
    index(7,dataset+body[0]+ref_key(4),9)
    clock = [ref(2),clock_resolution,None,clock_flags]
    fact(10,'event-receive',[body[0],received,ref(2),clock,None])
    index(8,dataset+body[0]+ref_key(4),10)
    fact(11,'provenance',[body[0],ref(1),ref(2),ref(3),received,clock,ref(5),None])
    index(9,dataset+body[0]+ref_key(1)+ref_key(2)+ref_key(3)+ref_key(4),11)
    original = b'bigbrotr/producer-request/1\0'+cbor([[slot,1,2],'admit',1,
        [1,scope,[*body,signature],'network',None,[received,ref(2),clock,[ref(1),ref(3),ref(5)]]]])
    batch = [refs[n] for n in range(7,12)]
    receipt = b'bigbrotr/receipt/1\0'+cbor([1,[slot,1,2],sha(original),
        [dataset,0,1,bytes([67])*16,1],batch,[refs[n] for n in [1,3,4,5,6,7,8,9,10,11]]])
    rows['default',b'\x01\x02'+dataset+be(0,4)+be(1)] = b'\x01\x02'+be(11)
    rows['facts',b'\x01\x21'+src_key(7)] = b'\x01\x21'+be(11)+sha(b'bigbrotr/admission-batch/1\0'+cbor([dataset,0,1,7,11,batch]))
    rows['receipts',b'\x01\x30'+slot+be(1)+be(2)] = receipt
    rows['receipts',b'\x01\x31'+slot+be(1)] = b'\x01\x31'+be(2)+be(0)+be(0)
    assert len(rows) == 42
    return sorted((cf,k,v) for (cf,k),v in rows.items()), [first_input,original], [first_receipt,receipt]


def run(context):
    lab, pg, client, output, source, rows, clone, report, save, require, oracle = (
        context[k] for k in ('lab','pg','client','output','SOURCE','rows','clone','report','save','require','oracle'))
    report['not_run'] = ['authority changes after receive, lost/late rollback and COMMIT recovery in this composed path',
        'production scheduler, verifier pool and durable EOSE/progress joins',
        'installed dictionary append, same-owner command delivery, restart/M1',
        'load/aged-state, production capacity, arm64 or release']
    database = clone('socket-pass','bb_source_template')
    config = lab.config(output,dict(pg,client=client),database,endpoint='127.0.0.1:5432',boundary='loopback')
    migrated = lab.cli(dict(pg,client=client),'migrate','apply','--file',config,
        '--user','bigbrotr_migrator','--password-ref','file:/run/secrets/migrator','--target','24')
    (output/'socket-migration.stdout').write_text(migrated.stdout)
    (output/'socket-migration.stderr').write_text(migrated.stderr)
    require(migrated.returncode == 0,'socket-current-schema')
    lab.sql(pg,database,'GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes,'
        'control.acquisition_jobs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control; '
        "INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'roles.control','dataset',dataset_id "
        "FROM control.local_accounts WHERE username='alice';")
    before = rows(database)
    require(len(before) == 67 and not before['owner_epochs'] and not before['producer_requests'],'fresh-socket-clone')
    lab.write_json(output/'socket-before.json',before)
    started = time.time_ns()//1000
    result = lab.docker('exec',client,'/build/ingest_bootstrap_probe','source-socket-pass',database,
        '/run/secrets/application','/lab/socket',check=False,timeout=60)
    finished = time.time_ns()//1000
    (output/'socket.stdout').write_text(result.stdout)
    (output/'socket.stderr').write_text(result.stderr)
    require(result.returncode == 0 and not result.stderr,'actual-socket-admission')
    messages = [json.loads(s) for s in result.stdout.splitlines()]
    captured = [m for m in messages if m['event'] == 'socket-captured']
    admitted = [m for m in messages if m['event'] == 'socket-admitted']
    require(len(captured) == 3 and len(admitted) == 1,'three-frames-one-admission')
    require([m['sequence'] for m in captured] == [1,2,3],'wire-order')
    resolution = math.ceil(time.clock_getres(time.CLOCK_REALTIME)*1_000_000)
    recorded = int(admitted[0]['recorded_at'])
    require(all(started <= int(m['received_at']) <= recorded <= finished for m in captured),'independent-realtime-bracket')
    after = rows(database)
    lab.write_json(output/'socket-after.json',after)
    dataset = bytes.fromhex(after['datasets'][0]['dataset_id'][2:])
    expected_clock = dict(allocation_domain='41'*16,dataset_id=dataset.hex(),dictionary_kind=2,ordinal='1')
    require(all(m['clock']['clock_id'] == expected_clock and int(m['clock']['resolution_us']) == resolution
        and m['clock']['uncertainty_us'] is None and m['clock']['anomaly_flags'] == (4 if resolution > 1 else 0)
        for m in captured),'actual-clock-identity-quality')
    artifact = (source/'tests/fixtures/ingest/import-event.jsonl').read_bytes()
    require(hashlib.sha256(artifact).hexdigest() == 'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae','signed-input-identity')
    wanted,inputs,receipts = expected(dataset,json.loads(artifact),int(captured[0]['received_at']),
        resolution,captured[0]['clock']['anomaly_flags'],recorded,oracle.cbor)
    dumped = lab.docker('exec',client,'/build/ingest_bootstrap_probe','dump','/lab/socket')
    (output/'socket-native.json').write_text(dumped.stdout)
    actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    require(actual == wanted,'complete-independent-native-state')
    originals = sorted((output/'socket/producer-inputs-v1').glob('*.cbor'))
    require(len(originals) == 2 and [p.read_bytes() for p in originals] == inputs,'original-producer-bytes')
    require(len(after['producer_requests']) == len(after['producer_outcomes']) == 2,'no-forged-or-duplicate-mutation')
    actual_receipts = sorted(after['producer_outcomes'],key=lambda r:int(r['request_sequence']))
    require([r['outcome_bytes'] for r in actual_receipts] == ['\\x'+r.hex() for r in receipts],'literal-pg-outcomes')
    require(admitted[0]['receipt'] == receipts[1].hex(),'delivered-exact-receipt')
    identity = next(m for m in messages if m['event'] == 'socket-before')
    admitted_request = next(r for r in after['producer_requests'] if int(r['request_sequence']) == 2)
    require(admitted_request['acquisition_job_id'] == '\\x'+identity['job']
        and admitted_request['acquisition_scope_id'] == '\\x'+identity['scope'],'original-job-scope-custody')
    require(messages[-1]['event'] == 'socket-finished' and messages[-1]['status'] == 'PASS','completed-consumer')
    require(lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0','unjoined-driver')
    report['cases'].append(dict(name='signed-forged-duplicate-private-socket',status='PASS',frames=3,native_rows=42,
        original_requests=2,admitted_events=1,clock_bracket_us=[started,finished],native_outcome='five new facts, no invalid witness or duplicate mutation'))
    save()
