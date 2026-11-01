"""Existing same-Control probe, current PG schema and independent refusal checks."""
import copy
import hashlib
import json
import time

CASES = ('pressure', 'missing-outcome', 'reserved-ahead', 'outcome-conflict')


def injection(before, case):
    """Exact privileged changes to one disposable clone, never product guards."""
    if case == 'pressure':
        return '', copy.deepcopy(before)
    expected = copy.deepcopy(before)
    if case == 'missing-outcome':
        assert len(expected['producer_outcomes']) == 1
        expected['producer_outcomes'] = []
        return 'DELETE FROM control.producer_outcomes', expected
    table = 'producer_epochs' if case == 'reserved-ahead' else 'producer_outcomes'
    row, = expected[table]
    row.pop('lab_xmin')
    if case == 'reserved-ahead':
        row['reserved_through_request'] = 2
        return 'UPDATE control.producer_epochs SET reserved_through_request=2', expected
    assert case == 'outcome-conflict'
    value = bytearray.fromhex(row['outcome_bytes'][2:])
    value[0] ^= 1
    digest = hashlib.sha256(value).hexdigest()
    row.update(outcome_bytes='\\x'+value.hex(), outcome_sha256='\\x'+digest)
    return ("UPDATE control.producer_outcomes SET outcome_bytes=decode('"+value.hex()+
            "','hex'),outcome_sha256=decode('"+digest+"','hex')"), expected


def verify_injection(before, expected, actual, case):
    comparable = copy.deepcopy(actual)
    if case in ('reserved-ahead', 'outcome-conflict'):
        table = 'producer_epochs' if case == 'reserved-ahead' else 'producer_outcomes'
        row, = comparable[table]
        assert int(row.pop('lab_xmin')) > int(before[table][0]['lab_xmin'])
    assert comparable == expected, 'unexpected clone-only injection effect'


def ordered(rows):
    return {name: sorted(values,key=lambda row: json.dumps(row,sort_keys=True))
            for name,values in rows.items()}


def tables(event):
    return ordered({name: json.loads(rows) for name, rows in event['state']['control_tables']})


def verify_events(events, case, baseline, wanted, inventory):
    def one(name, **fields):
        value, = [e for e in events if e['event'] == name
                  and all(e.get(k) == v for k,v in fields.items())]
        return value
    def external(rows):
        result = copy.deepcopy(rows)
        for values in result.values():
            for row in values:
                row['xmin'] = row.pop('lab_xmin')
        return ordered(result)
    before = one('quota-state', phase='before-proof')
    assert sorted(tables(before)) == inventory['tables']
    assert tables(before) == external(baseline), 'probe/external PG observation differs'
    expected_rows = [dict(cf=cf,key=key.hex(),value=value.hex()) for cf,key,value in wanted]
    assert before['rows'] == expected_rows, 'independent complete native seed'
    for phase in ('seed','reopened'):
        observed = one('quota-state', phase=phase)
        assert observed['rows'] == expected_rows
        for field in ('native_rows','native_sha256','sequence','originals'):
            assert observed['state'][field] == before['state'][field]
    phases = ['after-proof','after-next']
    if case == 'pressure': phases.insert(1,'after-known-retry')
    for phase in phases:
        observed = one('quota-state', phase=phase)
        assert observed['rows'] == before['rows'] and observed['state'] == before['state'], phase
    proof = one('quota-proof')
    expected_error = ('Ok(Pressure)' if case == 'pressure' else 'Err(Outcome(Conflict))'
                      if case == 'outcome-conflict' else 'Err(Control(InvalidMetadata))')
    assert proof['result'] == expected_error
    assert proof['mode'] == 'quota-reconcile-'+case
    assert proof['status'] == ('Pressure' if case == 'pressure' else 'RecoveryBlocked')
    refused = one('quota-next-refused')['error']
    if case == 'pressure': assert refused == 'Pressure'
    else: assert refused == 'Control(Unavailable)', refused
    complete = one('quota-complete')
    assert complete == events[-1] and complete['next_input'] is False
    assert complete['mode'] == 'quota-reconcile-'+case
    assert complete['status'] == ('Pressure' if case == 'pressure' else 'RecoveryBlocked')


def run(state, output, contents, record, prepare, call, inventory, source):
    import lab
    from fault_cases import Process
    from oracle import cbor, expected
    activation = ('--request-id','04'*16,'--expected-revision','1')
    for case in CASES:
        name = 'quota_'+case.replace('-','_')
        database, config, local = prepare(name, 'managed')
        call(name+'-initialize',database,'initialize',config)
        call(name+'-activate',database,'activate-initial',config,extra=activation)
        path = output/name
        path.mkdir(mode=0o700)
        store = path/'store'
        args = ['docker','exec',state['client'],'/build/ingest_bootstrap_probe',
                'quota-reconcile-'+case,database,'/lab/secrets/application','/lab/'+name+'/store']
        process = Process(args,limit=16*1024*1024)
        try:
            deadline = time.monotonic()+20
            while not store.with_suffix('.quota-ready').exists():
                assert process.process.poll() is None, 'probe ended before PG injection gate'
                assert time.monotonic()<deadline, 'quota readiness deadline'
                time.sleep(.02)
            before = contents(database)
            sql, planned = injection(before,case)
            if sql:
                lab.sql(state,database,'BEGIN; SET LOCAL session_replication_role=replica; '+sql+'; COMMIT;')
            baseline = contents(database)
            verify_injection(before,planned,baseline,case)
            lab.write_json(output/(name+'-injection.json'),dict(before=before,after=baseline))
            temporary = store.with_suffix('.quota-tmp')
            temporary.write_bytes(b'go\n')
            temporary.rename(store.with_suffix('.quota-go'))
            code = process.wait(30)
            stdout, stderr = bytes(process.stdout).decode(),bytes(process.stderr).decode()
            (output/(name+'.stdout')).write_text(stdout)
            (output/(name+'.stderr')).write_text(stderr)
            assert code == 0 and not stderr, (case,code,stderr)
            events = [json.loads(line) for line in stdout.splitlines()]
            dataset = bytes.fromhex(baseline['datasets'][0]['dataset_id'][2:])
            wanted,receipt,_ = expected(dataset,True)
            verify_events(events,case,baseline,wanted,inventory)
            # The original is independently encoded from the existing two-map
            # fixture vocabulary; it is never copied from a native/PG result.
            ref = lambda n: [dataset,4,bytes([64])*16,n]
            original = b'bigbrotr/producer-request/1\0'+cbor([[bytes([66])*16,1,1],
                'admit-evidence',1,[1,ref(1),[['dictionary',1,[ref(n),1,'visibility',1,{'class':label}]]
                                           for n,label in [(1,'public'),(2,'private')]]]])
            inputs = sorted((store/'producer-inputs-v1').glob('*.cbor'))
            assert len(inputs)==1 and inputs[0].read_bytes()==original
            seed_outcome, = before['producer_outcomes']
            assert seed_outcome['outcome_bytes']=='\\x'+receipt.hex()
            assert seed_outcome['outcome_sha256']=='\\x'+hashlib.sha256(receipt).hexdigest()
            request, = before['producer_requests']
            assert request['request_digest']=='\\x'+hashlib.sha256(original).hexdigest()
            assert request['input_bytes']==len(original)
            after = contents(database)
            assert after == baseline, 'proof/refused intake changed PG rows or xmin'
            lab.write_json(output/(name+'-final.json'),after)
            assert lab.sql(state,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                           "AND usename='bigbrotr_control'") == '0', 'unjoined quota driver'
            record(name,True,result=events[-1],original_bytes=len(original),native_rows=len(wanted),
                   pg_unchanged=True,quota_runtime='EXECUTED')
        finally:
            process.stop()
            (output/(name+'.stdout')).write_text(bytes(process.stdout).decode())
            (output/(name+'.stderr')).write_text(bytes(process.stderr).decode())
    return dict(status='PASS_SCOPED_CONNECTED_QUOTA',cases=list(CASES),
                not_run=['ROLLBACK/lease delivery loss and competing writers at this proof',
                         'deployment-grant profile (existing broad non-owner lab role only)',
                         'physical filesystem funding/ENOSPC, OPEN65, load, arm64 or release'])
