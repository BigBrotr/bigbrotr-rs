"""Actual append COMMIT faults and fresh-Control recovery on isolated PG clones."""
import hashlib
import json
import sys
import time
from dictionary_append import expected as append

CASES = ('pass', 'reservation-before', 'reservation-after', 'outcome-before', 'outcome-after',
         'cancel-reservation', 'cancel-outcome', 'reuse-before', 'reuse-after')


def event(process, wanted):
    deadline = time.monotonic() + 15
    with process.condition:
        while True:
            assert not process.overflow, 'probe-output-overflow'
            end = process.stdout.find(b'\n', process.offset)
            if end >= 0:
                value = json.loads(bytes(process.stdout[process.offset:end]))
                process.offset = end + 1
                assert value['event'] == wanted, (wanted, value['event'])
                return value
            assert time.monotonic() < deadline and (
                process.process.poll() is None or process.threads[0].is_alive()), 'missing-' + wanted
            process.condition.wait(.02)


def state(value):
    return {name: json.loads(rows) for name, rows in value['state']['control_tables']}


def unchanged(before, after, allowed, require):
    require(before.keys() == after.keys()
        and all(before[k] == after[k] for k in before if k not in allowed), 'unrelated-Control-effect')


def native_expected(dataset, cbor, sequence, committed, closing):
    rows, inputs, receipts = append(dataset, cbor)
    rows = {(cf, k): v for cf,k,v in rows}
    be = lambda n: n.to_bytes(8, 'big')
    slot = bytes([66]) * 16
    if not committed:
        from bootstrap_oracle import expected as bootstrap
        domains = [(k, bytes([d])*16) for k,d in [(1,70),(2,65),(3,71),(4,64),(5,72),(8,68)]]
        original, _, _ = bootstrap(dataset, True, namespaces=domains)
        rows = {(cf, k): v for cf,k,v in original}
    elif sequence == 2:
        del rows['receipts', b'\x01\x30'+slot+be(1)+be(3)]
    issued = sequence if committed else 1
    rows['receipts', b'\x01\x31'+slot+be(1)] = (
        (b'\x02\x31' if closing else b'\x01\x31') + be(issued)+be(0)+be(0)
        + (b'\x01' if closing else b''))
    return sorted((cf,k,v) for (cf,k),v in rows.items()), inputs[:sequence], receipts[:issued]


def run(context):
    lab, pg, client, output, source, rows, clone, report, save, require, oracle = (
        context[k] for k in ('lab','pg','client','output','SOURCE','rows','clone','report','save','require','oracle'))
    sys.path.insert(0, str(source/'experiments/control-migrations'))
    from fault_cases import Process
    report['not_run'] = ['native syscall/process interruption, ENOSPC or power loss in this append path',
        'general intern preparation and installed finite execution/M1', 'load/aged-state, capacity, arm64/release']
    for name in CASES:
        database = clone('dictionary-'+name, 'bb_source_template')
        config = lab.config(output,dict(pg,client=client),database,endpoint='127.0.0.1:5432',boundary='loopback')
        migrated = lab.cli(dict(pg,client=client),'migrate','apply','--file',config,
            '--user','bigbrotr_migrator','--password-ref','file:/run/secrets/migrator','--target','24')
        (output/(name+'-migration.stdout')).write_text(migrated.stdout)
        (output/(name+'-migration.stderr')).write_text(migrated.stderr)
        require(migrated.returncode == 0, 'current-fixture-schema')
        lab.sql(pg,database,'GRANT SELECT ON control.acquisition_selectors,control.acquisition_scopes,'
            'control.acquisition_jobs TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        reuse = name.startswith('reuse-')
        sequence = 3 if reuse else 2
        unresolved = name.startswith('reservation-') or name == 'cancel-reservation'
        committed = not unresolved
        reserved = name != 'reservation-before'
        copied = committed and name not in ('outcome-before', 'reuse-before')
        baseline = 6 if reuse else 4
        commit = baseline+(1 if unresolved else 2)
        wire_mode = ('hold' if name.startswith('cancel-') else 'before-forward' if name.endswith('-before')
            else 'after-complete' if name.endswith('-after') else 'pass')
        processes = []
        try:
            proxy = Process(['docker','exec','-i',client,'python3','-B',
                '/source/experiments/control-migrations/fault_proxy.py','--upstream-port','5432',
                '--listen-port','6543','--mode',wire_mode,'--commit-number',str(commit),
                '--later-commits',str(64-commit),'--deadline-seconds','60'], input_pipe=True)
            processes.append(('proxy',proxy))
            ready_proxy = proxy.event('ready',10)
            require(ready_proxy['proxy_sha256'] == hashlib.sha256(
                (source/'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest(), 'exact-wire-peer')
            process = Process(['docker','exec',client,'/build/ingest_bootstrap_probe','dictionary-fault-'+name,
                database,'/run/secrets/application','/lab/'+name],limit=8*1024**2)
            processes.append(('probe',process))
            event(process,'initialized')
            ready = event(process,'dictionary-ready')
            before = rows(database)
            require(len(before) == 67 and before == state(ready), 'ready-complete-Control-state')
            proxy.send('status')
            wire = proxy.event('status',5)
            require(all(wire[k] == baseline for k in ('commits_seen','commits_forwarded',
                'server_commit_complete','client_commit_complete')), 'original-setup-COMMITs')
            (output/(name+'.go')).touch(exist_ok=False)
            if wire_mode == 'hold':
                held = proxy.event('held',5)
                require(held['commits_seen'] == commit and all(held[k] == commit-1 for k in (
                    'commits_forwarded','server_commit_complete','client_commit_complete')), 'actual-held-COMMIT')
                held_rows = rows(database)
                (output/(name+'.cancel')).touch(exist_ok=False)
                require(event(process,'dictionary-cancelled')['unavailable'], 'poisoned-original-session')
                custody = json.loads(lab.sql(pg,database,f"""
                    SELECT json_build_object('pid',a.pid,'backend_start',a.backend_start::text,
                        'fenced',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid AND locktype='advisory'
                            AND classid=1111651442::oid AND objid=1129599564::oid AND objsubid=2
                            AND mode='ExclusiveLock' AND granted))
                    FROM pg_stat_activity a WHERE a.pid={ready['pid']} AND a.datname=current_database()
                """))
                require(custody and custody['backend_start'] == ready['backend_start'] and custody['fenced']
                    and rows(database) == held_rows, 'cancel-retains-actual-backend-custody')
                proxy.send('release')
                proxy.event('released',5)
                deadline = time.monotonic()+2
                while True:
                    proxy.send('status')
                    forwarded = proxy.event('status',5)
                    if forwarded['client_commit_complete'] == commit: break
                    require(time.monotonic()<deadline, 'cancelled-COMMIT-incomplete')
                    time.sleep(.02)
                require(forwarded['server_commit_complete'] == commit, 'actual-commit-after-cancel')
                lab.write_json(output/(name+'-interruption.json'),dict(held=held,rows=held_rows,custody=custody,forwarded=forwarded))
                (output/(name+'.finish')).touch(exist_ok=False)
            finished = event(process,'dictionary-operation-finished')
            require(finished['cancelled'] == name.startswith('cancel-'), 'actual-cancellation-kind')
            released = event(process,'dictionary-recovery-ready')
            require(released['driver_joined'] and lab.sql(pg,database,
                f"SELECT count(*) FROM pg_stat_activity WHERE pid={ready['pid']}") == '0', 'old-backend-reaped')
            interrupted = rows(database)
            require(interrupted == state(finished), 'operation-complete-state')
            unchanged(before,interrupted,('producer_epochs','producer_requests','producer_outcomes'),require)
            wanted_requests = sequence if reserved else sequence-1
            wanted_outcomes = sequence if copied else sequence-1
            require(len(interrupted['producer_requests']) == wanted_requests
                and len(interrupted['producer_outcomes']) == wanted_outcomes, 'exact-interrupted-SQL-effects')
            for table in ('producer_requests','producer_outcomes'):
                require(all(r in interrupted[table] for r in before[table]), 'original-row-xmin-preserved')
            old_producer,new_producer = before['producer_epochs'][0],interrupted['producer_epochs'][0]
            require(len(before['producer_epochs']) == len(interrupted['producer_epochs']) == 1
                and new_producer == dict(old_producer,reserved_through_request=wanted_requests,
                    xmin=new_producer['xmin'] if reserved else old_producer['xmin']), 'exact-reservation-counter')
            (output/(name+'.recover')).touch(exist_ok=False)
            closed = event(process,'dictionary-closed')
            resolved = [event(process,'dictionary-resolved') for _ in range(1 if unresolved else 2)]
            require(event(process,'dictionary-fault-done')['driver_joined'] and process.wait(10) == 0
                and not process.stderr, 'actual-recovery-completion')
            wire = proxy.event('receipt',10)
            require(proxy.wait(5) == 0 and not proxy.stderr, 'wire-peer-completed')
            counters = [wire[k] for k in ('commits_seen','commits_forwarded','server_commit_complete','client_commit_complete')]
            wanted_wire = ([commit,commit-1,commit-1,commit-1] if wire_mode == 'before-forward' else
                [commit,commit,commit,commit-1] if wire_mode == 'after-complete' else [baseline+2]*4 if name == 'pass' else [commit]*4)
            require(counters == wanted_wire, 'exact-wire-ambiguity-boundary')
            final = rows(database)
            lab.write_json(output/(name+'-final.json'),final)
            dataset = bytes.fromhex(final['datasets'][0]['dataset_id'][2:])
            wanted,inputs,receipts = native_expected(dataset,oracle.cbor,sequence,committed,True)
            dumped = lab.docker('exec',client,'/build/ingest_bootstrap_probe','dump','/lab/'+name)
            (output/(name+'-native.json')).write_text(dumped.stdout)
            actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
            require(actual == wanted, 'literal-closed-native-state')
            require([p.read_bytes() for p in sorted((output/name/'producer-inputs-v1').glob('*.cbor'))] == inputs,
                'exact-staged-original-inputs')
            require([r['outcome_bytes'] for r in sorted(final['producer_outcomes'],key=lambda r:int(r['request_sequence']))]
                == ['\\x'+r.hex() for r in receipts], 'original-PG-outcomes')
            for value in resolved:
                require(value['unresolved'] == unresolved and value['receipt'] == (None if unresolved else receipts[-1].hex()),
                    'historical-result-not-negative-inference')
                unchanged(state(closed),state(value),('producer_outcomes',),require)
            if unresolved:
                require(state(resolved[0]) == state(closed), 'unresolved-is-not-a-write')
            else:
                require(state(resolved[0]) == state(resolved[1]), 'identical-recovery-no-write')
                for row in interrupted['producer_outcomes']:
                    require(row in final['producer_outcomes'], 'prior-outcome-not-rewritten')
                copied_row = next(r for r in final['producer_outcomes'] if int(r['request_sequence']) == sequence)
                require(int(copied_row['recorder_epoch']) == (1 if copied else 2), 'actual-original-or-recovery-recorder')
            require(lab.sql(pg,database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'all-drivers-joined')
            report['cases'].append(dict(name=name,status='PASS',wire_counts=counters,original_request=sequence,
                result='Unresolved' if unresolved else 'AlreadyDurable' if reuse else 'NewFacts',
                native_rows=len(actual),original_inputs=len(inputs),original_or_recovery_copy='original' if copied else 'missing' if unresolved else 'recovered'))
            save()
        finally:
            for kind,process in processes:
                process.stop()
                (output/(name+'-'+kind+'.stdout')).write_bytes(process.stdout)
                (output/(name+'-'+kind+'.stderr')).write_bytes(process.stderr)
