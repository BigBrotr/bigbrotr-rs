#!/usr/bin/env python3
"""Actual positive-only recovery, precise refusals and ambiguous PG commits.

Fresh-process/held-owner recovery and complete drain remain separate gates.
Every case retains observations before interpreting fixture assertions.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/archive-engine'))
sys.path.insert(0, str(SOURCE/'experiments/ingest-bootstrap'))
from signed_publication_cases import command, event, proxy_observation
from fault_cases import Process
from closing import sha, check_state, same_window, check_recovery_control
from recovery_outcomes import inputs as expected_originals, native as expected_case

SCENARIOS = ['normal', 'pg-only', 'input-size', 'input-digest', 'input-manifest',
             'resource', 'cancelled', 'policy', 'expired', 'rollback', 'late-sql',
             'before-forward', 'after-complete', 'already', 'rejected', 'missing-detail', 'missing-reservation', 'copy-conflict']
SUCCESSES = ['normal', 'already', 'rejected']
PERMANENT = ['pg-only', 'input-size', 'input-digest', 'input-manifest', 'missing-detail', 'missing-reservation', 'copy-conflict']
ERRORS = dict(zip(SCENARIOS[1:13], [
    'Native(ProducerOpen)', 'Outcome(Conflict)', 'Native(Input(Codec(DigestMismatch)))',
    'Reservation(Conflict)', 'Native(Input(Codec(InputLimit)))',
    'Native(Native(AuthorityUnavailable))', 'Control(InvalidPolicy)', 'Control(Unavailable)',
    'Outcome(Backend)', 'DeliveryUnknown', 'CommitUnknown', 'CommitUnknown']))
ERRORS.update({'missing-detail':'Native(Unresolved)', 'missing-reservation':'Native(Unresolved)', 'copy-conflict':'Outcome(Conflict)'})


def check_inputs(control, files, wanted, scenario):
    """Compare all originals and only the explicitly injected metadata mutation."""
    originals = expected_originals(scenario)
    sequences = list(originals)
    slot = bytes([66])*16
    names = {n:slot.hex()+'-0000000000000001-'+f'{n:016x}'+'.cbor' for n in sequences}
    assert files == {names[n]: originals[n] for n in sequences}
    manifest = next(v for cf, k, v in wanted if cf == 'default' and k == b'\x01\x01')
    assert [r['request_sequence'] for r in control['requests']] == sequences
    assert control['bindings'] == []
    for table in ['requests', 'outcomes']:
        for row in control[table]:
            n = row['request_sequence']
            assert n in sequences
            digest, manifest_sha = sha(originals[n]), sha(manifest)
            length = len(originals[n])
            if table == 'requests' and n == 2:
                if scenario == 'input-size': length += 1
                if scenario == 'input-digest':
                    digest = (bytes([bytes.fromhex(digest)[0] ^ 1])+bytes.fromhex(digest)[1:]).hex()
                if scenario == 'input-manifest':
                    manifest_sha = (bytes([bytes.fromhex(manifest_sha)[0] ^ 1])+bytes.fromhex(manifest_sha)[1:]).hex()
            assert row['dataset_id'] == '\\x'+(bytes([1])*16).hex()
            assert row['producer_slot_id'] == '\\x'+slot.hex() and row['producer_epoch'] == 1
            assert row['request_digest'] == '\\x'+digest and row['manifest_sha256'] == '\\x'+manifest_sha
            if table == 'requests':
                assert row['input_profile'] == 1 and row['input_bytes'] == length
                assert row['issuer_epoch'] == 1 and row['issuer_incarnation'] == '\\x'+(bytes([41])*16).hex()


def check_messages(messages, scenario, names, originals, control, seed_failure):
    wanted = expected_case(scenario, scenario != 'pg-only')
    def one(label):
        matches = [m['state'] for m in messages if m['event'] == label]
        assert len(matches) == 1, label
        return matches[0]
    closing, before = one('closing-before'), one('recovery-before')
    ct = check_state(closing, expected_case('normal' if scenario == 'missing-detail' else scenario, False), names, originals)
    bt = check_state(before, wanted, names, originals)
    same_window(ct, bt, scenario != 'pg-only')
    assert before['sequence'] == closing['sequence']+int(scenario != 'pg-only')+int(scenario == 'missing-detail')
    issued = 3 if scenario == 'already' else 2
    before_rows = sorted(bt['producer_outcomes'], key=lambda r:r['request_sequence'])
    before_count = 2 if scenario == 'copy-conflict' else issued-1
    assert [r['request_sequence'] for r in before_rows] == list(range(1, before_count+1))
    corruption = [m for m in messages if m['event'].startswith('recovery-corruption-')]
    if scenario == 'missing-detail':
        assert [m['event'] for m in corruption] == ['recovery-corruption-before', 'recovery-corruption-after']
        old, new = [m['state'] for m in corruption]
        old_tables = check_state(old, expected_case('normal', True), names, originals)
        new_tables = check_state(new, expected_case(scenario, True), names, originals)
        assert old_tables == new_tables and new['sequence'] == old['sequence']+1
        assert new == before
        assert old['sequence'] == closing['sequence']+1
    else:
        assert not corruption
    unreserved = [m for m in messages if m['event']=='recovery-unreserved-request']
    assert unreserved == ([dict(event='recovery-unreserved-request', request=3)] if scenario == 'missing-reservation' else [])
    original_owner = next(r for r in ct['owner_epochs'] if r['role'] == 'control' and r['epoch'] == 1)
    assert seed_failure['control_backend_pid'] == original_owner['control_backend_pid']
    assert seed_failure['control_backend_start'] == original_owner['control_backend_start']
    failure = seed_failure['lines']
    assert failure and all('['+str(original_owner['control_backend_pid'])+']' in line for line in failure)
    guard = scenario in ['input-digest', 'input-manifest']
    error = 'outcome copy requires its exact retained reservation' if guard else 'fixture rollback of original event outcome copy'
    function = 'guard_producer_outcome()' if guard else 'public.fixture_abort_original_copy()'
    if scenario == 'copy-conflict':
        assert sum('WARNING:  fixture corrupted original outcome bytes' in line for line in failure) == 1
        assert any('function public.fixture_corrupt_outcome()' in line for line in failure)
        assert len([m for m in messages if m['event'] == 'recovery-seed-copy-corrupted']) == 1
        assert not any(m['event'] == 'recovery-seed-copy-rolled-back' for m in messages)
    else:
        assert sum('ERROR:  '+error in line for line in failure) == 1
        assert any('function '+function in line and ' at RAISE' in line for line in failure)
        assert any('STATEMENT:  INSERT INTO control.producer_outcomes(' in line for line in failure)
        assert len([m for m in messages if m['event'] == 'recovery-seed-copy-rolled-back']) == 1
    permanent, replaced = scenario in PERMANENT, scenario not in SUCCESSES and scenario not in PERMANENT
    refused = [m for m in messages if m['event'] == 'recovery-refused']
    assert refused == ([] if scenario in SUCCESSES else [dict(event='recovery-refused',
        error=ERRORS[scenario], session_poison_checked=scenario != 'cancelled')])
    mid = None
    if scenario not in SUCCESSES:
        mid = one('recovery-interrupted')
        mt = check_state(mid, wanted, names, originals)
        assert mid['sequence'] == before['sequence']
        for name in names:
            if scenario == 'after-complete' and name == 'producer_outcomes': continue
            assert mt[name] == bt[name], name
        if scenario == 'after-complete':
            assert len(mt['producer_outcomes']) == 2 and bt['producer_outcomes'][0] in mt['producer_outcomes']
    else:
        assert not any(m['event'] == 'recovery-interrupted' for m in messages)
    after = mid if permanent else one('recovery-after')
    at = check_state(after, wanted, names, originals)
    assert after['sequence'] == before['sequence']
    if replaced:
        check_recovery_control(bt, at)
        assert [r for r in bt['owner_epochs'] if r['role'] == 'ingest'] == [r for r in at['owner_epochs'] if r['role'] == 'ingest']
    for name in names:
        if name == 'producer_outcomes' or replaced and name in ['owner_epochs', 'role_instances']: continue
        assert bt[name] == at[name], name
    rows = sorted(at['producer_outcomes'], key=lambda r:r['request_sequence'])
    assert len(rows) == (before_count if permanent else issued) and rows[:before_count] == before_rows
    if scenario == 'after-complete': assert mt['producer_outcomes'] == at['producer_outcomes']
    native = {(cf, k):v for cf, k, v in wanted}
    for n, row in enumerate(rows, 1):
        value = native['receipts', b'\x01\x30'+bytes([66])*16+(1).to_bytes(8, 'big')+n.to_bytes(8, 'big')]
        if scenario == 'copy-conflict' and n == 2:
            value = bytes([value[0] ^ 1])+value[1:]
        recorder = 1 if n <= before_count else 3 if replaced and scenario != 'after-complete' else 2
        kind = 3 if scenario == 'rejected' and n == 2 else 2 if n == 3 else 1
        assert row['outcome_kind'] == kind and row['policy_epoch'] == 1
        assert row['outcome_bytes'] == '\\x'+value.hex() and row['outcome_sha256'] == '\\x'+sha(value)
        assert row['recorder_epoch'] == recorder
        assert row['recorder_incarnation'] == '\\x'+(bytes([{1:41, 2:43, 3:44}[recorder]])*16).hex()
        assert row['recorder_resource'] == control['requests'][n-1]['issuer_resource']
        assert row['recorder_role'] == 'control' and row['recorded_at_us'] >= control['requests'][n-1]['reserved_at_us']
    for table, key in [('producer_requests', 'requests'), ('producer_outcomes', 'outcomes')]:
        assert [{k:v for k, v in r.items() if k != 'xmin'} for r in sorted(at[table], key=lambda r:r['request_sequence'])] == control[key]
    resolved = [m for m in messages if m['event'] == 'producer-resolved']
    order = [] if permanent else [3, 3, 2, 1] if scenario == 'already' else [2, 2, 1]
    assert [m['request'] for m in resolved] == order
    intermediate = [m for m in messages if m['event'] == 'recovery-observed']
    assert [(m['attempt'], m['request']) for m in intermediate] == list(enumerate(order))
    assert all(m['state'] == after for m in intermediate)
    for message in resolved:
        if scenario == 'rejected' and message['request'] == 2:
            assert message['rejection'] == 'Signature' and 'bytes' not in message
        else:
            assert message['bytes'] == rows[message['request']-1]['outcome_bytes'][2:]
        assert message['current_control_epoch'] == (3 if replaced else 2) and message['historical_control_epoch'] == 1
    closes = [m for m in messages if m['event'] == 'producer-closed']
    assert len(closes) == (0 if scenario == 'pg-only' else 2)
    assert all(m['issued'] == issued and m['current_control_epoch'] == 2 and m['historical_control_epoch'] == 1 for m in closes)
    assert at['producer_epochs'][0]['state'] == 'closing'
    assert at['producer_epochs'][0]['reserved_through_request'] == issued
    assert at['source_streams'][0]['state'] == 'open'
    for table in ['jobs', 'producer_checkpoints', 'producer_input_bindings', 'import_record_closures', 'import_checkpoints']:
        assert at[table] == []
    return dict(native_rows=len(wanted), control_tables=len(names), originals=len(originals),
                sequence=after['sequence'], resolved=[m['request'] for m in resolved])


def main(root, tag, scenario, fixture_build, fixture_directory='recovery-probe'):
    resources = json.loads((root/'resources.json').read_text())
    revision = command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD')
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    exceptions = [':!apps/bigbrotr/examples/ingest_bootstrap_probe.rs', ':!apps/bigbrotr/examples/support/ingest_closing.rs', ':!apps/bigbrotr/examples/support/ingest_recovery.rs', ':!apps/bigbrotr/examples/support/ingest_recovery_process.rs'] if fixture_build else []
    assert not command('git', '-C', str(SOURCE), 'diff', resources['source'], revision, '--',
                       'apps', 'crates', 'migrations', 'Cargo.lock', 'Cargo.toml', 'rust-toolchain.toml', 'tools', *exceptions)
    location = 'out/'+fixture_directory if fixture_build else 'out/native-tests'
    ready = json.loads((root/location/'ready.json').read_text())
    assert ready['status'] == 'READY' and ready['baseline_product' if fixture_build else 'source'] == resources['source']
    if fixture_build:
        assert not command('git', '-C', str(SOURCE), 'diff', ready['source'], revision, '--',
                           'apps', 'crates', 'migrations', 'Cargo.lock', 'Cargo.toml', 'rust-toolchain.toml', 'tools')
    assert sha((root/location/'ingest-bootstrap-probe').read_bytes()) == ready['sha256' if fixture_build else 'ingest_probe']
    for identity in [resources['container_id'], resources['postgres_id']]:
        owned = json.loads(command('docker', 'inspect', identity))[0]
        assert owned['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert owned['State']['Running'] and not owned['State']['OOMKilled']
    assert tag.isalnum() and 1 <= len(tag) <= 8
    target = root/('recovery-'+tag+'.json')
    assert not target.exists()
    report = dict(status='INCOMPLETE', source=resources['source'], harness_revision=revision,
                  artifacts=ready, scenario=scenario, processes={}, sql=[], process_output_limit=512*1024)
    def save(): target.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, text):
        observation = dict(database=db, sql_sha256=sha(text.encode()), status='INCOMPLETE')
        report['sql'].append(observation)
        save()
        try:
            result = subprocess.run(['docker', 'exec', '-i', resources['postgres_id'], 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db], input=text, text=True, capture_output=True, timeout=30)
            observation.update(status='RETURNED', exit=result.returncode, stdout=result.stdout, stderr=result.stderr)
            result.check_returncode()
            return result.stdout.strip()
        finally: save()
    binary = ['docker', 'exec', '-i', resources['container_id'], '/'+location+'/ingest-bootstrap-probe']
    def run(label, args):
        observation = report['processes'][label] = {}
        save()
        process = Process(binary+args, limit=report['process_output_limit'])
        observation.update(exit=None, remote_exclusion='UNKNOWN')
        try: observation['exit'] = process.wait(45)
        finally:
            process.stop()
            observation.update(stdout=process.stdout.decode(), stderr=process.stderr.decode(), overflow=process.overflow)
            save()
        return observation
    db, directory = 'bbrecovery_'+tag, '/out/recovery-'+tag
    save()
    sql('postgres', 'CREATE DATABASE '+db)
    setup = run('setup', ['setup-14', db, '/secrets/postgres', 'unused'])
    assert setup['exit'] == 0 and not setup['stderr']
    failed_request = 3 if scenario == 'already' else 2
    if scenario == 'copy-conflict':
        # Privileged lab corruption after ordinary guards, with a valid checksum.
        # SQL cannot attest native semantics; an identical recovery must refuse.
        sql(db, "CREATE FUNCTION public.fixture_corrupt_outcome() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.outcome_bytes:=set_byte(NEW.outcome_bytes,0,get_byte(NEW.outcome_bytes,0)#1); NEW.outcome_sha256:=sha256(NEW.outcome_bytes); RAISE WARNING 'fixture corrupted original outcome bytes'; RETURN NEW; END $$; CREATE TRIGGER zz_fixture_corrupt_outcome BEFORE INSERT ON control.producer_outcomes FOR EACH ROW WHEN (NEW.recorder_epoch=1 AND NEW.request_sequence=2) EXECUTE FUNCTION public.fixture_corrupt_outcome();")
        guards = report['outcome_guards'] = json.loads(sql(db, "SELECT jsonb_agg(jsonb_build_object('name',tgname,'enabled',tgenabled) ORDER BY tgname) FROM pg_trigger WHERE tgrelid='control.producer_outcomes'::regclass AND NOT tgisinternal AND (tgtype & 7)=7"))
        assert guards == [dict(name=name, enabled='O') for name in ['producer_outcome_guard', 'zz_fixture_corrupt_outcome']]
    else:
        sql(db, "CREATE FUNCTION public.fixture_abort_original_copy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback of original event outcome copy'; END $$; CREATE TRIGGER zz_fixture_abort_original_copy AFTER INSERT ON control.producer_outcomes FOR EACH ROW WHEN (NEW.recorder_epoch=1 AND NEW.request_sequence="+str(failed_request)+") EXECUTE FUNCTION public.fixture_abort_original_copy();")
    if scenario in ['input-size', 'input-digest', 'input-manifest']:
        field = {'input-size':'input_bytes', 'input-digest':'request_digest', 'input-manifest':'manifest_sha256'}[scenario]
        value = 'NEW.input_bytes+1' if scenario == 'input-size' else f'set_byte(NEW.{field},0,get_byte(NEW.{field},0)#1)'
        # A cross-request manifest conflict is already refused by migration0011.
        # To reach the defensive resolver check, inject that corruption AFTER
        # its BEFORE guards. This privileged fixture is not a legitimate writer.
        # Other faults run before guards. No applied function/trigger is changed.
        trigger = 'zz_fixture_corrupt_input' if scenario == 'input-manifest' else 'aa_fixture_corrupt_input'
        report['reservation_fault'] = dict(field=field, trigger=trigger,
            scope='privileged corruption after validation' if scenario == 'input-manifest' else 'corruption before validation')
        sql(db, f"CREATE FUNCTION public.fixture_corrupt_input() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.{field}:={value}; RETURN NEW; END $$; CREATE TRIGGER {trigger} BEFORE INSERT ON control.producer_requests FOR EACH ROW WHEN (NEW.request_sequence=2) EXECUTE FUNCTION public.fixture_corrupt_input();")
    if scenario in ['rollback', 'late-sql']:
        action = "RAISE EXCEPTION 'fixture rollback of recovered event outcome copy'" if scenario == 'rollback' else 'PERFORM pg_sleep(3)'
        sql(db, f"CREATE FUNCTION public.fixture_abort_recovered_copy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {action}; RETURN NEW; END $$; CREATE TRIGGER zz_fixture_abort_recovered_copy AFTER INSERT ON control.producer_outcomes FOR EACH ROW WHEN (NEW.recorder_epoch=2 AND NEW.request_sequence=2) EXECUTE FUNCTION public.fixture_abort_recovered_copy();")
    if scenario == 'input-manifest':
        guards = report['reservation_guards'] = json.loads(sql(db, "SELECT jsonb_agg(jsonb_build_object('name',tgname,'enabled',tgenabled) ORDER BY tgname) FROM pg_trigger WHERE tgrelid='control.producer_requests'::regclass AND NOT tgisinternal AND (tgtype & 7)=7"))
        assert guards == [dict(name=name, enabled='O') for name in ['producer_request_guard', 'producer_request_outcome_capacity', 'zz_fixture_corrupt_input']]
    command('docker', 'exec', resources['container_id'], 'mkdir', '-m', '700', directory)
    lost, proxy = scenario in ['before-forward', 'after-complete'], None
    try:
        if lost:
            proxy = Process(['docker', 'exec', '-i', resources['container_id'], 'python3', '-B', '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432', '--listen-port', '6543', '--mode', scenario, '--commit-number', '4', '--deadline-seconds', '45'], limit=65536)
            hello = event(proxy, 'ready')
            assert hello['proxy_sha256'] == sha((SOURCE/'experiments/control-migrations/fault_proxy.py').read_bytes())
        result = run('recover', ['event-recovery-'+('commit-lost' if lost else scenario), db, '/secrets/bigbrotr_control', directory+'/db'])
        if proxy:
            assert proxy.wait(5) == 0
            report['proxy'] = proxy_observation(proxy)
            save()
            fault = json.loads(report['proxy']['stdout'].splitlines()[-1])
            assert fault['event'] == 'receipt' and fault['commits_seen'] == 4 and fault['faults'] == 1
            assert fault['server_commit_complete'] == (4 if scenario == 'after-complete' else 3)
            assert fault['client_commit_complete'] == 3
    finally:
        if proxy: proxy.stop(); report['proxy'] = proxy_observation(proxy)
        save()
    control = report['control'] = json.loads(sql(db, "SELECT jsonb_build_object('requests',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY request_sequence),'[]'::jsonb) FROM control.producer_requests r),'outcomes',(SELECT coalesce(jsonb_agg(to_jsonb(o) ORDER BY request_sequence),'[]'::jsonb) FROM control.producer_outcomes o),'bindings',(SELECT coalesce(jsonb_agg(to_jsonb(b)),'[]'::jsonb) FROM control.producer_input_bindings b))"))
    dump = run('dump', ['dump', directory+'/db'])
    assert dump['exit'] == 0 and not dump['stderr']
    report['native'] = json.loads(dump['stdout'])
    files = {p.name:p.read_bytes() for p in (root/directory.lstrip('/')/'db/producer-inputs-v1').iterdir()}
    originals = report['originals'] = {name:dict(bytes=len(value), sha256=sha(value)) for name, value in files.items()}
    names = report['table_names'] = json.loads(sql(db, "SELECT jsonb_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    report['remaining_backends'] = int(sql(db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
    # An actual engine error plus backend identity distinguishes the intended
    # original-copy fault from a generic adapter Backend failure.
    owners = json.loads(sql(db, "SELECT jsonb_agg(to_jsonb(o) ORDER BY epoch) FROM control.owner_epochs o WHERE role='control'"))
    owner = next(r for r in owners if r['epoch'] == 1)
    log = subprocess.check_output(['docker', 'logs', '--tail', '1000', resources['postgres_id']], text=True, stderr=subprocess.STDOUT, timeout=30)
    report['seed_failure'] = dict(control_backend_pid=owner['control_backend_pid'],
        control_backend_start=owner['control_backend_start'], lines=[line for line in log.splitlines() if '['+str(owner['control_backend_pid'])+']' in line])
    if scenario == 'rollback':
        current = next(r for r in owners if r['epoch'] == 2)
        report['copy_failure'] = [line for line in log.splitlines() if '['+str(current['control_backend_pid'])+']' in line]
    save()
    wanted = expected_case(scenario, scenario != 'pg-only')
    assert [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in report['native']['rows']] == wanted
    check_inputs(control, files, wanted, scenario)
    assert report['remaining_backends'] == 0 and result['exit'] == 0 and not result['stderr'] and not result['overflow']
    if scenario == 'rollback':
        assert sum('ERROR:  fixture rollback of recovered event outcome copy' in line for line in report['copy_failure']) == 1
        assert any('function public.fixture_abort_recovered_copy() line 1 at RAISE' in line for line in report['copy_failure'])
    messages = [json.loads(line) for line in result['stdout'].splitlines()]
    report['checked'] = check_messages(messages, scenario, names, originals, control, report['seed_failure'])
    report['status'] = 'PASS'
    save()
    print(json.dumps(dict(status='PASS', scenario=scenario, **report['checked'])))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--run-tag', required=True)
    parser.add_argument('--scenario', choices=SCENARIOS, default='normal')
    parser.add_argument('--fixture-build', action='store_true')
    parser.add_argument('--fixture-directory', choices=['recovery-probe', 'recovery-probe-refusals', 'recovery-probe-detail'], default='recovery-probe')
    args = parser.parse_args()
    main(args.root.resolve(strict=True), args.run_tag, args.scenario, args.fixture_build, args.fixture_directory)
