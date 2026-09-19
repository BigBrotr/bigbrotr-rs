#!/usr/bin/env python3
"""Exact-source native import-position/reservation boundary, never full import acceptance."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE/'experiments/archive-engine'))
sys.path.insert(0, str(SOURCE/'experiments/ingest-bootstrap'))
from signed_publication_cases import command, execute, event, proxy_observation, send
from fault_cases import Process
from event_oracle import expected_event
from oracle import cbor
from existing_outcome import expected, check_all_outcome_copies
sys.path.insert(0, str(SOURCE/'experiments/producer-requests'))
from run import KILL


def main(root):
    state = json.loads((root/'resources.json').read_text())
    assert SOURCE == root/'source'
    assert command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == state['source']
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['status'] == 'READY' and ready['source'] == state['source']
    artifact = root/'out/native-tests/ingest-bootstrap-probe'
    assert hashlib.sha256(artifact.read_bytes()).hexdigest() == ready['ingest_probe']
    for identity in [state['container_id'], state['postgres_id']]:
        owned = json.loads(command('docker', 'inspect', identity))[0]
        assert owned['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert owned['State']['Running'] and not owned['State']['OOMKilled']
    report = dict(status='INCOMPLETE', source=state['source'], tree=state['tree'], artifacts=ready, cases=[], sql_operations=[])
    target = root/'input-binding-cases.json'
    assert not target.exists()
    def save(): target.write_text(json.dumps(report, indent=2)+'\n')
    def sql(db, text, phase='observe', timeout=30):
        observation = dict(database=db, phase=phase, status='INCOMPLETE', exit=None,
                           sql_sha256=hashlib.sha256(text.encode()).hexdigest(), remote_exclusion='UNKNOWN')
        report['sql_operations'].append(observation);save()
        try:
            process = subprocess.run(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-XqAt',
                '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db],
                input=text, text=True, capture_output=True, timeout=timeout)
            observation.update(exit=process.returncode, stdout=process.stdout, stderr=process.stderr,
                               status='RETURNED')
            process.check_returncode()
            return process.stdout.strip()
        except subprocess.TimeoutExpired as error:
            decode = lambda value: value.decode(errors='replace') if isinstance(value, bytes) else (value or '')
            observation.update(status='TIMEOUT', stdout=decode(error.stdout), stderr=decode(error.stderr))
            raise
        finally:
            # A joined docker-exec process alone does not exclude the PG backend.
            save()
    def rows(db):
        return json.loads(sql(db, "SELECT jsonb_build_object('requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY request_sequence) FROM control.producer_requests r),'outcomes',(SELECT jsonb_agg(to_jsonb(o) ORDER BY request_sequence) FROM control.producer_outcomes o),'bindings',(SELECT coalesce(jsonb_agg(to_jsonb(b) ORDER BY logical_record),'[]'::jsonb) FROM control.producer_input_bindings b),'jobs',(SELECT coalesce(jsonb_agg(to_jsonb(j)),'[]'::jsonb) FROM control.jobs j),'checkpoints',(SELECT count(*) FROM control.producer_checkpoints))"))
    binary = ['docker', 'exec', '-i', state['container_id'], '/out/native-tests/ingest-bootstrap-probe']
    dataset, slot = bytes([1])*16, bytes([66])*16
    raw = (SOURCE/'tests/fixtures/ingest/import-event.jsonl').read_bytes()
    source_digest = hashlib.sha256(raw*2).hexdigest()
    assert len(raw) == 353 and source_digest == 'ce439b495fd2f99031706e9625529e264cd3043ffc16af4fd6002243503e90fc'
    scope = [dataset,4,bytes([64])*16,1]
    lineage = [dataset,9,bytes([69])*16,1]
    dictionaries = [['dictionary',1,[[dataset,4,bytes([64])*16,n],1,'visibility',1,{'class':name}]]
                    for n,name in [(1,'public'),(2,'private')]]
    dictionaries.append(['dictionary',1,[lineage,1,'dataset-lineage',1,
        {'format':'nostr-jsonl','format_version':1,'source_dataset_id':None,'source_artifact_digest':bytes.fromhex(source_digest)}]])
    signed_event = json.loads(raw)
    signed = [bytes.fromhex(signed_event['id']),bytes.fromhex(signed_event['pubkey']),signed_event['created_at'],
              signed_event['kind'],signed_event['tags'],signed_event['content'],bytes.fromhex(signed_event['sig'])]
    preimage = lambda n,operation,intent: b'bigbrotr/producer-request/1\0'+cbor([[slot,1,n],operation,1,intent])
    originals = {1:preimage(1,'admit-evidence',[1,scope,dictionaries]),
                 2:preimage(2,'admit',[1,scope,signed,'import',lineage,None]),
                 3:preimage(3,'admit',[1,scope,signed,'import',lineage,None])}
    save()
    for label in ['complete', 'paused', 'pause-between', 'conflict', 'cancelled', 'rollback', 'before', 'before-forward', 'after-complete']:
        case = dict(name=label, status='INCOMPLETE', process={});report['cases'].append(case);save()
        db = 'bbbindings_'+label.replace('-', '_');case['database'] = db
        sql('postgres', 'CREATE DATABASE '+db, 'create-database')
        setup = case['setup'] = {};save()
        try: execute(binary+['setup-13', db, '/secrets/postgres', 'unused'], 30, setup)
        finally: save()
        assert setup['exit'] == 0 and not setup['stderr']
        if label == 'rollback':
            sql(db, "CREATE FUNCTION public.fixture_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.request_sequence=3 THEN RAISE EXCEPTION 'fixture reservation rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER zz_fixture_abort AFTER INSERT ON control.producer_requests FOR EACH ROW EXECUTE FUNCTION public.fixture_abort()", 'install-rollback-fault')
        mode = 'event-bindings'+('' if label == 'complete' else '-'+label)
        if label in ['before-forward', 'after-complete']: mode = 'event-bindings-commit-lost'
        directory = '/out/bindings-'+label
        command('docker', 'exec', state['container_id'], 'mkdir', '-m', '700', directory)
        proxy = None
        try:
            if label in ['before-forward', 'after-complete']:
                proxy = Process(['docker', 'exec', '-i', state['container_id'], 'python3', '-B',
                    '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
                    '--listen-port', '6543', '--mode', label, '--commit-number', '5', '--deadline-seconds', '35'], limit=65536)
                event(proxy, 'ready')
            args = [mode, db, '/secrets/bigbrotr_control', directory+'/db']
            if label == 'before':
                case['process'] = json.loads(command('docker', 'exec', state['container_id'], 'python3', '-c',
                    KILL.replace('reserved-before-native', 'input-batch-bound'), '/out/native-tests/ingest-bootstrap-probe', *args, timeout=35))
                assert case['process']['exit'] == -9 and case['process']['joined']
            else:
                def interact(process):
                    event(process, 'before-native-job-recheck')
                    case['before_pause'] = rows(db);save()
                    sql(db, "UPDATE control.jobs SET state='paused',reason_code='operator_requested',job_revision=job_revision+1", 'pause-before-native')
                    send(process, 'continue\n')
                execute(binary+args, 35, case['process'], interact if label=='pause-between' else None);save()
                assert case['process']['exit'] == 0 and not case['process']['stderr']
            if proxy:
                assert proxy.wait(5) == 0
                case['proxy'] = proxy_observation(proxy);save()
                final = [json.loads(line) for line in case['proxy']['stdout'].splitlines()][-1]
                assert final['event']=='receipt' and final['commits_seen']==5 and final['faults']==1
                assert final['server_commit_complete'] == (5 if label=='after-complete' else 4)
                assert final['client_commit_complete'] == 4
        finally:
            if proxy: proxy.stop();case['proxy']=proxy_observation(proxy)
            save()
        case['control'] = control = rows(db);save()
        dump = case['dump_process'] = {}
        try: execute(binary+['dump', directory+'/db'], 15, dump)
        finally: save()
        assert dump['exit'] == 0 and not dump['stderr']
        native = case['native'] = json.loads(dump['stdout']);save()
        wanted = expected(2)[0] if label=='complete' else expected_event(dataset, False, 2)[0]
        assert [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native['rows']] == wanted
        bound = label in ['complete', 'paused', 'pause-between', 'conflict', 'before', 'after-complete']
        assert len(control['bindings']) == (2 if bound else 0)
        assert len(control['requests']) == (3 if bound else 1)
        assert len(control['outcomes']) == (3 if label=='complete' else 1)
        assert control['checkpoints'] == 0 and len(control['jobs'])==1
        assert control['jobs'][0]['checkpoint_revision']==0
        if label=='complete': check_all_outcome_copies(control, wanted)
        for index, binding in enumerate(control['bindings']):
            request = control['requests'][index+1]
            assert binding['job_id']=='\\x'+(bytes([90])*16).hex()
            assert binding['dataset_id']=='\\x'+dataset.hex()
            assert binding['source_namespace']=='\\x'+(bytes([92])*32).hex()
            assert binding['source_sha256']=='\\x'+source_digest and binding['source_bytes']==706
            assert binding['record_sha256']=='\\x'+hashlib.sha256(raw).hexdigest()
            assert binding['logical_record']==index+1 and binding['byte_start']==index*353 and binding['byte_end']==(index+1)*353
            assert binding['batch_first_record']==1 and binding['batch_records']==2 and binding['obligation_index']==index
            assert binding['shard_id']==0 and binding['parser_profile']==binding['obligation_kind']==1
            for field in ['dataset_id','producer_slot_id','producer_epoch','request_sequence','request_digest','input_profile','input_bytes','manifest_sha256','issuer_resource','issuer_epoch','issuer_incarnation']:
                assert binding[field] == request[field], field
        messages = [json.loads(line) for line in case['process']['stdout'].splitlines()]
        positions = [m for m in messages if m.get('event')=='import-position']
        assert positions == [dict(event='import-position',line=n+1,byte_start=n*353,byte_end=(n+1)*353,
            source_bytes=706,source_sha256=source_digest,record_sha256=hashlib.sha256(raw).hexdigest(),
            completed_import_prefix_before_admission=0) for n in range(2)]
        spool = root/directory.lstrip('/')/'db/producer-inputs-v1'
        files = sorted(spool.iterdir());assert len(files)==(1 if label=='cancelled' else 3)
        case['inputs'] = {p.name:dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files}
        for n,original in originals.items():
            if label=='cancelled' and n>1: continue
            name=slot.hex()+'-0000000000000001-'+f'{n:016x}'+'.cbor'
            assert (spool/name).read_bytes()==original
        for request in control['requests']:
            name=slot.hex()+'-0000000000000001-'+f"{request['request_sequence']:016x}"+'.cbor'
            assert case['inputs'][name]['bytes']==request['input_bytes']
            assert '\\x'+case['inputs'][name]['sha256']==request['request_digest']
        case['remaining_client_backends'] = int(sql(db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()'))
        assert case['remaining_client_backends']==0
        case['status']='PASS';save()
    # Real synthetic SQL constraints remain distinct from signed native evidence.
    schema = report['schema'] = dict(status='INCOMPLETE');save()
    output = sql('bbbindings_complete', (SOURCE/'experiments/producer-requests/binding_schema_cases.sql').read_text(), 'binding-schema', 45)
    observed = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
    assert len(observed)==1 and observed[0]['status']=='PASS'
    expected_labels = {'nonempty_native_fixture', 'missing_batch_member', 'missing_original_reservation',
        'different_original_input_bytes', 'parser_profile_unknown', 'unsupported_claim_is_not_event_only',
        'impossible_prior_delimiters', 'late_bootstrap_binding_with_native_outcome', 'late_binding_without_pg_outcome',
        'complete_two_obligation_batch', 'update_immutable', 'delete_immutable', 'truncate_refused',
        'adjacent_gap_forward', 'adjacent_overlap_forward', 'changed_source', 'changed_namespace',
        'adjacent_exact_forward', 'adjacent_gap_reverse', 'adjacent_exact_reverse', 'genuine_record_hole_is_unresolved',
        'hole_fill_must_join_both_sides', 'whole_batch_rollback_after_first_reservation', 'inconsistent_declared_count',
        'exact_retained_ledger_limit', 'another_producer_does_not_multiply_job_limit', 'paused_job_refuses_new_input',
        'binding_never_advances_any_checkpoint'}
    assert {case['label'] for case in observed[0]['cases']} == expected_labels
    schema['remaining_client_backends'] = int(sql('bbbindings_complete', 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()', 'schema-backend-audit'))
    assert schema['remaining_client_backends']==0
    schema.update(status='PASS',result=observed[0]);save()
    report['status']='PASS';save();print(json.dumps(dict(status='PASS',cases=len(report['cases']),sql_cases=len(observed[0]['cases']))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    main(parser.parse_args().root.resolve(strict=True))
