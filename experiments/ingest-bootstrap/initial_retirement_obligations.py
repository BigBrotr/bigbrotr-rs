"""Independent retirement vetoes on explicitly synthetic retained import history.

These clones are not evidence of successful import execution. Each adds exactly
one kind of unresolved obligation to an otherwise qualified signed retirement.
All product guards remain active during the installed recovery invocation.
"""
import datetime
import hashlib
import json

import lab


def additions(state, mode, payload):
    """Literal relational inputs; never ask the retirement implementation for them."""
    producer, = state['producer_epochs']
    if mode == 'checkpoint':
        return [('producer_checkpoints', dict(
            producer_slot_id=producer['producer_slot_id'], producer_epoch=1,
            dataset_id=producer['dataset_id'], checkpoint_revision=1,
            complete_through_request=0, checkpoint_digest='\\x' + 'c1' * 32,
            committed_at_us=state['source_streams'][0]['closed_at_us']))]
    assert mode in ('claim', 'binding')
    original, = [r for r in state['jobs'] if r['kind'] == 'acquisition']
    slot, = state['producer_slots']
    job = dict(original, job_id='\\x' + 'b1' * 16, request_key='\\x' + 'b2' * 16,
        request_digest='\\x' + 'b3' * 32, kind='import', state='queued' if mode == 'claim' else 'accepted',
        job_revision=2 if mode == 'claim' else 1, checkpoint_revision=0,
        reason_code=None, result_manifest_digest=None, completed_at_us=None,
        acquisition_job_protocol=None, export_job_protocol=None,
        input_topology_generation=None, input_complete=True,
        owner_resource_id=slot['registered_by_resource_id'] if mode == 'claim' else None,
        owner_epoch=slot['registered_by_epoch'] if mode == 'claim' else None,
        owner_incarnation=slot['registered_by_incarnation'] if mode == 'claim' else None)
    job.pop('xmin')
    result = [('jobs', job), ('job_admissions', dict(job_id=job['job_id'],
        admission_ordinal=max(r['admission_ordinal'] for r in state['job_admissions']) + 1))]
    if mode == 'binding':
        request, = [r for r in state['producer_requests'] if r['request_sequence'] == 4]
        assert payload.endswith(b'\n') and payload.count(b'\n') == 1
        binding = {k: request[k] for k in ('dataset_id', 'producer_slot_id', 'producer_epoch',
            'request_sequence', 'request_digest', 'input_bytes', 'input_profile',
            'manifest_sha256', 'issuer_resource', 'issuer_epoch', 'issuer_incarnation')}
        binding.update(job_id=job['job_id'], shard_id=0,
            source_namespace='\\x' + hashlib.sha256(b'retirement/import-fixture/1\0' + payload).hexdigest(),
            source_bytes=len(payload), source_sha256='\\x' + hashlib.sha256(payload).hexdigest(),
            parser_profile=1, batch_first_record=1, batch_records=1, obligation_index=0,
            obligation_kind=1, logical_record=1, byte_start=0, byte_end=len(payload),
            record_sha256='\\x' + hashlib.sha256(payload).hexdigest())
        result.append(('producer_input_bindings', binding))
    return result


def assert_additions(before, after, records):
    assert before.keys() == after.keys()
    for table, old in before.items():
        expected = [row for name, row in records if name == table]
        assert all(row in after[table] for row in old), table
        actual = [dict(row) for row in after[table] if row not in old]
        assert len(actual) == len(expected), table
        for row in actual:
            row.pop('xmin')
        assert actual == expected, table


def guards(pg, database):
    assert lab.sql(pg, database, 'SHOW session_replication_role') == 'origin'
    value = json.loads(lab.sql(pg, database, "SELECT jsonb_agg(jsonb_build_object("
        "'table',c.relname,'name',t.tgname,'enabled',t.tgenabled) ORDER BY c.relname,t.tgname) "
        "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='control' AND NOT t.tgisinternal"))
    assert value and all(r['enabled'] in ('O', 'A') for r in value)
    assert ('producer_epochs', 'producer_epoch_guard') in {(r['table'], r['name']) for r in value}
    function = lab.sql(pg, database, "SELECT pg_get_functiondef("
        "'control.require_initial_retirement(bytea,numeric,bytea)'::regprocedure)")
    return dict(triggers=value, retirement_function_sha256=hashlib.sha256(function.encode()).hexdigest())


def run(pg, output, rows, ready, invoke, cases):
    payload = (output / 'signed-seed/event.jsonl').read_bytes()
    for mode in ('claim', 'binding', 'checkpoint'):
        name = 'retire_import_' + mode
        database, config, directory, _ = ready(name)
        before = rows(database)
        assert not before['producer_input_bindings'] and not before['producer_checkpoints']
        assert not any(r['kind'] == 'import' for r in before['jobs'])
        original_guards = guards(pg, database)
        records = additions(before, mode, payload)
        sql = []
        for table, row in records:
            literal = json.dumps(row).replace("'", "''")
            sql.append(f"INSERT INTO control.{table} SELECT * FROM jsonb_populate_record("
                f"NULL::control.{table},'{literal}'::jsonb);")
        # Privileged preparation models old, unsupported obligations, not a new
        # legal admission. SET LOCAL ends before every real product operation.
        lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
            + '\n'.join(sql) + ' COMMIT;')
        after = rows(database)
        assert_additions(before, after, records)
        assert guards(pg, database) == original_guards
        lab.write_json(output / (name + '-preparation.json'), dict(
            qualification='Synthetic retained relational obligation; not import execution',
            before=before, after=after, records=records, guards=original_guards))
        since = datetime.datetime.now(datetime.timezone.utc).isoformat()
        invoke(database, config, directory, name, error='Backend')
        terminal = json.loads((output / (name + '-after.json')).read_text())
        current = max((r for r in terminal['owner_epochs'] if r['role'] == 'control'),
            key=lambda r: r['epoch'])
        pid = current['control_backend_pid']
        assert isinstance(pid, int) and pid > 0
        logs = lab.docker('logs', '--since', since, pg['postgres'])
        lines = (logs.stdout + logs.stderr).splitlines()
        expected = ('original producer workflow claims remain unresolved' if mode == 'claim'
            else 'unsupported initial producer retirement history')
        errors = [line for line in lines if f'[{pid}] ERROR:  {expected}' in line]
        contexts = [line for line in lines if f'[{pid}] CONTEXT:  ' in line
            and 'function control.require_initial_retirement(bytea,numeric,bytea)' in line]
        assert len(errors) == len(contexts) == 1, 'expected retirement veto not observed'
        # Retain only this operation's exact error/context, never whole server logs.
        lab.write_json(output / (name + '-refusal.json'), dict(since=since, backend_pid=pid,
            backend_start=current['control_backend_start'], errors=errors, contexts=contexts))
        assert guards(pg, database) == original_guards
        cases[-1]['qualification'] = 'Synthetic retained import ' + mode + '; all product guards active'
        cases[-1]['postgres_refusal'] = expected
        lab.write_json(output / 'signed-recovery-cases.json', cases)
