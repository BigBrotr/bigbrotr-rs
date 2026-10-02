"""Full initial window: preserve four originals while observing later work."""
import copy
import hashlib
import json
import time

SECOND_ENDPOINT = 'ws://127.0.0.1:7448/installed-fixture'
SOURCE_TABLES = ('relay_endpoints', 'network_routes', 'relay_enrollments', 'acquisition_scopes',
    'relay_registration_commands', 'relay_enrollment_changes', 'acquisition_scope_commands')


def window_frames(event, artifact, *, distinct):
    from installed_sync_client import REFUSAL_SHA256
    assert hashlib.sha256(artifact).hexdigest() == REFUSAL_SHA256
    corpus = json.loads(artifact)
    selected, = [entry for entry in corpus['events'] if entry['name'] == 'included']
    second = selected['event']
    canonical = json.dumps([0, second['pubkey'], second['created_at'], second['kind'],
        second['tags'], second['content']], ensure_ascii=False, separators=(',', ':')).encode()
    assert canonical.hex() == selected['canonical_hex']
    assert hashlib.sha256(canonical).hexdigest() == second['id']
    assert second['id'] != event['id'] and second['kind'] == 1 and len(bytes.fromhex(second['sig'])) == 64
    # The later all-original round follows the refused new event. This avoids
    # inferring continued certification from data retained after an early yield.
    return ((event, second, event), ['signed', 'distinct-signed', 'duplicate']) if distinct else (
        (event, event, event), ['signed', 'duplicate', 'duplicate'])


def configure_window(pg, database, rows, output, name):
    import lab
    before = rows(database)
    expected = copy.deepcopy(before)
    statements = []
    for table in ('config_revisions', 'policy_revisions'):
        document = expected[table][0]['document']
        document['settings']['ingest.producer_outstanding_requests'] = 4
        encoded = json.dumps(document, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        digest = hashlib.sha256(b'bigbrotr/product-policy/2\0'+encoded).hexdigest()
        expected[table][0]['document_digest'] = '\\x'+digest
        statements.append(f"UPDATE control.{table} SET document=convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb,document_digest=decode('{digest}','hex')")
    # Same explicit pre-start clone policy setup used by installed.policy_setup;
    # no runtime authority, producer state, applied migration or old basis changes.
    lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '+';'.join(statements)+'; COMMIT;')
    after = rows(database)
    for table in before:
        if table in ('config_revisions', 'policy_revisions'):
            expected[table][0]['xmin'] = after[table][0]['xmin']
        assert expected[table] == after[table], table
    lab.write_json(output/(name+'-window-policy.json'), dict(before=before, after=after))


def validate_two_sources(before, after, initialized, observations, not_before):
    from initial_jobs import unchanged
    from installed_sync import validate_queued, validate_source
    from installed_sync_oracle import ENDPOINT
    changed = set(SOURCE_TABLES) | {'jobs', 'acquisition_jobs', 'job_admissions', 'management_commands', 'management_audit'}
    unchanged(before, after, changed)
    for table in changed:
        assert all(row in after[table] for row in before[table]), 'original row/xmin changed: '+table
        assert len(after[table]) == len(before[table])+(4 if table in ('management_commands', 'management_audit') else 1), table
    # The same visibility/access identity and exact selector are reused. Only
    # endpoint, route, enrollment, scope and their four command records are new.
    assert len(after['relay_access_contexts']) == len(after['acquisition_selectors']) == 1
    assert all(row['accepted_at_us'] >= not_before for row in after['management_commands']
        if row not in before['management_commands'])
    covered = {table: [] for table in SOURCE_TABLES}
    for ordinal, url, nonces, job_key in (
            (1, ENDPOINT, ('91', '92', 'a1'), 'b1'*16),
            (2, SECOND_ENDPOINT, ('93', '94', 'a2'), 'b2'*16)):
        wanted_keys = {nonce*16 for nonce in nonces} | {job_key}
        observed = [row for row in observations if row['key'] in wanted_keys]
        assert len(observed) == 4 and {row['key'] for row in observed} == wanted_keys
        registered, = [row['body'] for row in observed if row['path'] == 'relay-endpoints']
        endpoint_id = '\\x'+registered['endpoint_id']
        view = dict(after)
        for table in SOURCE_TABLES:
            view[table] = [row for row in after[table] if (
                row['request_key'] == '\\x'+nonces[2]*16 if table == 'acquisition_scope_commands'
                else row['endpoint_id'] == endpoint_id)]
            assert len(view[table]) == 1, (table, endpoint_id)
            covered[table].extend(view[table])
        validate_source(view, initialized, observed, url=url, nonces=nonces)
        validate_queued(view, observed, ordinal=ordinal, key=job_key, total_jobs=2)
        if ordinal == 2:
            validate_new_source_rows(view, observed, initialized, not_before)
    # Projection is only for reusing the complete per-command byte oracle: the
    # exact union must cover every real row, so foreign extras cannot disappear.
    canonical = lambda values: sorted(json.dumps(row, sort_keys=True) for row in values)
    for table in covered:
        assert canonical(covered[table]) == canonical(after[table]), 'unaccounted source row: '+table


def validate_new_source_rows(state, observations, initialized, lower_time):
    """Compare complete new operational source tuples; IDs/times are parameters."""
    receipts = {row['path']: row['body'] for row in observations}
    endpoint = '\\x'+receipts['relay-endpoints']['endpoint_id']
    scope = '\\x'+receipts['acquisition-scopes']['scope_id']
    access = '\\x'+receipts['relay-enrollments']['access_scope_id']
    route = '\\x'+receipts['relay-enrollments']['route_id']
    dataset = state['datasets'][0]['dataset_id']
    actor, = state['local_accounts']; session, = state['browser_sessions']
    owner, = [row for row in state['owner_epochs'] if row['role'] == 'control']
    attribution = dict(owner_resource_id=owner['resource_id'], owner_epoch=owner['epoch'],
        owner_incarnation=owner['process_incarnation'], owner_role='control')
    common = dict(dataset_id=dataset, config_revision=1, policy_revision=1, **attribution)
    for table, path, fields in (
            ('relay_endpoints', 'relay-endpoints', dict(endpoint_id=endpoint, normalization_version=1,
                url_bytes='\\x'+SECOND_ENDPOINT.encode().hex(),
                candidate_digest='\\x'+hashlib.sha256(SECOND_ENDPOINT.encode()).hexdigest())),
            ('acquisition_scopes', 'acquisition-scopes', dict(scope_id=scope, endpoint_id=endpoint,
                selector_id=state['acquisition_selectors'][0]['selector_id'], access_scope_id=access,
                visibility_kind=4, visibility_domain='\\x'+initialized['private_scope']['allocation_domain'],
                visibility_ordinal=3))):
        row, = state[table]
        timestamp = row['registered_at_us']
        assert lower_time <= timestamp <= int(receipts[path]['accepted_at_us'])
        assert {key:value for key,value in row.items() if key != 'xmin'} == dict(
            common, registered_at_us=timestamp, **fields)
    assert {key:value for key,value in state['network_routes'][0].items() if key != 'xmin'} == dict(
        route_id=route, dataset_id=dataset, endpoint_id=endpoint, route_version=1,
        route_bytes='\\x0104207f000001', actor_id=actor['principal_id'], request_key='\\x'+'94'*16)
    assert {key:value for key,value in state['relay_enrollments'][0].items() if key != 'xmin'} == dict(
        dataset_id=dataset, endpoint_id=endpoint, access_scope_id=access, route_id=route,
        enabled=True, generation=1, actor_id=actor['principal_id'], request_key='\\x'+'94'*16)
    for table, nonce, fields in (
            ('relay_registration_commands', '93', dict(endpoint_id=endpoint)),
            ('relay_enrollment_changes', '94', dict(endpoint_id=endpoint, access_scope_id=access,
                route_id=route, enabled=True, expected_generation=None, generation=1)),
            ('acquisition_scope_commands', 'a2', dict(scope_id=scope))):
        row, = state[table]
        omitted = {'xmin'}
        if table == 'relay_enrollment_changes':
            assert int(row['acceptance_transaction']) % (1 << 32) == int(row['xmin'])
            omitted.add('acceptance_transaction')
        expected = dict(dataset_id=dataset, actor_id=actor['principal_id'], request_key='\\x'+nonce*16,
            session_id=session['session_id'], **attribution, **fields)
        expected.update({'actor_'+field: actor[field] for field in (
            'credential_revision', 'authentication_revision', 'grant_revision')})
        assert {key:value for key,value in row.items() if key not in omitted} == expected
        command, = [value for value in state['management_commands'] if value['request_key'] == '\\x'+nonce*16]
        assert row['xmin'] == command['xmin']


def observe_window(pg, database, output, name, rows, filled, initialized, stage, filled_observed_at):
    import lab
    from initial_jobs import unchanged
    from installed import wait_for
    directory = output/name
    observations = stage('window-commands')
    assert len(filled['producer_requests']) == len(filled['producer_outcomes']) == 4
    assert filled['producer_epochs'][0]['outstanding_request_limit'] == 4
    assert filled['producer_epochs'][0]['reserved_through_request'] == 4
    source_case = name == 'sync-window-source'

    def observed_claim():
        assert not (directory/'relay-error.json').exists(), 'original relay failed'
        assert not (directory/'deferred-source/relay-error.json').exists(), 'deferred source was dialed'
        assert not output.joinpath(name+'.terminal.json').exists(), 'full window stopped initial owner'
        value = rows(database)
        assert value['producer_requests'] == filled['producer_requests']
        assert value['producer_outcomes'] == filled['producer_outcomes']
        assert value['producer_epochs'] == filled['producer_epochs']
        if source_case:
            pending, = [row for row in value['jobs'] if row['request_key'] == '\\x'+'b2'*16]
            if pending['state'] == 'accepted':
                return None
            assert pending['state'] == 'queued' and pending['job_revision'] == 2
        return value

    claimed = wait_for(observed_claim, 'full-window later job nomination', 5)
    lab.write_json(output/(name+'-window-claimed.json'), claimed)
    if source_case:
        validate_two_sources(filled, claimed, initialized, observations, filled_observed_at)
    else:
        unchanged(filled, claimed)
    released_us = time.time_ns()//1000
    (directory/'window-release').write_text('four original requests observed; send retained fixtures\n')

    def received():
        assert not (directory/'relay-error.json').exists()
        assert not (directory/'deferred-source/relay-error.json').exists()
        assert not output.joinpath(name+'.terminal.json').exists(), 'full window stopped initial owner'
        traces = [json.loads(path.read_text()) for path in sorted(directory.glob('relay-closed-*.json'))]
        traces = [trace for trace in traces if 'window_sequence' in trace]
        return traces if len(traces) >= 2 else None

    traces = wait_for(received, 'full-window event and subsequent certification rounds', 6)
    assert [trace['window_sequence'] for trace in traces] == [1, 2]
    for index, trace in enumerate(traces):
        assert trace['completion'] == 'request-and-websocket-close' and trace['sent_at_us'] >= released_us
        expected = ['signed', 'distinct-signed', 'duplicate', 'EOSE'] if not source_case and index == 0 else [
            'signed', 'duplicate', 'duplicate', 'EOSE']
        assert trace['frames'] == expected
    stable = rows(database); lab.write_json(output/(name+'-window-stable.json'), stable)
    unchanged(claimed, stable)
    now = int(lab.sql(pg, database, 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
    assert min(row['lease_expires_at_us'] for row in stable['owner_epochs']) > now
    assert stable['producer_epochs'][0]['reserved_through_request'] == 4
    (directory/'window-commands.continue').write_text('full-window preservation observed\n')
    return stable, dict(limit=4, filled_requests=4, later_claim_observed=source_case,
        source_command_count=4 if source_case else 0, released_at_us=released_us, rounds=traces,
        original_rows_xmin_unchanged=True, observed_at_us=now)


def validate_monitor(directory, state):
    monitor = json.loads((directory/'deferred-source/relay-monitor-ended.json').read_text())
    job, = [row for row in state['jobs'] if row['request_key'] == '\\x'+'b2'*16]
    assert not (directory/'deferred-source/relay-error.json').exists()
    assert monitor['connections'] == 0
    assert monitor['started_at_us'] <= job['created_at_us'] < monitor['finished_at_us']
    return monitor
