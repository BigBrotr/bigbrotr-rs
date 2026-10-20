"""Independent expected publication and acceptance, using complete retained state.

Only unpredictable identities/times are observed. Signed fields, native records,
receipts, batch digests, command digest and selected cuts are reconstructed.
No production codec, query or SQL authority function computes an expectation.
"""
import json

from bootstrap_oracle import cbor
from installed_sync_oracle import decode
from installed_archive_oracle import expected as archive_expected
from installed_archive_frontier import cut_digest, retained_inputs
from memory_oracle import event_expected
from memory_setup_io import require, sha, write
from memory_setup_client import ACTIONS

BX = '\\x'
SESSION_TABLES = {'owner_epochs', 'role_instances', 'browser_sessions', 'login_buckets', 'management_audit'}
PIN_TABLES = {'snapshot_leases', 'archive_snapshot_pins', 'retention_claims'}
ACCEPT_TABLES = SESSION_TABLES | PIN_TABLES | {
    'management_commands', 'jobs', 'job_input_bounds', 'job_admissions', 'export_jobs'}
PUBLICATION_TABLES = {
    'datasets', 'owner_epochs', 'role_instances', 'topology_generations', 'topology_streams',
    'source_streams', 'allocation_domains', 'producer_slots', 'producer_epochs',
    'producer_requests', 'producer_outcomes', 'browser_sessions', 'login_buckets',
    'management_audit', 'management_commands', 'relay_endpoints', 'relay_registration_commands',
    'relay_enrollments', 'network_routes', 'relay_access_contexts', 'relay_enrollment_changes',
    'acquisition_selectors', 'acquisition_scopes', 'acquisition_scope_commands', 'jobs',
    'acquisition_jobs', 'job_admissions', 'archive_attempts', 'archive_owner_sessions',
    'archive_frontier_snapshots', 'archive_frontier_batches', 'consumers',
    'consumer_frontier_revisions', 'consumer_frontiers'}


def one(rows):
    require(len(rows) == 1, 'exactly one expected row')
    return rows[0]


def clean(row):
    return {key: value for key, value in row.items() if key != 'xmin'}


def unchanged(before, after, allowed):
    require(set(before) == set(after) and len(after) == 75, 'whole 75-table universe')
    for table in before.keys() - allowed:
        require(before[table] == after[table], 'unexpected complete-table/xmin change: ' + table)


def append_only(before, after, tables):
    for table in tables:
        require(all(row in after[table] for row in before[table]), 'old row/xmin rewritten: ' + table)


def released(before, after):
    append_only(before, after, {'owner_epochs', 'role_instances'})
    owner = one([row for row in after['owner_epochs'] if row not in before['owner_epochs']])
    require(owner['role'] == 'control' and owner['state'] == 'released'
            and owner['released_at_us'] is not None and owner['exclusion_method'] == 'storage_fenced'
            and owner['exclusion_digest'] is not None, 'actual Control release')
    role = one([row for row in after['role_instances'] if row not in before['role_instances']])
    require(role['role_instance_id'] == owner['role_instance_id'] and role['retired_at_us'] is not None,
            'actual Control incarnation retired')
    return owner


def session_lifecycle(before, after, principal, extra_actions):
    old = one(before['browser_sessions'])
    new = one(after['browser_sessions'])
    require(old['revoked_at_us'] is not None and new['revoked_at_us'] is not None
            and new['session_id'] != old['session_id']
            and new['principal_id'] == old['principal_id'] == principal
            and new['issued_at_us'] >= old['revoked_at_us'], 'exact revoked browser replacement')
    added = [row for row in after['management_audit'] if row not in before['management_audit']]
    require(sorted(row['action'] for row in added) ==
            sorted(['browser.session.issue', 'browser.session.revoke', *extra_actions]),
            'complete independently expected audit actions')
    require(all(row['actor_id'] == principal and row['outcome'] == 'accepted' for row in added),
            'actual authenticated audit attribution')
    return new


def publication(io, initial, published, initialized, native, readback, transcript, source_receipts):
    state, catalog = published['control'], published['catalog']
    unchanged(initial['control'], state, PUBLICATION_TABLES)
    append_only(initial['control'], state, set(state) - {'datasets'})
    for table in ('schema_versions', 'config_revisions', 'policy_revisions', 'principals',
                  'local_accounts', 'local_credentials', 'principal_grants', 'owner_challenges'):
        require(state[table] == initial['control'][table], 'genuine initialized authority preserved: ' + table)
    for table in ('bigbrotr_archive.installation', 'bigbrotr_archive.scientific_schema',
                  'public.ducklake_schema', 'public.ducklake_table', 'public.ducklake_column'):
        require(catalog[table] == initial['catalog'][table], 'original native schema preserved: ' + table)
    event = event_expected(dict(kind='frozen-archive-recipe', path=io.spec['recipe']['path'],
                               sha256=io.spec['recipe']['sha256']))
    actual = [(row['cf'], bytes.fromhex(row['key']), bytes.fromhex(row['value'])) for row in native['rows']]
    facts = {int.from_bytes(key[-8:], 'big'): value for family, key, value in actual
             if family == 'facts' and key[:2] == b'\x01\x20'}
    fact_prefix = b'bigbrotr/fact/1\0'

    def fact(sequence):
        raw = facts[sequence][34:]
        require(raw.startswith(fact_prefix), 'native fact domain')
        return decode(raw[len(fact_prefix):], maximum=262144)

    dictionary_time, recorded = fact(4)[4], fact(8)[4]
    paths = sorted((io.root / 'store/producer-inputs-v1').glob('*.cbor'))
    inputs = [path.read_bytes() for path in paths]
    require(len(inputs) == 4, 'independent initial request cardinality')
    prefix = b'bigbrotr/producer-request/1\0'
    require(inputs[-1].startswith(prefix), 'native event request domain')
    received = decode(inputs[-1][len(prefix):], maximum=262144)[3][5][0]
    first = transcript[0]
    require(first['frames'] == ['signed', 'duplicate', 'EOSE']
            and first['completion'] == 'request-and-websocket-close', 'actual complete large relay round')
    require(int(source_receipts['acquisition']['accepted_at_us']) <= dictionary_time <= first['connected_at_us']
            and first['sent_at_us'] <= received <= first['closed_at_us'] and received <= recorded,
            'independently bounded real receive/record times')
    body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
            event['kind'], event['tags'], event['content']]
    wanted = archive_expected(state, initialized, event, dictionary_time, received, recorded,
        first['clock_resolution_us'], native_options=dict(expected_body_digest=sha(b'bigbrotr/body/1\0' + cbor(body))),
        fact_bytes=262144)
    require(actual == wanted['native'] and inputs == wanted['inputs'], 'entire independently encoded native/input bytes')
    requests = sorted(state['producer_requests'], key=lambda row: row['request_sequence'])
    outcomes = sorted(state['producer_outcomes'], key=lambda row: row['request_sequence'])
    require(len(requests) == len(outcomes) == len(inputs), 'complete original native outcomes')
    scope = one(state['acquisition_scopes'])
    acquisition = one(state['acquisition_jobs'])
    endpoint, enrollment = one(state['relay_endpoints']), one(state['relay_enrollments'])
    visibility = initialized['private_scope']
    normalized = b'ws://127.0.0.1:7447/installed-fixture'
    require(endpoint['url_bytes'] == BX + normalized.hex() and endpoint['candidate_digest'] == BX + sha(normalized)
            and endpoint['endpoint_id'] == scope['endpoint_id'] == enrollment['endpoint_id']
                == BX + source_receipts['endpoint']['endpoint_id']
            and scope['access_scope_id'] == enrollment['access_scope_id']
                == BX + source_receipts['enrollment']['access_scope_id']
            and enrollment['enabled'] and enrollment['generation'] == 1
            and scope['visibility_kind'] == 4 and scope['visibility_domain'] == BX + visibility['allocation_domain']
            and scope['visibility_ordinal'] == int(visibility['ordinal']), 'actual endpoint/access/visibility binding')
    selector, access, route = (one(state[table]) for table in
                              ('acquisition_selectors', 'relay_access_contexts', 'network_routes'))
    canonical = b'{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}'
    require(selector['document'] == BX + canonical.hex() and selector['candidate_digest'] == BX + sha(canonical)
            and selector['selector_id'] == scope['selector_id']
            and access['access_scope_id'] == scope['access_scope_id'] and access['access_class'] == 'public'
            and access['allocation_domain'] == scope['visibility_domain'] and access['ordinal'] == scope['visibility_ordinal']
            and route['route_id'] == enrollment['route_id'] and route['route_version'] == 1
            and route['route_bytes'] == BX + '0104207f000001', 'literal selector and no-auth private loopback route')
    require(scope['scope_id'] == BX + source_receipts['scope']['scope_id']
            and acquisition['scope_id'] == scope['scope_id']
            and acquisition['job_id'] == BX + source_receipts['acquisition']['job_id'],
            'observed authenticated acquisition binding')
    for n, (request, outcome, original, receipt) in enumerate(zip(requests, outcomes, inputs, wanted['receipts']), 1):
        require(request['request_sequence'] == n and request['request_digest'] == BX + sha(original)
                and request['input_bytes'] == len(original) and outcome['outcome_bytes'] == BX + receipt.hex()
                and outcome['outcome_sha256'] == BX + sha(receipt), 'actual native receipt and input')
        require(request['acquisition_job_id'] == (acquisition['job_id'] if n == len(inputs) else None)
                and request['acquisition_scope_id'] == (scope['scope_id'] if n == len(inputs) else None),
                'job-bound admission attribution')
        fields = ('relay_endpoint_id', 'relay_access_scope_id', 'relay_enrollment_generation', 'relay_route_id')
        require(tuple(request[key] for key in fields) ==
                ((endpoint['endpoint_id'], access['access_scope_id'], 1, route['route_id'])
                    if n == len(inputs) else (None,) * 4), 'complete retained network attribution')
    attempts = sorted(state['archive_attempts'], key=lambda row: row['ordinal'])
    native_attempts = sorted(catalog['bigbrotr_archive.attempts'], key=lambda row: row['ordinal'])
    require(len(attempts) == len(native_attempts) == len(wanted['batches']), 'all real publication attempts')
    dataset = one(state['datasets'])['dataset_id']
    topology = int(initialized['topology_generation'])
    receipts = []
    for n, (attempt, stored, batch) in enumerate(zip(attempts, native_attempts, wanted['batches']), 1):
        require(attempt['ordinal'] == n and attempt['expected_predecessor'] == n-1
                and attempt['batch_digest'] == BX + batch['archive_digest']
                and attempt['dataset_id'] == dataset and attempt['topology_generation'] == topology,
                'independent publication batch digest')
        require(all(stored[key] == attempt[key] for key in (
            'attempt_id', 'ordinal', 'coordinator_incarnation', 'worker_incarnation', 'batch_digest', 'topology_generation')),
            'actual matching native publication attempt')
        receipts.append(dict(dataset_id=dataset[2:].upper(), schema_version=1, topology_generation=topology,
            batch_digest=batch['archive_digest'].upper(), record_count=batch['last']-batch['first']+1,
            input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=batch['first'], last_sequence=batch['last'])],
            attempt_id=attempt['attempt_id'][2:].upper(), publication_ordinal=n))
    require(readback == dict(format='bigbrotr-independent-installed-archive-read/1',
        tables={**wanted['tables'], 'archive_commits': receipts}), 'all independent signed science and native receipts')
    # Snapshot and frontier selection derives from this genuine installation and
    # these expected batches. It never imports the old E_public snapshot/cut.
    baseline = one(initial['catalog']['bigbrotr_archive.scientific_schema'])['initialization_snapshot']
    consumer = one(state['consumers'])
    require(consumer['consumer_id'] == BX + io.config['archive']['catalog_id']
            and consumer['dataset_id'] == dataset and consumer['consumer_role'] == 'archive',
            'actual initialized Archive consumer')
    revisions = sorted(state['consumer_frontier_revisions'], key=lambda row: row['frontier_revision'])
    snapshots = sorted(state['archive_frontier_snapshots'], key=lambda row: row['frontier_revision'])
    bounds = sorted(state['consumer_frontiers'], key=lambda row: row['frontier_revision'])
    require(len(revisions) == len(snapshots) == len(bounds) > 0, 'complete single-stream frontier history')
    endpoints = [batch['last'] for batch in wanted['batches']]
    prior = 0
    for revision, snapshot, bound in zip(revisions, snapshots, bounds):
        through = bound['last_complete_sequence']
        require(through in endpoints and through > prior, 'frontier only on complete independent batch boundary')
        count = endpoints.index(through) + 1
        require(revision['sealed'] and revision['frontier_revision'] == snapshot['frontier_revision'] == bound['frontier_revision']
                and snapshot['snapshot_id'] == baseline + count and bound['shard_id'] == 0 and bound['owner_epoch'] == 1,
                'observed sealed same-stream native frontier')
        require(snapshot['request_digest'] == BX + cut_digest(dataset[2:], topology, receipts[:count], through, baseline+count),
                'independent complete archive-cut digest')
        selected = [row for row in state['archive_frontier_batches'] if row['frontier_revision'] == revision['frontier_revision']]
        require({row['attempt_id'] for row in selected} == {row['attempt_id'] for row in attempts[:count]},
                'exact frontier receipt set')
        require({row['xmin'] for row in [revision, snapshot, bound, *selected]} == {revision['xmin']},
                'atomic frontier publication including xmin')
        prior = through
    require(prior == endpoints[-1] and consumer['current_revision'] == revisions[-1]['frontier_revision'],
            'full current positive frontier')
    selected = snapshots[-1]
    counts = ('dictionary_count', 'payload_count', 'eligibility_count', 'receive_count', 'provenance_count')
    require([selected[key] for key in counts] == [len(wanted['tables'][key]) for key in
            ('dictionaries', 'events', 'event_eligibility_facts', 'event_receive_facts', 'provenance_facts')],
            'complete native family counts')
    require(max(row['snapshot_id'] for row in catalog['public.ducklake_snapshot']) == selected['snapshot_id'],
            'actual catalog final snapshot')
    require(not state['export_jobs'], 'no Export accepted by initial Serve')
    retained = retained_inputs(io.root, state, wanted)
    query = dict(dataset=dataset[2:], scope=initialized['private_scope'], event_id=event['id'],
        topology_generation=str(topology), snapshot_id=selected['snapshot_id'],
        bounds=[dict(shard_id=row['shard_id'], owner_epoch=row['owner_epoch'],
            complete_through=row['last_complete_sequence']) for row in bounds if row['frontier_revision'] == selected['frontier_revision']])
    result = dict(query=query, catalog=consumer['consumer_id'][2:],
        frontier_revision=selected['frontier_revision'], retained_inputs=retained,
        expected_tables=wanted['tables'], expected_receipts=receipts,
        original_input_sha256=[sha(value) for value in wanted['inputs']],
        native_row_count=len(wanted['native']))
    write(io.root / 'independent-publication.json', result)
    return result


def grants(before, after, principal, scope, selection, receipts):
    allowed = SESSION_TABLES | PIN_TABLES | {'local_accounts', 'management_commands', 'scope_grants', 'scope_grant_changes'}
    unchanged(before, after, allowed)
    append_only(before, after, PIN_TABLES | {'management_commands', 'management_audit'})
    released(before, after)
    session_lifecycle(before, after, BX + principal,
        ['browser.session.reauthenticate', 'scope-grant.change'] * len(ACTIONS))
    require(not before['scope_grants'] and not before['scope_grant_changes'] and not after['export_jobs'],
            'fresh real grants without Export acceptance')
    require(len(after['scope_grants']) == len(after['scope_grant_changes']) == len(receipts) == len(ACTIONS),
            'exact three scope grants')
    original = one(before['local_accounts'])
    account = one(after['local_accounts'])
    require({k: v for k, v in clean(account).items() if k != 'grant_revision'} ==
            {k: v for k, v in clean(original).items() if k != 'grant_revision'}
            and account['grant_revision'] == original['grant_revision'] + 3, 'only authentic grant revision changes')
    for action, receipt in zip(ACTIONS, receipts):
        row = one([row for row in after['scope_grants'] if row['action'] == action])
        change = one([row for row in after['scope_grant_changes'] if row['action'] == action])
        require(row['principal_id'] == row['actor_id'] == change['principal_id'] == change['actor_id'] == BX + principal
                and row['enabled'] and row['generation'] == change['scope_generation'] == 1
                and row['dataset_id'] == BX + scope['dataset_id'] and row['dictionary_kind'] == 4
                and row['allocation_domain'] == BX + scope['allocation_domain'] and row['ordinal'] == int(scope['ordinal'])
                and row['request_key'] == change['request_key'] == BX + receipt['key']
                and change['catalog_id'] == BX + selection['catalog']
                and change['frontier_revision'] == selection['frontier_revision']
                and change['proof_id'] == BX + receipt['proof']['proof_id'], 'actual authenticated grant binding')
    require(not after['reauthentication_proofs'], 'proofs consumed')
    require(len(after['management_commands']) == len(before['management_commands']) + len(ACTIONS),
            'exact grant command count')
    new_pins = [row for row in after['archive_snapshot_pins'] if row not in before['archive_snapshot_pins']]
    require(len(new_pins) == len(ACTIONS)
            and {row['lease_id'] for row in new_pins} ==
                {row['pin_lease_id'] for row in after['scope_grant_changes']}
            and all(row['purpose'] == 'reader' and row['export_job_id'] is None for row in new_pins),
            'each grant has its distinct actual visibility reader pin')
    for table in ('snapshot_leases', 'retention_claims'):
        added = [row for row in after[table] if row not in before[table]]
        require(len(added) == len(ACTIONS) and all(row['state'] == 'released' for row in added)
                and {row['lease_id'] for row in added} == {row['lease_id'] for row in new_pins},
                'all grant reader obligations released')


def acceptance(before, after, case, selection, events):
    unchanged(before, after, ACCEPT_TABLES)
    append_only(before, after, ACCEPT_TABLES - {'browser_sessions', 'login_buckets', 'owner_epochs', 'role_instances'})
    owner = released(before, after)
    require(events[-1] == dict(event='export-accept-setup-complete', name=case['name'], status='COMPLETE',
        state='accepted', reservation='not-performed', execution='not-performed',
        event_presence='not-inferred-from-acceptance'), 'acceptance completed after actual joins')
    markers = {row['name']: row['fields'] for row in events[:-1]}
    require(len(markers) == len(events)-1, 'no duplicate caller lifecycle markers')
    for name in ('catalog-driver-joined', 'control-driver-joined'):
        require(markers[name] == dict(natural=True, failed=False), 'natural driver join')
    require(markers['grants-released'] == dict(all_released=True), 'actual grant release')
    require(markers['control-release-returned'] == dict(committed=True, error=None)
            and markers['accept-setup-returned'] == dict(error=None, cleanup_error=None,
                scope='acceptance-only', event_presence='not-inferred-from-acceptance'),
            'known release and complete setup lifecycle')
    returned = markers['accept-returned']
    require(returned['commit_started'] and returned['error'] is None, 'known acceptance COMMIT')
    receipt = returned['receipt']
    require(receipt['created'] and receipt['command_key'] == case['command_key'], 'actual retained command receipt')
    observed = markers['accepted-job']
    require(observed['receipt'] == receipt and observed['retry'] == dict(receipt, created=False)
            and observed['state'] == 'accepted' and observed['revision'] == 1 and observed['reservation'] is None
            and observed['catalog'] == selection['catalog']
            and observed['frontier_revision'] == selection['frontier_revision']
            and observed['query'] == selection['query'], 'same-key no-reservation readback')
    job = one([row for row in after['jobs'] if row not in before['jobs']])
    detail = one(after['export_jobs'])
    actor = one(after['local_accounts'])['principal_id']
    session = session_lifecycle(before, after, actor, ['job.export.accept'])
    require(job['job_id'] == detail['job_id'] == BX + receipt['job_id']
            and job['state'] == 'accepted' and job['job_revision'] == 1 and job['checkpoint_revision'] == 0
            and job['input_complete'] and job['export_job_protocol'] == 1
            and all(job[key] is None for key in ('owner_resource_id', 'owner_epoch', 'owner_incarnation', 'completed_at_us'))
            and job['principal_id'] == detail['actor_id'] == actor, 'exact unreserved Accepted revision1')
    admission = one([row for row in after['job_admissions'] if row not in before['job_admissions']])
    require(admission['job_id'] == job['job_id']
            and admission['admission_ordinal'] == max(row['admission_ordinal'] for row in before['job_admissions']) + 1,
            'one actual next committed job admission')
    command = one([row for row in after['management_commands'] if row not in before['management_commands']])
    audit = one([row for row in after['management_audit'] if row['action'] == 'job.export.accept'])
    raw = lambda value: bytes.fromhex(value)
    be = lambda value, width=8: int(value).to_bytes(width, 'big')
    body = case['command']
    scope = body['query']['scope']
    digest = sha(b'bigbrotr/export-event/1\0' + raw(case['dataset']) + raw(actor[2:]) + raw(case['command_key'])
        + raw(case['dataset']) + be(4, 2) + raw(scope['allocation_domain']) + be(scope['ordinal'])
        + raw(selection['query']['event_id']) + be(body['query'].get('page_size', 100), 2)
        + raw(selection['catalog']) + be(selection['frontier_revision']) + raw(body['destination']['id'])
        + raw(body['destination']['binding_sha256']) + b'\x01\x01'
        + be(body['expected_config_revision']) + be(body['expected_policy_revision']))
    require(command['request_digest'] == job['request_digest'] == BX + digest
            and command['action'] == audit['action'] == 'job.export.accept'
            and command['audit_id'] == audit['audit_id']
            and command['accepted_at_us'] == audit['created_at_us'] == job['created_at_us'] == receipt['accepted_at_us']
            and command['request_key'] == detail['request_key'] == audit['subject_id'] == BX + case['command_key'],
            'independent command/audit/job receipt identity')
    require(detail['catalog_id'] == BX + selection['catalog']
            and detail['dataset_id'] == job['dataset_id'] == BX + case['dataset']
            and detail['format'] == 'signed-events-jsonl' and detail['visibility'] == 'private'
            and detail['dictionary_kind'] == 4 and detail['page_size'] == body['query'].get('page_size', 100)
            and detail['session_id'] == session['session_id']
            and detail['frontier_revision'] == selection['frontier_revision']
            and detail['event_id'] == BX + selection['query']['event_id']
            and detail['allocation_domain'] == BX + scope['allocation_domain']
            and detail['ordinal'] == int(scope['ordinal'])
            and detail['destination_id'] == BX + body['destination']['id']
            and detail['destination_binding_sha256'] == BX + body['destination']['binding_sha256'],
            'immutable exact selection and real destination binding')
    bounds = [row for row in after['job_input_bounds'] if row['job_id'] == job['job_id']]
    require(sorted((row['shard_id'], row['owner_epoch'], row['last_complete_sequence']) for row in bounds) ==
            sorted((row['shard_id'], row['owner_epoch'], row['complete_through']) for row in selection['query']['bounds']),
            'entire selected frontier retained')
    pins = [row for row in after['archive_snapshot_pins'] if row not in before['archive_snapshot_pins']]
    require(len(pins) == 2 and {row['purpose'] for row in pins} == {'reader', 'export'}, 'distinct reader/export pins')
    for table in ('snapshot_leases', 'retention_claims'):
        require({row['lease_id'] for row in after[table] if row not in before[table]} ==
                {row['lease_id'] for row in pins}, 'no unrelated added pin obligation: ' + table)
    export = one([row for row in pins if row['purpose'] == 'export'])
    reader = one([row for row in pins if row['purpose'] == 'reader'])
    require(export['lease_id'] == detail['pin_lease_id'] and reader['lease_id'] == detail['allocation_pin_id']
            and export['export_job_id'] == job['job_id'] and reader['export_job_id'] is None, 'true immutable retention pin')
    for pin in pins:
        lease = one([row for row in after['snapshot_leases'] if row['lease_id'] == pin['lease_id']])
        require(pin['requested_ttl_us'] == 30_000_000 and pin['issuer_role'] == 'control'
                and pin['issuer_resource'] == owner['resource_id'] and pin['issuer_epoch'] == owner['epoch']
                and pin['issuer_incarnation'] == owner['process_incarnation']
                and pin['dataset_id'] == lease['dataset_id'] == BX + case['dataset']
                and pin['consumer_id'] == lease['consumer_id']
                and pin['frontier_revision'] == lease['frontier_revision']
                and lease['archive_pin_protocol'] == 1 and lease['holder_kind'] == 'role_instance'
                and lease['holder_role'] == 'control' and lease['holder_job_id'] is None
                and lease['holder_role_instance_id'] == owner['role_instance_id']
                and lease['holder_incarnation'] == owner['process_incarnation']
                and lease['consumer_id'] == BX + selection['catalog']
                and lease['frontier_revision'] == selection['frontier_revision']
                and lease['expires_at_us'] <= owner['lease_expires_at_us']
                and lease['state'] == ('active' if pin['purpose'] == 'export' else 'released'),
                'actual holder/lease bound; no expiry extension inferred')
        require(lease['lease_revision'] == (1 if pin['purpose'] == 'export' else 2),
                'no hidden renewal of the original pin')
        claims = [row for row in after['retention_claims'] if row['lease_id'] == pin['lease_id']]
        require(sorted((row['shard_id'], row['owner_epoch'], row['first_sequence'], row['last_sequence']) for row in claims) ==
                sorted((row['shard_id'], row['owner_epoch'], 1, row['complete_through']) for row in selection['query']['bounds']),
                'complete real retention claims')
        require(all(row['state'] == lease['state'] and row['purpose'] == pin['purpose'] for row in claims),
                'retention lifecycle')
        if pin['purpose'] == 'export':
            require({lease['xmin'], *[row['xmin'] for row in claims]} == {job['xmin']},
                    'export lease and complete claims atomically accepted')
    require({job['xmin'], detail['xmin'], command['xmin'], audit['xmin'], export['xmin'],
             admission['xmin'], *[row['xmin'] for row in bounds]} == {job['xmin']},
            'one acceptance transaction including xmin')
    require(not after['export_execution_units'] and not after['artifacts'] and not after['artifact_files'],
            'no reservation, execution unit or artifact')
    return receipt['job_id']
