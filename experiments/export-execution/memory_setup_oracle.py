"""Independent expected publication and acceptance, using complete retained state.

Only unpredictable identities/times are observed. Signed fields, native records,
receipts, batch digests, command digest and selected cuts are reconstructed.
No production codec, query or SQL authority function computes an expectation.
"""
import json
from pathlib import Path

from bootstrap_oracle import cbor
from installed_sync_oracle import decode
from installed_archive_oracle import expected as archive_expected
from installed_archive_frontier import cut_digest, retained_inputs
from memory_oracle import event_expected, verify_signed_files
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
    kind = io.spec.get('event_kind', 'frozen-archive-recipe')
    event = event_expected(dict(kind=kind, path=io.spec['recipe']['path'],
                               sha256=io.spec['recipe']['sha256']))
    fact_bytes = 524288 if kind == 'frozen-signed-event' else 262144
    decoder_options = dict(collection_items=65536, max_nodes=131072) if kind == 'frozen-signed-event' else {}
    actual = [(row['cf'], bytes.fromhex(row['key']), bytes.fromhex(row['value'])) for row in native['rows']]
    facts = {int.from_bytes(key[-8:], 'big'): value for family, key, value in actual
             if family == 'facts' and key[:2] == b'\x01\x20'}
    fact_prefix = b'bigbrotr/fact/1\0'

    def fact(sequence):
        raw = facts[sequence][34:]
        require(raw.startswith(fact_prefix), 'native fact domain')
        return decode(raw[len(fact_prefix):], maximum=fact_bytes, **decoder_options)

    dictionary_time, recorded = fact(4)[4], fact(8)[4]
    paths = sorted((io.root / 'store/producer-inputs-v1').glob('*.cbor'))
    inputs = [path.read_bytes() for path in paths]
    require(len(inputs) == 4, 'independent initial request cardinality')
    prefix = b'bigbrotr/producer-request/1\0'
    require(inputs[-1].startswith(prefix), 'native event request domain')
    received = decode(inputs[-1][len(prefix):], maximum=fact_bytes, **decoder_options)[3][5][0]
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
        fact_bytes=fact_bytes, decoder_options=decoder_options)
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


def installed_five(before_capture, after_capture, *, selection, principal,
                   commands, statuses, destination, science):
    """Audit five sequential ordinary-Serve exports after its actual shutdown.

    Commands are five {key, command, receipt} records; command is the exact HTTP
    POST body including kind. Statuses are the corresponding terminal GET bodies.
    The prepared selection/science and both complete captures are caller-pinned
    independent inputs. One login/logout and one new Control lifetime are required.
    Existing jobs, including acquisition jobs, remain byte/xmin identical.

    This returns SQL/file evidence only. The caller must separately authenticate
    physical native roots, old output files, backend/process/cgroup closure and
    memory observations before making an installed-runtime qualification claim.
    """
    require(len(commands) == len(statuses) == 5, 'exactly five installed commands and statuses')
    require(set(before_capture) == set(after_capture) == {'control', 'catalog'},
            'complete Control/catalog captures')
    require(before_capture['catalog'] == after_capture['catalog'], 'entire catalog/xmin preserved')
    before, after = before_capture['control'], after_capture['control']
    allowed = ACCEPT_TABLES | {'export_execution_units', 'artifacts', 'artifact_files'}
    unchanged(before, after, allowed)
    for state in (before, after):
        require(sum(len(rows) for rows in state.values()) <= 32768, 'finite installed laboratory capture')
        for rows in state.values():
            require(len({json.dumps(row, sort_keys=True) for row in rows}) == len(rows)
                    and all(isinstance(row.get('xmin'), str) and row['xmin'].isdecimal()
                            and int(row['xmin']) > 0 for row in rows), 'unique complete rows with xmin')
    append_only(before, after, allowed - {'browser_sessions', 'login_buckets'})
    added = {table: [row for row in after[table] if row not in before[table]] for table in allowed}

    def ident(value, size=16):
        require(isinstance(value, str) and len(value) == size * 2
                and bytes.fromhex(value).hex() == value and int(value, 16) > 0,
                'canonical nonzero observed identity')
        return value

    def number(value):
        require(type(value) is int and value > 0, 'positive observed integer')
        return value

    def same(row, expected, label):
        require(json.dumps(clean(row), sort_keys=True) == json.dumps(expected, sort_keys=True), label)

    def find(table, key, value):
        return one([row for row in added[table] if row[key] == value])

    query = selection['query']
    require(set(query) == {'dataset', 'scope', 'event_id', 'topology_generation', 'snapshot_id', 'bounds'}
            and 1 <= len(query['bounds']) <= 1024, 'closed bounded prepared query')
    require(science['query'] == query and science['catalog'] == selection['catalog']
            and science['frontier_revision'] == selection['frontier_revision'], 'same independent science selection')
    dataset, actor, catalog = BX + ident(query['dataset']), BX + ident(principal), BX + ident(selection['catalog'])
    scope, frontier = query['scope'], number(selection['frontier_revision'])
    require(set(scope) == {'dataset_id', 'dictionary_kind', 'allocation_domain', 'ordinal'}
            and scope['dataset_id'] == query['dataset'] and scope['dictionary_kind'] == 4,
            'prepared qualified visibility')
    topology = int(query['topology_generation'])
    require(str(topology) == query['topology_generation'] and topology >= 0, 'canonical topology generation')
    bound_values = [(row['shard_id'], row['owner_epoch'], row['complete_through']) for row in query['bounds']]
    require(len(set(bound_values)) == len(bound_values)
            and all(set(row) == {'shard_id', 'owner_epoch', 'complete_through'} for row in query['bounds']),
            'complete unique prepared bounds')
    claims_per_pin = sum(last > 0 for _, _, last in bound_values)
    counts = dict(jobs=5, export_jobs=5, job_admissions=5, management_commands=5,
        management_audit=7, job_input_bounds=5 * len(bound_values),
        export_execution_units=5, artifacts=5, artifact_files=5,
        owner_epochs=1, role_instances=1, snapshot_leases=15, archive_snapshot_pins=15,
        retention_claims=15 * claims_per_pin)
    require(all(len(added[table]) == count for table, count in counts.items()), 'exact installed SQL delta cardinalities')
    current = one(before['datasets'])
    revisions = dict(configuration=str(current['config_revision']), policy=str(current['policy_revision']))
    require(current['dataset_id'] == dataset and science['accepted_revisions'] == revisions,
            'actual unchanged accepted revisions')
    policy = one([row for row in before['policy_revisions'] if row['revision'] == current['policy_revision']])
    settings = policy['document']['settings']
    require(settings['export.memory.max_bytes'] == 268435456, 'actual accepted 256MiB export policy')
    account = one([row for row in before['local_accounts'] if row['principal_id'] == actor])
    require(account['active'] and account['dataset_id'] == dataset, 'unchanged active enrolled principal')
    snapshot = one([row for row in before['archive_frontier_snapshots']
                    if row['consumer_id'] == catalog and row['frontier_revision'] == frontier])
    require(snapshot['dataset_id'] == dataset and snapshot['catalog_id'] == catalog
            and snapshot['snapshot_id'] == query['snapshot_id'] and snapshot['topology_generation'] == topology
            and sorted((row['shard_id'], row['owner_epoch'], row['last_complete_sequence'])
                for row in before['consumer_frontiers']
                if row['consumer_id'] == catalog and row['frontier_revision'] == frontier) == sorted(bound_values),
            'independent selection equals actual retained frontier')
    for action in ACTIONS:
        grant = one([row for row in before['scope_grants'] if row['principal_id'] == actor
                     and row['action'] == action and row['dictionary_kind'] == 4
                     and row['allocation_domain'] == BX + scope['allocation_domain']
                     and row['ordinal'] == int(scope['ordinal'])])
        require(grant['dataset_id'] == dataset and grant['enabled'], 'actual unchanged scoped authority')
    owner = released(before, after)
    prior_owners = [row for row in before['owner_epochs'] if row['role'] == 'control']
    require(owner['dataset_id'] == dataset and owner['control_fence_version'] == 1
            and number(owner['control_backend_pid']) and owner['control_backend_start']
            and owner['acquired_at_us'] < owner['released_at_us']
            and owner['epoch'] == max(row['epoch'] for row in prior_owners) + 1
            and {row['resource_id'] for row in prior_owners} == {owner['resource_id']}
            and not any(row['role'] == 'control' and row['state'] == 'active' for row in after['owner_epochs']),
            'one actual terminal Control lifetime')
    for field in ('resource_id', 'process_incarnation', 'role_instance_id', 'exclusion_digest'):
        ident(owner[field].removeprefix(BX), 32 if field == 'exclusion_digest' else 16)
    same(owner, dict(dataset_id=dataset, role='control', resource_id=owner['resource_id'],
        epoch=owner['epoch'], role_instance_id=owner['role_instance_id'],
        process_incarnation=owner['process_incarnation'], state='released',
        acquired_at_us=number(owner['acquired_at_us']), lease_expires_at_us=number(owner['lease_expires_at_us']),
        released_at_us=number(owner['released_at_us']), exclusion_method='storage_fenced',
        exclusion_digest=owner['exclusion_digest'], archive_frontier_protocol=None,
        control_backend_pid=owner['control_backend_pid'], control_backend_start=owner['control_backend_start'],
        control_fence_version=1), 'closed actual Control ownership')
    role = one(added['role_instances'])
    same(role, dict(role_instance_id=owner['role_instance_id'], dataset_id=dataset, role='control',
        created_at_us=number(role['created_at_us']), retired_at_us=number(role['retired_at_us'])),
        'closed retired Control incarnation')
    require(role['dataset_id'] == dataset and role['role'] == 'control'
            and role['created_at_us'] <= owner['acquired_at_us'] <= owner['released_at_us'] <= role['retired_at_us']
            and role['xmin'] == owner['xmin'], 'atomic owner/incarnation retirement')
    session = session_lifecycle(before, after, actor, ['job.export.accept'] * 5)
    require(session['dataset_id'] == dataset
            and session['credential_revision'] == account['credential_revision']
            and session['authentication_revision'] == account['authentication_revision']
            and owner['acquired_at_us'] <= session['issued_at_us'] <= session['last_seen_at_us']
                <= session['revoked_at_us'] <= owner['released_at_us']
            and session['expires_at_us'] == session['issued_at_us'] + 28_800_000_000
            and session['idle_window_us'] == 1_800_000_000
            and session['idle_expires_at_us'] == min(session['last_seen_at_us'] + 1_800_000_000, session['expires_at_us']),
            'actual finite authenticated session lifecycle')
    for name in ('session_id', 'token_digest', 'csrf_digest'):
        ident(session[name].removeprefix(BX), 16 if name == 'session_id' else 32)
    same(session, dict(session_id=session['session_id'], dataset_id=dataset, principal_id=actor,
        credential_revision=account['credential_revision'], authentication_revision=account['authentication_revision'],
        token_digest=session['token_digest'], csrf_digest=session['csrf_digest'],
        issued_at_us=session['issued_at_us'], expires_at_us=session['issued_at_us'] + 28_800_000_000,
        idle_window_us=1_800_000_000, last_seen_at_us=session['last_seen_at_us'],
        idle_expires_at_us=session['idle_expires_at_us'], revoked_at_us=session['revoked_at_us']),
        'closed authenticated session')
    audit_issue = one([row for row in added['management_audit'] if row['action'] == 'browser.session.issue'])
    audit_revoke = one([row for row in added['management_audit'] if row['action'] == 'browser.session.revoke'])
    for row in added['management_audit']:
        ident(row['audit_id'].removeprefix(BX))
        same(row, dict(audit_id=row['audit_id'], dataset_id=dataset, actor_id=actor, action=row['action'],
            outcome='accepted', created_at_us=number(row['created_at_us']), config_revision=current['config_revision'],
            subject_id=row['subject_id'], policy_revision=current['policy_revision'], reason=None,
            requester_database_role=None, requester_os_uid=None), 'closed authenticated audit row')
    require(len({row['audit_id'] for row in added['management_audit']}) == 7
            and not {row['audit_id'] for row in added['management_audit']}
                & {row['audit_id'] for row in before['management_audit']}, 'fresh immutable audit identities')
    require(audit_issue['subject_id'] == audit_revoke['subject_id'] == session['session_id']
            and session['issued_at_us'] <= audit_issue['created_at_us']
            and session['revoked_at_us'] <= audit_revoke['created_at_us'] <= owner['released_at_us']
            and audit_revoke['xmin'] == session['xmin'], 'login/logout audit and revocation commit')
    # This finite fixture logs in once from the actual loopback IPv4 peer.
    charged = []
    for kind, domain, value, maximum in (
            ('account', b'account', account['username'].encode(), 5),
            ('peer', b'peer4', bytes([127, 0, 0, 1]), 20)):
        bucket = bytes.fromhex(sha(b'bigbrotr/login-bucket/1\0' + domain + b'\0' + value))[0]
        old = one([row for row in before['login_buckets'] if (row['kind'], row['bucket']) == (kind, bucket)])
        new = one([row for row in after['login_buckets'] if (row['kind'], row['bucket']) == (kind, bucket)])
        window = new['window_started_at_us']
        reset = window != old['window_started_at_us']
        require((not reset or old['window_started_at_us'] + 60_000_000 <= window)
                and window <= session['issued_at_us']
                and (not reset or window >= owner['acquired_at_us'])
                and 1 <= new['attempts'] <= maximum and new['xmin'] != old['xmin'],
                'natural charged login bucket window')
        same(new, dict(dataset_id=dataset, kind=kind, bucket=bucket, window_started_at_us=window,
                       attempts=1 if reset else old['attempts'] + 1), 'one actual password charge')
        charged.append((old, new))
    require(charged[0][1]['xmin'] == charged[1][1]['xmin']
            and [row for row in before['login_buckets'] if row not in [pair[0] for pair in charged]]
                == [row for row in after['login_buckets'] if row not in [pair[1] for pair in charged]],
            'only the two authenticated login buckets changed')
    marker_raw = (Path(destination) / 'destination.json').read_bytes()
    marker = json.loads(marker_raw)
    require(set(marker) == {'format', 'dataset_id', 'destination_id', 'root', 'device', 'inode'}
            and marker['format'] == 'bigbrotr-export-destination/1'
            and marker['dataset_id'] == query['dataset'] and marker['root'] == str(destination),
            'actual original destination binding')
    destination_id, binding = ident(marker['destination_id']), sha(marker_raw)
    expected_body = dict(kind='export', format='signed-events-jsonl',
        query=dict(version=1, resource='event_history', mode='snapshot', scope=scope,
                   filters=[dict(ids=[query['event_id']])]),
        allocation=dict(catalog_id=selection['catalog'], frontier_revision=str(frontier)),
        destination=dict(id=destination_id, binding_sha256=binding),
        expected_config_revision=revisions['configuration'], expected_policy_revision=revisions['policy'])
    require(all(set(record) == {'key', 'command', 'receipt'} and record['command'] == expected_body
                for record in commands), 'five exact independent HTTP commands')
    require(len({ident(record['key']) for record in commands}) == 5, 'five distinct command keys')
    jobs = [BX + ident(record['receipt']['job_id']) for record in commands]
    require(len(set(jobs)) == 5 and set(jobs) == {row['job_id'] for row in added['jobs']}
            and not set(jobs) & {row['job_id'] for row in before['jobs']}, 'five genuinely new job identities')
    require(len({row['artifact_id'] for row in added['artifacts']}) == 5
            and not {row['artifact_id'] for row in added['artifacts']}
                & {row['artifact_id'] for row in before['artifacts']}
            and len({row['lease_id'] for row in added['archive_snapshot_pins']}) == 15
            and not {row['lease_id'] for row in added['archive_snapshot_pins']}
                & {row['lease_id'] for row in before['snapshot_leases']}
            and len({row['claim_id'] for row in added['retention_claims']}) == 15 * claims_per_pin
            and not {row['claim_id'] for row in added['retention_claims']}
                & {row['claim_id'] for row in before['retention_claims']},
            'distinct artifacts and fifteen new pins')
    previous = audit_issue['created_at_us']
    results = []
    seen_pins = set()
    for index, (record, status, job_id) in enumerate(zip(commands, statuses, jobs), 1):
        job, detail, unit, artifact, admission = (find(table, 'job_id', job_id) for table in
            ('jobs', 'export_jobs', 'export_execution_units', 'artifacts', 'job_admissions'))
        artifact_id = ident(artifact['artifact_id'].removeprefix(BX))
        files = verify_signed_files(destination, artifact_id, job_id[2:], science)
        manifest_bytes = (Path(destination) / artifact_id / 'manifest.json').stat().st_size
        created, completed = number(job['created_at_us']), number(job['completed_at_us'])
        require(previous <= created <= unit['reserved_at_us'] < completed < session['revoked_at_us'],
                'sequential accepted/reserved/completed jobs inside authenticated lifetime')
        require(status == dict(format='bigbrotr-job-status/1', job_id=job_id[2:], kind='export',
            state='succeeded', job_revision='4', checkpoint_revision='1', created_at_us=str(created),
            completed_at_us=str(completed), observed_at_us=status['observed_at_us'],
            reason_code=None, authority='observation-only')
            and str(int(status['observed_at_us'])) == status['observed_at_us']
            and completed <= int(status['observed_at_us']) <= session['revoked_at_us'],
            'complete genuine terminal status')
        previous = int(status['observed_at_us'])
        require(record['receipt'] == dict(format='bigbrotr-job-acceptance/1', command_key=record['key'],
            job_id=job_id[2:], accepted_at_us=str(created), created=True, execution='unobserved',
            authority='acceptance-only'), 'exact actual new-command acceptance receipt')
        raw, be = bytes.fromhex, lambda value, width=8: int(value).to_bytes(width, 'big')
        digest = sha(b'bigbrotr/export-event/1\0' + raw(query['dataset']) + raw(principal) + raw(record['key'])
            + raw(query['dataset']) + be(4, 2) + raw(scope['allocation_domain']) + be(scope['ordinal'])
            + raw(query['event_id']) + be(100, 2) + raw(selection['catalog']) + be(frontier)
            + raw(destination_id) + raw(binding) + b'\x01\x01'
            + be(current['config_revision']) + be(current['policy_revision']))
        same(job, dict(job_id=job_id, dataset_id=dataset, principal_id=actor, request_key=BX + record['key'],
            request_digest=BX + digest, kind='export', config_revision=current['config_revision'],
            policy_revision=current['policy_revision'], created_at_us=created, input_topology_generation=topology,
            input_complete=True, export_job_protocol=1, acquisition_job_protocol=None,
            state='succeeded', job_revision=4, checkpoint_revision=1, owner_role='control',
            owner_resource_id=owner['resource_id'], owner_epoch=owner['epoch'],
            owner_incarnation=owner['process_incarnation'], completed_at_us=completed,
            reason_code=None, result_manifest_digest=BX + files['manifest_sha256']), 'closed completed job row')
        same(detail, dict(job_id=job_id, dataset_id=dataset, actor_id=actor, request_key=BX + record['key'],
            format='signed-events-jsonl', visibility='private', dictionary_kind=4,
            allocation_domain=BX + scope['allocation_domain'], ordinal=int(scope['ordinal']),
            event_id=BX + query['event_id'], page_size=100, catalog_id=catalog, frontier_revision=frontier,
            destination_id=BX + destination_id, destination_binding_sha256=BX + binding,
            pin_lease_id=detail['pin_lease_id'], allocation_pin_id=detail['allocation_pin_id'],
            session_id=session['session_id'], actor_credential_revision=account['credential_revision'],
            actor_authentication_revision=account['authentication_revision'],
            actor_grant_revision=account['grant_revision']), 'closed immutable export selection')
        command = find('management_commands', 'request_key', BX + record['key'])
        audit = find('management_audit', 'audit_id', command['audit_id'])
        same(command, dict(actor_id=actor, request_key=BX + record['key'], dataset_id=dataset,
            action='job.export.accept', request_digest=BX + digest, expected_config_revision=current['config_revision'],
            policy_revision=current['policy_revision'], audit_id=audit['audit_id'], accepted_at_us=created,
            authority_mode=None, document_digest=None), 'closed command receipt')
        require(audit['action'] == 'job.export.accept' and audit['subject_id'] == BX + record['key']
                and audit['created_at_us'] == created, 'command audit closure')
        same(admission, dict(job_id=job_id, admission_ordinal=
            max((row['admission_ordinal'] for row in before['job_admissions']), default=0) + index),
            'actual committed admission order')
        bounds = [row for row in added['job_input_bounds'] if row['job_id'] == job_id]
        require(sorted((row['shard_id'], row['owner_epoch'], row['last_complete_sequence']) for row in bounds)
                == sorted(bound_values), 'entire immutable job input cut')
        for row in bounds:
            same(row, dict(job_id=job_id, dataset_id=dataset, topology_generation=topology,
                shard_id=row['shard_id'], owner_epoch=row['owner_epoch'],
                last_complete_sequence=row['last_complete_sequence']), 'closed job bound')
        same(unit, dict(job_id=job_id, artifact_id=artifact['artifact_id'],
            max_output_bytes=settings['export.artifact.max_bytes'],
            max_temporary_bytes=2 * settings['export.artifact.max_bytes'],
            reservation_transaction=unit['xmin'], reserved_at_us=number(unit['reserved_at_us'])),
            'separately committed immutable output reservation')
        same(artifact, dict(artifact_id=artifact['artifact_id'], job_id=job_id, dataset_id=dataset,
            principal_id=actor, destination_id=BX + destination_id, destination_binding_sha256=BX + binding,
            manifest_format='bigbrotr-signed-events-export/1', manifest_bytes=manifest_bytes,
            manifest_sha256=BX + files['manifest_sha256'], file_count=1, total_bytes=files['bytes'] + manifest_bytes,
            verification_pin_id=artifact['verification_pin_id'], completed_at_us=completed), 'closed completed artifact')
        file_row = find('artifact_files', 'artifact_id', artifact['artifact_id'])
        same(file_row, dict(artifact_id=artifact['artifact_id'], file_id='events', relative_path='events.jsonl',
            format='nostr-jsonl/1', byte_count=files['bytes'], row_count=1, sha256=BX + files['sha256']),
            'closed full signed file inventory')
        require(artifact['total_bytes'] <= unit['max_output_bytes']
                and job['xmin'] == artifact['xmin'] == file_row['xmin'], 'atomic complete output inventory')
        acceptance_rows = [detail, command, audit, admission, *bounds]
        pin_rows = []
        for name, lease_id in (('allocation', detail['allocation_pin_id']), ('export', detail['pin_lease_id']),
                               ('execution', artifact['verification_pin_id'])):
            ident(lease_id.removeprefix(BX))
            require(lease_id not in seen_pins, 'no pin reused between consumers')
            seen_pins.add(lease_id)
            pin, lease = (find(table, 'lease_id', lease_id) for table in ('archive_snapshot_pins', 'snapshot_leases'))
            claims = [row for row in added['retention_claims'] if row['lease_id'] == lease_id]
            retained = name == 'export'
            purpose, state = ('export', 'active') if retained else ('reader', 'released')
            ttl = number(pin['requested_ttl_us'])
            require(ttl == 30_000_000 if retained else ttl <= 5_000_000, 'actual bounded per-consumer pin request')
            same(pin, dict(lease_id=lease_id, dataset_id=dataset, consumer_id=catalog,
                frontier_revision=frontier, issuer_role='control', issuer_resource=owner['resource_id'],
                issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'],
                purpose=purpose, requested_ttl_us=ttl, export_job_id=job_id if retained else None),
                'closed reader/export pin attribution')
            acquired, expires = number(lease['acquired_at_us']), number(lease['expires_at_us'])
            require(owner['acquired_at_us'] <= acquired < expires <= owner['lease_expires_at_us']
                    and expires - acquired <= ttl, 'nonrenewed Control-clamped pin expiry')
            released_at = None if retained else number(lease['released_at_us'])
            updated = number(lease['updated_at_us'])
            require(updated == acquired if retained else acquired <= updated <= released_at,
                    'nonrenewed lease update/release clock order')
            same(lease, dict(lease_id=lease_id, dataset_id=dataset, consumer_id=catalog,
                frontier_revision=frontier, topology_generation=topology, holder_kind='role_instance',
                holder_role_instance_id=owner['role_instance_id'], holder_role='control',
                holder_incarnation=owner['process_incarnation'], holder_job_id=None,
                acquired_at_us=acquired, updated_at_us=updated,
                expires_at_us=expires, archive_pin_protocol=1, lease_revision=1 if retained else 2,
                state=state, released_at_us=released_at, release_reason=None if retained else 'reader_joined'),
                'closed retained/released lease')
            require(sorted((row['shard_id'], row['owner_epoch'], row['first_sequence'], row['last_sequence'])
                           for row in claims) == sorted((s, e, 1, last) for s, e, last in bound_values if last > 0),
                    'all exact source retention claims')
            for claim in claims:
                ident(claim['claim_id'].removeprefix(BX))
                same(claim, dict(claim_id=claim['claim_id'], lease_id=lease_id, dataset_id=dataset,
                    topology_generation=topology, shard_id=claim['shard_id'], owner_epoch=claim['owner_epoch'],
                    first_sequence=1, last_sequence=claim['last_sequence'], purpose=purpose, state=state,
                    created_at_us=number(claim['created_at_us']),
                    released_at_us=None if retained else number(claim['released_at_us']),
                    release_reason=None if retained else 'reader_joined'), 'closed exact retention claim')
                require(acquired <= claim['created_at_us'] < expires
                        and (retained or claim['created_at_us'] <= claim['released_at_us'] <= released_at),
                        'claim lifetime inside its original pin')
            require(all(row['xmin'] == lease['xmin'] for row in claims), 'atomic claim/lease lifecycle')
            if retained:
                require(created <= acquired < unit['reserved_at_us'], 'export retention created during acceptance')
                acceptance_rows += [pin, lease, *claims]
            else:
                require(acquired <= (created if name == 'allocation' else completed) < expires
                        and (created if name == 'allocation' else completed) <= released_at <= owner['released_at_us'],
                        'reader live through its consumer then released')
                require(pin['xmin'] != lease['xmin'], 'reader release is a separate transaction')
            pin_rows.append((pin, lease))
        require({row['xmin'] for row in acceptance_rows} == {detail['xmin']},
                'one atomic acceptance with exact input and export retention')
        transaction_ids = [detail['xmin'], unit['xmin'], job['xmin'], owner['xmin'],
                           pin_rows[0][0]['xmin'], pin_rows[0][1]['xmin'],
                           pin_rows[2][0]['xmin'], pin_rows[2][1]['xmin']]
        require(len(set(transaction_ids)) == len(transaction_ids), 'distinct acceptance/reservation/read/completion/release commits')
        results.append(dict(job=job_id[2:], artifact=artifact_id, **files))
    require(seen_pins == {row['lease_id'] for row in added['archive_snapshot_pins']},
            'no unexplained pin obligations')
    return dict(status='PASS_INSTALLED_FIVE_SQL_AND_SIGNED_FILES', jobs=results, tables=75,
        accepted_export_memory_bytes=268435456, released_reader_pins=10, retained_export_pins=5,
        scope='SQL/xmin and independent complete signed files only; caller owns physical/process/backend/memory qualification')
