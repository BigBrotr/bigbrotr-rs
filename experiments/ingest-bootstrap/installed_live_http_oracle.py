"""Independent authenticated command rows for the installed successor fixture.

Only fresh identities, transaction IDs and bounded server clock scalars come
from observations. Historical rows and every command/document byte stay exact.
"""
from copy import deepcopy
from initial_succession_oracle import be, binary, bytea, only
from initial_live_oracle import clock, fresh_xmin, sha

ACTOR = '\\x' + '10' * 16
ENDPOINT = 'ws://127.0.0.1:7447/installed-live-fixture'
SELECTOR = b'{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}'
ACTIONS = ['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read']


def add_http(wanted, observed, original, result, phase, window, control):
    """Append the exact visible HTTP effects to a independently built SQL state."""
    ds = only(original['datasets'])['dataset_id']
    session = only(observed['browser_sessions'])
    sid = session['session_id']
    assert len(binary(sid)) == 16 and sid not in {r['session_id'] for r in original['browser_sessions']}
    logged_out = phase in ('renewed', 'before-stop', 'final')
    count = 3 if phase == 'prepared' else 4
    for at in (session['issued_at_us'], session['last_seen_at_us']):
        clock(at, window)
    assert session['issued_at_us'] <= session['last_seen_at_us']
    fresh_xmin(session['xmin'], original)
    wanted['browser_sessions'] = [dict(session_id=sid, dataset_id=ds, principal_id=ACTOR,
        credential_revision=1, authentication_revision=1,
        token_digest='\\x' + result['digests']['session'], csrf_digest='\\x' + result['digests']['csrf'],
        issued_at_us=session['issued_at_us'], expires_at_us=session['issued_at_us'] + 28_800_000_000,
        idle_window_us=1_800_000_000, last_seen_at_us=session['last_seen_at_us'],
        idle_expires_at_us=session['last_seen_at_us'] + 1_800_000_000,
        revoked_at_us=session['revoked_at_us'] if logged_out else None, xmin=session['xmin'])]
    for value in result['digests'].values():
        assert len(bytes.fromhex(value)) == 32 and int(value, 16)
    account = only(observed['login_buckets'], kind='account')
    at, tx = account['window_started_at_us'], account['xmin']
    fresh_xmin(tx, original); clock(at, window)
    assert at <= session['issued_at_us']
    wanted['login_buckets'] = [dict(dataset_id=ds, kind=kind, bucket=bucket,
        window_started_at_us=at, attempts=1, xmin=tx) for kind, bucket in [('account', 97), ('peer', 13)]]

    def audit(action, subject, at=None, tx=None, aid=None):
        row = only(observed['management_audit'], action=action, subject_id=subject)
        timestamp = row['created_at_us'] if at is None else at
        transaction = row['xmin'] if tx is None else tx
        assert len(binary(row['audit_id'])) == 16
        assert row['audit_id'] not in {r['audit_id'] for r in wanted['management_audit']}
        fresh_xmin(transaction, original); clock(timestamp, window)
        wanted['management_audit'].append(dict(dataset_id=ds, actor_id=ACTOR,
            audit_id=row['audit_id'] if aid is None else aid, action=action, outcome='accepted',
            subject_id=subject, created_at_us=timestamp, config_revision=2, policy_revision=2,
            reason=None, requester_database_role=None, requester_os_uid=None, xmin=transaction))
        return row

    issued = audit('browser.session.issue', sid)
    assert session['issued_at_us'] <= issued['created_at_us'] <= session['last_seen_at_us']
    if logged_out:
        clock(session['revoked_at_us'], window)
        assert session['last_seen_at_us'] <= session['revoked_at_us']
        revoked = audit('browser.session.revoke', sid, tx=session['xmin'])
        assert session['revoked_at_us'] <= revoked['created_at_us']
    owner = dict(owner_resource_id=control['resource_id'], owner_epoch=control['epoch'],
        owner_role='control', owner_incarnation=control['process_incarnation'])
    auth = dict(session_id=sid, actor_credential_revision=1, actor_authentication_revision=1,
        actor_grant_revision=4)
    endpoint = only(observed['relay_endpoints'], url_bytes=bytea(ENDPOINT.encode()))
    assert endpoint['endpoint_id'] not in {r['endpoint_id'] for r in original['relay_endpoints']}
    enrollment = only(observed['relay_enrollments'], endpoint_id=endpoint['endpoint_id'])
    scope = only(observed['acquisition_scopes'], endpoint_id=endpoint['endpoint_id'])
    old_scope = only(original['acquisition_scopes'])
    old_access = only(original['relay_access_contexts'])
    visibility_json = dict(dataset_id=ds[2:], dictionary_kind=4,
        allocation_domain=old_scope['visibility_domain'][2:], ordinal='3')
    revisions = dict(expected_config_revision='2', expected_policy_revision='2')
    paths = ('relay-endpoints', 'relay-enrollments', 'acquisition-scopes', 'jobs')
    requests = [dict(url=ENDPOINT, **revisions),
        dict(endpoint_id=endpoint['endpoint_id'][2:], visibility=visibility_json,
            route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True,
            expected_generation=None, **revisions),
        dict(endpoint_id=endpoint['endpoint_id'][2:], access_scope_id=old_access['access_scope_id'][2:],
            visibility=visibility_json, selector=dict(version=1, filters=[{'kinds': [1]}],
            history_since=0, ephemeral='retain'), **revisions),
        dict(kind='acquisition', scope_id=scope['scope_id'][2:], deadline_us='120000000', **revisions)]
    visibility = binary(ds) + be(4, 2) + binary(old_scope['visibility_domain']) + be(3)
    dsactor = binary(ds) + binary(ACTOR)
    digests = [sha(b'bigbrotr/relay-registration/1\0' + dsactor + bytes.fromhex('f1' * 16)
            + be(2) * 2 + be(1, 2) + be(len(ENDPOINT.encode()), 4) + ENDPOINT.encode()),
        sha(b'bigbrotr/relay-enrollment-change/1\0' + dsactor + bytes.fromhex('f2' * 16)
            + binary(endpoint['endpoint_id']) + visibility + b'\x01' + be(0) + be(2) * 2
            + bytes.fromhex('0104207f000001')),
        sha(b'bigbrotr/acquisition-scope-registration/1\0' + dsactor + bytes.fromhex('f3' * 16)
            + binary(endpoint['endpoint_id']) + binary(old_access['access_scope_id']) + visibility
            + be(2) * 2 + be(len(SELECTOR), 4) + SELECTOR),
        sha(b'bigbrotr/acquisition-job/1\0' + dsactor + bytes.fromhex('f4' * 16)
            + binary(scope['scope_id']) + be(120_000_000) + be(2) * 2)]
    actions = ('relay.endpoint.register', 'relay.enrollment.change', 'acquisition.scope.register', 'job.acquisition.accept')
    commands = []
    for n in range(count):
        key = '\\x' + f'f{n + 1}' * 16
        command = only(observed['management_commands'], actor_id=ACTOR, request_key=key)
        tx, at = command['xmin'], command['accepted_at_us']
        fresh_xmin(tx, original); clock(at, window)
        assert session['issued_at_us'] <= at
        if commands:
            assert commands[-1]['accepted_at_us'] <= at
        wanted['management_commands'].append(dict(dataset_id=ds, actor_id=ACTOR,
            request_key=key, request_digest=bytea(digests[n]), action=actions[n], accepted_at_us=at,
            expected_config_revision=2, policy_revision=2, audit_id=command['audit_id'],
            authority_mode=None, document_digest=None, xmin=tx))
        audit(actions[n], key, at, tx, command['audit_id'])
        observation = result['observations'][n]
        assert observation['key'] == key[2:] and observation['status'] == 202
        assert observation['body']['created'] is True
        assert observation['body']['command_key'] == key[2:]
        assert observation['body']['accepted_at_us'] == str(at)
        assert observation['path'] == paths[n] and observation['input'] == requests[n]
        assert set(observation) == {'path', 'input', 'key', 'status', 'body'}
        common = dict(command_key=key[2:], accepted_at_us=str(at), created=True)
        if n == 0:
            assert observation['body'] == dict(common, format='bigbrotr-relay-registration/1',
                endpoint_id=endpoint['endpoint_id'][2:], authority='identity-only')
        elif n == 1:
            assert observation['body'] == dict(common, format='bigbrotr-relay-enrollment/1',
                endpoint_id=endpoint['endpoint_id'][2:], access_scope_id=old_access['access_scope_id'][2:],
                route_id=enrollment['route_id'][2:], enabled=True, generation='1',
                application='unobserved', authority='acceptance-only')
        elif n == 2:
            assert observation['body'] == dict(common, format='bigbrotr-acquisition-scope-registration/1',
                scope_id=scope['scope_id'][2:], authority='identity-only')
        commands.append(command)
    tx = commands[0]['xmin']
    assert session['issued_at_us'] <= endpoint['registered_at_us'] <= commands[0]['accepted_at_us']
    wanted['relay_endpoints'].append(dict(only(original['relay_endpoints']), endpoint_id=endpoint['endpoint_id'],
        url_bytes=bytea(ENDPOINT.encode()), candidate_digest=bytea(sha(ENDPOINT.encode())),
        registered_at_us=endpoint['registered_at_us'], config_revision=2, policy_revision=2, xmin=tx, **owner))
    wanted['relay_registration_commands'].append(dict(only(original['relay_registration_commands']),
        request_key='\\x' + 'f1' * 16, endpoint_id=endpoint['endpoint_id'], xmin=tx, **owner, **auth))
    tx = commands[1]['xmin']
    change = only(observed['relay_enrollment_changes'], request_key='\\x' + 'f2' * 16)
    assert change['acceptance_transaction'].isdecimal()
    assert int(change['acceptance_transaction']) % (1 << 32) == int(tx)
    route = only(observed['network_routes'], route_id=enrollment['route_id'])
    assert route['route_id'] not in {r['route_id'] for r in original['network_routes']}
    wanted['network_routes'].append(dict(only(original['network_routes']), route_id=route['route_id'],
        endpoint_id=endpoint['endpoint_id'], request_key='\\x' + 'f2' * 16, xmin=tx))
    wanted['relay_enrollments'].append(dict(only(original['relay_enrollments']), endpoint_id=endpoint['endpoint_id'],
        route_id=route['route_id'], request_key='\\x' + 'f2' * 16, xmin=tx))
    wanted['relay_enrollment_changes'].append(dict(only(original['relay_enrollment_changes']),
        endpoint_id=endpoint['endpoint_id'], route_id=route['route_id'], request_key='\\x' + 'f2' * 16,
        acceptance_transaction=change['acceptance_transaction'], xmin=tx, **owner, **auth))
    tx = commands[2]['xmin']
    assert scope['scope_id'] not in {r['scope_id'] for r in original['acquisition_scopes']}
    assert session['issued_at_us'] <= scope['registered_at_us'] <= commands[2]['accepted_at_us']
    wanted['acquisition_scopes'].append(dict(old_scope, scope_id=scope['scope_id'],
        endpoint_id=endpoint['endpoint_id'], config_revision=2, policy_revision=2,
        registered_at_us=scope['registered_at_us'], xmin=tx, **owner))
    wanted['acquisition_scope_commands'].append(dict(only(original['acquisition_scope_commands']),
        scope_id=scope['scope_id'], request_key='\\x' + 'f3' * 16, xmin=tx, **owner, **auth))
    if count == 4:
        tx, at = commands[3]['xmin'], commands[3]['accepted_at_us']
        job = only(observed['jobs'], request_key='\\x' + 'f4' * 16)
        assert job['job_id'] not in {r['job_id'] for r in original['jobs']}
        fresh_xmin(job['xmin'], original)
        assert job['xmin'] != tx
        wanted['jobs'].append(dict(only(original['jobs'], kind='acquisition'), job_id=job['job_id'],
            request_key='\\x' + 'f4' * 16, request_digest=bytea(digests[3]), state='queued', job_revision=2,
            config_revision=2, policy_revision=2, created_at_us=at, completed_at_us=None, reason_code=None,
            xmin=job['xmin'], **owner))
        wanted['job_admissions'].append(dict(job_id=job['job_id'], admission_ordinal=3, xmin=tx))
        wanted['acquisition_jobs'].append(dict(only(original['acquisition_jobs']), job_id=job['job_id'],
            request_key='\\x' + 'f4' * 16, scope_id=scope['scope_id'], deadline_at_us=at + 120_000_000,
            xmin=tx, **auth))
        assert result['observations'][3]['body'] == dict(format='bigbrotr-job-acceptance/1', job_id=job['job_id'][2:],
            command_key='f4' * 16, accepted_at_us=str(at), created=True, execution='unobserved', authority='acceptance-only')
        status = result['job_status']
        if logged_out:
            clock(int(status['observed_at_us']), window)
        assert int(status['observed_at_us']) >= at
        assert status == dict(format='bigbrotr-job-status/1', job_id=job['job_id'][2:],
            kind='acquisition', state='queued', job_revision='2', checkpoint_revision='0',
            created_at_us=str(at), completed_at_us=None, observed_at_us=status['observed_at_us'],
            reason_code=None, authority='observation-only')
    return scope, enrollment
