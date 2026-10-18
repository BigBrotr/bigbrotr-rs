"""Independent HTTP rows for the new same-root journey.

Adapted explicitly from the separately qualified installed-family reducer.

Only fresh identities, transaction IDs and bounded server clock scalars come
from observations. Historical rows and every command/document byte stay exact.
"""
from history_replay import be, binary, only
from installed_family_replay import clock, fresh_xmin, digest_bytes as sha
from transition_replay import bx as bytea

ACTOR = '\\x' + '10' * 16
ENDPOINT = 'ws://127.0.0.1:7447/installed-successor-fixture'
SELECTOR = b'{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}'
ACTIONS = ['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read']


def add_http(wanted, observed, original, result, phase, window, control):
    """Append the exact visible HTTP effects to a independently built SQL state."""
    ds = only(original['datasets'])['dataset_id']
    prior_sessions = {r['session_id'] for r in original['browser_sessions']}
    session, = [r for r in observed['browser_sessions'] if r['session_id'] not in prior_sessions]
    dataset = only(original['datasets'])
    config_revision, policy_revision = dataset['config_revision'], dataset['policy_revision']
    sid = session['session_id']
    assert len(binary(sid)) == 16 and sid not in {r['session_id'] for r in original['browser_sessions']}
    logged_out = phase in ('renewed', 'before-stop', 'final')
    count = 3 if phase == 'prepared' else 4
    for at in (session['issued_at_us'], session['last_seen_at_us']):
        clock(at, window)
    assert session['issued_at_us'] <= session['last_seen_at_us']
    fresh_xmin(session['xmin'], original)
    # The genuine login transaction prunes only this account's terminal/expired
    # sessions. Historical command/audit references are independent retained IDs.
    wanted['browser_sessions'] = [r for r in wanted['browser_sessions'] if not (
        r['dataset_id'] == ds and r['principal_id'] == ACTOR and (r['revoked_at_us'] is not None
        or r['idle_expires_at_us'] <= session['issued_at_us'] or r['expires_at_us'] <= session['issued_at_us']))]
    wanted['browser_sessions'].append(dict(session_id=sid, dataset_id=ds, principal_id=ACTOR,
        credential_revision=1, authentication_revision=1,
        token_digest='\\x' + result['digests']['session'], csrf_digest='\\x' + result['digests']['csrf'],
        issued_at_us=session['issued_at_us'], expires_at_us=session['issued_at_us'] + 28_800_000_000,
        idle_window_us=1_800_000_000, last_seen_at_us=session['last_seen_at_us'],
        idle_expires_at_us=session['last_seen_at_us'] + 1_800_000_000,
        revoked_at_us=session['revoked_at_us'] if logged_out else None, xmin=session['xmin']))
    for value in result['digests'].values():
        assert len(bytes.fromhex(value)) == 32 and int(value, 16)
    account = only(observed['login_buckets'], kind='account')
    at, tx = account['window_started_at_us'], account['xmin']
    fresh_xmin(tx, original)
    assert at <= session['issued_at_us']
    wanted['login_buckets'] = []
    for kind,bucket in [('account',97),('peer',13)]:
        old = only(original['login_buckets'],kind=kind)
        new = only(observed['login_buckets'],kind=kind)
        reset = new['window_started_at_us'] != old['window_started_at_us']
        if reset:
            assert old['window_started_at_us'] + 60_000_000 <= new['window_started_at_us'] <= session['issued_at_us']
            clock(new['window_started_at_us'],window)
        else: assert old['window_started_at_us'] <= session['issued_at_us']
        wanted['login_buckets'].append(dict(old,window_started_at_us=new['window_started_at_us'],
            attempts=1 if reset else old['attempts']+1,xmin=tx))

    def audit(action, subject, at=None, tx=None, aid=None):
        row = only(observed['management_audit'], action=action, subject_id=subject)
        timestamp = row['created_at_us'] if at is None else at
        transaction = row['xmin'] if tx is None else tx
        assert len(binary(row['audit_id'])) == 16
        assert row['audit_id'] not in {r['audit_id'] for r in wanted['management_audit']}
        fresh_xmin(transaction, original); clock(timestamp, window)
        wanted['management_audit'].append(dict(dataset_id=ds, actor_id=ACTOR,
            audit_id=row['audit_id'] if aid is None else aid, action=action, outcome='accepted',
            subject_id=subject, created_at_us=timestamp, config_revision=config_revision, policy_revision=policy_revision,
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
        actor_grant_revision=only(original['local_accounts'],principal_id=ACTOR)['grant_revision'])
    endpoint = only(observed['relay_endpoints'], url_bytes=bytea(ENDPOINT.encode()))
    assert endpoint['endpoint_id'] not in {r['endpoint_id'] for r in original['relay_endpoints']}
    enrollment = only(observed['relay_enrollments'], endpoint_id=endpoint['endpoint_id'])
    scope = only(observed['acquisition_scopes'], endpoint_id=endpoint['endpoint_id'])
    old_scope = only(original['acquisition_scopes'])
    old_access = only(original['relay_access_contexts'])
    visibility_json = dict(dataset_id=ds[2:], dictionary_kind=4,
        allocation_domain=old_scope['visibility_domain'][2:], ordinal='3')
    revisions = dict(expected_config_revision=str(config_revision), expected_policy_revision=str(policy_revision))
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
    digests = [sha(b'bigbrotr/relay-registration/1\0' + dsactor + bytes.fromhex('e1' * 16)
            + be(config_revision) + be(policy_revision) + be(1, 2) + be(len(ENDPOINT.encode()), 4) + ENDPOINT.encode()),
        sha(b'bigbrotr/relay-enrollment-change/1\0' + dsactor + bytes.fromhex('e2' * 16)
            + binary(endpoint['endpoint_id']) + visibility + b'\x01' + be(0) + be(config_revision) + be(policy_revision)
            + bytes.fromhex('0104207f000001')),
        sha(b'bigbrotr/acquisition-scope-registration/1\0' + dsactor + bytes.fromhex('e3' * 16)
            + binary(endpoint['endpoint_id']) + binary(old_access['access_scope_id']) + visibility
            + be(config_revision) + be(policy_revision) + be(len(SELECTOR), 4) + SELECTOR),
        sha(b'bigbrotr/acquisition-job/1\0' + dsactor + bytes.fromhex('e4' * 16)
            + binary(scope['scope_id']) + be(120_000_000) + be(config_revision) + be(policy_revision))]
    actions = ('relay.endpoint.register', 'relay.enrollment.change', 'acquisition.scope.register', 'job.acquisition.accept')
    commands = []
    for n in range(count):
        key = '\\x' + f'e{n + 1}' * 16
        command = only(observed['management_commands'], actor_id=ACTOR, request_key=key)
        tx, at = command['xmin'], command['accepted_at_us']
        fresh_xmin(tx, original); clock(at, window)
        assert session['issued_at_us'] <= at
        if commands:
            assert commands[-1]['accepted_at_us'] <= at
        wanted['management_commands'].append(dict(dataset_id=ds, actor_id=ACTOR,
            request_key=key, request_digest=bytea(digests[n]), action=actions[n], accepted_at_us=at,
            expected_config_revision=config_revision, policy_revision=policy_revision, audit_id=command['audit_id'],
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
    wanted['relay_endpoints'].append(dict(original['relay_endpoints'][0], endpoint_id=endpoint['endpoint_id'],
        url_bytes=bytea(ENDPOINT.encode()), candidate_digest=bytea(sha(ENDPOINT.encode())),
        registered_at_us=endpoint['registered_at_us'], config_revision=config_revision, policy_revision=policy_revision, xmin=tx, **owner))
    wanted['relay_registration_commands'].append(dict(original['relay_registration_commands'][0],
        request_key='\\x' + 'e1' * 16, endpoint_id=endpoint['endpoint_id'], xmin=tx, **owner, **auth))
    tx = commands[1]['xmin']
    change = only(observed['relay_enrollment_changes'], request_key='\\x' + 'e2' * 16)
    assert change['acceptance_transaction'].isdecimal()
    assert int(change['acceptance_transaction']) % (1 << 32) == int(tx)
    route = only(observed['network_routes'], route_id=enrollment['route_id'])
    assert route['route_id'] not in {r['route_id'] for r in original['network_routes']}
    wanted['network_routes'].append(dict(original['network_routes'][0], route_id=route['route_id'],
        endpoint_id=endpoint['endpoint_id'], request_key='\\x' + 'e2' * 16, xmin=tx))
    wanted['relay_enrollments'].append(dict(original['relay_enrollments'][0], endpoint_id=endpoint['endpoint_id'],
        route_id=route['route_id'], request_key='\\x' + 'e2' * 16, xmin=tx))
    wanted['relay_enrollment_changes'].append(dict(original['relay_enrollment_changes'][0],
        endpoint_id=endpoint['endpoint_id'], route_id=route['route_id'], request_key='\\x' + 'e2' * 16,
        acceptance_transaction=change['acceptance_transaction'], xmin=tx, **owner, **auth))
    tx = commands[2]['xmin']
    assert scope['scope_id'] not in {r['scope_id'] for r in original['acquisition_scopes']}
    assert session['issued_at_us'] <= scope['registered_at_us'] <= commands[2]['accepted_at_us']
    wanted['acquisition_scopes'].append(dict(old_scope, scope_id=scope['scope_id'],
        endpoint_id=endpoint['endpoint_id'], config_revision=config_revision, policy_revision=policy_revision,
        registered_at_us=scope['registered_at_us'], xmin=tx, **owner))
    wanted['acquisition_scope_commands'].append(dict(original['acquisition_scope_commands'][0],
        scope_id=scope['scope_id'], request_key='\\x' + 'e3' * 16, xmin=tx, **owner, **auth))
    if count == 4:
        tx, at = commands[3]['xmin'], commands[3]['accepted_at_us']
        job = only(observed['jobs'], request_key='\\x' + 'e4' * 16)
        assert job['job_id'] not in {r['job_id'] for r in original['jobs']}
        fresh_xmin(job['xmin'], original)
        assert job['xmin'] != tx
        wanted['jobs'].append(dict(only(original['jobs'], request_key='\\x' + 'b1' * 16), job_id=job['job_id'],
            request_key='\\x' + 'e4' * 16, request_digest=bytea(digests[3]), state='queued', job_revision=2,
            config_revision=config_revision, policy_revision=policy_revision, created_at_us=at, completed_at_us=None, reason_code=None,
            xmin=job['xmin'], **owner))
        wanted['job_admissions'].append(dict(job_id=job['job_id'], admission_ordinal=max(r['admission_ordinal'] for r in original['job_admissions']) + 1, xmin=tx))
        wanted['acquisition_jobs'].append(dict(only(original['acquisition_jobs'], request_key='\\x' + 'b1' * 16), job_id=job['job_id'],
            request_key='\\x' + 'e4' * 16, scope_id=scope['scope_id'], deadline_at_us=at + 120_000_000,
            xmin=tx, **auth))
        assert result['observations'][3]['body'] == dict(format='bigbrotr-job-acceptance/1', job_id=job['job_id'][2:],
            command_key='e4' * 16, accepted_at_us=str(at), created=True, execution='unobserved', authority='acceptance-only')
        status = result['job_status']
        if logged_out:
            clock(int(status['observed_at_us']), window)
        assert int(status['observed_at_us']) >= at
        assert status == dict(format='bigbrotr-job-status/1', job_id=job['job_id'][2:],
            kind='acquisition', state='queued', job_revision='2', checkpoint_revision='0',
            created_at_us=str(at), completed_at_us=None, observed_at_us=status['observed_at_us'],
            reason_code=None, authority='observation-only')
    return scope, enrollment


def ordinary_http(model, phase, selection, restart):
    """Exact ordinary browser/job/pin effects; all other 75-table rows stay exact."""
    import json
    from installed_family_replay import OLD_EVENT, NEW_EVENT
    wanted, observed, original = model.pg, phase['after']['control'], phase['before']['control']
    window, control = phase['window'], model.control
    trace = phase['fixture']['http']; result = phase['fixture']['http_result']
    by = lambda label: only(trace, case=label)
    assert result['status']=='PASS'
    ds = only(original['datasets'])['dataset_id']; scope=selection['scope']
    assert scope==dict(dataset_id=ds[2:],dictionary_kind=4,
        allocation_domain=only(original['allocation_domains'],owner_epoch=1,dictionary_kind=4)['allocation_domain'][2:],ordinal='3')
    catalog='\\x'+selection['catalog_id']; frontier=selection['frontier_revision']
    config,policy=(only(original['datasets'])[k] for k in ('config_revision','policy_revision'))
    account=only(wanted['local_accounts'],principal_id=ACTOR); prior_grant=account['grant_revision']
    old_sessions={r['session_id'] for r in original['browser_sessions']}
    session,=[r for r in observed['browser_sessions'] if r['session_id'] not in old_sessions]
    sid=model.identity(session['session_id'][2:]); login=by('login')
    for field in ('issued_at_us','last_seen_at_us','revoked_at_us'):clock(session[field],window)
    assert control['acquired_at_us']<=session['issued_at_us']<=session['last_seen_at_us']<=session['revoked_at_us']<=control['released_at_us']
    fresh_xmin(session['xmin'],original)
    wanted['browser_sessions']=[r for r in wanted['browser_sessions'] if not (r['dataset_id']==ds and r['principal_id']==ACTOR
        and (r['revoked_at_us'] is not None or r['idle_expires_at_us']<=session['issued_at_us'] or r['expires_at_us']<=session['issued_at_us']))]
    assert set(login['digests'])=={'session','csrf'}
    for value in login['digests'].values():assert len(bytes.fromhex(value))==32 and int(value,16)
    wanted['browser_sessions'].append(dict(session_id=sid,dataset_id=ds,principal_id=ACTOR,credential_revision=1,
        authentication_revision=1,token_digest='\\x'+login['digests']['session'],csrf_digest='\\x'+login['digests']['csrf'],
        issued_at_us=session['issued_at_us'],expires_at_us=session['issued_at_us']+28_800_000_000,idle_window_us=1_800_000_000,
        last_seen_at_us=session['last_seen_at_us'],idle_expires_at_us=session['last_seen_at_us']+1_800_000_000,
        revoked_at_us=session['revoked_at_us'],xmin=session['xmin']))
    # The one successful /me is this client's only idle touch. Later commands
    # and reads must preserve the expiry observed here through final logout.
    assert login['me']==dict(format='bigbrotr-session/1',principal_id=ACTOR[2:],credential_revision='1',
        grant_revision=str(prior_grant),config_revision=str(config),policy_revision=str(policy),
        expires_at_us=str(session['expires_at_us']),idle_expires_at_us=str(session['idle_expires_at_us']),
        stored_actions=ACTIONS,authority='observation-only')
    def audit(action,subject,at=None,tx=None):
        row=only(observed['management_audit'],action=action,subject_id=subject)
        aid=model.identity(row['audit_id'][2:]);clock(row['created_at_us'],window);fresh_xmin(row['xmin'],original)
        assert session['issued_at_us']<=row['created_at_us']<=control['released_at_us']
        if at is not None:assert row['created_at_us']==at
        if tx is not None:assert row['xmin']==tx
        wanted['management_audit'].append(dict(audit_id=aid,dataset_id=ds,actor_id=ACTOR,action=action,outcome='accepted',
            created_at_us=row['created_at_us'],config_revision=config,subject_id=subject,policy_revision=policy,
            reason=None,requester_database_role=None,requester_os_uid=None,xmin=row['xmin']))
        return row
    audit('browser.session.issue',sid)
    revoked=audit('browser.session.revoke',sid,tx=session['xmin'])
    assert session['revoked_at_us']<=revoked['created_at_us']
    # Reauthentication charges the same fixed account/peer buckets as login.
    # First phase explicitly waited for the old window to expire, then made4
    # real charges; restart adds one actual login and no reauthentication.
    for kind in ('account','peer'):
        old=only(wanted['login_buckets'],kind=kind);new=only(observed['login_buckets'],kind=kind)
        fresh_xmin(new['xmin'],original)
        if restart and new['window_started_at_us']==old['window_started_at_us']:
            count=old['attempts']+1
            assert session['issued_at_us']<old['window_started_at_us']+60_000_000
        else:
            assert old['window_started_at_us']+60_000_000<=new['window_started_at_us']<=session['issued_at_us']
            clock(new['window_started_at_us'],window);count=1 if restart else 4
        assert count<=5 if kind=='account' else count<=20
        old.update(window_started_at_us=new['window_started_at_us'],attempts=count,xmin=new['xmin'])
    allocation=dict(catalog_id=catalog[2:],frontier_revision=str(frontier))
    visibility=binary(ds)+be(4,2)+bytes.fromhex(scope['allocation_domain'])+be(3)
    def command(key,action,digest):
        row=only(observed['management_commands'],actor_id=ACTOR,request_key=key)
        at,tx=row['accepted_at_us'],row['xmin'];clock(at,window);fresh_xmin(tx,original)
        # Commands/reads need not touch idle time. This sequential client logs
        # out after completing its commands, reads and export.
        assert session['issued_at_us']<=at<=session['revoked_at_us']
        assert at<min(session['expires_at_us'],session['idle_expires_at_us'])
        a=audit(action,key,at,tx)
        wanted['management_commands'].append(dict(dataset_id=ds,actor_id=ACTOR,request_key=key,action=action,
            request_digest=bytea(digest),expected_config_revision=config,policy_revision=policy,audit_id=a['audit_id'],
            accepted_at_us=at,authority_mode=None,document_digest=None,xmin=tx))
        return at,tx
    grant_pins=[]
    if not restart:
        denied=by('no-implicit-scope-authority');assert denied['export'][0]==denied['history'][0]==403
        assert not original['scope_grants'] and not original['scope_grant_changes'] and not original['reauthentication_proofs']
        for index,action in enumerate(('history.read','export','private-export'),1):
            key='\\x'+f'{0xc0+index:02x}'*16
            digest=sha(b'bigbrotr/scope-grant-change/1\0'+binary(ds)+binary(ACTOR)+binary(key)+binary(ACTOR)+visibility
                +bytes([index,1])+be(config)+be(policy)+be(prior_grant+index-1)+b'\x01'+binary(catalog)+be(frontier))
            at,tx=command(key,'scope-grant.change',digest)
            detail=only(observed['scope_grant_changes'],request_key=key)
            proof=model.identity(detail['proof_id'][2:]);issued=audit('browser.session.reauthenticate',proof)
            assert session['issued_at_us']<=issued['created_at_us']<=at<detail['proof_expires_at_us']
            assert detail['proof_expires_at_us']==issued['created_at_us']+300_000_000
            reauth=by('reauthenticate-'+action);assert reauth['status']==201 and reauth['response']['proof_id']==proof[2:]
            assert reauth['response']['command_digest']==digest.hex()
            expected_command=dict(principal_id=ACTOR[2:],action=action,enabled=True,scope=scope,
                expected_config_revision=str(config),expected_policy_revision=str(policy),
                expected_grant_revision=str(prior_grant+index-1),allocation=allocation)
            grant=by('enable-'+action)
            assert grant['command']==expected_command and grant['key']==key[2:] and grant['status']==200
            # The HTTP presentation uses lossless decimal timestamps.
            assert grant['receipt']==dict(format='bigbrotr-scope-grant/1',command_key=key[2:],principal_id=ACTOR[2:],scope=scope,enabled=True,
                scope_generation='1',account_grant_revision=str(prior_grant+index),accepted_at_us=str(at),created=True,application='unobserved',authority='acceptance-only')
            wanted['scope_grant_changes'].append(dict(actor_id=ACTOR,request_key=key,dataset_id=ds,principal_id=ACTOR,
                action=action,dictionary_kind=4,allocation_domain='\\x'+scope['allocation_domain'],ordinal=3,enabled=True,
                scope_generation=1,expected_grant_revision=prior_grant+index-1,resulting_grant_revision=prior_grant+index,
                proof_id=proof,session_id=sid,actor_credential_revision=1,actor_authentication_revision=1,
                actor_grant_revision=prior_grant+index-1,proof_expires_at_us=issued['created_at_us']+300_000_000,
                catalog_id=catalog,frontier_revision=frontier,pin_lease_id=detail['pin_lease_id'],xmin=tx))
            wanted['scope_grants'].append(dict(dataset_id=ds,principal_id=ACTOR,action=action,dictionary_kind=4,
                allocation_domain='\\x'+scope['allocation_domain'],ordinal=3,enabled=True,generation=1,
                actor_id=ACTOR,request_key=key,xmin=tx))
            account.update(grant_revision=prior_grant+index,xmin=tx);grant_pins.append(detail['pin_lease_id'])
    assert not observed['reauthentication_proofs']
    histories=[]
    for event in (OLD_EVENT,NEW_EVENT):
        item=by('history-'+event['id']);query=dict(version=1,resource='event_history',mode='snapshot',scope=scope,filters=[dict(ids=[event['id']])])
        assert item['query']==query and item['status']==200
        answer=item['response'];assert answer['items']==[event] and answer['next_cursor'] is None
        assert answer['view_stamp']['input_generations']==[dict(kind='archive',**allocation,snapshot_id='8')]
        stamp=answer['view_stamp'];clock(int(stamp['visibility_as_of_us']),window)
        assert answer==dict(version=1,resource='event_history',items=[event],effective_page_size=100,next_cursor=None,mode='snapshot',candidate_cut=None,
            view_stamp=dict(source_cut=dict(dataset_id=ds[2:],topology_generation='2',bounds=[dict(stream=dict(shard_id=0,owner_epoch=str(e)),
                last_complete_sequence=str(c)) for e,c in ((1,12),(2,0),(3,10))]),input_generations=[dict(kind='archive',**allocation,snapshot_id='8')],
                target_generations=[],visibility_as_of_us=stamp['visibility_as_of_us'],temporal_generation=None,expiry_deadline_us=None,policy_epoch=str(policy)),
            eligibility_lag_us=None)
        histories.append(answer)
    assert result['histories']==histories
    export=by('accept-export');key='\\x'+'d1'*16
    body=dict(kind='export',format='signed-events-jsonl',query=dict(version=1,resource='event_history',mode='snapshot',scope=scope,
        filters=[dict(ids=[NEW_EVENT['id']])]),allocation=allocation,destination=dict(id=selection['destination_id'],
        binding_sha256=selection['binding_sha256']),expected_config_revision=str(config),expected_policy_revision=str(policy))
    assert export['key']==key[2:] and export['command']==body and export['status']==(200 if restart else 202)
    job=only(observed['jobs'],request_key=key);detail=only(observed['export_jobs'],job_id=job['job_id'])
    if not restart:
        jobid=model.identity(job['job_id'][2:])
        digest=sha(b'bigbrotr/export-event/1\0'+binary(ds)+binary(ACTOR)+binary(key)+visibility+bytes.fromhex(NEW_EVENT['id'])
            +be(100,2)+binary(catalog)+be(frontier)+bytes.fromhex(selection['destination_id'])
            +bytes.fromhex(selection['binding_sha256'])+b'\x01\x01'+be(config)+be(policy))
        accepted,tx=command(key,'job.export.accept',digest)
        completed=job['completed_at_us'];clock(completed,window);fresh_xmin(job['xmin'],original)
        assert accepted<=completed<=session['revoked_at_us'] and job['xmin']!=tx
        base=only(original['jobs'],request_key='\\x'+'e4'*16)
        wanted['jobs'].append(dict(base,job_id=jobid,request_key=key,request_digest=bytea(digest),kind='export',state='succeeded',
            job_revision=4,checkpoint_revision=1,input_topology_generation=2,input_complete=True,export_job_protocol=1,acquisition_job_protocol=None,
            config_revision=config,policy_revision=policy,created_at_us=accepted,completed_at_us=completed,
            reason_code=None,result_manifest_digest=job['result_manifest_digest'],owner_resource_id=control['resource_id'],
            owner_epoch=control['epoch'],owner_incarnation=control['process_incarnation'],xmin=job['xmin']))
        wanted['job_admissions'].append(dict(job_id=jobid,admission_ordinal=max(r['admission_ordinal'] for r in original['job_admissions'])+1,xmin=tx))
        for epoch,cut in ((1,12),(2,0),(3,10)):
            wanted['job_input_bounds'].append(dict(job_id=jobid,dataset_id=ds,topology_generation=2,shard_id=0,owner_epoch=epoch,last_complete_sequence=cut,xmin=tx))
        wanted['export_jobs'].append(dict(job_id=jobid,dataset_id=ds,actor_id=ACTOR,request_key=key,format='signed-events-jsonl',visibility='private',
            dictionary_kind=4,allocation_domain='\\x'+scope['allocation_domain'],ordinal=3,event_id='\\x'+NEW_EVENT['id'],page_size=100,
            catalog_id=catalog,frontier_revision=frontier,destination_id='\\x'+selection['destination_id'],
            destination_binding_sha256='\\x'+selection['binding_sha256'],pin_lease_id=detail['pin_lease_id'],allocation_pin_id=detail['allocation_pin_id'],
            session_id=sid,actor_credential_revision=1,actor_authentication_revision=1,actor_grant_revision=prior_grant+3,xmin=tx))
        unit=only(observed['export_execution_units']);artifact=only(observed['artifacts']);file=only(observed['artifact_files'])
        artifactid=model.identity(unit['artifact_id'][2:]);clock(unit['reserved_at_us'],window);fresh_xmin(unit['xmin'],original)
        assert unit['reservation_transaction'].isdecimal() and int(unit['reservation_transaction'])%2**32==int(unit['xmin'])
        assert accepted<=unit['reserved_at_us']<=completed
        wanted['export_execution_units'].append(dict(job_id=jobid,artifact_id=artifactid,reservation_transaction=unit['reservation_transaction'],
            max_output_bytes=256*1024**2,max_temporary_bytes=512*1024**2,reserved_at_us=unit['reserved_at_us'],xmin=unit['xmin']))
        raw=(json.dumps({k:NEW_EVENT[k] for k in ('id','pubkey','created_at','kind','tags','content','sig')},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        data=phase['artifact_contents'];basepath=artifactid[2:]+'/'
        assert bytes.fromhex(data[basepath+'events.jsonl'])==raw
        manifest_raw=bytes.fromhex(data[basepath+'manifest.json']);manifest=json.loads(manifest_raw)
        expected_manifest=dict(format='bigbrotr-signed-events-export/1',artifact_id=artifactid[2:],job_id=jobid[2:],visibility='private',
            fidelity='full-signed-events',origin='retained-scoped-archive',source=dict(cut=dict(dataset_id=ds[2:],topology_generation='2',
                bounds=[dict(stream=dict(shard_id=0,owner_epoch=str(e)),last_complete_sequence=str(c)) for e,c in ((1,12),(2,0),(3,10))]),
                catalog_id=catalog[2:],frontier_revision=str(frontier),snapshot_id='8'),selection=dict(event_id=NEW_EVENT['id'],scope=scope),
            accepted_revisions=dict(configuration=str(config),policy=str(policy)),included_families=['signed-events'],
            excluded_families=['internal-payload-envelopes','eligibility-facts','first-local-receive','relay-provenance','imported-claims','measurements',
                'documents','dictionaries','coverage','derived-results','deployment-state'],
            files=[dict(file_id='events',path='events.jsonl',format='nostr-jsonl/1',bytes=str(len(raw)),rows='1',sha256=sha(raw).hex())])
        assert manifest==expected_manifest and job['result_manifest_digest']==bytea(sha(manifest_raw))
        wanted['artifacts'].append(dict(artifact_id=artifactid,job_id=jobid,dataset_id=ds,principal_id=ACTOR,destination_id=detail['destination_id'],
            destination_binding_sha256=detail['destination_binding_sha256'],manifest_format='bigbrotr-signed-events-export/1',manifest_bytes=len(manifest_raw),
            manifest_sha256=bytea(sha(manifest_raw)),file_count=1,total_bytes=len(raw)+len(manifest_raw),verification_pin_id=artifact['verification_pin_id'],
            completed_at_us=completed,xmin=job['xmin']))
        wanted['artifact_files'].append(dict(artifact_id=artifactid,file_id='events',relative_path='events.jsonl',format='nostr-jsonl/1',
            byte_count=len(raw),row_count=1,sha256=bytea(sha(raw)),xmin=job['xmin']))
    else:
        accepted=only(original['management_commands'],request_key=key)['accepted_at_us']
        assert phase['artifact_contents']==phase['prior_artifact_contents'] and phase['artifact_files']==phase['prior_artifact_files']
    receipt=dict(format='bigbrotr-job-acceptance/1',job_id=job['job_id'][2:],command_key=key[2:],accepted_at_us=str(accepted),
        created=not restart,execution='unobserved',authority='acceptance-only')
    assert export['receipt']==result['receipt']==receipt
    assert by('retry-export')==dict(case='retry-export',status=200,receipt=dict(receipt,created=False))
    statuses=[r for r in trace if r['case']=='export-status'];assert 1<=len(statuses)<=76
    for status in statuses:
        answer=status['response'];clock(int(answer['observed_at_us']),window)
        assert status['status']==200 and answer['job_id']==job['job_id'][2:] and answer['kind']=='export'
        assert answer['created_at_us']==str(accepted) and answer['authority']=='observation-only'
        assert answer['state'] in ('accepted','queued','running','succeeded')
    assert statuses[-1]['response']==result['completed']==dict(format='bigbrotr-job-status/1',job_id=job['job_id'][2:],kind='export',
        state='succeeded',job_revision='4',checkpoint_revision='1',created_at_us=str(accepted),completed_at_us=str(job['completed_at_us']),
        observed_at_us=statuses[-1]['response']['observed_at_us'],reason_code=None,authority='observation-only')
    expected_labels=['login']+([] if restart else ['no-implicit-scope-authority']+[x for a in ('history.read','export','private-export')
        for x in ('reauthenticate-'+a,'enable-'+a)])+['history-'+e['id'] for e in (OLD_EVENT,NEW_EVENT)]+['accept-export','retry-export']
    assert [r['case'] for r in trace]==expected_labels+['export-status']*len(statuses)+['logout']
    add_pins(model,observed,original,window,frontier,catalog,restart,detail,grant_pins)


def add_pins(model,observed,original,window,frontier,catalog,restart,detail,grant_pins):
    wanted,control=model.pg,model.control;ds=only(original['datasets'])['dataset_id']
    old={r['lease_id'] for r in original['snapshot_leases']}
    pins=[r for r in observed['archive_snapshot_pins'] if r['lease_id'] not in old]
    assert len(pins)==(2 if restart else 8)
    assert sum(p['purpose']=='reader' for p in pins)==(2 if restart else 7)
    assert sum(p['purpose']=='export' for p in pins)==(0 if restart else 1)
    actual={p['lease_id']:p for p in pins}
    if not restart:
        assert len(set(grant_pins+[detail['allocation_pin_id'],only(observed['artifacts'])['verification_pin_id']]))==5
        assert all(actual[p]['purpose']=='reader' for p in grant_pins+[detail['allocation_pin_id'],only(observed['artifacts'])['verification_pin_id']])
        assert actual[detail['pin_lease_id']]['purpose']=='export'
    for p in pins:
        lid=model.identity(p['lease_id'][2:]);lease=only(observed['snapshot_leases'],lease_id=lid)
        reader=p['purpose']=='reader';tx=p['xmin'];fresh_xmin(tx,original)
        acquired,updated,expires=(lease[k] for k in ('acquired_at_us','updated_at_us','expires_at_us'))
        for at in (acquired,updated):clock(at,window)
        assert control['acquired_at_us']<=acquired<=updated<=control['released_at_us']
        assert 1<=p['requested_ttl_us']<=(5_000_000 if reader else 30_000_000) and acquired<expires<=min(acquired+p['requested_ttl_us'],control['lease_expires_at_us'])
        if not reader:assert p['requested_ttl_us']==30_000_000 and lease['xmin']==tx
        else:assert lease['xmin']!=tx;fresh_xmin(lease['xmin'],original)
        wanted['archive_snapshot_pins'].append(dict(lease_id=lid,dataset_id=ds,consumer_id=catalog,frontier_revision=frontier,
            issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_role='control',issuer_incarnation=control['process_incarnation'],
            purpose=p['purpose'],requested_ttl_us=p['requested_ttl_us'],export_job_id=None if reader else detail['job_id'],xmin=tx))
        wanted['snapshot_leases'].append(dict(lease_id=lid,dataset_id=ds,consumer_id=catalog,frontier_revision=frontier,topology_generation=2,
            holder_kind='role_instance',holder_job_id=None,holder_role_instance_id=control['role_instance_id'],holder_role='control',
            holder_incarnation=control['process_incarnation'],lease_revision=2 if reader else 1,acquired_at_us=acquired,updated_at_us=updated,
            expires_at_us=expires,state='released' if reader else 'active',released_at_us=updated if reader else None,
            release_reason='reader_joined' if reader else None,archive_pin_protocol=1,xmin=lease['xmin']))
        if not reader:assert updated==acquired
        for epoch,cut in ((1,12),(3,10)):
            claim=only(observed['retention_claims'],lease_id=lid,owner_epoch=epoch);cid=model.identity(claim['claim_id'][2:])
            clock(claim['created_at_us'],window);assert acquired<=claim['created_at_us']<=updated if reader else claim['created_at_us']>=acquired
            if reader:clock(claim['released_at_us'],window);assert claim['created_at_us']<=claim['released_at_us']<=updated
            wanted['retention_claims'].append(dict(claim_id=cid,lease_id=lid,dataset_id=ds,topology_generation=2,shard_id=0,
                owner_epoch=epoch,first_sequence=1,last_sequence=cut,purpose=p['purpose'],created_at_us=claim['created_at_us'],
                state='released' if reader else 'active',released_at_us=claim['released_at_us'] if reader else None,
                release_reason='reader_joined' if reader else None,xmin=lease['xmin']))
