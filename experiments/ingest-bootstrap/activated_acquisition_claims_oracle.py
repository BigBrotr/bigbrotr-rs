"""Actual same-activator intent selection, independently checked without projection.

The finite seed preserves a genuine AlreadyDurable request and creates a second
queued claim with no request plus one accepted unclaimed intent. Installed
abandonment must select both claims and preserve the unclaimed job and session.
"""
import copy
import json
import math
import re

import activated_acquisition_oracle as abandonment
import activated_source_durable_oracle as durable
import activated_source_closure_oracle as closure
import activated_recovery_oracle as recovery
import initial_live_oracle as live
from activated_source_durable_oracle import authenticated_job, durable_science, admitted
from initial_live_routes_oracle import backend_checked as finite_backend
from initial_succession_oracle import binary, claim, ended, only, prepared, renewed, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import catalog_identity, instant

MODE = 'installed-activated-acquisition-claims'
CASES = (MODE,)
PROBE = 'successor-live-acquisition-claims'


def accepted(before, after, report, control, clock, purpose, key, lower):
    wanted = copy.deepcopy(before)
    command = only(after['management_commands'], request_key=key, actor_id=durable.ACTOR)
    tx, at = command['xmin'], command['accepted_at_us']
    fresh_xmin(tx, before); live.clock(at, clock)
    assert lower <= at < control['lease_expires_at_us']
    ds = only(before['datasets'])['dataset_id']
    digest = live.bytea(live.sha(b'bigbrotr/acquisition-job/1\0' + binary(ds) + binary(durable.ACTOR)
        + binary(key) + binary(durable.SCOPE) + live.be(30_000_000) + live.be(1) * 2))
    audit = only(after['management_audit'], action='job.acquisition.accept', subject_id=key)
    assert len(binary(audit['audit_id'])) == 16
    assert audit['audit_id'] not in {r['audit_id'] for r in before['management_audit']}
    wanted['management_audit'].append(dict(only(before['management_audit'], action='job.acquisition.accept',
        subject_id=durable.REQUEST_KEY), audit_id=audit['audit_id'], subject_id=key, created_at_us=at, xmin=tx))
    wanted['management_commands'].append(dict(only(before['management_commands'], request_key=durable.REQUEST_KEY),
        request_key=key, request_digest=digest, accepted_at_us=at, audit_id=audit['audit_id'], xmin=tx))
    job = only(after['jobs'], request_key=key)
    assert len(binary(job['job_id'])) == 16 and job['job_id'] not in {r['job_id'] for r in before['jobs']}
    wanted['jobs'].append(dict(only(before['jobs'], request_key=durable.REQUEST_KEY),
        job_id=job['job_id'], request_key=key, request_digest=digest, state='accepted', job_revision=1,
        created_at_us=at, owner_resource_id=None, owner_epoch=None, owner_incarnation=None, xmin=tx))
    wanted['acquisition_jobs'].append(dict(only(before['acquisition_jobs'], request_key=durable.REQUEST_KEY),
        job_id=job['job_id'], request_key=key, deadline_at_us=at+30_000_000, xmin=tx))
    wanted['job_admissions'].append(dict(job_id=job['job_id'],
        admission_ordinal=max(r['admission_ordinal'] for r in before['job_admissions'])+1, xmin=tx))
    same(wanted, after)
    assert report == dict(event='live-extra-acceptance', purpose=purpose, error=None, commit_started=True,
        receipt=dict(command_key=key[2:], job_id=job['job_id'][2:], accepted_at_us=at, created=True))
    return job


def extra_claim(before, after, report, control, job):
    wanted = copy.deepcopy(before)
    actual = only(after['jobs'], job_id=job['job_id'])
    fresh_xmin(actual['xmin'], before)
    only(wanted['jobs'], job_id=job['job_id']).update(state='queued', job_revision=2,
        owner_resource_id=control['resource_id'], owner_epoch=8,
        owner_incarnation=control['process_incarnation'], xmin=actual['xmin'])
    same(wanted, after)
    assert report == dict(event='live-extra-claim', error=None, commit_started=True,
        job=dict(id=job['job_id'][2:], scope=durable.SCOPE[2:], state='queued', job_revision=2,
            checkpoint_revision=0, accepted_at_us=job['created_at_us'], deadline_at_us=job['created_at_us']+30_000_000,
            owner=dict(resource=control['resource_id'][2:], epoch=8, incarnation=control['process_incarnation'][2:])))

def seed_state(events, original, native, inputs, *, retired, server_window, backend, response,
        after, native_after, inputs_after, input_contents, catalog_before, catalog_after, receive_clock):
    live.check_window(server_window)
    live.migrated(retired, original, native, inputs, server_window)
    catalog_identity(catalog_before)
    assert catalog_before == catalog_after
    assert response['returncode'] == 0 and response['stderr'] == ''
    assert [json.loads(line) for line in response['stdout'].splitlines()] == events
    observed = only(events, event='successor-plan')
    live.clock(observed['created_at_us'], server_window)
    plan, old_rows, prepared_rows = live.minted_plan(original, native, inputs, observed)
    capture = live.Capture(events, original, old_rows, inputs)
    assert capture.take('successor-control', 'initial', 'claim-planned') == dict(event='successor-control',
        stage='initial', phase='claim-planned', role='d3' * 16, incarnation='dd' * 16, lease_ttl_us='20000000')
    assert plan['target_role'] != 'd3' * 16 and plan['target_incarnation'] != 'dd' * 16
    same(original, capture.snapshot('initial-before-claim'))
    claimed = capture.take('successor-control', 'initial', 'claimed')
    current = capture.snapshot('initial-after-claim')
    control = claim(original, current, 1)
    assert control['epoch'] == 8
    assert claimed == dict(event='successor-control', stage='initial', phase='claimed',
        resource=control['resource_id'][2:], epoch=8, role='d3' * 16, incarnation='dd' * 16)
    live.clock(control['acquired_at_us'], server_window)
    live.clock(instant(control['control_backend_start']), server_window)
    live.clock(only(current['role_instances'], role_instance_id=control['role_instance_id'])['created_at_us'], server_window)
    assert capture.take('successor-plan') == plan
    following = capture.snapshot('before-prepare')
    control = renewed(current, following, control, plan['created_at_us'])
    fresh_xmin(control['xmin'], current)
    live.clock(control['lease_expires_at_us'] - 20_000_000, server_window)
    current = following
    capture.native, capture.sequence = prepared_rows, 62
    following = capture.snapshot('prepared')
    # Only the independently checked empty activation table is excluded from
    # this older71-table preparation reducer; all72 tables remain captured.
    assert current[live.ACTIVATIONS] == following[live.ACTIVATIONS] == []
    prepared({k: v for k, v in current.items() if k != live.ACTIVATIONS},
        {k: v for k, v in following.items() if k != live.ACTIVATIONS}, plan, control)
    current = following
    physical = capture.take('successor-backend', 'initial')
    assert physical == dict(event='successor-backend', stage='initial',
        **backend['exact']['expected'], present=True)
    assert capture.take('live-activation-start') == dict(event='live-activation-start', mode=PROBE, control_epoch=8)
    result = capture.take('live-activation-result')
    assert result == dict(event='live-activation-result', writer=True, error=None,
        activated_at_us=result['activated_at_us'])
    following = capture.snapshot('activated-zero')
    live.activated(current, following, plan, control, server_window, result['activated_at_us'])
    current = following
    job = capture.take('live-durable-job')
    following = capture.snapshot('durable-job-claimed')
    control = authenticated_job(current, following, job, control, server_window)
    current = following
    wire = capture.take('live-durable-wire')
    subscription = wire['request'][1]
    assert re.fullmatch(r'[0-9a-f]{32}0000000000000001', subscription)
    assert int(subscription[:32], 16) != 0
    assert wire == dict(event='live-durable-wire', request=['REQ', subscription, dict(kinds=[1], limit=1)],
        signed_event=durable.SIGNED, events=1, eose=True)
    assert capture.take('live-durable-peer-closed') == dict(event='live-durable-peer-closed', eof=True)
    same(current, capture.snapshot('durable-received'))
    observation = capture.take('live-durable-input')
    assert observation['wire_request'] == subscription
    assert job['accepted_at_us'] <= observation['received_at_us'] < job['deadline_at_us']
    science, filename, raw, receipt = durable_science(plan, prepared_rows, inputs, input_contents,
        observation, receive_clock, server_window)
    capture.native, capture.sequence, capture.inputs = science['rows'], 64, science['inputs']
    following = capture.snapshot('already-durable')
    admitted(current, following, science, raw, receipt, control, observation, server_window)
    current = following
    assert capture.take('live-already-durable') == dict(event='live-already-durable', request=1,
        receipt=receipt.hex(), dependencies=10, source_cut=0, peer_joined=True, job_complete=False, session_revoked=False)
    following = capture.snapshot('claims-control-refreshed')
    first = only(events, event='live-extra-acceptance', purpose='zero-request')['receipt']['accepted_at_us']
    previous_outcome = only(current['producer_outcomes'], producer_epoch=2)['recorded_at_us']
    control = renewed(current, following, control, first)
    fresh_xmin(control['xmin'], current)
    refresh = control['lease_expires_at_us']-20_000_000
    live.clock(refresh, server_window)
    assert previous_outcome <= refresh <= first
    current = following
    acceptance = capture.take('live-extra-acceptance')
    following = capture.snapshot('claims-zero-accepted')
    claimed = accepted(current, following, acceptance, control, server_window,
        'zero-request', '\\x'+'a7'*16, refresh)
    current = following
    claimed_report = capture.take('live-extra-claim')
    following = capture.snapshot('claims-zero-claimed')
    extra_claim(current, following, claimed_report, control, claimed)
    current = following
    acceptance = capture.take('live-extra-acceptance')
    following = capture.snapshot('claims-unclaimed-accepted')
    unclaimed = accepted(current, following, acceptance, control, server_window,
        'unclaimed', '\\x'+'a8'*16, claimed['created_at_us'])
    current = following
    expected_inputs = [dict(name=k, bytes=input_contents[k]) for k in sorted(input_contents)]
    assert capture.take('live-extra-intents') == dict(event='live-extra-intents',
        claimed_job=claimed['job_id'][2:], unclaimed_job=unclaimed['job_id'][2:], extra_executions=0,
        native_rows=53, native_sequence=64, inputs_before=expected_inputs, inputs_after=expected_inputs)
    same(current, capture.snapshot('final-before-control-end'))
    assert capture.take('successor-control', 'final', 'released-and-joined') == dict(event='successor-control',
        stage='final', phase='released-and-joined', resource=control['resource_id'][2:], epoch=8)
    following = capture.snapshot('final-after-control-end')
    ended(current, following, control, True)
    released = only(following['owner_epochs'], role='control', epoch=8)
    fresh_xmin(released['xmin'], current)
    live.clock(released['released_at_us'], server_window)
    assert unclaimed['created_at_us'] <= released['released_at_us']
    live.clock(only(following['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    current = following
    assert capture.take('successor-backend', 'initial') == dict(physical, present=False)
    same(current, capture.snapshot('final'))
    assert capture.take('live-complete') == dict(event='live-complete', mode=PROBE, native='closed',
        control='released-and-joined', writer=False, admission=True)
    assert capture.position == len(events)
    same(current, after)
    assert recovery.native(native_after) == science['rows'] and native_after['sequence'] == 64
    assert inputs_after == science['inputs'] and len(inputs_after) == 5
    assert set(input_contents) == set(inputs_after) and input_contents[filename] == raw.hex()
    for name, value in input_contents.items():
        value = bytes.fromhex(value)
        assert inputs_after[name] == dict(bytes=len(value), sha256=live.digest(value))
    assert len(science['rows']) == 53
    finite_backend(backend, released)
    assert math.isfinite(backend['absence']['seconds']) and 0 <= backend['absence']['seconds'] <= 120
    return science, capture.snapshots


def verify_seed(**context):
    science, count = seed_state(**context)
    return dict(status='PASS', snapshots=count, control_tables=72, native_rows=53, native_sequence=64,
        retained_inputs=5, issued=1, reserved=1, source_sequence=0, outcome='AlreadyDurable',
        source_closure_sha256=science['digest'], actual_network=True, claimed_jobs=2,
        claimed_zero_request_jobs=1, unclaimed_jobs=1, session_revoked=False, installed_scheduler=False)


def migrated(seed_context, setup, clock):
    wanted = durable.migrated_seed(seed_context, setup, clock)
    grant = setup['grant']
    assert grant['before'] == setup['source_upgrade']['catalog_ungranted'] and grant['statement'] == closure.GRANT
    assert grant['response']['returncode'] == 0 and grant['response']['stdout'] == grant['response']['stderr'] == ''
    expected_catalog = copy.deepcopy(grant['before'])
    only(expected_catalog['functions'], name=closure.SOURCE_FUNCTION)['execute'] = True
    assert grant['after'] == expected_catalog
    upgrade = setup['abandonment_upgrade']
    same(wanted, upgrade['before'])
    assert setup['abandonment_migration_sha256'] == abandonment.MIGRATION
    row = only(upgrade['after']['schema_versions'], migration=36)
    fresh_xmin(row['xmin'], wanted)
    assert type(row['applied_at_us']) is int
    assert max(r['applied_at_us'] for r in wanted['schema_versions']) <= row['applied_at_us'] <= clock['before_us']
    assert row == dict(only(wanted['schema_versions'], migration=35), migration=36,
        name='0036_activated_acquisition_abandonment.sql', checksum='\\x'+abandonment.MIGRATION,
        applied_at_us=row['applied_at_us'], xmin=row['xmin'])
    wanted['schema_versions'].append(row)
    same(wanted, upgrade['after']); same(wanted, upgrade['reapplied'])
    assert upgrade['physical_before'] == upgrade['physical_after'] and set(upgrade['physical_before']) == set(wanted)
    assert upgrade['grant_statements'] == [] and not any(k in upgrade for k in ('outcome','incomplete_response'))
    assert len(upgrade['responses']) == 2 and all(r['returncode']==0 and r['stderr']=='' for r in upgrade['responses'])
    abandonment.catalog_old(upgrade['catalog_before'], expected_catalog)
    abandonment.catalog_migrated(upgrade['catalog_before'], upgrade['catalog_after'])
    assert setup['catalog_before'] == setup['catalog_after'] == upgrade['catalog_after']
    return wanted


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed_context, setup, server_window, backend, predecessor=None):
    assert mode in (MODE, MODE+'-retry')
    window(server_window)
    science, _ = seed_state(**seed_context)
    assert seed_context['server_window']['after_us'] <= server_window['before_us']
    wanted = migrated(seed_context, setup, server_window)
    first = mode == MODE
    if first:
        assert predecessor is None and native_before == seed_context['native_after']
    else:
        assert predecessor is not None and predecessor['mode'] == MODE
        assert predecessor['seed_context'] == seed_context and predecessor['setup'] == setup
        verify(**predecessor)
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        wanted = predecessor['after']
        assert native_before == predecessor['native_after']
    same(wanted, before)
    assert inputs_before == inputs_after == seed_context['inputs_after'] and input_contents == seed_context['input_contents']
    closed = dict(science['rows'])
    closed['receipts', b'\x01\x31'+science['slot']+live.be(2)] = b'\x02\x31'+live.be(1)+live.be(0)*2+b'\x01'
    assert native(native_after) == closed and native_after['sequence'] == 65
    projected, control = control_delta(before, after, 'normal', server_window)
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    source = only(wanted['source_streams'], owner_epoch=2)
    if first:
        assert producer['state'] == source['state'] == 'open'
        changed = only(projected['producer_epochs'], producer_epoch=2)
        fresh_xmin(changed['xmin'], before)
        producer.update(state='closing', xmin=changed['xmin'])
        changed = only(projected['source_streams'], owner_epoch=2)
        fresh_xmin(changed['xmin'], before); live.clock(changed['closed_at_us'], server_window)
        assert control['acquired_at_us'] <= changed['closed_at_us'] <= control['lease_expires_at_us']-20_000_000
        source.update(state='closed',last_complete_sequence=0,closure_digest='\\x'+science['digest'],
            closed_at_us=changed['closed_at_us'],xmin=changed['xmin'])
        activation = only(before[live.ACTIVATIONS])
        txs = set()
        for key in (durable.REQUEST_KEY, '\\x'+'a7'*16):
            job = only(wanted['jobs'],request_key=key)
            assert job['state']=='queued' and job['job_revision']==2 and job['checkpoint_revision']==0
            assert (job['owner_resource_id'],job['owner_epoch'],job['owner_incarnation']) == (
                activation['activated_by_resource'],8,activation['activated_by_incarnation'])
            gap = only(projected['acquisition_gaps'],job_id=job['job_id'])
            fresh_xmin(gap['xmin'], before); txs.add(gap['xmin'])
            at = gap['abandoned_at_us']
            assert type(at) is int and max(source['closed_at_us'],job['created_at_us'],control['lease_expires_at_us']-20_000_000) <= at <= control['released_at_us']
            assert at < control['lease_expires_at_us']
            assert gap == dict(job_id=job['job_id'],dataset_id=job['dataset_id'],producer_slot_id=live.bytea(science['slot']),
                producer_epoch=2,shard_id=0,source_owner_epoch=2,last_complete_sequence=0,source_closure_digest='\\x'+science['digest'],
                previous_job_revision=2,previous_owner_resource=activation['activated_by_resource'],previous_owner_epoch=8,
                previous_owner_incarnation=activation['activated_by_incarnation'],issuer_resource=control['resource_id'],
                issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],owner_role='control',
                abandoned_at_us=at,reason=abandonment.REASON,xmin=gap['xmin'])
            wanted['acquisition_gaps'].append(gap)
            job.update(state='cancelled',job_revision=4,reason_code=abandonment.REASON,completed_at_us=at,
                owner_resource_id=control['resource_id'],owner_epoch=control['epoch'],
                owner_incarnation=control['process_incarnation'],xmin=gap['xmin'])
        assert len(txs)==1 and len(txs | {producer['xmin'],source['xmin'],control['xmin']})==4
    else:
        assert producer['state']=='closing' and source['state']=='closed'
    same(wanted, projected)
    report = dict(format='bigbrotr-ingest-recovery/1',state='activated-acquisition-abandoned',
        dataset_id=science['ds'].hex(),owner_resource_id=science['plan']['resource'],producer_slot_id=science['slot'].hex(),
        producer_epoch='2',issued_through='1',reserved_through='1',resolved_requests='1',
        source_closure=dict(last_complete_sequence='0',sha256=science['digest']),
        acquisition_abandonment=dict(jobs='2',reason=abandonment.REASON,coverage='unknown'),
        native='closed',owner_release='committed',ready=False)
    abandonment.response_checked(response,mode,report)
    recovery.backend_checked(backend,control,response,server_window)
    previous = predecessor if predecessor is not None else seed_context
    old = previous['backend']['exact']['expected']
    assert backend['absence']['database']==previous['backend']['absence']['database']
    assert (control['control_backend_pid'],control['control_backend_start'])!=(old['pid'],old['backend_start'])
    assert previous['server_window']['after_us'] <= instant(control['control_backend_start'])
    if predecessor:
        old = predecessor['response']['terminal']; now=response['terminal']
        assert (now['pid'],now['start_ticks'])!=(old['pid'],old['start_ticks'])
    return dict(status='PASS',mode=mode,observed_states=2,control_tables=72,native_rows=53,native_sequence=65,
        retained_inputs=5,issued=1,reserved=1,source_sequence=0,source_closure_sha256=science['digest'],
        abandoned_jobs=2,new_gaps=2 if first else 0,unclaimed_jobs=1,new_native_writes=int(first),
        session_revoked=False,source_closed=True,producer_retired=False,ready=False)


def seed_negative_controls(**context):
    verify_seed(**context)
    rejected=[]
    def check(name,mutate):
        value=copy.deepcopy(context);mutate(value)
        value['response']['stdout']=''.join(json.dumps(e)+'\n' for e in value['events'])
        try: verify_seed(**value)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError):rejected.append(name);return
        raise AssertionError('accepted seed counterexample: '+name)
    def sql(c,stage,mutate):
        e=only(c['events'],event='successor-state',stage=stage)
        pg={k:json.loads(v) for k,v in e['state']['control_tables']};mutate(pg)
        e['state']['control_tables']=[[k,live.encoded(pg[k]).decode()] for k in sorted(pg)]
        e['state']['control_sha256']=live.digest(live.encoded(e['state']['control_tables']))
    check('wrong-key',lambda c: only(c['events'],event='live-extra-acceptance',purpose='zero-request')['receipt'].update(command_key='a8'*16))
    check('unconfirmed-acceptance',lambda c: only(c['events'],event='live-extra-acceptance',purpose='unclaimed').update(commit_started=False))
    check('zero-request-claim-missing',lambda c: sql(c,'claims-zero-claimed',lambda p: only(p['jobs'],request_key='\\x'+'a7'*16).update(state='accepted',job_revision=1)))
    check('foreign-zero-request-claim',lambda c: sql(c,'claims-zero-claimed',lambda p: only(p['jobs'],request_key='\\x'+'a7'*16).update(owner_epoch=3)))
    check('unclaimed-adopted',lambda c: sql(c,'claims-unclaimed-accepted',lambda p: only(p['jobs'],request_key='\\x'+'a8'*16).update(owner_epoch=8)))
    check('session-touched',lambda c: sql(c,'claims-unclaimed-accepted',lambda p: only(p['browser_sessions']).update(last_seen_at_us=1)))
    check('new-native-execution',lambda c: only(c['events'],event='live-extra-intents').update(native_sequence=65,extra_executions=1))
    check('input-list-rewritten',lambda c: only(c['events'],event='live-extra-intents')['inputs_after'][0].update(bytes='00'))
    check('job-detail-rewritten',lambda c: sql(c,'claims-unclaimed-accepted',lambda p: only(p['acquisition_jobs'],request_key='\\x'+'a7'*16).update(duration_us=1)))
    check('old-gap-mutated',lambda c: sql(c,'final',lambda p: p['acquisition_gaps'][0].update(xmin='999999')))
    check('unjoined-wire',lambda c: only(c['events'],event='live-durable-peer-closed').update(eof=False))
    check('false-source-cut',lambda c: only(c['events'],event='live-already-durable').update(source_cut=1))
    check('backend-present',lambda c:c['backend']['exact'].update(present=True))
    return dict(status='PASS',rejected=rejected,count=len(rejected))


def negative_controls(**context):
    verify(**context)
    rejected=[]
    def check(name,mutate):
        value=copy.deepcopy(context);mutate(value)
        try: verify(**value)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError):rejected.append(name);return
        raise AssertionError('accepted abandonment counterexample: '+name)
    def job(c,key):return only(c['after']['jobs'],request_key='\\x'+key*16)
    def gap(c,key):return only(c['after']['acquisition_gaps'],job_id=job(c,key)['job_id'])
    check('zero-request-job-not-selected',lambda c:c['after']['acquisition_gaps'].remove(gap(c,'a7')))
    check('unclaimed-job-cancelled',lambda c:job(c,'a8').update(state='cancelled',job_revision=3))
    check('unclaimed-intent-lost',lambda c:c['after']['acquisition_jobs'].remove(only(c['after']['acquisition_jobs'],request_key='\\x'+'a8'*16)))
    check('zero-request-wrong-owner',lambda c:gap(c,'a7').update(previous_owner_epoch=3))
    check('split-selected-job-transactions',lambda c:(gap(c,'a7').update(xmin='999999'),job(c,'a7').update(xmin='999999')))
    check('old-gap-rewritten',lambda c:only(c['after']['acquisition_gaps'],producer_epoch=1).update(issuer_epoch=9))
    check('new-request',lambda c:only(c['after']['producer_epochs'],producer_epoch=2).update(reserved_through_request=2))
    check('outcome-recorder-rewritten',lambda c:only(c['after']['producer_outcomes'],producer_epoch=2).update(recorder_epoch=9))
    check('false-source-closure',lambda c:only(c['after']['source_streams'],owner_epoch=2).update(last_complete_sequence=1))
    check('session-revoked',lambda c:only(c['after']['browser_sessions']).update(revoked_at_us=1))
    check('extra-native-write',lambda c:c['native_after'].update(sequence=66))
    check('input-original-mutated',lambda c:c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    check('backend-present',lambda c:c['backend']['exact'].update(present=True))
    check('not-joined',lambda c:c['response']['terminal'].update(reaped=False))
    check('wrong-cli',lambda c:c['response']['terminal']['command'].__setitem__(-1,'--close-activated-source'))
    if context['mode'] == MODE:
        def expired(c):
            owner=max((r for r in c['after']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            deadline=owner['lease_expires_at_us']
            owner['released_at_us']=deadline+1
            only(c['after']['role_instances'],role_instance_id=owner['role_instance_id'])['retired_at_us']=deadline+2
            c['server_window']['after_us']=deadline+100
            for key in ('a6','a7'):
                gap(c,key)['abandoned_at_us']=job(c,key)['completed_at_us']=deadline
        check('coherent-expired-gap-authority',expired)
    return dict(status='PASS',rejected=rejected,count=len(rejected))
