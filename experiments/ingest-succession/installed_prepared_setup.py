"""Independent full-state reader for the two genuine interrupted setup calls.

The qualified finite transcript/control and literal portable row constructors
are reused unchanged. This owner binds the exact dynamic head4 empty closure
and C12 boundary, in addition to the old terminal2 C11 preparation boundary.
Neither interrupted setup is reported as successful installed recovery.
"""
import copy
import json
import re
import transition_replay as base
import transition_fault_replay as faults
import installed_prepared_model as model

history=base.history
encoded,sha,binary,be=base.encoded,base.sha,base.binary,base.be
only,same,loads,instant=base.only,base.same,base.loads,base.instant
PROXY_SHA256,COUNTERS=faults.PROXY_SHA256,faults.COUNTERS

def wire_checked(fault, operation, port):
    assert operation in ('prepare', 'empty')
    point, mode, commit = ('prepare', 'after-complete', 11) if operation=='prepare' else ('empty', 'before-forward', 12)
    assert set(fault) == {'point', 'mode', 'commit_number', 'proxy_port', 'wire'}
    assert fault['point'] == point and fault['mode'] == mode and fault['commit_number'] == commit
    assert type(port) is int and 1 <= port <= 65535 and port != 5432
    assert fault['proxy_port'] == port
    wire = fault['wire']
    assert wire['status'] == 'CAPTURED'
    assert type(wire['exit_code']) is int and wire['exit_code'] == 0
    assert type(wire['wrapper_exit']) is int and wire['wrapper_exit'] == 0
    assert wire['forced_wrapper_stop'] is False and wire['stderr_nonempty'] is False
    assert wire['output_overflow'] is False and wire['proxy_sha256'] == PROXY_SHA256
    assert not any(k in wire for k in ('error', 'cleanup_error', 'metadata_error'))
    events = wire['events']
    assert events == [wire['ready'], wire['fault'], wire['receipt']]
    assert [e['event'] for e in events] == ['ready', 'fault', 'receipt']
    vector = [commit, commit, commit, commit-1, 1] if operation=='prepare' else [commit, commit-1, commit-1, commit-1, 1]
    elapsed = 0
    for number, event in enumerate(events):
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1'
        assert event['mode'] == mode and event['commit_number'] == commit
        assert event['held'] is False and event['hold_action'] is None
        assert type(event['elapsed_ns']) is int and elapsed <= event['elapsed_ns'] <= 35_000_000_000
        elapsed = event['elapsed_ns']
        assert all(type(event[k]) is int for k in COUNTERS)
        assert [event[k] for k in COUNTERS] == (vector if number else [0] * 5)
        if number:
            assert event['reason'] == mode
    ready, _, receipt = events
    assert ready['proxy_sha256'] == PROXY_SHA256 and ready['listen_host'] == '127.0.0.1'
    assert ready['listen_port'] == port and ready['upstream_port'] == 5432
    assert type(ready['pid']) is int and ready['pid'] > 0
    assert ready['deadline_seconds'] == 30 and ready['max_frame_bytes'] == 1024**2
    assert ready['max_wire_bytes'] == 64 * 1024**2 and ready['max_messages'] == 20000
    assert set(receipt['frames']) == set(receipt['tags']) == {'frontend', 'backend'}
    for direction, tags in receipt['tags'].items():
        assert all(re.fullmatch('[0-9a-f]{2}', key) and type(n) is int and n > 0 for key, n in tags.items())
        assert type(receipt['frames'][direction]) is int
        assert receipt['frames'][direction] == sum(tags.values()) > 0
    assert sum(receipt['frames'].values()) <= ready['max_messages']
    assert receipt['tags']['frontend']['51'] >= commit
    assert receipt['tags']['backend']['43'] >= receipt['server_commit_complete']
    assert type(receipt['declared_wire_bytes']) is int and 0 < receipt['declared_wire_bytes'] <= ready['max_wire_bytes']
    raw = ''.join(json.dumps(e, sort_keys=True) + '\n' for e in events).encode()
    assert wire['stdout'].encode() == raw
    assert wire['stdout_sha256'] == sha(raw) and wire['stdout_bytes'] == len(raw)


def lineage(pg,native):
    legacy,values,head,registered=model.coordinates(pg,native)
    assert head==registered and head in (2,4)
    through=2 if head==2 else 3
    slot,ds,resource=(binary(legacy[k]) for k in ('producer_slot_id','dataset_id','owner_resource_id'))
    entries,summaries=[],[]
    for epoch in range(1,head+1):
        if epoch==1:manifest,incoming=binary(legacy['previous_manifest']),bytes(32)
        else:
            intent=legacy if epoch==2 else only(pg['ingest_successions'],target_epoch=epoch)
            manifest,incoming=binary(intent['target_manifest']),bytes.fromhex(sha(binary(intent['handoff'])))
            assert values['default',(b'\x01\x0a'+ds+resource+be(epoch-1)).hex()]==intent['handoff'][2:]
        if epoch<=through:
            summary=model.summary(pg,epoch);summaries.append(summary)
            count,cut=summary['resolved_requests'],summary['last_complete_sequence']
        else:count,cut=0,0
        closing=epoch<=through
        marker=(b'\x02\x31'+be(count)+bytes(16)+b'\x01') if closing else b'\x01\x31'+bytes(24)
        assert values['receipts',(b'\x01\x31'+slot+be(epoch)).hex()]==marker.hex()
        assert values['default',(b'\x01\x02'+ds+be(0,4)+be(epoch)).hex()]==(b'\x01\x02'+be(cut)).hex()
        entries.append(dict(manifest=manifest.hex(),issued_through=count,source_through=cut,closing=closing,
            incoming_sha256=incoming.hex(),terminal_kind=summaries[-1]['terminal_kind'] if epoch<head else None,
            terminal_digest=summaries[-1]['digest'] if epoch<head else None))
    assert values['default','0101']==entries[-1]['manifest']
    return dict(slot=slot.hex(),head=head,registered=head,pending=None,through=through,
        state='Terminal' if head==2 else 'PreparedOpen',entries=entries,summaries=summaries,
        counts=[s['resolved_requests'] for s in summaries])

def verify(events, before, native, inputs, case, server_window, *, fault=None):
    """Later baselines are accepted only with replay's complete previous link."""
    operation=case['operation'];assert operation in ('prepare','empty') and case['setup'] is True
    assert case['name']==('gap-prepare-loss' if operation=='prepare' else 'closing-empty-loss')
    assert case['clone']==('gap' if operation=='prepare' else 'closing')
    assert case['origin']==('terminal' if operation=='prepare' else 'closing') and case['predecessor'] is None
    assert case['metadata_bytes']==262144 and type(case['metadata_bytes']) is int
    assert re.fullmatch('[a-z0-9_]{1,63}',case['database']) and case['config']=='/lab/'+case['database']+'.toml'
    model.seed_checked((before,native,inputs),case['origin'],server_window)
    wire_checked(fault,operation,case['proxy_port'])
    failed=True
    m=faults.Model(before,native,inputs,server_window)
    known=lineage(before,native)
    t=faults.Transcript(events,m)
    candidate = None
    if operation == 'prepare':
        candidate = t.take('candidate', True)
        assert set(candidate) == {'event', 'role', 'incarnation', 'namespaces'}
        assert [r['kind'] for r in candidate['namespaces']] == list(range(1, 10))
        assert all(set(r) == {'kind', 'id'} and type(r['kind']) is int for r in candidate['namespaces'])
        ids = [candidate['role'], candidate['incarnation']] + [r['id'] for r in candidate['namespaces']]
        assert len(set(ids)) == 11
        assert all(re.fullmatch('[0-9a-f]{32}', value) and int(value, 16) and value not in m.identities for value in ids)
    resource = t.take('resources')
    assert set(resource) == {'event', 'metadata_bytes', 'metadata_scratch_bytes', 'metadata_entry_bytes',
        'input_bytes', 'outcome_bytes', 'effect_bytes', 'native_bytes', 'transport_bytes',
        'observation_bytes', 'operation_timeout_ms', 'frame_bytes'}
    assert resource == dict(resource, metadata_bytes=262144, metadata_scratch_bytes=131072,
        metadata_entry_bytes=512, native_bytes=128 * 1024**2, transport_bytes=4 * 1024**2,
        observation_bytes=32 * 1024**2, operation_timeout_ms=10000, frame_bytes=16 * 1024**2)
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resource[field]) is int and 0 < resource[field] <= 256 * 1024**2
    mutation = {'prepare': 8192, 'gap': 8192, 'empty': 4096, 'empty-retry': 0}[operation]
    assert sum(resource[k] for k in ('metadata_bytes', 'input_bytes', 'outcome_bytes', 'effect_bytes',
        'transport_bytes', 'observation_bytes')) + mutation <= 1024**3
    assert t.take('resources', True) == dict(event='lineage-transition-resources', operation=operation,
        mutation_data_bytes=mutation, mutation_native_bytes=mutation, lease_ttl_us='20000000', outstanding_request_limit=32)
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    if candidate:
        assert not ({planned['role'], planned['incarnation']} & set(ids))
    t.snapshot('before-claim')
    if failed:
        transport = t.take('transport', True)
        assert transport['proxied'] is True
        assert transport == dict(event='lineage-transition-transport',
            control_port=case['proxy_port'], observer_port=5432, proxied=True)
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    assert claimed == dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation'])
    assert backend == dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'], control_epoch=m.control['epoch'], present=True)
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-read')
    t.history(known)
    event = t.take('plan', True)
    observation = None
    if operation=='prepare':
        created, deadline = event['created_at_us'], event['lease_expires_at_us']
        assert re.fullmatch('[1-9][0-9]*', created) and re.fullmatch('[1-9][0-9]*', deadline)
        plan = m.plan(candidate['role'], candidate['incarnation'], [r['id'] for r in candidate['namespaces']],
            3, int(created), int(deadline))
        assert event == dict(event='lineage-transition-plan', operation=operation, record=base.record_value(plan['record'], 3),
            role=candidate['role'], created_at_us=created, lease_expires_at_us=deadline, outstanding_request_limit=32,
            policy=dict(config_revision=m.ds['config_revision'], policy_revision=m.ds['policy_revision'],
                config_digest=only(m.pg['config_revisions'], revision=m.ds['config_revision'])['document_digest'][2:],
                policy_digest=only(m.pg['policy_revisions'], revision=m.ds['policy_revision'])['document_digest'][2:]))
        t.snapshot('planned')
        t.snapshot('transition-returned', lambda pg: m.prepare_sql(pg, plan))
    else:
        epoch=known['head'];assert epoch==4
        intent=only(m.pg['ingest_successions'],target_epoch=epoch)
        digest=base.empty_digest(binary(intent['handoff']),binary(intent['target_manifest']),binary(m.slot),epoch)
        assert event==dict(event='lineage-transition-plan',operation='empty',
            record=base.record_value(binary(intent['handoff']),epoch),closure_digest=digest)
        t.snapshot('planned')
        def closing(pg):
            same(m.pg,pg);model.close(m.native,m.slot,epoch)
        t.snapshot('transition-returned',closing)
    assert t.take('result', True) == dict(event='lineage-transition-result', operation=operation,
        stage=operation if failed else 'complete', epoch=None, sequence=None,
        error='CommitUnknown' if failed else None, observation=observation)
    t.snapshot('after-transition')
    assert t.take('native') == dict(event='lineage-history-native', state='closed')
    t.snapshot('before-control-end')
    assert t.take('driver') == dict(event='lineage-history-driver', driver='control', failed=failed, joined=True)
    assert t.take('backend') == dict(backend, present=False)
    phase = 'dropped-and-joined' if failed else 'released-and-joined'
    assert t.take('control') == dict(event='lineage-history-control', phase=phase,
        resource=m.control['resource_id'][2:], epoch=m.control['epoch'])
    t.snapshot('after-control-end', None if failed else m.release)
    t.snapshot('final')
    assert t.take('driver') == dict(event='lineage-history-driver', driver='observer', failed=False, joined=True)
    assert t.take('complete', True) == dict(event='lineage-transition-complete', operation=operation,
        native='closed', control=phase, observer='joined', success=not failed, admission=False, activation=False)
    assert t.position == len(events)
    assert t.snapshots == (28 if operation=='prepare' else 30)
    assert m.refreshes == (9 if operation=='prepare' else 10)
    return dict(status='PASS_SETUP_COMMIT_UNKNOWN', case=case['name'], database=case['database'], snapshots=t.snapshots, tables=75,
        native_rows=len(m.native['rows']), sequence=m.native['sequence'], inputs=len(inputs),
        control_epoch=m.control['epoch'], refreshes=m.refreshes, caller_unknown=failed,
        persistent_outcome='prepared-native-old' if operation=='prepare' else 'native-closing-SQL-open',
        scope='Two actual finite setup boundaries; failed caller is not successful installed recovery')

def negative_controls(events,before,native,inputs,case,server_window,*,fault):
    original=dict(events=events,before=before,native=native,inputs=inputs,case=case,server_window=server_window,fault=fault)
    rejected=[]
    def reject(name,change):
        value=copy.deepcopy(original);change(value)
        try:verify(**value)
        except (AssertionError,KeyError,ValueError,TypeError,StopIteration):rejected.append(name)
        else:raise AssertionError('accepted setup counterexample: '+name)
    def event(v,kind,**fields):return only(v['events'],event=kind,**fields)
    reject('unknown-proxy',lambda v:v['fault']['wire'].update(status='UNKNOWN'))
    reject('wrong-commit-ordinal',lambda v:v['fault'].update(commit_number=10))
    reject('wrong-wire-boundary',lambda v:v['fault'].update(mode='before-forward' if case['operation']=='prepare' else 'after-complete'))
    reject('missing-native-close',lambda v:event(v,'lineage-history-native').update(state='open'))
    reject('not-joined',lambda v:event(v,'lineage-history-driver',driver='control').update(joined=False))
    reject('unknown-is-success',lambda v:event(v,'lineage-transition-result').update(error=None))
    reject('missing-request',lambda v:v['events'].pop(next(i for i,e in enumerate(v['events']) if e['event']=='lineage-history-request')))
    reject('old-input-loss',lambda v:v['inputs'].pop(next(iter(v['inputs']))))
    reject('native-sequence',lambda v:event(v,'lineage-history-state',stage='transition-returned')['state'].update(sequence=999))
    reject('old-xmin',lambda v:v['before']['producer_outcomes'][0].update(xmin='4294967000'))
    reject('ancestral-digest',lambda v:event(v,'lineage-history-epoch',summary=next(e['summary'] for e in v['events'] if e['event']=='lineage-history-epoch'))['summary'].update(digest='00'*32))
    return dict(status='PASS',count=len(rejected),rejected=rejected)
