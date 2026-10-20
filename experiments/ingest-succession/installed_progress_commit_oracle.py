"""Proof-only COMMIT ambiguity: independent state, wire and recovery predicates.

No engine or product codec is used. Existing signed science and literal typed
record encoders are shared; error custody never becomes a successful Serve.
"""
import copy
import re

import installed_family_replay as family
import installed_progress_replay as progress
import installed_cycle_model as cycle
from installed_cycle_replay import CONTEXT_FIELDS, process_closed as recovery_closed
from installed_family_capture import number, backend_checked, loads
from installed_family_http import add_http

only, same, sha, binary, be = family.only, family.same, family.sha, family.binary, family.be
MODES = ('before-forward', 'after-complete')
PROXY_SHA256 = 'c4675911c3ce302bee19d2c826a93220bdc5c7ad751cda1018203524b32d3425'
DIRECTORY = 'installed-progress-commit-loss'
LABEL = 'org.bigbrotr.installed-progress-commit-loss'
PREFIX = 'bb_progress_commit_'
CLIENT_PREFIX = 'bigbrotr-progress-commit-'
STATUS = 'PASS_INSTALLED_PROGRESS_COMMIT_LOSS'
CASES = tuple(mode + '-' + step for mode in MODES for step in ('loss', 'recover', 'retry'))
ARTIFACTS = {'bigbrotr','ingest_bootstrap_probe','native_components','sync_progress',
    'fact_codec','evidence_intent','control_schema','migrate_entrypoint'}
GROUPS = {'producer_progress','producer_checkpoint','certification','captured_input','captured_binding',
    'sync_progress','fact_codec','evidence_intent','control_schema','migrate_entrypoint'}


def qualification_checked(built, component, qualification):
    assert component['status'] == 'PASS' and qualification['status'] == 'PASS_PRODUCER_PROGRESS_COMPONENTS'
    assert built['source'] == component['source'] == qualification['source'] == progress.require_frozen_profile()['product_source']
    for field in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert component[field] == built[field] == qualification[field]
    assert set(component['executables_sha256']) == ARTIFACTS
    assert component['executables_sha256']['bigbrotr'] == built['binary_sha256']
    assert component['test_counts'] == qualification['test_counts']
    assert set(component['test_counts']) == set(component['test_names']) == GROUPS
    assert component['missing_controls'] == {}
    assert set(component['required_controls']) == GROUPS-{'certification','control_schema','migrate_entrypoint'}
    for group,count in component['test_counts'].items():
        assert type(count) is int and count > 0
        names = component['test_names'][group]
        assert len(names) == len(set(names)) == count
        assert set(component['required_controls'].get(group,[])) <= set(names)


def process_closed(response, case):
    assert set(response) == {'returncode', 'stdout', 'stderr', 'outcome', 'terminal'}
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int
    assert response['returncode'] == 4
    terminal = response['terminal']
    assert set(terminal) == {'pid', 'start_ticks', 'config', 'prefix', 'command',
        'exit_code', 'reaped', 'forced', 'elapsed_seconds'}
    number(terminal['pid'], 1, integer=True); number(terminal['start_ticks'], 1, integer=True)
    number(terminal['elapsed_seconds'], 0, 60)
    assert terminal['config'] == case['config'] and terminal['prefix'] == '/lab/' + case['name']
    assert terminal['command'] == ['/build/bigbrotr', 'serve', '--file', case['config'],
        '--activate-successor', '--roles', 'control,ingest,sync']
    assert type(terminal['exit_code']) is int and terminal['exit_code'] == 4
    assert terminal['reaped'] is True and terminal['forced'] is False
    report, = [loads(line) for line in response['stdout'].splitlines()]
    assert report['format'] == 'bigbrotr-ingest-successor/1' and report['state'] == 'activated'
    prefix = 'error: initial Sync unavailable: Progress(CommitUnknown); sync_dispatch='
    assert response['stderr'].startswith(prefix) and response['stderr'].endswith('\n')
    diagnostic = loads(response['stderr'][len(prefix):])
    fields = {'format','reason','scope','timing','captured_us','authority_checked_us',
        'renewal_timeout_us','authority_remaining_us','due_remaining_us','maximum_us',
        'renewal_due_us','parent_deadline_us','dispatch_deadline_us','outer_deadline_us',
        'stage','entered_us','returned_us','frame_ordinal','request_sequence','last_delivered_admission_sequence'}
    assert set(diagnostic) == fields
    assert diagnostic['format'] == 'bigbrotr-initial-sync-diagnostic/1'
    assert diagnostic['reason'] == 'operation-error' and diagnostic['scope'] == 'caller-progress-not-commit-outcome'
    assert diagnostic['timing'] == 'monotonic-offset-microseconds-saturating'
    assert diagnostic['stage'] == 'certification' and diagnostic['frame_ordinal'] == 0
    assert diagnostic['request_sequence'] is None and diagnostic['last_delivered_admission_sequence'] == 2
    for key in fields - {'format','reason','scope','timing','stage','request_sequence'}:
        assert type(diagnostic[key]) is int and 0 <= diagnostic[key] < 1 << 63
    assert diagnostic['entered_us'] <= diagnostic['returned_us'] <= diagnostic['captured_us']
    assert diagnostic['renewal_timeout_us'] == 1_000_000 and 0 < diagnostic['maximum_us'] <= 1_000_000
    assert diagnostic['dispatch_deadline_us'] <= diagnostic['outer_deadline_us']
    return report


def wire_checked(value, current, *, gate_only=False):
    assert set(value) == {'format','mode','proxy_sha256','command','ready','gate','process','closed'}
    assert value['format'] == 'bigbrotr-progress-commit-fault/1' and value['mode'] in MODES
    assert value['proxy_sha256'] == PROXY_SHA256
    command = value['command']
    assert command[:3] == ['docker','exec','-i']
    assert command[4:] == ['python3','-B','/lab/progress-commit-proxy.py',
        '--target','control-producer-progress','--mode',value['mode']]
    process = value['process']
    assert set(process) == {'exit','stdout','stderr','overflow','readers_joined'}
    assert process['stderr'] == '' and process['overflow'] is False
    assert len(process['stdout'].encode()) <= 256 * 1024
    events = [loads(line) for line in process['stdout'].splitlines()]
    ready = events[0]; assert ready == value['ready'] and ready['event'] == 'ready'
    number(ready['port'], 1024, 65535, integer=True)
    assert ready['source_sha256'] == PROXY_SHA256 and ready['backend_pid'] is None
    updates = [event for event in events if event['event'] == 'progress-update-complete']
    assert [event['progress_updates_complete'] for event in updates] == ([1] if gate_only else [1,2])
    counters = {'connections','bytes','frames','data_queries','data_commits','server_complete','client_complete','faults',
        'owner_requests','owner_forwarded','owner_complete','guard_rejections','serialization_rejections',
        'auth_finals_omitted','frontend_queries','coordinator_inserts','pin_claim_injections',
        'session_mutations_complete','reader_queries','capture_verify_requests','resume_authority_requests',
        'resume_expiry_rejections','resume_expiry_idle','progress_updates_complete','target_occurrence'}
    common = counters | {'event','mode','target','rejection_sequence','pin_rejections','backend_pid'}
    for event in events:
        extra = {'port','source_sha256'} if event['event'] == 'ready' else {'unexpected'} if event['event'] == 'terminal' else set()
        assert set(event) == common | extra
        assert event['mode'] == value['mode'] and event['target'] == 'control-producer-progress'
        assert event['target_occurrence'] == 2 and event['rejection_sequence'] == event['pin_rejections'] == []
        for key in counters: number(event[key], 0, 64*1024**2, integer=True)
        assert event['connections'] <= 1 and event['frames'] <= 20000
        for key in counters - {'connections','bytes','frames','data_queries','data_commits','server_complete',
                'client_complete','faults','frontend_queries','progress_updates_complete','target_occurrence'}:
            assert event[key] == 0
        if event['event'] != 'ready': assert event['backend_pid'] == current['control_backend_pid']
    first = updates[0]
    assert ready['connections'] == 0
    assert all(ready[key] == 0 for key in counters-{'target_occurrence'})
    assert first['data_queries'] == first['data_commits'] == first['faults'] == 0
    assert first['server_complete'] == first['client_complete'] == 0
    if gate_only:
        assert value['closed'] is False and process['exit'] is None
        assert process['readers_joined'] is False
        assert [event['event'] for event in events] == ['ready','progress-update-complete']
        return first
    assert value['closed'] is True and type(process['exit']) is int and process['exit'] == 0
    assert process['readers_joined'] is True
    assert [event['event'] for event in events] == ['ready','progress-update-complete','progress-update-complete','fault','terminal']
    assert set(value['gate']) == {'captured','event'}
    assert value['gate']['event'] == first
    assert updates[1]['data_queries'] == 1
    assert all(updates[1][key] == 0 for key in ('data_commits','faults','server_complete','client_complete'))
    for key in counters:
        assert [event[key] for event in events] == sorted(event[key] for event in events)
    for event in events[-2:]:
        assert {key:event[key] for key in ('data_queries','data_commits','progress_updates_complete','faults',
            'server_complete','client_complete')} == dict(data_queries=1,data_commits=1,
                progress_updates_complete=2,faults=1,server_complete=int(value['mode']=='after-complete'),client_complete=0)
    assert events[-1]['unexpected'] == 0
    return events


def specification(case):
    assert set(case) == {'name','clone','origin','operation','predecessor','database','config','status','custody'}
    mode = case['clone']; assert mode in MODES and case['origin'] == 'activated'
    assert case['name'] == mode+'-loss' and case['operation'] == 'installed-live'
    assert case['predecessor'] is None and case['status'] == 'RUNNING'
    assert case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}',case['database'])
    assert case['config'] == '/lab/'+case['database']+'.toml'
    return mode


def verify(context):
    mode = specification(context['case'])
    assert set(context) == {'case','before','after','native_before','native_after','inputs_before','inputs_after',
        'input_contents','snapshots','response','journey','fixture','server_window','backend',
        'catalog_before','catalog_after','fixture_input','progress_profile','fault'}
    assert context['progress_profile'] == 'commit-loss' and context['fault']['mode'] == mode
    report = process_closed(context['response'],context['case'])
    window = context['server_window']; assert set(window) == {'before_us','after_us'}
    number(window['before_us'],1,integer=True)
    number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    before, after = (progress.legacy_state(context[k]) for k in ('before','after'))
    family.baseline(dict(context,before=before))
    cuts = context['snapshots']; assert [s['stage'] for s in cuts] == ['activated','prepared','progress-mutation']
    last = window['before_us']
    for cut in cuts:
        assert set(cut) == {'stage','server_us','observed_by_us','control','inputs'}
        number(cut['server_us'],last,window['after_us'],integer=True)
        last = number(cut['observed_by_us'],cut['server_us'],window['after_us'],integer=True)
    active = progress.legacy_state(cuts[0]['control'])
    model = family.Model(before,context['native_before'],context['inputs_before'],
        dict(before_us=window['before_us'],after_us=cuts[0]['observed_by_us']))
    model.startup(active,report)
    assert model.plan['epoch'] == 3 and model.plan['limit'] == 2
    assert model.ds['policy_revision'] == model.ds['config_revision'] == 3
    policy = only(before['policy_revisions'],revision=3)
    assert policy['document']['settings']['sync.provenance'] == 'off'
    scope = only(before['allocation_domains'],owner_epoch=1,dictionary_kind=4)
    assert context['fixture_input'] == dict(config_revision='3',policy_revision='3',visibility=dict(
        dataset_id=model.ds['dataset_id'][2:],dictionary_kind=4,allocation_domain=scope['allocation_domain'][2:],ordinal='3'))
    assert context['fixture'] == dict(cleanup_errors=[],client_terminal=context['journey']['terminal'])
    first = family.wire_checked(context['journey'],context['fixture'],report,before,
        progress.legacy_state(cuts[1]['control']),window,progress='split',commit_loss=True)
    science = family.NetworkScience(model,model.plan)
    values = family.native_map(context['native_after'])
    def fact(n):
        raw = bytes.fromhex(values['facts',(b'\x01\x20'+science.source(3,n)).hex()])
        assert raw[:2] == b'\x01\x20' and raw[34:].startswith(b'bigbrotr/fact/1\0')
        return family.decode(raw[34:].split(b'\0',1)[1])
    dictionary_at, recorded = fact(1)[4], fact(2)[4]
    received, resolution = fact(5)[5][1], fact(5)[5][3][1]
    assert resolution == first['clock_resolution_us'] and 0 < resolution <= 1_000_000
    assert first['sent_at_us']-resolution <= received <= first['closed_at_us']+resolution and received <= recorded
    for at in (dictionary_at,recorded,received): family.clock(at,window)
    assert only(active['ingest_activations'],target_epoch=3)['activated_at_us'] <= dictionary_at <= first['request_at_us'] <= recorded
    science.network(False,dictionary_at,received,recorded,resolution)
    assert model.native == context['native_after'] and model.inputs == context['inputs_after']
    assert cuts[0]['inputs'] == cuts[1]['inputs'] == context['inputs_before']
    assert cuts[2]['inputs'] == context['inputs_after']
    previous = active
    for cut in cuts[1:]+[dict(stage='readback',control=context['after'],observed_by_us=window['after_us'])]:
        observed = progress.legacy_state(cut['control']); wanted = copy.deepcopy(active)
        bound = dict(before_us=window['before_us'],after_us=cut['observed_by_us'])
        family.leases(wanted,observed,previous,bound,model.control['epoch'],3)
        phase = 'prepared' if cut['stage']=='prepared' else 'submitted'
        acquisition, enrollment = add_http(wanted,observed,before,context['journey']['result'],
            phase,bound,model.control,status_observed=False)
        if phase != 'prepared': family.admissions(wanted,observed,science,acquisition,enrollment,model.control,bound)
        same(wanted,observed); previous = observed
    mutation = cuts[2]
    phases = {w['progress_phase']:w for w in context['journey']['closed'] if 'progress_phase' in w}
    progress.require_phase(mutation['control'],'activated',False,'mutation',3,phases['mutation'])
    final = dict(control=context['after'],inputs=context['inputs_after'],observed_by_us=window['after_us'])
    pairs = [('mutation',mutation)]
    if mode == 'after-complete':
        progress.require_phase(context['after'],'activated',False,'proof-only',3,phases['proof-only'])
        progress.proof_only_boundary(mutation,final,3)
        old, new = only(mutation['control']['producer_checkpoints']),only(context['after']['producer_checkpoints'])
        family.fresh_xmin(new['xmin'],mutation['control'])
        assert 0 < (int(new['xmin'])-int(old['xmin']))%2**32 < 2**31
        pairs.append(('proof-only',final))
    else:
        for table in (*progress.TABLES,'producer_checkpoints'):
            assert context['after'][table] == mutation['control'][table]
    typed = progress.verify_records(context,model,science,pairs)
    for table in context['after']:
        if table not in {'owner_epochs','role_instances','producer_checkpoints','acquisition_progress'}:
            same({table:mutation['control'][table]},{table:context['after'][table]})
    current = only(context['after']['owner_epochs'],role='control',epoch=model.control['epoch'])
    assert current['state'] == 'active' and current['released_at_us'] is None
    wire_checked(context['fault'],current)
    assert context['fault']['gate']['captured'] == dict(phase='mutation',request_id=phases['mutation']['request'][1],
        observation_sha256=context['fault']['gate']['captured']['observation_sha256'])
    assert re.fullmatch('[0-9a-f]{64}',context['fault']['gate']['captured']['observation_sha256'])
    assert mutation['observed_by_us'] <= phases['proof-only']['sent_at_us']
    backend_checked(context['backend'],context['case']['database'],context['before'],context['after'])
    assert context['catalog_before'] == context['catalog_after']
    assert set(context['input_contents']) == set(model.inputs)
    for name,value in context['input_contents'].items():
        raw = bytes.fromhex(value); assert model.inputs[name] == dict(bytes=len(raw),sha256=sha(raw))
        if '-0000000000000003-' in name: assert raw == science.inputs[int(name[-21:-5],16)]
    return dict(status='PASS',mode=mode,caller='Progress(CommitUnknown)',tables=77,typed=typed,
        head_revision=3 if mode=='before-forward' else 4,acquisition_revision=2 if mode=='before-forward' else 3,
        complete_through_request=2,dependencies=11,native_rows=len(model.native['rows']),
        native_sequence=model.native['sequence'],native_ack=0,native_retired=0)


def verify_recovery(context, seed):
    assert set(context) == CONTEXT_FIELDS
    case = context['case']; mode = case['clone']; assert mode in MODES and case['origin'] == 'activated'
    assert set(case) == {'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}',case['database'])
    assert case['config'] == '/lab/'+case['database']+'.toml'
    assert case['operation'] == 'recover' and case['status'] == 'RUNNING' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    retry = case['name'] == mode+'-retry'
    assert case['name'] == mode+('-retry' if retry else '-recover')
    assert case['predecessor'] == mode+('-recover' if retry else '-loss')
    reports = recovery_closed(context['response'],case)
    before,after,native,inputs,window = [context[k] for k in ('before','after','native_before','inputs_before','server_window')]
    assert set(before) == set(after) == set(seed[0]) and len(before) == 77
    assert inputs == context['inputs_after'] == seed[2]
    assert context['fixture'] == dict(cleanup_errors=[]) and context['snapshots'] == []
    assert context['catalog_before'] == context['catalog_after']
    assert set(window) == {'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    expected_native = copy.deepcopy(seed[1]); slot = only(seed[0]['initial_ingest_successions'])['producer_slot_id']
    if retry:
        values = family.native_map(expected_native); key=('receipts',(b'\x01\x31'+binary(slot)+be(3)).hex())
        assert values[key] == (b'\x01\x31'+be(2)+bytes(16)).hex()
        values[key]=(b'\x02\x31'+be(2)+bytes(16)+b'\x01').hex()
        expected_native.update(rows=family.native_rows(values),sequence=expected_native['sequence']+1)
    else: same(before,seed[0])
    assert native == expected_native
    m = cycle.Model(before,native,inputs,window)
    if not retry:
        old = max((r for r in m.pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        assert old['state'] == 'active' and old['released_at_us'] is None
        observed = only(after['owner_epochs'],role='control',epoch=old['epoch'])
        fresh = only(after['owner_epochs'],role='control',epoch=old['epoch']+1)
        oldrole = only(m.pg['role_instances'],role_instance_id=old['role_instance_id'])
        role = only(after['role_instances'],role_instance_id=old['role_instance_id'])
        newrole = only(after['role_instances'],role_instance_id=fresh['role_instance_id'])
        family.fresh_xmin(observed['xmin'],before)
        for at in (observed['released_at_us'],role['retired_at_us']): family.clock(at,window)
        assert observed['released_at_us'] <= role['retired_at_us'] <= newrole['created_at_us'] <= fresh['acquired_at_us']
        exclusion='\\x'+sha(b'bigbrotr/control-exclusion/1\0'+binary(old['dataset_id'])+binary(old['resource_id'])+
            be(old['epoch'])+binary(old['process_incarnation'])+binary(fresh['process_incarnation']))
        old.update(state='released',released_at_us=observed['released_at_us'],exclusion_method='storage_fenced',
            exclusion_digest=exclusion,xmin=observed['xmin'])
        oldrole.update(retired_at_us=role['retired_at_us'],xmin=observed['xmin'])
        assert observed == old and role == oldrole
        assert observed['xmin'] != fresh['xmin'], 'claim/fence and final release are separate transactions'
    m.completed_control(after); m.mutation(after,'recover',cycle.science(seed))
    assert reports == [dict(format='bigbrotr-ingest-recovery/1',state='successor-positive-outcomes-resolved',
        dataset_id=m.ds['dataset_id'][2:],owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],
        producer_epoch='3',issued_through='2',reserved_through='2',resolved_requests='2',verified_through='3',
        native='closed',owner_release='committed',ready=False,source_closure='not-performed')]
    assert m.native == context['native_after']; same(m.pg,after)
    for table in ('producer_checkpoints','acquisition_progress','producer_checkpoint_dependencies','jobs','producer_requests','producer_outcomes'):
        same({table:seed[0][table]},{table:after[table]})
    backend_checked(context['backend'],case['database'],before,after)
    assert set(context['input_contents']) == set(inputs)
    for name,value in context['input_contents'].items():
        raw=bytes.fromhex(value); assert inputs[name] == dict(bytes=len(raw),sha256=sha(raw))
    return dict(status='PASS',case=case['name'],tables=77,old_failed_control_fenced=not retry,
        native_writes=0 if retry else 1,typed_issuer_xmin_preserved=True,native_ack=0,native_retired=0)


def controls(context, seed=None):
    check = verify if seed is None else lambda c:verify_recovery(c,seed)
    check(context)
    changes = [
        ('unjoined',lambda c:c['response']['terminal'].update(reaped=False)),
        ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)])),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('native-sequence',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('old-outcome-xmin',lambda c:c['after']['producer_outcomes'][0].update(xmin='4294967000')),
        ('new-input',lambda c:c['inputs_after'].update(unexpected=dict(bytes=1,sha256='00'*32))),
        ('closure-loss',lambda c:c['after']['producer_checkpoint_dependencies'].pop()),
        ('issuer-rewrite',lambda c:c['after']['producer_checkpoints'][0].update(
            issuer_epoch=c['after']['producer_checkpoints'][0]['issuer_epoch']+1)),
        ('job-completion',lambda c:only(c['after']['jobs'],request_key='\\x'+'e4'*16).update(state='completed')),
        ('wrong-head',lambda c:c['after']['producer_checkpoints'][0].update(checkpoint_revision=99)),
    ]
    if seed is None:
        changes += [('lost-original-error',lambda c:c['response'].update(stderr='error: unavailable\n')),
            ('wrong-fault-mode',lambda c:c['fault'].update(mode=MODES[1-MODES.index(c['fault']['mode'])])),
            ('proxy-unjoined',lambda c:c['fault']['process'].update(readers_joined=False)),
            ('wrong-gate',lambda c:c['fault']['gate']['event'].update(progress_updates_complete=2)),
            ('wrong-selected-backend',lambda c:max((r for r in c['after']['owner_epochs']
                if r['role']=='control'),key=lambda r:r['epoch']).update(control_backend_pid=1))]
    else:
        changes += [('historical-control-rewritten',lambda c:only(c['after']['owner_epochs'],role='control',
                epoch=max(r['epoch'] for r in seed[0]['owner_epochs'] if r['role']=='control')).update(exclusion_digest='\\x'+'00'*32)),
            ('old-control-unreleased',lambda c:only(c['after']['owner_epochs'],role='control',
                epoch=max(r['epoch'] for r in seed[0]['owner_epochs'] if r['role']=='control')).update(state='active',released_at_us=None)),
            ('historical-head-xmin',lambda c:c['after']['producer_checkpoints'][0].update(xmin='4294967001'))]
    rejected=[]
    for name,change in changes:
        value=copy.deepcopy(context); change(value)
        try:check(value)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration):rejected.append(name)
        else:raise AssertionError('accepted control: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)
