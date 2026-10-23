"""Independent literal initial-floor reader expectations; no engine or product calls.

The qualified signed seed supplies the immutable original science. Only actual
installed abandonment and retirement may establish this fixture's eligibility.
The subsequent raw floor installation is synthetic, never reclamation evidence.
"""
import copy
from pathlib import Path
import re
import stat

import installed_progress_recovery_oracle as retained
from installed_cycle_client import argv
from installed_cycle_replay import CONTEXT_FIELDS
from installed_family_capture import backend_checked, loads, number

model = retained.model
only, same, sha, binary, be = model.only, model.same, model.sha, model.binary, model.be
ORIGIN = retained.ACK_ORIGIN
DIRECTORY = 'installed-compacted-reader'
LABEL = 'org.bigbrotr.installed-compacted-reader'
STATUS = 'PASS_SYNTHETIC_POST_GC_INITIAL_READER'
CLONES = ('positive','incompatible-head','floor-digest','missing-pending')
STEPS = {name:('abandon','retire',*(('residual','absent') if name == 'positive' else ('refuse',))) for name in CLONES}
CASES = tuple(name+'-'+step for name in CLONES for step in STEPS[name])
# The retained ACK origin has recovery grants, but never exercised abandonment
# or retirement. Provision their existing prerequisites before the new reader's
# three column reads; these are fixture setup, not product privilege changes.
GRANTS = ('GRANT SELECT,INSERT ON control.acquisition_gaps TO bigbrotr_control;',
          'GRANT EXECUTE ON FUNCTION control.guard_acquisition_gap(), '
          'control.require_acquisition_gap_complete(), '
          'control.require_initial_retirement(bytea,numeric,bytea) TO bigbrotr_control;',
          'GRANT SELECT(owner_resource_id) ON control.initial_ingest_successions, '
          'control.ingest_successions, control.ingest_activations TO bigbrotr_control;')
NOT_RUN = ('Actual product reclamation/GC and unlink', 'Compacted successor preparation/activation/recovery',
           'Crash/unknown-commit and interrupted unlink', 'Mixed restore/backup and capacity')
MANIFEST = '4d6754fa296d44b3b140da79b6829cf1b7f777ff109c30170db473ce124bb042'
HEAD = 'a3948e3f2727a98d9b07bb0cd6815ca1004391143604c4a18e44683f57f00894'
ERRORS = {'incompatible-head':'error: producer operation unavailable: Progress(InvalidContext)\n',
          'floor-digest':'error: producer operation unavailable: Control(Backend)\n',
          'missing-pending':'error: event admission unavailable: StateMismatch\n'}


def catalog_checked(original, current):
    def statements(value):
        return [line for line in value.splitlines() if line.strip() and not line.startswith('--')]
    before,after = statements(original),statements(current)
    added = [line for line in after if line not in before]
    tables = ('initial_ingest_successions','ingest_successions','ingest_activations')
    assert len(added) == 7
    for table in tables:
        assert len([line for line in added if re.fullmatch(
            r'GRANT SELECT\s*\(owner_resource_id\) ON (?:TABLE )?control\.'+table+r' TO bigbrotr_control;',line)]) == 1
    assert 'GRANT SELECT,INSERT ON TABLE control.acquisition_gaps TO bigbrotr_control;' in added
    # PostgreSQL dumps the sole function privilege EXECUTE as ALL. This does
    # not allow ALL table rights or any function outside these exact signatures.
    for function in ('guard_acquisition_gap()', 'require_acquisition_gap_complete()',
            'require_initial_retirement(wanted_slot bytea, wanted_epoch numeric, wanted_digest bytea)'):
        assert len([line for line in added if re.fullmatch(
            r'GRANT (?:ALL|EXECUTE) ON FUNCTION control\.'+re.escape(function)+r' TO bigbrotr_control;',line)]) == 1
    assert [line for line in after if line not in added] == before


def qualification_checked(built, component, qualification, source):
    assert re.fullmatch(r'[0-9a-f]{40}',source)
    assert component['status'] == 'PASS' and qualification['status'] == 'PASS_COMPACTED_PREFIX_COMPONENTS'
    assert built['source'] == component['source'] == qualification['source'] == source
    for key in ('tree','source_manifest_sha256','lockfile_sha256'):
        assert built[key] == component[key] == qualification[key]
    assert component['missing_controls'] == {}
    counts = component['test_counts']
    assert counts == qualification['test_counts']
    assert set(counts) == {'compacted_prefix','compacted_progress','initial_reconstruction','request_inputs'}
    assert all(type(n) is int and n > 0 for n in counts.values())
    assert counts['compacted_prefix'] == 7 and counts['compacted_progress'] == 1
    assert set(component['test_names']) == set(counts)
    for name, names in component['test_names'].items():
        assert len(names) == len(set(names)) == counts[name]
    expected = ['compacted_floor_codec_has_exact_bytes_and_legacy_refusal',
        'compacted_prefix_validates_all_outcome_kinds_and_residual_originals_without_writes',
        'compacted_prefix_requires_pending_indices_and_complete_scientific_census',
        'compacted_outcome_reducer_refuses_changed_rows_and_incomplete_closure',
        'compacted_prefix_binds_custody_inputs_cancellation_and_grants',
        'compacted_outcomes_check_protection_batch_digest_and_residual_names',
        'compacted_outcomes_preserve_full_width_historical_policy_and_reject_row_mismatch']
    assert set(component['test_names']['compacted_prefix']) == {
        'storage::rocks::store::admission_tests::compacted_prefix_tests::'+n for n in expected}
    assert component['test_names']['compacted_progress'] == [
        'storage::postgres::control_owner::producer_progress::recovery::tests::compacted_floor_requires_exact_retained_checkpoint']
    assert set(component['executables_sha256']) == {'bigbrotr','ingest_bootstrap_probe','native_components'}
    assert built['binary_sha256'] == component['executables_sha256']['bigbrotr']
    assert built['build_info']['source'] == dict(state='builder-supplied',revision=source,
        tree=built['tree'],dirty=False,manifest_sha256=built['source_manifest_sha256'])


def known(seed):
    pg,native,inputs = seed
    assert len(pg) == 77 and len(native['rows']) == 88 and native['sequence'] == 113 and len(inputs) == 7
    p = only(pg['producer_epochs']); head = only(pg['producer_checkpoints'])
    assert p['producer_epoch'] == p['owner_epoch'] == 1 and p['reserved_through_request'] == 7
    assert head['manifest_sha256'] == '\\x'+MANIFEST and head['checkpoint_digest'] == '\\x'+HEAD
    assert (head['checkpoint_revision'],head['complete_through_request'],head['dependency_count']) == (8,7,27)
    values = model.native_map(native)
    assert sha(bytes.fromhex(values['default','0101'])) == MANIFEST
    slot,ds = binary(p['producer_slot_id']),binary(p['dataset_id'])
    state_key = (b'\x01\x31'+slot+be(1)).hex()
    assert values['receipts',state_key] == (b'\x02\x31'+be(7)+be(7)+be(0)+b'\x01').hex()
    source_key = (b'\x01\x02'+ds+bytes(4)+be(1)).hex()
    assert values['default',source_key] == (b'\x01\x02'+be(27)).hex()
    preimage = b'bigbrotr/initial-source-closure/1\0'+bytes.fromhex(MANIFEST)+slot+be(1)+be(7)
    receipts = []
    assert len(pg['producer_requests']) == len(pg['producer_outcomes']) == 7
    for sequence in range(1,8):
        r = only(pg['producer_requests'],request_sequence=sequence)
        o = only(pg['producer_outcomes'],request_sequence=sequence)
        key = (b'\x01\x30'+slot+be(1)+be(sequence)).hex()
        assert o['request_digest'] == r['request_digest'] and o['manifest_sha256'] == r['manifest_sha256'] == '\\x'+MANIFEST
        assert o['outcome_sha256'] == '\\x'+sha(binary(o['outcome_bytes']))
        assert values['receipts',key] == o['outcome_bytes'][2:]
        receipts.append(dict(cf='receipts',key=key,value=values['receipts',key]))
        preimage += be(sequence)+binary(r['request_digest'])+binary(o['outcome_sha256'])
    return dict(digest=sha(preimage+be(27)),state_key=state_key,receipts=receipts,
        pending_key=(b'\x01\x21'+ds+bytes(4)+be(1)+be(1)).hex())


def floor_bytes(seed, mode):
    k = known(seed); digest = bytearray.fromhex(k['digest'])
    if mode == 'floor-digest': digest[0] ^= 1
    value = b'\x03\x31'+be(7)*3+b'\x01'+bytes.fromhex(MANIFEST)+be(27)+digest+be(8)+bytes.fromhex(HEAD)
    assert len(value) == 139
    return value


def compacted_native(seed, mode, *, missing=False):
    k = known(seed); result = copy.deepcopy(seed[1]); values = model.native_map(result)
    for row in k['receipts']: assert values.pop((row['cf'],row['key'])) == row['value']
    values['receipts',k['state_key']] = floor_bytes(seed,mode).hex()
    if missing:
        assert mode == 'missing-pending'
        removed = bytes.fromhex(values.pop(('facts',k['pending_key'])))
        assert len(removed) == 42 and removed[:10] == b'\x01\x21'+be(2)
    result.update(rows=model.native_rows(values),sequence=121+int(missing))
    return result


def terminal_checked(pg, seed):
    k = known(seed); digest = '\\x'+k['digest']
    p = only(pg['producer_epochs']); s = only(pg['source_streams'])
    owner = only(pg['owner_epochs'],role='ingest'); role = only(pg['role_instances'],role_instance_id=owner['role_instance_id'])
    assert p['state'] == 'retired' and s['state'] == 'closed' and s['last_complete_sequence'] == 27
    assert p['closure_digest'] == s['closure_digest'] == owner['exclusion_digest'] == digest
    assert owner['state'] == 'released' and owner['exclusion_method'] == 'storage_fenced'
    at = p['retired_at_us']; assert s['closed_at_us'] <= at == owner['released_at_us'] == role['retired_at_us']
    assert len(pg['allocation_domains']) == 9
    assert all(r['state'] == 'closed' and r['closed_at_us'] == at and r['closure_digest'] == digest for r in pg['allocation_domains'])
    gap = only(pg['acquisition_gaps']); job = only(pg['jobs'])
    assert gap['reason'] == job['reason_code'] == 'initial_producer_abandoned'
    assert gap['source_closure_digest'] == digest and gap['last_complete_sequence'] == 27
    assert job['state'] == 'cancelled' and job['job_revision'] == 4 and job['checkpoint_revision'] == 0
    assert s['closed_at_us'] <= gap['abandoned_at_us'] == job['completed_at_us'] <= at
    for table in ('producer_requests','producer_outcomes','producer_checkpoints','acquisition_progress','producer_checkpoint_dependencies'):
        same({table:pg[table]},{table:seed[0][table]})
    for table in ('initial_ingest_successions','initial_ingest_activations','ingest_successions','ingest_activations','ingest_terminal_transactions','producer_input_bindings'):
        assert pg[table] == []
    return k


def mutation_checked(mutation, seed, expected, wanted, *, complete=True):
    assert set(mutation) == {'kind','mode','clone','floor','before','control','inputs','after','response'}
    assert mutation['kind'] == 'privileged-synthetic-post-GC-reader-fixture'
    mode,name = mutation['mode'],mutation['clone']
    assert mode in ('install','missing-pending') and name in CLONES
    terminal_checked(mutation['control'],seed); same(mutation['control'],expected[0])
    assert mutation['before'] == expected[1] and mutation['inputs'] == expected[2] == seed[2]
    assert mutation['floor'] == floor_bytes(seed,name).hex()
    assert wanted == compacted_native(seed,name,missing=mode == 'missing-pending')
    response = mutation['response']; assert set(response) == {'returncode','stdout','stderr'}
    assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
    first,last = [loads(line) for line in response['stdout'].splitlines()]
    k = known(seed)
    removed = k['receipts'] if mode == 'install' else [only(expected[1]['rows'],cf='facts',key=k['pending_key'])]
    assert first == dict(event='synthetic-post-gc-reader-fixture',phase='before',mode=mode,
        rows=expected[1]['rows'],sequence=expected[1]['sequence'],floor=mutation['floor'],removed=removed)
    assert last == dict(event='synthetic-post-gc-reader-fixture',phase='after',mode=mode,
        rows=wanted['rows'],sequence=wanted['sequence'],native='closed')
    assert mutation['after'] == (wanted if complete else None)
    return wanted


def head_mutation_checked(mutation, before, seed):
    assert set(mutation) == {'kind','before','statement','sql_output','after'}
    assert mutation['kind'] == 'privileged-isolated-valid-head-corruption'
    same(mutation['before'],before); terminal_checked(before,seed)
    assert mutation['statement'] == retained.ack_statement(seed)
    returned = loads(mutation['sql_output']); model.fresh_xmin(returned['xmin'],before)
    wanted = retained.ack_head(seed); wanted['xmin'] = returned['xmin']
    assert returned == wanted
    after = copy.deepcopy(before); after['producer_checkpoints'] = [wanted]
    same(mutation['after'],after)
    return after


def quarantine_checked(record, seed):
    assert set(record) == {'kind','before','after','path','files','quarantine_after','status'}
    assert record['kind'] == 'reversible-fixture-original-quarantine' and record['status'] == 'RETAINED'
    assert record['path'] == 'positive/quarantined-originals'
    assert set(record['before']) == set(record['quarantine_after']) == {'.',*seed[2]}
    assert set(record['after']) == {'.'} and set(record['files']) == set(seed[2])
    for name,identity in seed[2].items():
        saved = record['files'][name]; assert set(saved) == {'hex','metadata'}
        raw = bytes.fromhex(saved['hex']); assert identity == dict(bytes=len(raw),sha256=sha(raw))
        value = saved['metadata']; assert value == record['before'][name] == record['quarantine_after'][name]
        assert stat.S_ISREG(value['mode']) and value['mode'] & 0o777 == 0o600 and value['links'] == 1
        assert {k:value[k] for k in ('bytes','sha256')} == identity
    assert stat.S_ISDIR(record['after']['.']['mode'])
    assert {k:v for k,v in record['before']['.'].items() if k != 'bytes'} == {
        k:v for k,v in record['after']['.'].items() if k != 'bytes'}


def initial_mutation(m, after, step, seed):
    digest = '\\x'+known(seed)['digest']; source = only(m.pg['source_streams'])
    producer = only(m.pg['producer_epochs']); job = only(m.pg['jobs'])
    def coordinates(row, field, lower):
        at,tx = row[field],row['xmin']; model.clock(at,m.window); model.fresh_xmin(tx,m.pg)
        assert max(lower,m.control['acquired_at_us']) <= at <= m.control['released_at_us'] < m.control['lease_expires_at_us']
        return at,tx
    if step == 'abandon':
        assert source['state'] == 'open' and producer['state'] == 'closing' and job['state'] == 'queued'
        at,tx = coordinates(only(after['source_streams']),'closed_at_us',source['opened_at_us'])
        source.update(state='closed',last_complete_sequence=27,closure_digest=digest,closed_at_us=at,xmin=tx)
        gap = only(after['acquisition_gaps']); at,tx = coordinates(gap,'abandoned_at_us',at)
        slot = only(m.pg['producer_slots'])
        assert (job['owner_resource_id'],job['owner_epoch'],job['owner_incarnation']) == tuple(
            slot[k] for k in ('registered_by_resource_id','registered_by_epoch','registered_by_incarnation'))
        assert m.pg['acquisition_gaps'] == [] and job['job_revision'] == 2
        m.pg['acquisition_gaps'].append(dict(job_id=job['job_id'],dataset_id=m.ds['dataset_id'],
            producer_slot_id=m.slot,producer_epoch=1,shard_id=0,source_owner_epoch=1,last_complete_sequence=27,
            source_closure_digest=digest,previous_job_revision=2,previous_owner_resource=job['owner_resource_id'],
            previous_owner_epoch=job['owner_epoch'],previous_owner_incarnation=job['owner_incarnation'],
            issuer_resource=m.control['resource_id'],issuer_epoch=m.control['epoch'],
            issuer_incarnation=m.control['process_incarnation'],owner_role='control',abandoned_at_us=at,
            reason='initial_producer_abandoned',xmin=tx))
        job.update(state='cancelled',job_revision=4,reason_code='initial_producer_abandoned',completed_at_us=at,
            owner_resource_id=m.control['resource_id'],owner_epoch=m.control['epoch'],
            owner_incarnation=m.control['process_incarnation'],xmin=tx)
    else:
        assert step == 'retire' and source['state'] == 'closed' and producer['state'] == 'closing'
        gap = only(m.pg['acquisition_gaps']); assert job['state'] == 'cancelled'
        at,tx = coordinates(only(after['producer_epochs']),'retired_at_us',gap['abandoned_at_us'])
        producer.update(state='retired',retired_at_us=at,closure_digest=digest,xmin=tx)
        owner = only(m.pg['owner_epochs'],role='ingest'); assert owner['state'] == 'active'
        owner.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=digest,xmin=tx)
        only(m.pg['role_instances'],role_instance_id=owner['role_instance_id']).update(retired_at_us=at,xmin=tx)
        assert len(m.pg['allocation_domains']) == 9
        for row in m.pg['allocation_domains']:
            assert row['state'] == 'active'
            row.update(state='closed',closed_at_us=at,closure_digest=digest,xmin=tx)
    same(m.pg,after)


def specification(case):
    mode = case['clone']; assert mode in CLONES
    step = case['name'].removeprefix(mode+'-'); assert step in STEPS[mode]
    assert case['name'] == mode+'-'+step and case['origin'] == 'initial'
    index = STEPS[mode].index(step)
    assert case['predecessor'] == (mode+'-'+STEPS[mode][index-1] if index else None)
    assert case['operation'] == ('abandon-initial' if step == 'abandon' else 'retire-initial')
    assert re.fullmatch(r'[a-z0-9_]{1,63}',case['database'])
    assert case['config'] == '/lab/'+case['database']+'.toml'
    return mode,step


def process_closed(response, case):
    mode,step = specification(case); failed = step == 'refuse'; code = 4 if failed else 0
    assert set(response) == {'returncode','stdout','stderr','outcome','terminal'}
    assert response['outcome'] == 'PROCESS_EXITED' and type(response['returncode']) is int and response['returncode'] == code
    assert response['stderr'] == (ERRORS[mode] if failed else '')
    t = response['terminal']
    assert set(t) == {'pid','start_ticks','config','prefix','command','exit_code','reaped','forced','error','elapsed_seconds'}
    number(t['pid'],1,integer=True); number(t['start_ticks'],1,integer=True); number(t['elapsed_seconds'],0,60)
    assert t['config'] == case['config'] and t['prefix'] == '/lab/'+case['name']
    assert t['command'] == argv(case['operation'],Path(case['config']))
    assert type(t['exit_code']) is int and t['exit_code'] == code and t['reaped'] is True and t['forced'] is False and t['error'] is None
    if failed: assert response['stdout'] == ''; return None
    report, = [loads(line) for line in response['stdout'].splitlines()]
    assert report['native'] == ('compacted' if step in ('residual','absent') else 'closed')
    assert report['owner_release'] == 'committed' and report['ready'] is False
    return report


def verify(context, seed, expected, *, diagnostic=None):
    assert set(context) == CONTEXT_FIELDS
    case = context['case']; mode,step = specification(case)
    assert set(case) == {'name','clone','origin','operation','predecessor','database','config','status','custody'}
    assert case['status'] == 'RUNNING' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
    before,after = context['before'],context['after']; same(before,expected[0])
    assert context['native_before'] == context['native_after'] == expected[1]
    assert context['inputs_before'] == context['inputs_after'] == expected[2]
    assert set(context['input_contents']) == set(expected[2])
    for name,value in context['input_contents'].items():
        raw = bytes.fromhex(value); assert expected[2][name] == dict(bytes=len(raw),sha256=sha(raw))
    assert context['snapshots'] == [] and context['fixture'] == dict(cleanup_errors=[])
    assert context['catalog_before'] == context['catalog_after']
    window = context['server_window']; assert set(window) == {'before_us','after_us'}
    number(window['before_us'],1,integer=True); number(window['after_us'],window['before_us'],window['before_us']+120_000_000,integer=True)
    report = process_closed(context['response'],case)
    if step == 'refuse' and mode == 'missing-pending':
        # Complete native census precedes Control acquisition in this branch.
        backend = context['backend']; assert set(backend) == {'absence','exact','all'}
        assert backend['exact'] == backend['all'] == []
        absence = backend['absence']
        assert set(absence) == {'database','user','limit_seconds','status','attempts','seconds'}
        assert absence['database'] == case['database'] and absence['user'] == 'bigbrotr_control'
        assert absence['status'] == 'OBSERVED_ABSENT' and absence['limit_seconds'] == 5
        assert absence['attempts']
        for attempt in absence['attempts']:
            assert set(attempt) == {'seconds','count','backends'}
            number(attempt['seconds'],0,5)
            assert type(attempt['count']) is int and attempt['count'] == 0 and attempt['backends'] == []
        number(absence['seconds'],absence['attempts'][-1]['seconds'],6)
    else:
        backend_checked(context['backend'],case['database'],before,after)
    m = model.Model(before,expected[1],expected[2],window,initial=True)
    if step == 'refuse':
        if mode == 'missing-pending': same(before,after)
        else:
            current = retained.failed_control(m,after); same(m.pg,after)
            if mode == 'floor-digest':
                assert diagnostic is not None and diagnostic['returncode'] == 0
                lines = (diagnostic['stdout']+diagnostic['stderr']).splitlines()
                pattern = rf'\[{current["control_backend_pid"]}\] ERROR:  (?:[0-9A-Z]{{5}}: )?retirement requires the closed source and fenced original Control$'
                index, = [i for i,line in enumerate(lines) if re.search(pattern,line)]
                number(retained.instant(lines[index].split()[0].replace('Z','+00:00')),window['before_us'],window['after_us'],integer=True)
                assert 'require_initial_retirement' in '\n'.join(lines[index+1:index+4])
    else:
        assert diagnostic is None
        m.completed_control(after)
        if step in ('abandon','retire'): initial_mutation(m,after,step,seed)
        else: same(m.pg,after); terminal_checked(after,seed)
        compacted = step in ('residual','absent')
        wanted = dict(format='bigbrotr-ingest-recovery/1',state='acquisition-abandoned' if step == 'abandon' else 'initial-retired',
            dataset_id=m.ds['dataset_id'][2:],owner_resource_id=m.resource[2:],producer_slot_id=m.slot[2:],producer_epoch='1',
            issued_through='7',reserved_through='7',resolved_requests='0' if compacted else '7',
            native='compacted' if compacted else 'closed',owner_release='committed',ready=False,
            source_closure=dict(last_complete_sequence='27',sha256=known(seed)['digest']))
        if step == 'abandon': wanted['acquisition_abandonment'] = dict(jobs='1',reason='initial_producer_abandoned',coverage='unknown')
        else: wanted['ingest_retirement'] = dict(allocation_domains='9',acquisition_gaps='1',owner_release='committed',
            native_acknowledged='7',native_retired='7' if compacted else '0')
        if compacted: wanted.update(compacted_requests='7',recovery_basis='closed-producer-floor-v3')
        assert report == wanted
    return dict(status='PASS',case=case['name'],kind='synthetic-post-GC-reader-fixture',
        native_rows=len(expected[1]['rows']),native_sequence=expected[1]['sequence'],input_files=len(expected[2]),
        product_native_writes=0,tables=77,result='REFUSED' if step == 'refuse' else 'READ' if step in ('residual','absent') else 'TERMINAL_PREREQUISITE')


def negative_controls(context, seed, expected, *, diagnostic=None):
    changes = [('unjoined',lambda c:c['response']['terminal'].update(reaped=False)),
        ('unknown-outcome',lambda c:c['response'].update(outcome='OUTCOME_UNKNOWN')),
        ('wrong-command',lambda c:c['response']['terminal']['command'].append('--successor')),
        ('live-backend',lambda c:c['backend'].update(all=[dict(pid=1)])),
        ('native-write',lambda c:c['native_after'].update(sequence=c['native_after']['sequence']+1)),
        ('native-row-loss',lambda c:c['native_after']['rows'].pop()),
        ('historical-checkpoint',lambda c:only(c['after']['producer_checkpoints']).update(xmin='4294967000')),
        ('historical-request',lambda c:c['after']['producer_requests'][0].update(input_bytes=1)),
        ('catalog-change',lambda c:c.update(catalog_after='changed'))]
    if context['inputs_after']: changes.append(('input-loss',lambda c:c['inputs_after'].pop(next(iter(c['inputs_after'])))))
    else: changes.append(('input-reappeared',lambda c:c['inputs_after'].update(seed[2])))
    if context['case']['name'] == 'positive-residual':
        def wrong_report(c):
            import json
            r = loads(c['response']['stdout']); r['resolved_requests'] = '7'; c['response']['stdout'] = json.dumps(r)+'\n'
        changes.append(('original-replay-claim',wrong_report))
    rejected = []
    for name,change in changes:
        value = copy.deepcopy(context); change(value)
        try: verify(value,seed,expected,diagnostic=diagnostic)
        except (AssertionError,KeyError,TypeError,ValueError,StopIteration): rejected.append(name)
        else: raise AssertionError('accepted negative control: '+name)
    return dict(status='PASS',count=len(rejected),rejected=rejected)
